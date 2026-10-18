param([Parameter(Mandatory = $true)][string]$SpecFile)
$ErrorActionPreference = 'Stop'

# The Linux watchdog owns host pressure decisions. This guardian owns the
# Windows process tree, including termination when its controlling pipe closes.
$source = @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace AlopexVerification {
    public sealed class JobResult {
        public int ExitCode;
        public bool ParentLost;
        public bool CleanupComplete;
        public uint ProcessId;
        public uint TotalProcesses;
        public ulong PeakJobMemoryBytes;
        public ulong MemoryLimitBytes;
        public uint CpuRate;
        public int LogicalProcessors;
    }

    public static class WindowsJob {
        [StructLayout(LayoutKind.Sequential)] struct BasicLimits {
            public long ProcessTime, JobTime;
            public uint Flags;
            public UIntPtr MinimumWorkingSet, MaximumWorkingSet;
            public uint ActiveProcesses;
            public UIntPtr Affinity;
            public uint Priority, Scheduling;
        }
        [StructLayout(LayoutKind.Sequential)] struct IoCounters {
            public ulong ReadOps, WriteOps, OtherOps, ReadBytes, WriteBytes, OtherBytes;
        }
        [StructLayout(LayoutKind.Sequential)] struct ExtendedLimits {
            public BasicLimits Basic;
            public IoCounters Io;
            public UIntPtr ProcessMemory, JobMemory, PeakProcessMemory, PeakJobMemory;
        }
        [StructLayout(LayoutKind.Sequential)] struct CpuLimits { public uint Flags, Rate; }
        [StructLayout(LayoutKind.Sequential)] struct Accounting {
            public long User, Kernel, PeriodUser, PeriodKernel;
            public uint PageFaults, TotalProcesses, ActiveProcesses, TerminatedProcesses;
        }
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)] struct Startup {
            public uint Size;
            public string Reserved, Desktop, Title;
            public uint X, Y, Width, Height, XChars, YChars, Fill, Flags;
            public ushort Show, ReservedSize;
            public IntPtr ReservedData, Input, Output, Error;
        }
        [StructLayout(LayoutKind.Sequential)] struct ProcessInfo {
            public IntPtr Process, Thread;
            public uint ProcessId, ThreadId;
        }
        [StructLayout(LayoutKind.Sequential)] struct Security {
            public int Size;
            public IntPtr Descriptor;
            [MarshalAs(UnmanagedType.Bool)] public bool Inherit;
        }
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern IntPtr CreateJobObject(IntPtr attributes, string name);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool SetInformationJobObject(IntPtr job, int kind, IntPtr info, uint size);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool QueryInformationJobObject(IntPtr job, int kind, IntPtr info, uint size, IntPtr returned);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool TerminateJobObject(IntPtr job, uint code);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool TerminateProcess(IntPtr process, uint code);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool CloseHandle(IntPtr handle);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern uint ResumeThread(IntPtr thread);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool GetExitCodeProcess(IntPtr process, out uint code);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool GetProcessTimes(IntPtr process, out long created, out long exited,
                                           out long kernel, out long user);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool SetHandleInformation(IntPtr handle, uint mask, uint flags);
        [DllImport("kernel32.dll")] static extern IntPtr GetStdHandle(int kind);
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern IntPtr CreateFile(string name, uint access, uint share, ref Security security,
                                       uint creation, uint flags, IntPtr template);
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        static extern bool CreateProcess(string application, StringBuilder command, IntPtr processAttributes,
            IntPtr threadAttributes, bool inherit, uint flags, IntPtr environment, string directory,
            ref Startup startup, out ProcessInfo process);

        static void Require(bool value) { if (!value) throw new Win32Exception(); }
        static void Configure<T>(IntPtr job, int kind, T value) {
            int size = Marshal.SizeOf(typeof(T));
            IntPtr info = Marshal.AllocHGlobal(size);
            try {
                Marshal.StructureToPtr(value, info, false);
                Require(SetInformationJobObject(job, kind, info, (uint)size));
            } finally { Marshal.FreeHGlobal(info); }
        }
        static T Query<T>(IntPtr job, int kind) {
            int size = Marshal.SizeOf(typeof(T));
            IntPtr info = Marshal.AllocHGlobal(size);
            try {
                Require(QueryInformationJobObject(job, kind, info, (uint)size, IntPtr.Zero));
                return (T)Marshal.PtrToStructure(info, typeof(T));
            } finally { Marshal.FreeHGlobal(info); }
        }
        static string Quote(string value) {
            StringBuilder text = new StringBuilder("\"");
            int slashes = 0;
            foreach (char c in value) {
                if (c == '\\') { slashes++; continue; }
                text.Append('\\', c == '"' ? slashes * 2 + 1 : slashes);
                text.Append(c); slashes = 0;
            }
            return text.Append('\\', slashes * 2).Append('"').ToString();
        }

        public static JobResult Run(string program, string[] args, string directory, string log) {
            IntPtr job = CreateJobObject(IntPtr.Zero, null);
            if (job == IntPtr.Zero) throw new Win32Exception();
            ProcessInfo process = new ProcessInfo();
            IntPtr input = new IntPtr(-1);
            bool assigned = false;
            try {
                ExtendedLimits limits = new ExtendedLimits();
                limits.Basic.Flags = 0x2000 | 0x200 | 0x8; // kill-on-close, job RAM, process count
                limits.Basic.ActiveProcesses = 128;
                limits.JobMemory = new UIntPtr(4UL * 1024 * 1024 * 1024);
                Configure(job, 9, limits);
                CpuLimits cpu = new CpuLimits();
                cpu.Flags = 1 | 4; // enabled, hard cap
                cpu.Rate = (uint)Math.Max(1, 10000 / Environment.ProcessorCount);
                Configure(job, 15, cpu);
                ExtendedLimits actual = Query<ExtendedLimits>(job, 9);
                CpuLimits actualCpu = Query<CpuLimits>(job, 15);
                if ((actual.Basic.Flags & limits.Basic.Flags) != limits.Basic.Flags ||
                    actual.JobMemory != limits.JobMemory || actualCpu.Flags != cpu.Flags || actualCpu.Rate != cpu.Rate)
                    throw new InvalidOperationException("Windows job limits did not apply");

                // Cargo receives NUL input. It cannot keep the controlling pipe
                // open and hide a disconnected Linux watchdog from this guardian.
                Require(SetHandleInformation(GetStdHandle(-10), 1, 0));
                Security security = new Security();
                security.Size = Marshal.SizeOf(typeof(Security)); security.Inherit = true;
                input = CreateFile("NUL", 0x80000000, 3, ref security, 3, 0, IntPtr.Zero);
                if (input == new IntPtr(-1)) throw new Win32Exception();
                using (FileStream output = new FileStream(log, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite)) {
                    IntPtr outputHandle = output.SafeFileHandle.DangerousGetHandle();
                    Require(SetHandleInformation(outputHandle, 1, 1));
                    Startup startup = new Startup();
                    startup.Size = (uint)Marshal.SizeOf(typeof(Startup)); startup.Flags = 0x100;
                    startup.Input = input; startup.Output = startup.Error = outputHandle;
                    StringBuilder command = new StringBuilder(Quote(program));
                    foreach (string arg in args) command.Append(' ').Append(Quote(arg));
                    Require(CreateProcess(program, command, IntPtr.Zero, IntPtr.Zero, true,
                                          4 | 0x08000000, IntPtr.Zero, directory, ref startup, out process));
                    Require(AssignProcessToJobObject(job, process.Process)); assigned = true;
                    long created, exited, kernel, user;
                    Require(GetProcessTimes(process.Process, out created, out exited, out kernel, out user));
                    if (ResumeThread(process.Thread) == 0xffffffff) throw new Win32Exception();
                    CloseHandle(process.Thread); process.Thread = IntPtr.Zero;
                    Process guardian = Process.GetCurrentProcess();
                    Console.WriteLine("{\"event\":\"windows_job_started\",\"pid\":" + process.ProcessId +
                        ",\"start_ticks\":" + DateTime.FromFileTimeUtc(created).Ticks +
                        ",\"guardian_pid\":" + guardian.Id + ",\"guardian_start_ticks\":" +
                        guardian.StartTime.ToUniversalTime().Ticks + "}");
                    Console.Out.Flush();
                    long lastHeartbeat = Stopwatch.GetTimestamp();
                    int parentGone = 0;
                    Thread observer = new Thread(delegate() {
                        try {
                            while (Console.In.ReadLine() != null)
                                Interlocked.Exchange(ref lastHeartbeat, Stopwatch.GetTimestamp());
                        } catch (IOException) {
                            // A lost control pipe has the same ownership meaning as EOF.
                        } finally { Interlocked.Exchange(ref parentGone, 1); }
                    });
                    observer.IsBackground = true; observer.Start();
                    bool lost = false;
                    while (true) {
                        uint wait = WaitForSingleObject(process.Process, 100);
                        if (wait == 0) break;
                        if (wait != 258) throw new Win32Exception();
                        if (Interlocked.CompareExchange(ref parentGone, 0, 0) != 0 ||
                            (Stopwatch.GetTimestamp() - Interlocked.Read(ref lastHeartbeat)) / (double)Stopwatch.Frequency > 20) {
                            lost = true; Require(TerminateJobObject(job, 125));
                            if (WaitForSingleObject(process.Process, 5000) != 0)
                                throw new InvalidOperationException("Windows job root did not stop");
                            break;
                        }
                    }
                    uint exit; Require(GetExitCodeProcess(process.Process, out exit));
                    // A successful root must not leave background descendants.
                    Require(TerminateJobObject(job, 125));
                    Stopwatch cleanup = Stopwatch.StartNew();
                    Accounting accounting;
                    do {
                        accounting = Query<Accounting>(job, 1);
                        if (accounting.ActiveProcesses == 0) break;
                        Thread.Sleep(50);
                    } while (cleanup.Elapsed.TotalSeconds < 5);
                    actual = Query<ExtendedLimits>(job, 9);
                    return new JobResult {
                        ExitCode = lost ? 125 : unchecked((int)exit), ParentLost = lost,
                        CleanupComplete = accounting.ActiveProcesses == 0,
                        ProcessId = process.ProcessId, TotalProcesses = accounting.TotalProcesses,
                        PeakJobMemoryBytes = actual.PeakJobMemory.ToUInt64(),
                        MemoryLimitBytes = actual.JobMemory.ToUInt64(), CpuRate = actualCpu.Rate,
                        LogicalProcessors = Environment.ProcessorCount
                    };
                }
            } finally {
                if (process.Process != IntPtr.Zero && !assigned) TerminateProcess(process.Process, 125);
                CloseHandle(job); // Kernel-enforced kill even if managed cleanup fails.
                if (process.Thread != IntPtr.Zero) CloseHandle(process.Thread);
                if (process.Process != IntPtr.Zero) CloseHandle(process.Process);
                if (input != new IntPtr(-1)) CloseHandle(input);
            }
        }
    }
}
'@

try {
    $spec = Get-Content -LiteralPath $SpecFile -Raw | ConvertFrom-Json
    if (Test-Path -LiteralPath $spec.receipt) { throw 'Windows job receipt already exists' }
    foreach ($property in $spec.environment.PSObject.Properties) {
        [Environment]::SetEnvironmentVariable($property.Name, [string]$property.Value, 'Process')
    }
    Add-Type -TypeDefinition $source
    $result = [AlopexVerification.WindowsJob]::Run(
        [string]$spec.program, [string[]]$spec.arguments, [string]$spec.directory, [string]$spec.log)
    $json = $result | ConvertTo-Json -Compress
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($json)
    # A disconnected bridge observes this independently. Publish only a fully
    # flushed receipt; existence must never expose a partially written JSON file.
    $pending = [string]$spec.receipt + '.pending'
    $receipt = [IO.FileStream]::new($pending, [IO.FileMode]::CreateNew)
    try { $receipt.Write($bytes, 0, $bytes.Length); $receipt.Flush($true) }
    finally { $receipt.Dispose() }
    [IO.File]::Move($pending, [string]$spec.receipt)
    [Console]::WriteLine($json)
    if (-not $result.CleanupComplete) { exit 125 }
    exit $result.ExitCode
} catch {
    [Console]::Error.WriteLine($_.Exception.ToString())
    exit 125
}
