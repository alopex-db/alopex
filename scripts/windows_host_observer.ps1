$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
public static class HostResources {
    [StructLayout(LayoutKind.Sequential)]
    public struct Memory {
        public uint Length, Load;
        public ulong Total, Available, CommitLimit, CommitAvailable;
        public ulong VirtualTotal, VirtualAvailable, ExtendedAvailable;
    }
    [DllImport("kernel32.dll", SetLastError=true)]
    public static extern bool GlobalMemoryStatusEx(ref Memory value);
    [DllImport("kernel32.dll", SetLastError=true)]
    public static extern bool GetSystemTimes(out ulong idle, out ulong kernel, out ulong user);
    public static Task<string> Heartbeat() { return Task.Run(() => Console.ReadLine()); }
}
'@
$previous = $null
$requestId = 0
$clock = [Diagnostics.Stopwatch]::StartNew()
$heartbeat = [HostResources]::Heartbeat()
$lastHeartbeat = $clock.Elapsed.TotalSeconds
while (($null -ne $heartbeat) -and (($clock.Elapsed.TotalSeconds - $lastHeartbeat) -lt 10)) {
    if ($heartbeat.IsCompleted) {
        if ($null -eq $heartbeat.Result) { break }
        $requestId = [long]::Parse($heartbeat.Result)
        $lastHeartbeat = $clock.Elapsed.TotalSeconds
        $heartbeat = [HostResources]::Heartbeat()
    }
    $memory = [HostResources+Memory]::new()
    $memory.Length = [Runtime.InteropServices.Marshal]::SizeOf($memory)
    if (-not [HostResources]::GlobalMemoryStatusEx([ref]$memory)) { exit 2 }
    [uint64]$idle = 0; [uint64]$kernel = 0; [uint64]$user = 0
    if (-not [HostResources]::GetSystemTimes([ref]$idle, [ref]$kernel, [ref]$user)) { exit 2 }
    $total = $kernel + $user
    if (($null -ne $previous) -and ($total -gt $previous.Total)) {
        $busy = 1.0 - (($idle - $previous.Idle) / ($total - $previous.Total))
        [ordered]@{
            request_id = $requestId
            observed_unix_ms = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
            observed_elapsed_ms = $clock.Elapsed.TotalMilliseconds
            host_total_bytes = $memory.Total
            host_available_bytes = $memory.Available
            # This process's commit headroom is a conservative bound on host headroom.
            host_commit_available_bytes = $memory.CommitAvailable
            host_cpu_busy = [Math]::Max([double]0.0, [Math]::Min([double]1.0, [double]$busy))
        } | ConvertTo-Json -Compress
        [Console]::Out.Flush()
    }
    $previous = @{ Idle = $idle; Total = $total }
    Start-Sleep -Milliseconds 1000
}
