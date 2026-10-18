"""Keep a Windows job owned by the surrounding Linux watchdog unit.

Run this bridge as the command of verification_watchdog's unit child. The
surrounding host monitor owns admission and pressure decisions; this bridge
owns the control pipe, and the Windows guardian owns all Windows descendants.
"""
import hashlib
import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading


def windows_path(path):
    return subprocess.check_output(['wslpath', '-w', str(Path(path).resolve())], text=True).strip()


def launch_arguments(spec):
    shell = shutil.which('powershell.exe')
    if shell is None:
        shell = str(Path('/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'))
    source = Path(__file__).with_name('windows_job_runner.ps1').read_text()
    # The source remains plain text in command records. The only inserted value
    # is a single-quoted PowerShell literal, never an executable expression.
    literal = windows_path(spec).replace("'", "''")
    command = "& {\n" + source + "\n} -SpecFile '" + literal + "'"
    return [shell, '-NoProfile', '-NonInteractive', '-Command', command]


def run(spec):
    spec = Path(spec).resolve()
    arguments = launch_arguments(spec)
    sources = (Path(__file__), Path(__file__).with_name('windows_job_runner.ps1'))
    print(json.dumps({'event': 'windows_job_bridge_start',
                      'spec_sha256': hashlib.sha256(spec.read_bytes()).hexdigest(),
                      'source_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                        for p in sources}}), flush=True)
    process = subprocess.Popen(arguments, stdin=subprocess.PIPE)
    stopping = threading.Event()

    def heartbeat():
        try:
            while not stopping.is_set():
                process.stdin.write(b'heartbeat\n')
                process.stdin.flush()
                stopping.wait(1)
        except (BrokenPipeError, OSError):
            pass

    worker = threading.Thread(target=heartbeat, name='windows-job-heartbeat', daemon=True)
    worker.start()
    try:
        return process.wait()
    finally:
        stopping.set()
        worker.join(timeout=2)
        try:
            process.stdin.close()
        except OSError:
            pass
        # These are cleanup deadlines, not a limit on build/test duration.
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    def interrupt(signum, frame):
        raise KeyboardInterrupt()

    signal.signal(signal.SIGTERM, interrupt)
    try:
        return run(sys.argv[1])
    except KeyboardInterrupt:
        return 125


if __name__ == '__main__':
    raise SystemExit(main())
