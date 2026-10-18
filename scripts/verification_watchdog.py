"""Stop a bounded verification unit when host observations become unsafe."""
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time


def keep_watchdog_alive(heartbeat):
    last_mtime = heartbeat.stat().st_mtime_ns
    last_change = time.monotonic()
    while True:
        try:
            current_mtime = heartbeat.stat().st_mtime_ns
            if current_mtime != last_mtime:
                last_mtime, last_change = current_mtime, time.monotonic()
            # Allow the 5-second observer read plus the 8-second unit query and IO.
            if time.monotonic() - last_change > 20:
                print('OS watchdog: parent heartbeat expired', file=sys.stderr, flush=True)
                return
            address = os.environ.get('NOTIFY_SOCKET')
            if not address:
                raise OSError('notification socket unavailable')
            if address.startswith('@'):
                address = '\0' + address[1:]
            # The persistent worker remains attributable after notification delivery.
            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as notification:
                notification.settimeout(2)
                notification.sendto(b'WATCHDOG=1', address)
        except (OSError, subprocess.SubprocessError) as error:
            print('OS watchdog: notification failed: ' + type(error).__name__,
                  file=sys.stderr, flush=True)
            return
        time.sleep(1)


@dataclass
class ResourcePolicy:
    cpu_grace_seconds: float = 10
    high_cpu_since: float | None = None

    def reason(self, sample, starting=False):
        total = sample['host_total_bytes']
        available = sample['host_available_bytes']
        cpu = sample['host_cpu_busy']
        commit = sample['host_commit_available_bytes']
        wsl = sample['wsl_available_bytes']
        if not (total > 0 and 0 <= available <= total and 0 <= cpu <= 1
                and commit >= 0 and wsl >= 0):
            raise ValueError('invalid host observation')
        reserve = (4 << 30) if starting else 0
        if available < max(8 << 30, int(total * 0.15)) + reserve:
            return 'host memory reserve'
        if commit < (4 << 30) + reserve:
            return 'host commit reserve'
        if wsl < (8 << 30) + reserve:
            return 'WSL memory reserve'
        if cpu >= 0.9:
            now = time.monotonic()
            if self.high_cpu_since is None:
                self.high_cpu_since = now
            if starting or now - self.high_cpu_since >= self.cpu_grace_seconds:
                return 'host CPU pressure'
        else:
            self.high_cpu_since = None
        return None


def unit_state(unit):
    result = subprocess.run(['systemctl', '--user', 'show', unit,
                             '--property=ActiveState', '--property=Result',
                             '--property=ExecMainStatus', '--property=ControlGroup'],
                            capture_output=True, text=True, timeout=8)
    return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)


def run_guarded(launch, unit, output_base, monitor, policy):
    """Record before launch; stop all descendants on pressure or lost monitoring."""
    base = Path(output_base)
    base.parent.mkdir(parents=True, exist_ok=True)
    events_path = base.with_name(base.name + '-events.jsonl')
    final_path = base.with_name(base.name + '-watch.json')
    log_path = base.with_name(base.name + '-resource-unit.log')
    receipt = {'owner': 'parent Codex', 'unit': unit, 'launch': launch,
               'launched': False, 'status': 'launch_failed', 'exit': 125,
               'cleanup_complete': False}
    process = None
    previous_handlers = {}
    events = None

    def record(event, **data):
        events.write(json.dumps({'event': event,
                                'utc': datetime.now(timezone.utc).isoformat(), **data}) + '\n')
        events.flush()
        os.fsync(events.fileno())

    def interrupt(signum, frame):
        raise KeyboardInterrupt()

    try:
        events = events_path.open('x')
        os.chmod(events_path, 0o600)
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, interrupt)
        record('start', unit=unit, launch=launch,
               head=subprocess.check_output(['git', '-C', str(Path(__file__).resolve().parents[1]),
                                             'rev-parse', 'HEAD'], text=True).strip(),
               source_sha256={path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in (Path(__file__),
                                           Path(__file__).with_name('host_resources.py'),
                                           Path(__file__).with_name('windows_host_observer.ps1'),
                                           Path(__file__).with_name('run_issue577_verification.py'))})
        observation = monitor.read()
        record('sample', phase='preflight', observation=observation)
        reason = policy.reason(observation, starting=True)
        if reason:
            receipt.update(status='rejected', reason=reason)
        else:
            with log_path.open('x') as output:
                os.chmod(log_path, 0o600)
                process = subprocess.Popen(launch, stdout=output, stderr=subprocess.STDOUT,
                                           start_new_session=True)
                receipt['launched'] = True
                record('launched', client_pid=process.pid)
                while process.poll() is None:
                    observation = monitor.read()
                    state = unit_state(unit)
                    usage = {}
                    if state.get('ControlGroup'):
                        group = Path('/sys/fs/cgroup') / state['ControlGroup'].lstrip('/')
                        for name in ('memory.current', 'memory.peak', 'cpu.stat', 'memory.events'):
                            try:
                                usage[name] = (group / name).read_text().strip()
                            except FileNotFoundError:
                                pass
                    record('sample', phase='running', observation=observation,
                           unit_state=state, unit_usage=usage)
                    reason = policy.reason(observation)
                    if reason:
                        receipt.update(status='pressure_stopped', reason=reason)
                        record('stop_requested', reason=reason)
                        break
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        pass
                if receipt['status'] != 'pressure_stopped':
                    state = unit_state(unit)
                    code = process.poll()
                    status = ('monitor_stopped' if state.get('Result') == 'watchdog' else
                              'resource_stopped' if state.get('Result') == 'oom-kill' else
                              'timeout' if state.get('Result') == 'timeout' else
                              'success' if code == 0 else
                              'failed' if state.get('Result') == 'exit-code' else 'launch_failed')
                    receipt.update(status=status, exit=code, unit_state=state)
    except KeyboardInterrupt:
        receipt.update(status='interrupted', exit=130)
        if events:
            record('stop_requested', reason='interruption')
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        receipt.update(status='monitor_stopped' if process else 'launch_failed',
                       reason=type(error).__name__, error_message=str(error)[:1024])
        if events:
            record('stop_requested', reason=receipt['reason'])
    finally:
        # Cleanup also runs after a failed observation or interrupted client.
        for signum in previous_handlers:
            signal.signal(signum, signal.SIG_IGN)
        try:
            if process is not None:
                subprocess.run(['systemctl', '--user', 'stop', unit],
                               capture_output=True, timeout=8)
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)
                receipt['cleanup_complete'] = unit_state(unit).get('ActiveState') in ('inactive', 'failed')
                receipt['client_exit'] = process.returncode
                subprocess.run(['systemctl', '--user', 'reset-failed', unit],
                               capture_output=True, timeout=8)
            else:
                receipt['cleanup_complete'] = True
        except (OSError, subprocess.SubprocessError):
            receipt['cleanup_complete'] = False
        finally:
            try:
                monitor.close()
            except (OSError, subprocess.SubprocessError):
                receipt['cleanup_complete'] = False
            finally:
                for signum, handler in previous_handlers.items():
                    signal.signal(signum, handler)
        if events:
            if log_path.exists():
                receipt['log_sha256'] = hashlib.sha256(log_path.read_bytes()).hexdigest()
            record('end', **receipt)
            events.close()
            temporary = final_path.with_suffix('.tmp')
            with temporary.open('x') as output:
                os.chmod(temporary, 0o600)
                json.dump(receipt, output, indent=2)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, final_path)
    return receipt


if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[1] != '--unit-child':
        sys.exit('usage: verification_watchdog.py --unit-child EVENTS COMMAND [ARGS...]')
    import threading

    subprocess.run(['systemd-notify', '--ready'], check=True, timeout=2)
    threading.Thread(target=keep_watchdog_alive, args=(Path(sys.argv[2]),),
                     daemon=True).start()
    code = subprocess.call(sys.argv[3:])
    sys.exit(code if code >= 0 else 128 - code)
