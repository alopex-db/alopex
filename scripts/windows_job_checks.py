"""Real Windows job lifecycle checks; run inside the Linux host watchdog.

Fixtures sleep briefly and use small memory. These checks never exhaust host
resources. The guardian's kernel limit readback proves configuration; process
identity checks prove descendant termination.
"""
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

from windows_job_bridge import windows_path


SHELL = '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def powershell(command):
    return subprocess.check_output([SHELL, '-NoProfile', '-NonInteractive', '-Command', command],
                                   text=True).strip()


def wait_for_file(path):
    # Startup/cleanup observation deadline, not a build or workload time limit.
    deadline = time.monotonic() + 30
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError('Windows fixture did not produce its receipt')
        time.sleep(0.05)


def assert_gone(identity):
    actual = powershell(
        f'$p=Get-Process -Id {int(identity["pid"])} -ErrorAction SilentlyContinue; '
        'if($p){$p.StartTime.ToUniversalTime().Ticks}; exit 0')
    assert actual != str(identity['ticks']), 'an owned Windows process survived'


def run_case(root, name, code, expected, stop=None):
    directory = root / name
    directory.mkdir()
    child_record = directory / 'child.json'
    if code is None:
        # A finite fallback lifetime also bounds a deliberately broken guardian.
        code = (
            '$exe=(Get-Process -Id $PID).Path; '
            "$child=Start-Process -FilePath $exe -NoNewWindow -PassThru "
            "-ArgumentList @('-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 15'); "
            '$identity=@{pid=$child.Id;ticks=$child.StartTime.ToUniversalTime().Ticks}; '
            f'$record={literal(windows_path(child_record))}; '
            '[IO.File]::WriteAllText(($record+".tmp"),'
            '($identity|ConvertTo-Json -Compress)); '
            '[IO.File]::Move(($record+".tmp"),$record); '
            + ('exit 0' if stop is None else 'Start-Sleep -Seconds 15'))
    receipt = directory / 'receipt.json'
    spec = directory / 'spec.json'
    spec.write_text(json.dumps({
        'program': r'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe',
        'arguments': ['-NoProfile', '-NonInteractive', '-Command', code],
        'directory': windows_path(directory), 'log': windows_path(directory / 'command.log'),
        'receipt': windows_path(receipt), 'environment': {}}))
    started = time.time()
    log = directory / 'bridge.log'
    with log.open('wb') as output:
        process = subprocess.Popen(
            [sys.executable, '-B', str(Path(__file__).with_name('windows_job_bridge.py')), str(spec)],
            stdout=output, stderr=subprocess.STDOUT)
        try:
            if stop:
                wait_for_file(child_record)
                events = [json.loads(line) for line in log.read_text().splitlines()]
                event = next(x for x in events if x.get('event') == 'windows_job_started')
                if stop == 'guardian':
                    # Match creation time as well as PID before ending this test's guardian.
                    pid, ticks = int(event['guardian_pid']), int(event['guardian_start_ticks'])
                    powershell(f'$p=Get-Process -Id {pid}; '
                               f'if($p.StartTime.ToUniversalTime().Ticks -ne {ticks})'
                               "{throw 'fixture guardian identity changed'}; $p.Kill(); exit 0")
                else:
                    process.send_signal(signal.SIGTERM if stop == 'eof' else signal.SIGKILL)
            result = process.wait(timeout=30)
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
    if expected is not None:
        assert result == expected, f'{name}: exit {result}, expected {expected}; see bridge log'
        wait_for_file(receipt)
        observed = json.loads(receipt.read_text())
        assert observed['CleanupComplete']
        assert observed['MemoryLimitBytes'] == 4 * 1024**3
        assert observed['CpuRate'] == max(1, 10000 // observed['LogicalProcessors'])
        assert observed['ParentLost'] == (stop == 'eof')
    else:
        assert result != 0
        if stop == 'killed_bridge':
            wait_for_file(receipt)
            observed = json.loads(receipt.read_text())
            assert observed['CleanupComplete'] and observed['ParentLost']
    if child_record.exists():
        assert_gone(json.loads(child_record.read_text()))
    events = [json.loads(line) for line in log.read_text().splitlines()]
    root_event = next(x for x in events if x.get('event') == 'windows_job_started')
    assert_gone({'pid': root_event['pid'], 'ticks': root_event['start_ticks']})
    record = {'case': name, 'started_unix': started, 'finished_unix': time.time(),
              'exit': result, 'result': 'pass'}
    (directory / 'check.json').write_text(json.dumps(record, indent=2))
    return record


def main():
    root = Path(sys.argv[1]).resolve()
    root.mkdir(exist_ok=False)
    results = []
    for name, code, expected, stop in (
            ('success', 'exit 0', 0, None), ('nonzero', 'exit 7', 7, None),
            ('root_exit_descendant', None, 0, None),
            ('control_eof', None, 125, 'eof'),
            ('bridge_loss', None, None, 'killed_bridge'),
            ('guardian_loss', None, None, 'guardian')):
        try:
            results.append(run_case(root, name, code, expected, stop))
        except BaseException as error:
            results.append({'case': name, 'result': 'fail' if isinstance(error, AssertionError)
                            else 'invalid', 'error': repr(error), 'finished_unix': time.time()})
            raise
        finally:
            (root / 'results.json').write_text(json.dumps(results, indent=2))
    print(json.dumps({'windows_job_checks_passed': len(results)}), flush=True)


if __name__ == '__main__':
    main()
