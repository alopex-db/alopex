"""Interleave real engine cases under a declared background-load qualification."""
from pathlib import Path
import json
import os
import shutil
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from host_resources import HostMonitor
from verification_watchdog import ResourcePolicy, run_guarded
from issue577_cases import CaseRunner, digest, save


class GeneratedOutputBudget:
    """Estimate storage reserve with filesystem counters, without a workspace walk."""

    def __init__(self, root, config):
        self.root, self.config = root, config
        assert 0 < config['planned_growth_bytes'] <= 8_000_000_000
        assert config['project_baseline_bytes'] + config['planned_growth_bytes'] < config['operating_limit_bytes']
        if config['operating_limit_bytes'] > 40_000_000_000:
            raise ValueError('host operating limit must not exceed 40 GB')
        self.starting_used = shutil.disk_usage(root).used

    def reason(self):
        usage = shutil.disk_usage(self.root)
        estimate = (self.config['project_baseline_bytes']
                    + max(0, usage.used - self.config['filesystem_used_at_baseline']))
        self.latest = {'project_estimate_bytes': estimate,
                       'filesystem_free_bytes': usage.free,
                       'filesystem_growth_bytes': usage.used - self.starting_used}
        if usage.free < 16 * 1024**3:
            return 'filesystem free-space reserve'
        if estimate >= self.config['operating_limit_bytes']:
            return 'project output operating reserve'
        if usage.used - self.starting_used > self.config['planned_growth_bytes']:
            return 'planned generated-output reserve'
        return None


class ComparisonPolicy(ResourcePolicy):
    def __init__(self, admission_cpu, running_cpu, output_budget):
        super().__init__()
        assert 0 <= admission_cpu < running_cpu < 0.9
        self.admission_cpu = admission_cpu
        self.running_cpu = running_cpu
        self.output_budget = output_budget

    def reason(self, sample, starting=False):
        reason = super().reason(sample, starting)
        storage_reason = self.output_budget.reason()
        if storage_reason:
            return storage_reason
        if reason:
            return reason
        maximum = self.admission_cpu if starting else self.running_cpu
        if sample['host_cpu_busy'] > maximum:
            return 'comparison CPU load'
        return None


class ComparisonRejected(Exception):
    pass


def await_admission(policy, count, interval, output, state):
    monitor = HostMonitor()
    consecutive = 0
    try:
        with (output / 'admission.jsonl').open('a') as log:
            while consecutive < count:
                sample = monitor.read()
                reason = policy.reason(sample, starting=True)
                consecutive = consecutive + 1 if reason is None else 0
                log.write(json.dumps({'sample': sample, 'reason': reason}) + '\n')
                log.flush()
                state.update(status='waiting_for_admission', admission_reason=reason,
                             consecutive_samples=consecutive, storage=policy.output_budget.latest)
                save(output / 'state.json', state)
                if reason in ('filesystem free-space reserve', 'project output operating reserve',
                              'planned generated-output reserve'):
                    raise RuntimeError(reason)
                if consecutive < count:
                    time.sleep(interval)
    finally:
        monitor.close()


def guarded_case(command, prefix, policy, worktree, sequence):
    unit = f'codex-issue577-comparison-{os.getpid()}-{sequence}'
    launch = ['systemd-run', '--user', '--wait', '--pipe', '--unit=' + unit,
              '--property=MemoryMax=4G', '--property=MemoryHigh=3G',
              '--property=MemorySwapMax=0', '--property=CPUQuota=100%',
              '--property=TasksMax=128', '--property=KillMode=control-group',
              '--property=Type=notify', '--property=NotifyAccess=all',
              '--property=WatchdogSec=4', '--property=WatchdogSignal=SIGTERM',
              '--property=TimeoutAbortSec=2', '--property=TimeoutStopSec=5',
              '--working-directory=' + str(worktree), '/usr/bin/env',
              'PATH=' + os.environ['PATH'], 'LD_LIBRARY_PATH=' + os.environ.get('LD_LIBRARY_PATH', ''),
              sys.executable, '-B', str(worktree / 'scripts/verification_watchdog.py'),
              '--unit-child', str(prefix.with_name(prefix.name + '-events.jsonl')), *command]
    result = run_guarded(launch, unit, prefix, HostMonitor(), policy)
    assert result['cleanup_complete'], 'owned case unit did not stop'
    if result['status'] in ('rejected', 'pressure_stopped'):
        if result.get('reason') in ('filesystem free-space reserve', 'project output operating reserve',
                                    'planned generated-output reserve'):
            raise RuntimeError(result['reason'])
        raise ComparisonRejected(result.get('reason'))
    if result['status'] != 'success' or result['exit'] != 0:
        raise RuntimeError(f'case execution failed: {result["status"]}')


def main():
    profile_path, fixtures, binaries_path, output = map(Path, sys.argv[1:])
    profile_path, fixtures, binaries_path, output = (
        path.resolve() for path in (profile_path, fixtures, binaries_path, output))
    profile = json.loads(profile_path.read_text())
    qualification = profile['measurement_admission']
    bundle = json.loads(binaries_path.read_text())
    worktree = Path(__file__).resolve().parents[2]
    output_budget = GeneratedOutputBudget(worktree, bundle['storage'])
    policy = ComparisonPolicy(qualification['maximum_cpu_before_case'],
                              qualification['maximum_cpu_during_case'], output_budget)
    assert qualification['consecutive_samples'] >= 3
    assert 0 < qualification['sample_interval_seconds'] <= 60
    assert profile['cpu_affinity'] == [0]
    binaries = bundle['engines']
    assert set(binaries) == {'before', 'after', 'duckdb'}
    output.mkdir(parents=True, exist_ok=False)
    state = {'owner': 'parent Codex', 'controller_pid': os.getpid(),
             'accepted_cells': 0, 'attempts': 0,
             'profile_sha256': digest(profile_path), 'status': 'starting'}
    runners, manifests = {}, {}
    for engine in ('before', 'after', 'duckdb'):
        binary = Path(binaries[engine]['binary']).resolve()
        assert digest(binary) == binaries[engine]['sha256']
        expected_revision = profile[engine] if engine != 'duckdb' else profile['reference']['revision']
        assert binaries[engine]['revision'] == expected_revision
        directory = output / engine
        directory.mkdir()
        runners[engine] = CaseRunner(engine, binary, profile_path, fixtures)
        manifests[engine] = {'owner': 'parent Codex', 'engine': engine,
                             'engine_revision': expected_revision, 'binary_sha256': digest(binary),
                             'profile_sha256': state['profile_sha256'],
                             'status': 'incomplete', 'results': []}
        save(directory / 'manifest.json', manifests[engine])
    try:
        for case in profile['cases']:
            for limit in profile['memory_limits_bytes']:
                policy_name = 'none' if limit is None else str(limit)
                name = case['id'] + '-' + policy_name + '.json'
                for engine in ('before', 'after', 'duckdb'):
                    while True:
                        assert digest(profile_path) == state['profile_sha256'], 'comparison profile changed'
                        assert shutil.disk_usage(output).free >= 16 * 1024**3
                        state.update(engine=engine, case=case['id'], memory_limit_bytes=limit)
                        await_admission(policy, qualification['consecutive_samples'],
                                        qualification['sample_interval_seconds'], output, state)
                        state['attempts'] += 1
                        state['status'] = 'measuring'
                        save(output / 'state.json', state)
                        attempt = output / 'attempts' / str(state['attempts'])
                        attempt.mkdir(parents=True)
                        target = attempt / name

                        def execute(command):
                            guarded_case(command, attempt / 'guard', policy, worktree,
                                         state['attempts'])

                        try:
                            entry = runners[engine].run(case, limit, target, execute)
                        except ComparisonRejected as error:
                            save(attempt / 'quality.json', {'status': 'rejected', 'reason': str(error)})
                            continue
                        assert digest(profile_path) == state['profile_sha256'], 'comparison profile changed'
                        assert digest(runners[engine].binary) == manifests[engine]['binary_sha256']
                        destination = output / engine / name
                        with destination.open('xb') as copied:
                            copied.write(target.read_bytes())
                            copied.flush()
                            os.fsync(copied.fileno())
                        assert digest(destination) == entry['sha256']
                        manifests[engine]['results'].append(entry)
                        save(output / engine / 'manifest.json', manifests[engine])
                        state['accepted_cells'] += 1
                        state['storage'] = policy.output_budget.latest
                        save(output / 'state.json', state)
                        print(json.dumps({'accepted_cells': state['accepted_cells'],
                                          'engine': engine, 'case': case['id'], 'policy': limit}), flush=True)
                        break
        for engine, manifest in manifests.items():
            assert digest(profile_path) == state['profile_sha256'], 'comparison profile changed'
            assert len(manifest['results']) == len(profile['cases']) * len(profile['memory_limits_bytes'])
            manifest['status'] = 'engine_cases_measured'
            save(output / engine / 'manifest.json', manifest)
        state['status'] = 'all_cells_measured'
    except BaseException as error:
        state.update(status='failed', error=type(error).__name__, reason=str(error)[:1024])
        raise
    finally:
        save(output / 'state.json', state)


if __name__ == '__main__':
    main()
