"""Fail-closed #804 admission and host-wide monitor for the frozen #682 comparator.

This is task-private integration source, not a dispatched workflow. Both checkouts
must be staged and counted before this entry point runs. No security knobs change.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

REQUIRED_CASES = {*(f'E{i:02}' for i in range(1, 8)),
                  *(f'S{i:02}' for i in range(1, 4)), 'F01', 'F02', 'P01', 'R01'}
FROZEN_COMMIT = '490f002dfb2c7f509a2444891808d9704fe6b001'
STOP_BYTES = 40_000_000_000
MAX_BYTES = 50_000_000_000
COMPARATOR_PEAK = 1_000_000_000
COLLECT_RESERVE = 1_000_000_000


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    require(path.is_file() and not path.is_symlink(), f'missing or linked evidence: {path.name}')
    return json.loads(path.read_bytes())


def head(path):
    return subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'],
                                   stderr=subprocess.PIPE, text=True, timeout=10).strip()


def load_qualification_module(repo):
    script = repo / 'scripts/ops/hosted_test_env_qualification.py'
    require(script.is_file() and not script.is_symlink(), 'qualification controller missing')
    sys.path[:0] = [str(repo / 'scripts/test-env'), str(repo / 'scripts/ops')]
    spec = importlib.util.spec_from_file_location('same_job_qualification', script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def same_host(previous, current):
    """Bind later phases without #804's 1 GB admission-to-acquisition delta."""
    for key in ('context', 'boot_id', 'kernel', 'lsm', 'apparmor', 'root_metadata',
                'temp_snapshot', 'cpus'):
        require(previous[key] == current[key], f'host identity changed: {key}')
    for key in ('DockerRootDir', 'ServerVersion', 'Architecture', 'CgroupVersion',
                'SecurityOptions'):
        require(previous['docker_info'][key] == current['docker_info'][key],
                f'runtime identity changed: {key}')
    require(previous['storage'] == current['storage'], 'storage scope changed')


def verify_frozen_checkout(repo, config, expected_sha):
    require(head(repo) == FROZEN_COMMIT, 'frozen comparator checkout differs')
    require(sha(repo / 'scripts/ops/hosted_wal_cost.py') == config['sources']['scripts/ops/hosted_wal_cost.py'],
            'frozen comparator differs')
    for name, digest in config['sources'].items():
        require(sha(repo / name) == digest, f'frozen source differs: {name}')
    require(expected_sha == sha(repo / 'scripts/ops/wal_cost_contract.py'), 'frozen contract differs')
    require(config['mode'] == 'full' and config['cell_seconds'] == 30 and
            config['stage_seconds'] == 5400 and config['rss_bytes'] == 1_073_741_824,
            'frozen limits differ')


def verify_qualification(public, private, module, run_id, facts):
    require(public['schema'] == 'alopex.hosted-test-env-qualification/v1' and
            public['case'] == 'HOST_QUALIFICATION' and public['outcome'] == 'passed' and
            public['error_code'] is None, 'qualification failed')
    cases = public['cases']
    require(len(cases) == len(REQUIRED_CASES) and
            {entry['id'] for entry in cases} == REQUIRED_CASES and
            all(entry['status'] == 'passed' and entry['error_code'] is None for entry in cases),
            'qualification cases incomplete')
    require(public['source_commit'] == 'a4158b873963dd79c7844953d64a8b511ee75fde' and
            public['asset_digest'] == module.ASSETS_DIGEST, 'qualification source or asset differs')
    prior = read(private / 'admission.private.json')
    profile = read(private / 'profile.json')
    after = read(private / 'storage-after.private.json')
    same_host(prior, facts)
    require(profile['run_id'] == 'hosted-804-' + run_id and
            profile['storage'] == prior['storage'] and
            profile['assets'] == str(Path(os.environ['ALOPEX_804_ASSETS']) / 'assets/assets.json'),
            'qualification profile or run binding differs')
    require(0 <= after['allocated_bytes'] < STOP_BYTES, 'qualification storage result invalid')
    return prior


def run_phase(repo, flag, environment):
    command = [sys.executable, '-B', str(repo / 'scripts/ops/hosted_test_env_qualification.py'), flag]
    outcome = subprocess.run(command, cwd=repo, env=environment, stdin=subprocess.DEVNULL,
                             capture_output=True, text=True, timeout=11_000, check=False)
    require(outcome.returncode == 0, f'#804 phase failed: {flag}')


def monitor_comparator(repo, config_path, module, prior, private, env):
    budget = module.Budget(prior['storage'])
    before = budget.used()
    require(before + COMPARATOR_PEAK + COLLECT_RESERVE < STOP_BYTES and before < MAX_BYTES,
            'insufficient host-wide comparator headroom')
    command = [sys.executable, '-B', str(repo / 'scripts/ops/hosted_wal_cost.py')]
    with (private / 'comparator.stdout.private.log').open('xb') as output:
        child = subprocess.Popen(command, cwd=repo, env=env, stdin=subprocess.DEVNULL,
                                 stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        original_error = None
        try:
            deadline = time.monotonic() + config_path['stage_seconds'] + 120
            while child.poll() is None:
                require(time.monotonic() < deadline, 'comparator deadline exceeded')
                used = budget.used()
                require(used < STOP_BYTES and used < MAX_BYTES and
                        used <= before + COMPARATOR_PEAK, 'host-wide growth exceeded')
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            require(child.returncode == 0, f'comparator failed: {child.returncode}')
        except BaseException as error:
            original_error = repr(error)
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)
        cleanup_error = None
        try:
            same_host(prior, module.admission_facts())
            final = budget.used()
            require(final < STOP_BYTES and final < MAX_BYTES, 'post-comparator host storage exceeded')
        except BaseException as error:
            cleanup_error = repr(error)
            final = None
        (private / 'wrapper-result.private.json').write_text(json.dumps({
            'run_id': os.environ['GITHUB_RUN_ID'], 'original_error': original_error,
            'cleanup_error': cleanup_error, 'comparator_exit': child.returncode,
            'allocated_before_bytes': before, 'allocated_after_bytes': final,
            'all_owned_children_waited': child.poll() is not None}, sort_keys=True) + '\n')
        require(original_error is None and cleanup_error is None, 'comparator or collection failed')


def run(args):
    require(os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch' and
            os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted' and
            os.environ.get('RUNNER_KIND') == 'github-hosted', 'unexpected job boundary')
    require(args.qualification_checkout.resolve() != args.frozen_checkout.resolve(), 'checkouts overlap')
    require(sha(args.config) == args.config_sha256, 'comparator config differs')
    config = read(args.config)
    verify_frozen_checkout(args.frozen_checkout, config, config['sources']['scripts/ops/wal_cost_contract.py'])
    require(head(args.qualification_checkout) == os.environ['GITHUB_SHA'] ==
            os.environ['EXPECTED_CONTROLLER_SHA'], '#804 source checkout differs')
    module = load_qualification_module(args.qualification_checkout)
    module.verified_controller()
    private = Path(os.environ['RUNNER_TEMP']) / 'alopex-804-qualification'
    # The exact #804 workflow's bound-checkout step must already have created
    # this same-job preflight receipt before the second checkout was staged.
    preflight = read(private / 'checkout-preflight.private.json')
    require(preflight['expected_controller_sha'] == os.environ['GITHUB_SHA'] and
            preflight['workspace_root'] == str(Path(os.environ['GITHUB_WORKSPACE']).parent),
            'checkout preflight identity differs')
    require(not (private / 'admission.private.json').exists() and
            not (args.qualification_checkout / 'native-qualification-export/public.json').exists(),
            'stale qualification evidence')
    facts = module.admission_facts()
    require(module.Budget(facts['storage']).used() + sum(module.PLANNED.values()) +
            COMPARATOR_PEAK + COLLECT_RESERVE < STOP_BYTES, 'combined peak exceeds host budget')
    environment = dict(os.environ)
    for flag in ('--admission-only', '--acquire-assets', '--execute'):
        run_phase(args.qualification_checkout, flag, environment)
    public = read(args.qualification_checkout / 'native-qualification-export/public.json')
    prior = verify_qualification(public, private, module, os.environ['GITHUB_RUN_ID'], module.admission_facts())
    environment['WAL_COST_CONFIG'] = base64.b64encode(args.config.read_bytes()).decode('ascii')
    monitor_comparator(args.frozen_checkout, config, module, prior, private, environment)


def seal_wrapper_failure(error):
    """Keep wrapper failures private even when no comparator was started."""
    private = Path(os.environ['RUNNER_TEMP']) / 'alopex-804-qualification'
    require(private.is_dir() and not private.is_symlink() and
            private.stat().st_mode & 0o077 == 0, 'private evidence directory differs')
    receipt = private / 'wrapper-result.private.json'
    if receipt.exists():
        return  # Preserve the comparator's original error and cleanup result.
    payload = {'run_id': os.environ.get('GITHUB_RUN_ID'),
               'pre_comparator_error': True, 'original_error': repr(error)[:4096],
               'cleanup_error': None, 'comparator_exit': None,
               'all_owned_children_waited': False}
    with receipt.open('x', encoding='utf-8') as output:
        json.dump(payload, output, sort_keys=True)
        output.write('\n')
    receipt.chmod(0o600)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--qualification-checkout', type=Path, required=True)
    parser.add_argument('--frozen-checkout', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--config-sha256', required=True)
    args = parser.parse_args()
    try:
        run(args)
        return 0
    except BaseException as error:
        try:
            seal_wrapper_failure(error)
            code = 'WRAPPER_FAILURE'
        except BaseException:
            code = 'EVIDENCE_FAILURE'
        print(json.dumps({'case': 'WAL_COST_682', 'outcome': 'failed',
                          'error_code': code}, sort_keys=True))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
