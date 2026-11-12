"""Same-job #804 qualification followed by one bounded #682 diagnostic replay.

The frozen comparator entry point is never called. Detailed failures and host
facts remain in task-owned private evidence for the separate encryption step.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

if __package__:
    from . import same_job_wrapper as shared
else:
    import same_job_wrapper as shared

EXACT_CONFIG_SHA256 = '24c31ed86f5643edfef6789b345e12d610e71d05c1c3a905d84e17c3771c055b'
DIAGNOSTIC_LOCK_SHA256 = '623bd487f0ce0b47236ddebc4401e5aff7c157ed37049cfc7ca46bae9ff0cf3a'
DIAGNOSTIC_PEAK = 1_000_000_000
COLLECT_RESERVE = 1_000_000_000
DIAGNOSTIC_DEADLINE_SECONDS = 1_800


def require_private_dir(path):
    shared.require(path.is_dir() and not path.is_symlink() and
                   path.stat().st_mode & 0o077 == 0, 'private evidence directory differs')


def receipt_path():
    return Path(os.environ['RUNNER_TEMP']) / 'alopex-804-qualification' / 'diagnostic-wrapper-result.private.json'


def save_receipt(path, payload):
    require_private_dir(path.parent)
    shared.require(not path.exists(), 'stale diagnostic wrapper receipt')
    with path.open('x', encoding='utf-8') as output:
        json.dump(payload, output, sort_keys=True)
        output.write('\n')
    path.chmod(0o600)


def stop_group(child):
    if child.poll() is None:
        # The runner owns cell children in separate process groups. Interrupt
        # Python first so frozen run_cell/Budget.run finally blocks reap them.
        os.killpg(child.pid, signal.SIGINT)
        try:
            child.wait(timeout=45)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=5)
    shared.require(child.poll() is not None, 'diagnostic child was not reaped')


def cleanup_owned(diagnostic_private):
    shared.require(not diagnostic_private.is_symlink(), 'diagnostic root changed')
    work = diagnostic_private / 'diagnostic'
    shared.require(not work.is_symlink(), 'diagnostic work changed')
    for name in ('imports', 'venvs', 'fixtures', 'tmp'):
        path = work / name
        shared.require(not path.is_symlink(), 'owned cleanup path changed')
        if path.exists():
            shutil.rmtree(path)


def supervise_diagnostic(command, *, cwd, env, module, prior, private, diagnostic_private):
    """Own the runner process group and count host storage through its cleanup."""
    budget = module.Budget(prior['storage'])
    before = budget.used()
    shared.require(before + DIAGNOSTIC_PEAK + COLLECT_RESERVE < shared.STOP_BYTES and
                   before < shared.MAX_BYTES, 'diagnostic storage headroom insufficient')
    log = private / 'diagnostic.stdout.private.log'
    shared.require(not log.exists(), 'stale diagnostic log')
    original_error = None
    cleanup_error = None
    exit_code = None
    reaped = False
    final = None
    with log.open('xb') as output:
        child = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                 stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + DIAGNOSTIC_DEADLINE_SECONDS
            while child.poll() is None:
                shared.require(time.monotonic() < deadline, 'diagnostic deadline exceeded')
                used = budget.used()
                shared.require(used < shared.STOP_BYTES and used < shared.MAX_BYTES and
                               used <= before + DIAGNOSTIC_PEAK,
                               'diagnostic host storage growth exceeded')
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            exit_code = child.returncode
            shared.require(exit_code == 0, 'diagnostic runner failed')
        except BaseException as error:
            original_error = repr(error)[:4096]
        finally:
            try:
                stop_group(child)
                reaped = True
                exit_code = child.returncode
                cleanup_owned(diagnostic_private)
            except BaseException as error:
                cleanup_error = repr(error)[:4096]
            try:
                shared.same_host(prior, module.admission_facts())
                final = budget.used()
                shared.require(final < shared.STOP_BYTES and final < shared.MAX_BYTES and
                               final <= before + DIAGNOSTIC_PEAK,
                               'post-diagnostic host storage exceeded')
            except BaseException as error:
                cleanup_error = repr(error)[:4096] if cleanup_error is None else cleanup_error
            save_receipt(private / 'diagnostic-wrapper-result.private.json',
                         dict(run_id=os.environ['GITHUB_RUN_ID'], original_error=original_error,
                              cleanup_error=cleanup_error, diagnostic_exit=exit_code,
                              all_owned_children_waited=reaped,
                              allocated_before_bytes=before, allocated_after_bytes=final))
    shared.require(original_error is None and cleanup_error is None and reaped,
                   'diagnostic execution or cleanup failed')


def run(args):
    shared.require(os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch' and
                   os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted' and
                   os.environ.get('RUNNER_KIND') == 'github-hosted',
                   'unexpected diagnostic job boundary')
    shared.require(args.qualification_checkout.resolve() != args.frozen_checkout.resolve(),
                   'checkouts overlap')
    shared.require(shared.sha(args.config) == EXACT_CONFIG_SHA256 and
                   shared.sha(args.diagnostic_sources) == DIAGNOSTIC_LOCK_SHA256,
                   'pinned diagnostic input differs')
    config = shared.read(args.config)
    shared.verify_frozen_checkout(args.frozen_checkout, config,
                                  config['sources']['scripts/ops/wal_cost_contract.py'])
    shared.require(shared.head(args.qualification_checkout) == os.environ['GITHUB_SHA'] ==
                   os.environ['EXPECTED_CONTROLLER_SHA'], 'controller checkout differs')
    module = shared.load_qualification_module(args.qualification_checkout)
    module.verified_controller()
    private = Path(os.environ['RUNNER_TEMP']) / 'alopex-804-qualification'
    require_private_dir(private)
    diagnostic_private = Path(os.environ['RUNNER_TEMP']) / 'alopex-682-diagnostic-private'
    diagnostic_export = args.qualification_checkout / 'wal-diagnostic-export'
    shared.require(not diagnostic_private.exists() and not diagnostic_private.is_symlink() and
                   not diagnostic_export.exists() and not diagnostic_export.is_symlink(),
                   'stale diagnostic evidence')
    preflight = shared.read(private / 'checkout-preflight.private.json')
    shared.require(preflight['expected_controller_sha'] == os.environ['GITHUB_SHA'] and
                   preflight['workspace_root'] == str(Path(os.environ['GITHUB_WORKSPACE']).parent),
                   'checkout preflight identity differs')
    shared.require(not (private / 'admission.private.json').exists() and
                   not (args.qualification_checkout / 'native-qualification-export/public.json').exists() and
                   not receipt_path().exists(), 'stale qualification or diagnostic evidence')
    facts = module.admission_facts()
    shared.require(module.Budget(facts['storage']).used() + sum(module.PLANNED.values()) +
                   DIAGNOSTIC_PEAK + COLLECT_RESERVE < shared.STOP_BYTES,
                   'combined qualification and diagnostic peak exceeds host budget')
    environment = dict(os.environ)
    for flag in ('--admission-only', '--acquire-assets', '--execute'):
        shared.run_phase(args.qualification_checkout, flag, environment)
    qualification_public = args.qualification_checkout / 'native-qualification-export/public.json'
    public = shared.read(qualification_public)
    prior = shared.verify_qualification(public, private, module,
                                        os.environ['GITHUB_RUN_ID'], module.admission_facts())
    diagnostic_private.mkdir(mode=0o700)
    diagnostic_export.mkdir(mode=0o700)
    command = [sys.executable, '-B', str(args.qualification_checkout /
               'scripts/ops/wal_cost_next/diagnostic_runner.py'),
               '--frozen-checkout', str(args.frozen_checkout), '--exact-config', str(args.config),
               '--diagnostic-sources', str(args.diagnostic_sources),
               '--diagnostic-sources-sha256', DIAGNOSTIC_LOCK_SHA256,
               '--surface', args.surface, '--value-bytes', str(args.value_bytes),
               '--qualification-private', str(private),
               '--qualification-public', str(qualification_public),
               '--private-root', str(diagnostic_private), '--export-root', str(diagnostic_export)]
    supervise_diagnostic(command, cwd=args.qualification_checkout, env=environment,
                         module=module, prior=prior, private=private,
                         diagnostic_private=diagnostic_private)


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--qualification-checkout', required=True, type=Path)
    parser.add_argument('--frozen-checkout', required=True, type=Path)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--diagnostic-sources', required=True, type=Path)
    parser.add_argument('--surface', required=True, choices=('kv', 'sql'))
    parser.add_argument('--value-bytes', required=True, type=int, choices=(16, 2048))
    args = parser.parse_args()
    try:
        run(args)
        return 0
    except BaseException as error:
        try:
            path = receipt_path()
            if not path.exists():
                save_receipt(path, dict(run_id=os.environ.get('GITHUB_RUN_ID'),
                                        pre_diagnostic_error=repr(error)[:4096],
                                        original_error=None, cleanup_error=None,
                                        diagnostic_exit=None,
                                        all_owned_children_waited=False))
            code = 'DIAGNOSTIC_WRAPPER_FAILURE'
        except BaseException:
            code = 'DIAGNOSTIC_EVIDENCE_FAILURE'
        print(json.dumps(dict(case='WAL_COST_682_DIAGNOSTIC', outcome='failed',
                              error_code=code), sort_keys=True))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
