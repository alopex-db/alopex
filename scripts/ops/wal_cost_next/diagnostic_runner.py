"""One-pair #682 explanatory replay. Caller must have completed same-job #804.

The pinned comparator checkout supplies installation, supervision and recovery
contracts, but its matrix entry point is never called. All raw data stay private.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile

if __package__:
    from . import same_job_wrapper
else:
    import same_job_wrapper

FROZEN = '490f002dfb2c7f509a2444891808d9704fe6b001'
EXACT_CONFIG_SHA256 = '24c31ed86f5643edfef6789b345e12d610e71d05c1c3a905d84e17c3771c055b'
STOP_BYTES = 40_000_000_000
MAX_BYTES = 50_000_000_000
PEAK_ADDITIONAL_BYTES = 1_000_000_000
EVIDENCE_LIMIT = 16_000_000
DIAGNOSTIC_FILES = ('diagnostic_runner.py', 'diagnostic_worker.py',
                    'diagnostic_commit_observer.py', 'schedstat_observer.py')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def read_exact(path, digest):
    require(path.is_file() and not path.is_symlink() and sha(path) == digest,
            'pinned input differs')
    return json.loads(path.read_bytes())


def cleanup_owned(work):
    for name in ('imports', 'venvs', 'fixtures', 'tmp'):
        path = work / name
        require(not path.is_symlink(), 'owned cleanup path differs')
        if path.exists():
            shutil.rmtree(path)


def discard_unsealed(work, export):
    if work.is_symlink():
        work.unlink()
    elif work.exists():
        shutil.rmtree(work)
    encrypted = export / 'private-evidence.cms'
    if encrypted.exists() or encrypted.is_symlink():
        encrypted.unlink()


def verify_inputs(args):
    require(sys.platform == 'linux' and os.getuid() != 0 and not sys.flags.optimize and
            os.environ.get('RUNNER_KIND') == 'github-hosted' and
            os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch',
            'diagnostic job boundary differs')
    require(subprocess.check_output(['git', '-C', str(args.frozen_checkout), 'rev-parse', 'HEAD'],
                                    text=True, stderr=subprocess.DEVNULL, timeout=10).strip() == FROZEN,
            'frozen checkout differs')
    config = read_exact(args.exact_config, EXACT_CONFIG_SHA256)
    require(args.qualification_private.is_dir() and not args.qualification_private.is_symlink() and
            args.qualification_public.is_file() and not args.qualification_public.is_symlink(),
            'qualification evidence differs')
    qualification = json.loads(args.qualification_public.read_bytes())
    admission = json.loads((args.qualification_private / 'admission.private.json').read_bytes())
    profile = json.loads((args.qualification_private / 'profile.json').read_bytes())
    require(qualification['schema'] == 'alopex.hosted-test-env-qualification/v1' and
            qualification['case'] == 'HOST_QUALIFICATION' and
            qualification['outcome'] == 'passed' and qualification['error_code'] is None and
            profile['run_id'] == 'hosted-804-' + os.environ['GITHUB_RUN_ID'] and
            profile['storage'] == admission['storage'],
            'same-job qualification receipt differs')
    require(config['cell_seconds'] == 30 and config['rss_bytes'] == 1_073_741_824 and
            config['cpu_selection'] == 'first_allowed', 'cell limits differ')
    for name, digest in config['sources'].items():
        source = args.frozen_checkout / name
        require(source.is_file() and not source.is_symlink() and sha(source) == digest,
                'frozen source differs')
    pinned = read_exact(args.diagnostic_sources, args.diagnostic_sources_sha256)
    require(set(pinned) == set(DIAGNOSTIC_FILES), 'diagnostic source list differs')
    for name, digest in pinned.items():
        source = Path(__file__).parent / name
        require(source.is_file() and not source.is_symlink() and sha(source) == digest,
                'diagnostic source differs')
    require((args.surface, args.value_bytes) in (('kv', 16), ('kv', 2048),
            ('sql', 16), ('sql', 2048)), 'shape differs')
    require(args.private_root.is_dir() and not args.private_root.is_symlink() and
            args.private_root.stat().st_mode & 0o077 == 0, 'private root differs')
    require(args.export_root.is_dir() and not args.export_root.is_symlink() and
            args.export_root.stat().st_mode & 0o077 == 0, 'private export differs')
    require(args.private_root.resolve() != args.export_root.resolve() and
            not args.private_root.resolve().is_relative_to(args.export_root.resolve()) and
            not args.export_root.resolve().is_relative_to(args.private_root.resolve()),
            'evidence roots overlap')
    require(not any(args.private_root.iterdir()) and not any(args.export_root.iterdir()),
            'stale diagnostic evidence')
    return config, admission['storage']


def verify_same_job(args, early_storage):
    controller = Path(__file__).resolve().parents[3]
    require(args.qualification_checkout.resolve(strict=True) == controller and
            same_job_wrapper.head(controller) == os.environ['GITHUB_SHA'] ==
            os.environ['EXPECTED_CONTROLLER_SHA'],
            'qualification controller source differs')
    module = same_job_wrapper.load_qualification_module(controller)
    module.verified_controller()
    public = json.loads(args.qualification_public.read_bytes())
    prior = same_job_wrapper.verify_qualification(
        public, args.qualification_private, module, os.environ['GITHUB_RUN_ID'],
        module.admission_facts())
    require(prior['storage'] == early_storage, 'qualification storage scope changed')
    return prior['storage']


def load_frozen(frozen):
    sys.path[:0] = [str(frozen / 'scripts/ops'), str(frozen / 'scripts/test-env'),
                    str(frozen / 'scripts/performance')]
    script = frozen / 'scripts/ops/hosted_wal_cost.py'
    spec = importlib.util.spec_from_file_location('frozen_wal_cost_helpers', script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def wheel_native_sha(wheel):
    with zipfile.ZipFile(wheel) as archive:
        matches = [entry for entry in archive.infolist()
                   if entry.filename == 'alopex/_alopex.abi3.so' and not entry.is_dir()]
        require(len(matches) == 1 and 0 < matches[0].file_size < 64_000_000,
                'native wheel member differs')
        digest = hashlib.sha256()
        with archive.open(matches[0]) as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()


def verify_product(product, producer, helper):
    require(helper.sha(product['manifest']) == producer['manifest_sha256'], 'asset manifest changed')
    manifest = json.loads(product['manifest'].read_bytes())
    entries = manifest['files']
    require(len(entries) == len(product['files']) and
            {entry['role'] for entry in entries} == set(product['files']),
            'asset inventory changed')
    for entry in entries:
        path = product['files'][entry['role']]
        require(path.is_file() and not path.is_symlink() and
                path.stat().st_size == entry['size'] and
                helper.sha(path) == entry['sha256'], 'product asset changed')
    require(helper.sha(product['files']['wheel']) == producer['wheel_sha256'] and
            wheel_native_sha(product['files']['wheel']) == producer['native_sha256'],
            'product asset changed')


def validate_cell(record, cell, phase, producer, helper, output):
    require(output.joinpath('worker.log').stat().st_size <= 1_000_000, 'worker log exceeded')
    identity = json.loads((output / 'identity.json').read_bytes())
    require(identity['manifest_sha256'] == cell['subject_manifest_sha256'],
            'worker subject differs')
    helper.verify_record(record, cell, phase, producer['native_sha256'])
    if phase == 'write':
        observation = json.loads((output / 'commit-observation.json').read_bytes())
        require(observation['acceptance_sample'] is False and
                observation['schedstat']['acceptance_sample'] is False and
                observation['schedstat']['schema'] == 'alopex.682-schedstat-diagnostic/v1' and
                observation['timer_nanoseconds'] == record['commit_ns'] ==
                observation['schedstat']['original_timer_ns'],
                'diagnostic observation differs')


def seal_evidence(work, export, certificate, helper):
    members = [p for p in work.rglob('*') if p.is_file()]
    require(all(not p.is_symlink() for p in work.rglob('*')) and
            sum(p.stat().st_size for p in members) < EVIDENCE_LIMIT,
            'private evidence bound exceeded')
    archive = work / 'evidence.tar.gz'
    with tarfile.open(archive, 'w:gz') as output:
        for path in members:
            output.add(path, arcname=path.relative_to(work).as_posix(), recursive=False)
    encrypted = export / 'private-evidence.cms'
    subprocess.run(['openssl', 'cms', '-encrypt', '-aes-256-cbc', '-binary',
                    '-outform', 'DER', '-in', str(archive), '-out', str(encrypted),
                    str(certificate)], capture_output=True, check=True, timeout=30)
    require(encrypted.is_file() and 0 < encrypted.stat().st_size < EVIDENCE_LIMIT,
            'encrypted evidence differs')
    return helper.sha(archive), helper.sha(encrypted)


def run(args):
    config, early_storage = verify_inputs(args)
    storage = verify_same_job(args, early_storage)
    helper = load_frozen(args.frozen_checkout)
    os.umask(0o077)
    allowed = sorted(os.sched_getaffinity(0))
    require(len(allowed) >= 2, 'CPU allocation unavailable')
    allocation = {'client': allowed[:1], 'server': allowed[1:2]}
    roots = [Path(p) for p in storage['roots']]
    require(all(p.is_absolute() and p.exists() for p in roots) and
            any(args.private_root.resolve().is_relative_to(p.resolve()) for p in roots) and
            any(args.export_root.resolve().is_relative_to(p.resolve()) for p in roots),
            'host storage scope differs')
    budget = helper.Budget(dict(roots=list(map(str, roots)), stop_bytes=STOP_BYTES,
                                reserve_bytes=256_000_000, poll_seconds=2,
                                planned_bytes=dict(prepare=PEAK_ADDITIONAL_BYTES,
                                                   measure=128_000_000, collect=32_000_000)))
    require(budget.used() + PEAK_ADDITIONAL_BYTES + 256_000_000 < STOP_BYTES,
            'host storage headroom insufficient')
    work = args.private_root / 'diagnostic'
    work.mkdir(mode=0o700)
    for name in ('imports', 'venvs', 'identities', 'cells', 'fixtures', 'tmp'):
        (work / name).mkdir(mode=0o700)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PIP_NO_CACHE_DIR='1',
               TMPDIR=str(work / 'tmp'))
    for name in ('GH_TOKEN', 'GITHUB_TOKEN', 'WAL_COST_CONFIG', 'PYTHONPATH',
                 'PYTHONHOME', 'LD_LIBRARY_PATH', 'ALOPEX_PERF_SUBJECT_MANIFEST',
                 'ALOPEX_PERF_SUBJECT_SHA256'):
        env.pop(name, None)
    for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                 'POLARS_MAX_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'):
        env[name] = '1'
    complete = 0
    status = 'inconclusive'
    error_code = None
    try:
        budget.before('prepare')
        products = {name: helper.install_product(name, config['products'][name], work, budget, env)
                    for name in ('before', 'candidate')}
        before, candidate = (products[name]['receipt'] for name in ('before', 'candidate'))
        for key in ('tools', 'parser_record_sha256', 'parser_contract'):
            require(before[key] == candidate[key], 'product toolchain differs')
        require(before['source']['profile'] == candidate['source']['profile'] == 'release',
                'product profile differs')
        for name, product in products.items():
            verify_product(product, config['products'][name], helper)
            subject = dict(schema='alopex.performance-subject/v1', version=product['receipt']['version'],
                           source=product['receipt']['source'], cpu=allocation,
                           python_wheel=dict(path=str(product['files']['wheel']),
                                             sha256=helper.sha(product['files']['wheel'])),
                           build_receipt=dict(path=str(product['files']['receipt']),
                                              sha256=helper.sha(product['files']['receipt'])))
            path = work / 'identities' / name / 'subject.json'
            helper.atomic_json(path, subject)
            product['subject'] = path
        budget.before('measure')
        for name in ('before', 'candidate'):
            product = products[name]
            manifest = product['subject']
            cell = dict(id=f'{name}-{args.surface}-{args.value_bytes}', round=0,
                        subject=name, surface=args.surface, value_bytes=args.value_bytes)
            directory = work / 'cells' / name
            fixture = work / 'fixtures' / name
            directory.mkdir(); fixture.mkdir()
            child_env = dict(env, PYTHONPATH=str(args.frozen_checkout / 'scripts/performance'),
                             ALOPEX_PERF_SUBJECT_MANIFEST=str(manifest),
                             ALOPEX_PERF_SUBJECT_SHA256=helper.sha(manifest),
                             LD_LIBRARY_PATH=str(product['files']['library'].parent))
            try:
                for index, phase in enumerate(('write', 'verify', 'verify')):
                    output = directory / str(index)
                    output.mkdir()
                    parameters = dict(cell, phase=phase, fixture=str(fixture), output=str(output),
                                      affinity=allocation['client'], options=config['options'],
                                      sources=config['sources'],
                                      subject_manifest_sha256=helper.sha(manifest))
                    path = output / 'configuration.json'
                    helper.atomic_json(path, parameters)
                    command = ['taskset', '--cpu-list', ','.join(map(str, allocation['client'])),
                               str(product['python']), '-B',
                               str(Path(__file__).parent / 'diagnostic_worker.py'),
                               '--configuration', str(path), '--config-sha256', helper.sha(path),
                               '--frozen-checkout', str(args.frozen_checkout)]
                    helper.run_cell(command, child_env, output, budget, 30, 1_073_741_824)
                    record = json.loads((output / 'worker.log').read_bytes())
                    validate_cell(record, dict(cell, subject_manifest_sha256=helper.sha(manifest)),
                                  phase, config['products'][name], helper, output)
                    budget.check()
                complete += 1
            finally:
                shutil.rmtree(fixture)
                budget.check()
            verify_product(product, config['products'][name], helper)
        status = 'observed'
    except BaseException as error:
        error_code = 'DIAGNOSTIC_FAILED'
        helper.atomic_json(work / 'failure.private.json',
                           dict(error=repr(error)[:4096], type=type(error).__name__))
    finally:
        try:
            cleanup_owned(work)
            require(budget.check() < MAX_BYTES, 'host storage ceiling exceeded')
            evidence_sha, encrypted_sha = seal_evidence(work, args.export_root,
                args.frozen_checkout / '.github/ops/native-evidence-cert.pem', helper)
            shutil.rmtree(work)
            budget.check()
            helper.atomic_json(args.export_root / 'result.json',
                dict(schema='alopex.wal-diagnostic/v1', case='WAL_COST_682',
                     outcome=status, error_code=error_code, shape=f'{args.surface}-{args.value_bytes}',
                     complete_subjects=complete, evidence_sha256=evidence_sha,
                     encrypted_sha256=encrypted_sha, performance_acceptance=False))
        except BaseException:
            # The workflow uploads only export_root. Remove any raw or partial
            # encrypted output before returning its fixed failure code.
            try:
                discard_unsealed(work, args.export_root)
            except BaseException:
                pass
            raise RuntimeError('DIAGNOSTIC_EVIDENCE_FAILURE') from None
    return 0 if status == 'observed' else 1


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--frozen-checkout', required=True, type=Path)
    parser.add_argument('--exact-config', required=True, type=Path)
    parser.add_argument('--diagnostic-sources', required=True, type=Path)
    parser.add_argument('--diagnostic-sources-sha256', required=True)
    parser.add_argument('--surface', choices=('kv', 'sql'), required=True)
    parser.add_argument('--value-bytes', type=int, required=True)
    parser.add_argument('--qualification-private', type=Path, required=True)
    parser.add_argument('--qualification-public', type=Path, required=True)
    parser.add_argument('--qualification-checkout', type=Path, required=True)
    parser.add_argument('--private-root', type=Path, required=True)
    parser.add_argument('--export-root', type=Path, required=True)
    args = parser.parse_args()
    try:
        return run(args)
    except BaseException:
        print(json.dumps(dict(case='WAL_COST_682', outcome='failed',
                              error_code='DIAGNOSTIC_FAILURE', performance_acceptance=False)))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
