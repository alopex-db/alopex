"""Exercise the RC qualification entry point using an existing source-bound wheel."""
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/test-env'))
from common import atomic_json, private_dir, require, verify_assets
from capacity import Budget


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024): value.update(block)
    return value.hexdigest()


def main():
    os.umask(0o077)
    require(sys.platform == 'linux' and os.getuid() != 0
            and os.environ.get('RUNNER_KIND') == 'github-hosted', 'VM_SCOPE')
    config = json.loads(base64.b64decode(os.environ['WHEEL_QUALIFICATION_CONFIG'], validate=True))
    work = private_dir(ROOT / 'wheel-qualification-private')
    export = private_dir(ROOT / 'wheel-qualification-export')
    imported = ROOT / 'wheel-qualification-import'
    roots = [ROOT.parent, Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent / '_actions', Path.home() / '.cache', Path.home() / '.cargo') if p.exists())
    budget = Budget(dict(roots=list(map(str, roots)), stop_bytes=40_000_000_000,
        reserve_bytes=2_000_000_000, poll_seconds=2,
        planned_bytes=dict(fetch=256_000_000, run=2_000_000_000, collect=256_000_000)))
    status, error, report = 'failed', None, None
    signal.alarm(850)
    try:
        initial = budget.before('fetch')
        atomic_json(work / 'storage-before.json', dict(allocated_bytes=initial))
        for name, expected in config['sources'].items():
            require(sha(ROOT / name) == expected, 'IDENTITY')
        def metadata(endpoint):
            result = subprocess.run(['gh', 'api', endpoint], capture_output=True, text=True, check=True, timeout=30)
            return json.loads(result.stdout)
        producer = config['producer']
        endpoint = 'repos/alopex-db/alopex/actions/runs/' + str(producer['run'])
        run = metadata(endpoint)
        require(run['head_sha'] == producer['controller'] and run['status'] == 'completed'
                and run['conclusion'] == 'success' and run['event'] == 'workflow_dispatch'
                and run['head_repository']['full_name'] == 'alopex-db/alopex', 'IDENTITY')
        matches = [item for item in metadata(endpoint + '/artifacts')['artifacts'] if item['name'] == 'native-product-assets']
        require(len(matches) == 1 and matches[0]['id'] == producer['artifact_id']
                and not matches[0]['expired'] and matches[0]['digest'] == producer['artifact_digest']
                and 0 < matches[0]['size_in_bytes'] < 64_000_000, 'IDENTITY')
        require(not imported.exists() and not (ROOT / 'dist').exists(), 'STATE')
        budget.run(['gh', 'run', 'download', str(producer['run']), '--repo', 'alopex-db/alopex',
                    '--name', 'native-product-assets', '--dir', str(imported)],
                   timeout=120, log=work / 'download.log')
        manifest_path = imported / 'assets/assets.json'
        require(sha(manifest_path) == producer['assets_digest'], 'IDENTITY')
        manifest = verify_assets(manifest_path, producer['platform'])
        require(manifest['source_commit'] == producer['source_commit'], 'IDENTITY')
        wheel_entry = next(item for item in manifest['files'] if item['role'] == 'wheel')
        wheel = manifest_path.parent / wheel_entry['path']
        require(wheel_entry['sha256'] == producer['wheel_sha256'], 'IDENTITY')
        receipt = next(item for item in manifest['files'] if item['role'] == 'receipt')
        receipt_path = manifest_path.parent / receipt['path']
        require(json.loads(receipt_path.read_bytes())['source']['tree'] == producer['source_tree'], 'IDENTITY')
        shutil.copyfile(manifest_path, work / 'assets.json')
        shutil.copyfile(receipt_path, work / 'build-receipt.json')
        dist = private_dir(ROOT / 'dist')
        shutil.copyfile(wheel, dist / wheel.name)
        temporary = private_dir(work / 'temporary')
        env = dict(os.environ, GITHUB_WORKSPACE=str(ROOT), RUNNER_TEMP=str(temporary), TMPDIR=str(temporary),
                   PYTHONDONTWRITEBYTECODE='1', PIP_DISABLE_PIP_VERSION_CHECK='1')
        for name in ('CI', 'GH_TOKEN', 'GITHUB_TOKEN', 'WHEEL_QUALIFICATION_CONFIG',
                     'PYTHONPATH', 'PYTHONHOME', 'PYTEST_ADDOPTS', 'PYTEST_PLUGINS',
                     'LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH', 'DYLD_FALLBACK_LIBRARY_PATH',
                     'NIM_SQL_PARSER_LIB_DIR', 'ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD'):
            env.pop(name, None)
        budget.before('run')
        from wheel_parquet_legacy_runtime import run as legacy_run
        report = legacy_run(ROOT, work, temporary, dist / wheel.name, budget, env)
        atomic_json(work / 'qualification.json', report)
        require(sha(wheel) == producer['wheel_sha256'], 'IDENTITY')
        atomic_json(work / 'identity.json', dict(controller=os.environ['GITHUB_SHA'], configuration=config))
        status = 'passed'
    except BaseException as exc:
        error = type(exc).__name__
        atomic_json(work / 'failure.json', dict(error=repr(exc)))
    finally:
        # Remove owned venv/fixtures before archiving compact diagnostic evidence.
        for path in (work / 'temporary', imported, ROOT / 'dist'):
            if path.exists(): shutil.rmtree(path)
        report_path = ROOT / 'artifacts/python-release/vector-parquet.json'
        if report_path.exists():
            if not (work / 'qualification.json').exists(): shutil.copyfile(report_path, work / 'qualification.json')
            report_path.unlink()
        budget.before('collect')
        atomic_json(work / 'cleanup.json', dict(venv_and_fixtures_removed=True, imports_removed=True,
                                              allocated_bytes=budget.check()))
        members = [p for p in work.rglob('*') if p.is_file()]
        require(sum(p.stat().st_size for p in members) < 80_000_000, 'CAPACITY')
        archive = work / 'evidence.tar.gz'
        with tarfile.open(archive, 'w:gz') as tar:
            for path in members:
                require(not path.is_symlink(), 'IDENTITY')
                tar.add(path, arcname=path.relative_to(work).as_posix())
        subprocess.run(['openssl', 'cms', '-encrypt', '-aes-256-cbc', '-binary', '-outform', 'DER',
            '-in', str(archive), '-out', str(export / 'private-evidence.cms'),
            str(ROOT / '.github/ops/native-evidence-cert.pem')], capture_output=True, check=True, timeout=30)
        report_digest = sha(work / 'qualification.json') if (work / 'qualification.json').exists() else None
        shutil.rmtree(work)
        budget.check()
        atomic_json(export / 'result.json', dict(schema='alopex.wheel-parquet-legacy-runtime/v1', status=status,
            error_type=error, controller=os.environ['GITHUB_SHA'], sources=config['sources'],
            product_source=config['producer']['source_commit'], wheel_sha256=config['producer']['wheel_sha256'],
            qualification_report_sha256=report_digest, cases=2 if status == 'passed' else 0,
            owned_resources_removed=True, rc_asset_qualification=False))
        signal.alarm(0)
    return 0 if status == 'passed' else 1


if __name__ == '__main__':
    def interrupted(signum, frame): raise InterruptedError('qualification interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    try: code = main()
    except BaseException: code = 1
    print(json.dumps(dict(status='passed' if code == 0 else 'failed')))
    raise SystemExit(code)
