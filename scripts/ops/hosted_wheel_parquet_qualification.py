"""Exercise the RC qualification entry point using an existing source-bound wheel."""
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/test-env'))
from common import atomic_json, private_dir, require
from capacity import Budget


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024): value.update(block)
    return value.hexdigest()


def verify_wheel_assets(root, manifest, producer):
    require(manifest.get('schema') == 'alopex.native-wheel-assets/v1'
            and manifest.get('platform') == producer['platform']
            and manifest.get('source_commit') == producer['source_commit']
            and manifest.get('source_tree') == producer['source_tree'], 'IDENTITY')
    files = manifest.get('files')
    require(isinstance(files, list) and len(files) == 3
            and {item.get('role') for item in files} == {'wheel', 'library', 'receipt'}, 'IDENTITY')
    require(sum(item['size'] for item in files) <= 100_000_000, 'CAPACITY')
    by_role = {}
    for item in files:
        relative = PurePosixPath(item['path'])
        require(not relative.is_absolute() and '..' not in relative.parts
                and '\\' not in item['path'] and item['role'] not in by_role, 'IDENTITY')
        path = root.joinpath(*relative.parts)
        require(path.resolve().is_relative_to(root.resolve()) and path.is_file()
                and not path.is_symlink() and path.stat().st_size == item['size']
                and sha(path) == item['sha256'], 'IDENTITY')
        by_role[item['role']] = path
    receipt = json.loads(by_role['receipt'].read_bytes())
    require(receipt.get('schema') == 'alopex.performance-wheel-build/v1'
            and receipt.get('source') == dict(commit=producer['source_commit'],
                tree=producer['source_tree'], profile='release')
            and receipt.get('tools') == dict(rust=producer['rust_version'],
                maturin=producer['maturin_version'])
            and receipt.get('python_wheel_sha256') == producer['wheel_sha256']
            and receipt.get('parser_sha256') == sha(by_role['library']), 'IDENTITY')
    return by_role


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
        planned_bytes=dict(fetch=256_000_000, run=2_000_000_000, collect=64_000_000)))
    status, error, report = 'failed', None, None
    signal.alarm(750)
    try:
        initial = budget.before('fetch')
        atomic_json(work / 'storage-before.json', dict(allocated_bytes=initial))
        for name, expected in config['sources'].items():
            require(sha(ROOT / name) == expected, 'IDENTITY')
        source = config['producer']['source_commit']
        ancestry = subprocess.run(['git', 'merge-base', '--is-ancestor', source, 'HEAD'],
            cwd=ROOT, capture_output=True)
        changed = subprocess.run(['git', 'diff', '--name-only', source, 'HEAD'],
            cwd=ROOT, capture_output=True, text=True, check=True).stdout.splitlines()
        require(ancestry.returncode == 0 and set(changed) == {
            '.github/ops/native-evidence-cert.pem',
            '.github/workflows/alopex-performance.yml',
            'scripts/ops/hosted_wheel_parquet_qualification.py'}, 'IDENTITY')
        def metadata(endpoint):
            result = subprocess.run(['gh', 'api', endpoint], capture_output=True, text=True, check=True, timeout=30)
            return json.loads(result.stdout)
        producer = config['producer']
        endpoint = 'repos/alopex-db/alopex/actions/runs/' + str(producer['run'])
        run = metadata(endpoint)
        require(run['head_sha'] == producer['controller'] and run['status'] == 'completed'
                and run['conclusion'] == 'success' and run['event'] == 'workflow_dispatch'
                and run['head_repository']['full_name'] == 'alopex-db/alopex', 'IDENTITY')
        matches = [item for item in metadata(endpoint + '/artifacts')['artifacts'] if item['name'] == 'native-wheel-assets']
        require(len(matches) == 1 and matches[0]['id'] == producer['artifact_id']
                and not matches[0]['expired'] and matches[0]['digest'] == producer['artifact_digest']
                and 0 < matches[0]['size_in_bytes'] < 64_000_000, 'IDENTITY')
        require(not imported.exists() and not (ROOT / 'dist').exists(), 'STATE')
        budget.run(['gh', 'run', 'download', str(producer['run']), '--repo', 'alopex-db/alopex',
                    '--name', 'native-wheel-assets', '--dir', str(imported)],
                   timeout=120, log=work / 'download.log')
        manifest_path = imported / 'assets/assets.json'
        require(sha(manifest_path) == producer['assets_digest'], 'IDENTITY')
        manifest = json.loads(manifest_path.read_bytes())
        assets = verify_wheel_assets(manifest_path.parent, manifest, producer)
        wheel = assets['wheel']
        receipt_path = assets['receipt']
        require(sha(wheel) == producer['wheel_sha256'], 'IDENTITY')
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
        budget.run([sys.executable, '-B', str(ROOT / 'scripts/release/verify_python_rc.py'), '--kind', 'linux'],
                   timeout=480, log=work / 'qualification.log', env=env)
        report_path = ROOT / 'artifacts/python-release/vector-parquet.json'
        report = json.loads(report_path.read_bytes())
        require(report['status'] == 'passed' and len(report['cases']) == 38
                and report['wheel_sha256'] == producer['wheel_sha256']
                and report['source_commit'] == os.environ['GITHUB_SHA'], 'QUALIFICATION')
        shutil.copyfile(report_path, work / 'qualification.json')
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
        require(sum(p.stat().st_size for p in members) < 16_000_000, 'CAPACITY')
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
        atomic_json(export / 'result.json', dict(schema='alopex.wheel-parquet-runtime/v1', status=status,
            error_type=error, controller=os.environ['GITHUB_SHA'], sources=config['sources'],
            product_source=config['producer']['source_commit'], wheel_sha256=config['producer']['wheel_sha256'],
            qualification_report_sha256=report_digest, cases=38 if status == 'passed' else 0,
            owned_resources_removed=True, rc_asset_qualification=False))
        signal.alarm(0)
    return 0 if status == 'passed' else 1


if __name__ == '__main__':
    def interrupted(signum, frame): raise InterruptedError('qualification interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    try: code = main()
    except BaseException as error:
        code = 1
        last = error.__traceback__
        while last and last.tb_next:
            last = last.tb_next
        print(json.dumps(dict(status='failed', error_type=type(error).__name__,
                              source_line=last.tb_lineno if last else None)))
    else:
        print(json.dumps(dict(status='passed' if code == 0 else 'failed')))
    raise SystemExit(code)
