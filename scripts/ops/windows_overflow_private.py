"""Keep final-candidate Windows acceptance diagnostics private."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

PRODUCT = 'b92827f24a7fd86e391a95f32d1c773053c5da6d'
PHASES = ('prepare', 'parser', 'normal', 'instrumented', 'legacy', 'verify', 'cleanup')


def require(value):
    if not value:
        raise RuntimeError('acceptance evidence incomplete')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def phase_success(phases):
    return set(phases) == set(PHASES) and all(type(phases[p]) is int and phases[p] == 0 for p in PHASES)


def openssl_path():
    git = Path(shutil.which('git') or '').resolve()
    require(git.is_file() and git.name.lower() == 'git.exe')
    openssl = git.parent.parent / 'usr/bin/openssl.exe'
    require(openssl.is_file())
    return openssl


def manifest(root):
    result = {}
    for path in root.rglob('*'):
        require(not path.is_symlink())
        if path.is_file():
            require(path.stat().st_size <= 32_000_000)
            result[path.relative_to(root).as_posix()] = dict(bytes=path.stat().st_size, sha256=digest(path))
    require(sum(item['bytes'] for item in result.values()) <= 64_000_000)
    return result


def main():
    args = argparse.ArgumentParser()
    args.add_argument('phase', choices=(*PHASES, 'collect'))
    args = args.parse_args()
    require(sys.platform == 'win32' and os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted'
            and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch')
    root = Path(__file__).resolve().parents[2]
    private = Path(os.environ['RUNNER_TEMP']) / 'alopex-overflow-private-evidence'
    private.mkdir(exist_ok=True)
    state = private / 'phases.json'
    phases = json.loads(state.read_bytes()) if state.exists() else {}
    if args.phase != 'collect':
        require(args.phase not in phases)
        if args.phase == 'prepare':
            require(subprocess.run([str(openssl_path()), 'x509', '-in',
                str(root/'.github/ops/native-evidence-cert.pem'), '-noout'],
                capture_output=True, timeout=30).returncode == 0)
        require(subprocess.run(['git', 'diff', '--quiet', PRODUCT, 'HEAD', '--',
            'crates', 'Cargo.toml', 'Cargo.lock', 'scripts/compatibility'], cwd=root,
            capture_output=True, timeout=30).returncode == 0)
        with (private / (args.phase+'.log')).open('wb') as log:
            process = subprocess.run([sys.executable, str(root/'scripts/compatibility/windows_overflow_acceptance.py'), args.phase],
                cwd=root, stdout=log, stderr=subprocess.STDOUT)
        phases[args.phase] = process.returncode
        state.write_text(json.dumps(phases)+'\n')
        print(json.dumps(dict(phase=args.phase, status='passed' if process.returncode == 0 else 'failed')))
        return process.returncode
    sys.path.insert(0, str(root/'scripts/compatibility'))
    import storage
    require(storage.check(extra=[private])['allocated_bytes'] + 256_000_000 < storage.STOP_BYTES)
    evidence = root / 'artifacts/windows-overflow-public'
    files = manifest(evidence)
    logs = manifest(private)
    require(sum(item['bytes'] for item in files.values()) + sum(item['bytes'] for item in logs.values()) <= 64_000_000)
    export = root / 'windows-overflow-private-export'
    export.mkdir(exist_ok=False)
    archive = private / 'evidence.tar.gz'
    with tarfile.open(archive, 'w:gz') as tar:
        for name in files:
            tar.add(evidence/name, arcname=name)
        for name in logs:
            tar.add(private/name, arcname='phase-logs/'+name)
    result = subprocess.run([str(openssl_path()), 'cms', '-encrypt', '-aes-256-cbc', '-binary', '-outform', 'DER',
        '-in', str(archive), '-out', str(export/'private-evidence.cms'),
        str(root/'.github/ops/native-evidence-cert.pem')], capture_output=True, timeout=60)
    require(result.returncode == 0)
    passed = phase_success(phases)
    public = dict(schema='alopex.windows-overflow-private/v1', status='passed' if passed else 'failed',
        controller=os.environ['GITHUB_SHA'], product_source=PRODUCT, phases=phases,
        evidence_sha256=digest(archive), files=len(files), encrypted_sha256=digest(export/'private-evidence.cms'))
    (export/'result.json').write_text(json.dumps(public, indent=2)+'\n')
    shutil.rmtree(private)
    if evidence.exists():
        shutil.rmtree(evidence)
    storage.check(extra=[export])
    print(json.dumps(dict(status=public['status'], private_evidence_encrypted=True)))
    return 0 if passed else 1


if __name__ == '__main__':
    try:
        code = main()
    except BaseException:
        print('{"status":"failed","error_code":"PRIVATE_ACCEPTANCE"}')
        code = 1
    raise SystemExit(code)
