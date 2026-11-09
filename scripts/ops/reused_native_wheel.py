"""Reuse a pinned development wheel while retaining its original build receipt."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import zipfile

from hosted_native_wheel_assets import verify_wheel_assets

MAXIMUM = 100_000_000


def require(condition):
    if not condition:
        raise ValueError('reused wheel identity or archive differs')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_archive(infos, expected):
    require(0 < len(infos) <= 20)
    names = set()
    files = []
    for item in infos:
        name = PurePosixPath(item.filename)
        require(not name.is_absolute() and name.parts and '..' not in name.parts
                and '\\' not in item.filename and item.filename not in names)
        names.add(item.filename)
        kind = stat.S_IFMT(item.external_attr >> 16)
        require(kind in (0, stat.S_IFREG, stat.S_IFDIR))
        if not item.is_dir():
            require(kind in (0, stat.S_IFREG) and 0 < item.file_size <= MAXIMUM)
            files.append(item)
    require({item.filename for item in files} == set(expected['files']))
    require(sum(item.file_size for item in files) <= MAXIMUM)
    return files


def verify_reuse(root, expected, config, *, version, cargo_lock_sha256, parser_sha256, parser_contract):
    require(root.is_dir() and not root.is_symlink())
    require(not any(path.is_symlink() for path in root.rglob('*')))
    actual = {path.relative_to(root).as_posix(): digest(path)
              for path in root.rglob('*') if path.is_file()}
    require(actual == expected['files'])
    result = json.loads((root / 'result.json').read_bytes())
    manifest_path = root / 'assets/assets.json'
    manifest = json.loads(manifest_path.read_bytes())
    require(result['schema'] == 'alopex.native-wheel-assets-result/v1'
            and result['status'] == 'passed'
            and result['source'] == dict(commit=config['source_commit'], tree=config['source_tree'], profile='release')
            and result['assets_digest'] == digest(manifest_path) == expected['assets_digest'])
    verify_wheel_assets(root / 'assets', manifest, config)
    paths = {item['role']: root / 'assets' / item['path'] for item in manifest['files']}
    receipt = json.loads(paths['receipt'].read_bytes())
    require(receipt['version'] == version and receipt['cargo_lock_sha256'] == cargo_lock_sha256
            and receipt['parser_sha256'] == parser_sha256 and receipt['parser_contract'] == parser_contract
            and receipt['parser_record_sha256'] == config['parser_record_sha256']
            and receipt['build']['incremental'] is False and receipt['build']['release_debug'] is False
            and receipt['build']['path_remap'] == {'control': '/alopex-build/control', 'work': '/alopex-build/work'}
            and receipt['build']['wheel'] == 'maturin build --locked --release --compatibility linux')
    return paths, receipt


def fetch_reuse(root, expected, budget):
    require(not root.exists() and type(expected['run']) is int and expected['run'] > 0
            and type(expected['artifact_id']) is int and expected['artifact_id'] > 0)
    api_root = 'repos/alopex-db/alopex/'
    def metadata(path):
        response = subprocess.run(['gh', 'api', api_root + path], capture_output=True,
                                  check=True, text=True, timeout=30)
        return json.loads(response.stdout)
    run = metadata('actions/runs/' + str(expected['run']))
    require(run['head_sha'] == expected['controller'] and run['event'] == 'workflow_dispatch'
            and run['status'] == 'completed' and run['conclusion'] == 'success')
    artifact = metadata('actions/artifacts/' + str(expected['artifact_id']))
    require(artifact['workflow_run']['id'] == expected['run'] and not artifact['expired']
            and artifact['name'] == 'native-wheel-assets' and artifact['digest'] == expected['artifact_digest']
            and artifact['size_in_bytes'] == expected['artifact_size_bytes']
            and 0 < artifact['size_in_bytes'] <= MAXIMUM)
    budget.before('prepare')
    root.mkdir(mode=0o700)
    archive_path = root.parent / 'reused-wheel.zip'
    require(not archive_path.exists())
    try:
        budget.run(['gh', 'api', api_root + 'actions/artifacts/' + str(expected['artifact_id']) + '/zip'],
                   timeout=120, log=archive_path)
        require(0 < archive_path.stat().st_size <= MAXIMUM
                and 'sha256:' + digest(archive_path) == artifact['digest'])
        with zipfile.ZipFile(archive_path) as archive:
            for item in validate_archive(archive.infolist(), expected):
                budget.check()
                target = root.joinpath(*PurePosixPath(item.filename).parts)
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                with archive.open(item) as source, target.open('xb') as output:
                    shutil.copyfileobj(source, output)
                target.chmod(0o600)
                require(digest(target) == expected['files'][item.filename])
        budget.check()
    finally:
        archive_path.unlink(missing_ok=True)


def preserve_receipt(source, destination):
    require(source.is_file() and not source.is_symlink() and not destination.exists())
    original = source.read_bytes()
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with destination.open('xb') as output:
        output.write(original)
    destination.chmod(0o600)
    require(destination.read_bytes() == original)
    return digest(destination)


def verify_composed_receipts(root, manifest):
    paths = {}
    for role in ('receipt', 'wheel_receipt', 'wheel', 'server', 'library'):
        entries = [item for item in manifest['files'] if item['role'] == role]
        require(len(entries) == 1)
        entry = entries[0]
        relative = PurePosixPath(entry['path'])
        require(not relative.is_absolute() and '..' not in relative.parts and '\\' not in entry['path'])
        path = root.joinpath(*relative.parts)
        require(not path.is_symlink() and path.resolve().is_relative_to(root.resolve())
                and path.is_file() and path.stat().st_size == entry['size'] and digest(path) == entry['sha256'])
        paths[role] = path
    combined = json.loads(paths['receipt'].read_bytes())
    original = json.loads(paths['wheel_receipt'].read_bytes())
    reuse = combined['build']['wheel']
    require(combined['schema'] == 'alopex.performance-build/v1'
            and original['schema'] == 'alopex.performance-wheel-build/v1'
            and reuse['mode'] == 'reused_development_asset'
            and reuse['receipt_path'] == paths['wheel_receipt'].relative_to(root).as_posix()
            and reuse['receipt_sha256'] == digest(paths['wheel_receipt'])
            and combined['server_sha256'] == digest(paths['server']))
    for field in ('source', 'version', 'tools', 'cargo_lock_sha256', 'parser_contract', 'parser_record_sha256'):
        require(combined[field] == original[field])
    for role, field in (('wheel', 'python_wheel_sha256'), ('library', 'parser_sha256')):
        require(combined[field] == original[field] == digest(paths[role]))
    require(combined['source']['commit'] == manifest['source_commit']
            and combined['source']['profile'] == 'release')
    return paths
