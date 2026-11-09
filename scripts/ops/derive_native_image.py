"""Offline two-file derivation of an accepted, immutable test environment image."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil

CHANGES = ('capacity.py', 'tests/test_capacity_snapshot.py')


def require(value):
    if not value:
        raise ValueError('derived image identity differs')


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def source_digest(files):
    require(files and all(isinstance(name, str) and
        not PurePosixPath(name).is_absolute() and '..' not in PurePosixPath(name).parts
        and '\\' not in name and re.fullmatch('[a-f0-9]{64}', value)
        for name, value in files.items()))
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()


def validate_source_delta(before, after, original_record, actual_source):
    require(source_digest(before) == original_record['controller_digest'])
    require(source_digest(after) == actual_source['controller_digest'])
    changed = {name for name in set(before) | set(after) if before.get(name) != after.get(name)}
    require(changed == set(CHANGES) and all(name in after for name in CHANGES))
    require(re.fullmatch('sha256:[a-f0-9]{64}', original_record['id']))


def verify_image_delta(original, updated):
    require(original['Id'] != updated['Id'])
    for name in ('Os', 'Architecture', 'Variant'):
        require(original.get(name) == updated.get(name))
    # Config.Image is legacy build metadata, not an execution setting.
    configs = [{name: value for name, value in image['Config'].items() if name != 'Image'}
               for image in (original, updated)]
    require(configs[0] == configs[1])
    before, after = original['RootFS'], updated['RootFS']
    require(before['Type'] == after['Type'] == 'layers' and before['Layers'])
    require(after['Layers'][:-2] == before['Layers'] and len(after['Layers']) == len(before['Layers']) + 2)
    return after['Layers'][-2:]


def dockerfile(tag):
    require(re.fullmatch('alopex-derived-base:[a-f0-9]{16}', tag))
    return ('FROM ' + tag + '\n'
            'COPY capacity.py /tool/capacity.py\n'
            'COPY test_capacity_snapshot.py /tool/tests/test_capacity_snapshot.py\n')


def prepare(repo, profile, docker, budget, work, inputs):
    from common import atomic_json, contained, private_dir
    from envctl import source_identity, verified_image
    config = inputs['derived_image']
    original_record = config['image_record']
    source = source_identity()
    validate_source_delta(config['source_before'], config['source_after'], original_record, source)
    require(original_record['platform'] == profile['platform'] and
            original_record['lock_digest'] == digest(profile['image_lock']))
    archive = contained(repo, config['image_archive'])
    require(archive.is_file() and not archive.is_symlink() and
            digest(archive) == config['image_archive_sha256'])
    def command(args, phase, name, seconds=300):
        budget.before(phase)
        budget.run(args, timeout=seconds, log=work / (name + '.log'))
    command([*docker.prefix, 'load', '--input', str(archive)], 'build', 'load-base-image')
    original = json.loads(docker.call('image', 'inspect', original_record['id']))[0]
    require(original['Id'] == original_record['id'])
    require(profile['platform'].startswith(original['Os'] + '/' + original['Architecture']))
    atomic_json(work / 'base-image-inspection.json', original)
    tag = 'alopex-derived-base:' + original['Id'].removeprefix('sha256:')[:16]
    command([*docker.prefix, 'tag', original['Id'], tag], 'build', 'tag-base-image', 30)
    context = private_dir(work / 'cache/derive-context')
    try:
        for name in CHANGES:
            path = contained(repo / 'scripts/test-env', name)
            require(path.is_file() and not path.is_symlink() and digest(path) == config['source_after'][name])
            target = context / Path(name).name
            shutil.copyfile(path, target)
            target.chmod(0o644)
        (context / 'Dockerfile').write_text(dockerfile(tag))
        (context / 'Dockerfile').chmod(0o644)
        # No RUN instruction, network access, package install or mutable image tag lookup.
        command([*docker.prefix, 'build', '--network=none', '--pull=false',
                 '--platform', profile['platform'], '--iidfile', str(work / 'cache/derived.id'),
                 str(context)], 'build', 'derive-image', profile['limits']['run_seconds'])
        identity = (work / 'cache/derived.id').read_text().strip()
        require(re.fullmatch('sha256:[a-f0-9]{64}', identity))
        updated = json.loads(docker.call('image', 'inspect', identity))[0]
        added = verify_image_delta(original, updated)
        atomic_json(work / 'derived-image-inspection.json', updated)
        receipt = dict(schema='alopex.derived-test-image/v1', original_image=original_record,
            original_archive_sha256=config['image_archive_sha256'], producer_run=config['producer_run'],
            producer_controller=config['producer_controller'], producer_artifact_digest=config['artifact_digest'],
            source_before=config['source_before'], source_after=config['source_after'],
            changed_files={name:config['source_after'][name] for name in CHANGES},
            added_layers=added, execution_config_unchanged=True, dependencies_unchanged=True)
        record = dict(id=identity, lock_digest=original_record['lock_digest'],
                      platform=profile['platform'], **source, derivation=receipt)
        atomic_json(work / 'image-derivation.json', receipt)
        atomic_json(work / 'cache/image.json', record)
        verified_image(profile, docker)
        return {name:value for name,value in config['source_after'].items() if name.endswith('.py')}
    finally:
        shutil.rmtree(context, ignore_errors=True)


# This executes inside the already isolated probe container, before its sudo/I/O checks.
PROBE_VALIDATION = r'''
import hashlib, runpy, stat
expected = request["image_source_files"]
tool = pathlib.Path("/tool")
actual = {path.relative_to(tool).as_posix(): path for path in
          [*tool.glob("*.py"), *(tool / "tests").glob("*.py")]}
assert set(actual) == set(expected)
for name, path in actual.items():
    info = path.lstat()
    assert stat.S_ISREG(info.st_mode) and info.st_uid == info.st_gid == 0
    assert stat.S_IMODE(info.st_mode) == 0o644
    assert hashlib.sha256(path.read_bytes()).hexdigest() == expected[name]
for path in (tool, tool / "tests"):
    info = path.lstat()
    assert stat.S_ISDIR(info.st_mode) and info.st_uid == info.st_gid == 0
    assert stat.S_IMODE(info.st_mode) == 0o755
manifest = json.loads((tool / "image-manifest.json").read_text())
assert hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest() == request["image_manifest_digest"]
runpy.run_path(str(tool / "image_verify.py"))
'''
