"""Offline venv bootstrap without pip's transient bytecode compilation."""
import ensurepip
import hashlib
from importlib import resources
import json
import os
from pathlib import Path
import sys

from common import atomic_json


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def bundled_wheels(work):
    """Retain the interpreter's own ensurepip payload until controller cleanup."""
    directory = work / 'bootstrap-wheels'
    directory.mkdir(exist_ok=True)
    records = []
    total = 0
    for name, package in sorted(ensurepip._get_packages().items()):
        if package.wheel_name:
            source = resources.files('ensurepip') / '_bundled' / package.wheel_name
        else:
            source = Path(package.wheel_path)
        raw = source.read_bytes()
        total += len(raw)
        if not 0 < total <= 16_000_000:
            raise ValueError('Bundled installer exceeds bound')
        target = directory / source.name
        if target.exists():
            if target.read_bytes() != raw:
                raise ValueError('Bundled installer changed')
        else:
            with target.open('xb') as output:
                output.write(raw)
        records.append(dict(name=name, version=package.version,
                            filename=target.name, sha256=digest(raw), size=len(raw)))
    if sum(row['name'] == 'pip' for row in records) != 1:
        raise ValueError('Bundled pip missing or duplicated')
    return directory, records


BOOTSTRAP = '''import runpy,sys
sys.path.insert(0,sys.argv[1])
sys.argv=['pip','install','--no-index','--no-deps','--no-cache-dir','--no-compile',*sys.argv[2:]]
runpy.run_module('pip',run_name='__main__',alter_sys=True)
'''

VERIFY = '''import importlib.metadata,json,pathlib,pip,sys
root=pathlib.Path(sys.prefix).resolve()
assert root!=pathlib.Path(sys.base_prefix).resolve()
assert pathlib.Path(pip.__file__).resolve().is_relative_to(root)
expected=json.loads(sys.argv[1]);installed={row['name']:importlib.metadata.version(row['name']) for row in expected}
assert installed=={row['name']:row['version'] for row in expected}
assert not list(root.rglob('*.pyc'))
pathlib.Path(sys.argv[2]).write_text(json.dumps(dict(installed=installed,no_bytecode=True,isolated_prefix=True)))
'''


def create_venv(target, work, budget, env, log):
    if target.exists() or target.parent != work / 'venvs':
        raise ValueError('Venv target must be fresh and owned')
    budget.check()
    directory, records = bundled_wheels(work)
    budget.check()
    # Ignore operator pip configuration exactly as ensurepip does. Explicit
    # --no-compile is required: Python's -B does not disable pip's compileall.
    isolated = {k: v for k, v in env.items()
                if not k.startswith('PIP_') and k not in ('PYTHONPATH', 'PYTHONHOME', 'ENSUREPIP_OPTIONS')}
    isolated.update(PIP_CONFIG_FILE=os.devnull, PYTHONDONTWRITEBYTECODE='1')
    budget.run([sys.executable, '-I', '-B', '-m', 'venv', '--without-pip', str(target)],
               timeout=90, log=log, env=isolated)
    python = target / 'bin/python'
    pip_wheel = directory / next(row['filename'] for row in records if row['name'] == 'pip')
    budget.run([str(python), '-I', '-B', '-c', BOOTSTRAP, str(pip_wheel),
                *(str(directory / row['filename']) for row in records)], timeout=90, log=log, env=isolated)
    receipt = work / 'identities' / ('venv-' + target.name + '.json')
    budget.run([str(python), '-I', '-B', '-c', VERIFY, json.dumps(records), str(receipt)],
               timeout=30, log=log, env=isolated)
    installed = json.loads(receipt.read_bytes())
    atomic_json(receipt, dict(schema='alopex.diagnostic-venv/v1', **installed,
        bundled=records, ensurepip_source_sha256=digest(Path(ensurepip.__file__).read_bytes()),
        helper_sha256=digest(Path(__file__).read_bytes()), bootstrap='bundled wheels, offline, no bytecode'))
    budget.check()
    return python
