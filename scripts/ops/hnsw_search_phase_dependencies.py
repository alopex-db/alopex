"""Install only locked binary dependencies needed for fixed diagnostic inputs."""
import json
import sys

from common import atomic_json
from hnsw_search_dependencies import PREPARE, download
from hnsw_search_phase_venv import create_venv


SELECTOR_CODE = '''import json,sys
from pip._vendor.packaging.tags import sys_tags
from pip._vendor.packaging.utils import parse_wheel_filename
tags=list(sys_tags());records=json.load(open(sys.argv[1]));selected={}
for name,record in records.items():
 candidates=[]
 for entry in record['files']:
  if entry['kind']!='bdist_wheel':continue
  matches=[tags.index(t) for t in parse_wheel_filename(entry['filename'])[3] if t in tags]
  if matches:candidates.append((min(matches),entry['filename'],entry))
 assert candidates,'A locked binary dependency is unavailable'
 selected[name]=sorted(candidates,key=lambda x:x[:2])[0][2]
json.dump(selected,open(sys.argv[2],'w'))
'''

def install(lock, work, budget, env, root):
    selected_lock = {name: lock[name] for name in PREPARE}
    wheels = work / 'reference-wheels'
    wheels.mkdir()
    selector = work / 'venvs/dependency-selector'
    create_venv(selector, work, budget, env, work / 'dependencies.log')
    lockpath, selected = work / 'dependency-lock.json', work / 'dependency-selection.json'
    atomic_json(lockpath, selected_lock)
    budget.run([str(selector / 'bin/python'), '-B', '-c', SELECTOR_CODE, str(lockpath), str(selected)],
               timeout=30, log=work / 'dependencies.log', env=env)
    records = json.loads(selected.read_bytes())
    if set(records) != set(PREPARE) or any(record['kind'] != 'bdist_wheel' for record in records.values()):
        raise ValueError('Diagnostic binary dependency selection differs')
    return {name: download(record, wheels, budget) for name, record in records.items()}, records
