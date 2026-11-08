"""Pinned SQL streaming reference installation and installed-file identity."""
import json,subprocess,sys,urllib.request
from pathlib import Path
from common import atomic_json
from hosted_dataframe_comparison import require,sha

VERSIONS = {'datafusion':'50.0.0','pyarrow':'26.0.0','typing-extensions':'4.15.0'}

def install_reference(lock, work, budget, env):
    target=work/'venvs/datafusion'
    budget.run([sys.executable,'-B','-m','venv',str(target)],timeout=90,log=work/'sqlstream-install.log',env=env)
    python=target/'bin/python';lock_path=work/'reference-lock.json';atomic_json(lock_path,lock)
    selection=work/'selected-wheels.json'
    code="""import json,sys
from pip._vendor.packaging.tags import sys_tags
from pip._vendor.packaging.utils import parse_wheel_filename
tags=list(sys_tags()); records=json.load(open(sys.argv[1])); selected=[]
for name in ('datafusion','pyarrow','typing-extensions'):
 candidates=[]
 for item in records[name]:
  parsed=parse_wheel_filename(item['filename']); matches=[tags.index(t) for t in parsed[3] if t in tags]
  if matches:candidates.append((min(matches),item['filename'],item))
 assert candidates
 selected.append(sorted(candidates,key=lambda x:x[:2])[0][2])
with open(sys.argv[2],'w') as stream:json.dump(selected,stream)
"""
    budget.run([str(python),'-B','-c',code,str(lock_path),str(selection)],timeout=30,log=work/'sqlstream-select.log',env=env)
    selected=json.loads(selection.read_bytes())
    require(len(selected)==3 and {r['name'] for r in selected}==set(VERSIONS),'reference selection differs')
    wheels=work/'reference-wheels';wheels.mkdir()
    identity=dict(schema='alopex.reference-wheels/v1',engine='datafusion',wheels=[])
    for record in selected:
        require(record['name'] in VERSIONS and record['version']==VERSIONS[record['name']]
            and record['url'].startswith('https://files.pythonhosted.org/')
            and Path(record['filename']).name==record['filename'] and 0<record['size']<64_000_000,'reference lock invalid')
        destination=wheels/record['filename'];size=0
        with urllib.request.urlopen(record['url'],timeout=30) as response,destination.open('xb') as output:
            require(response.url==record['url'],'unexpected reference redirect')
            while block:=response.read(1024*1024):
                size+=len(block);require(size<=record['size'],'reference download oversized');output.write(block);budget.check()
        require(size==record['size'] and sha(destination)==record['sha256'],'reference wheel differs')
        identity['wheels'].append(dict(name=record['name'],version=record['version'],path=str(destination),sha256=record['sha256']))
    budget.run([str(python),'-B','-m','pip','install','--no-index','--no-deps','--no-cache-dir',
        *(entry['path'] for entry in identity['wheels'])],timeout=90,log=work/'sqlstream-install.log',env=env)
    budget.run([str(python),'-B','-m','pip','check'],timeout=30,log=work/'reference-check.log',env=env)
    path=work/'reference-identity.json';atomic_json(path,identity)
    return python,path
