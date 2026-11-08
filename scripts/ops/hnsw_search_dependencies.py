"""Bounded official downloads and one serial reference build; no product build."""
import json
from pathlib import Path
import subprocess
import sys
import urllib.request

from common import atomic_json
from hosted_dataframe_comparison import require, sha

PREPARE=('numpy','pandas','scikit-learn','scipy','joblib','threadpoolctl',
    'narwhals','cloudpickle','python-dateutil','six')
REFERENCE=('numpy','faiss-cpu','packaging','hnswlib')
BUILD=('numpy','setuptools','wheel','pybind11')
MODULES={'numpy':'numpy','pandas':'pandas','scikit-learn':'sklearn','scipy':'scipy',
    'joblib':'joblib','threadpoolctl':'threadpoolctl','narwhals':'narwhals',
    'cloudpickle':'cloudpickle','python-dateutil':'dateutil','six':'six',
    'faiss-cpu':'faiss','packaging':'packaging','hnswlib':'hnswlib'}


def download(record, directory, budget):
    require(record['url'].startswith('https://files.pythonhosted.org/')
        and Path(record['filename']).name==record['filename']
        and 0<record['size']<100_000_000,'dependency lock invalid')
    path=directory/record['filename'];total=0
    with urllib.request.urlopen(record['url'],timeout=30) as response,path.open('xb') as output:
        require(response.url==record['url'],'dependency redirected')
        while block:=response.read(1024*1024):
            total+=len(block);require(total<=record['size'],'dependency exceeds lock');output.write(block);budget.check()
    require(total==record['size'] and sha(path)==record['sha256'],'dependency download differs')
    return path


def install(lock, work, budget, env, root):
    wheels=work/'reference-wheels';wheels.mkdir()
    build=work/'venvs/reference-build'
    budget.run([sys.executable,'-B','-m','venv',str(build)],timeout=90,log=work/'dependencies.log',env=env)
    python=build/'bin/python';lockpath=work/'dependency-lock.json';atomic_json(lockpath,lock)
    selected=work/'dependency-selection.json'
    code="""import json,sys
from pip._vendor.packaging.tags import sys_tags
from pip._vendor.packaging.utils import parse_wheel_filename
tags=list(sys_tags());records=json.load(open(sys.argv[1]));selected={}
for name,record in records.items():
 candidates=[]
 for entry in record['files']:
  if entry['kind']=='sdist':
   assert name=='hnswlib';candidates.append((0,entry['filename'],entry));continue
  matches=[tags.index(t) for t in parse_wheel_filename(entry['filename'])[3] if t in tags]
  if matches:candidates.append((min(matches),entry['filename'],entry))
 assert candidates,name
 selected[name]=sorted(candidates,key=lambda x:x[:2])[0][2]
json.dump(selected,open(sys.argv[2],'w'))
"""
    budget.run([str(python),'-B','-c',code,str(lockpath),str(selected)],timeout=30,log=work/'dependencies.log',env=env)
    records=json.loads(selected.read_bytes());paths={name:download(r,wheels,budget) for name,r in records.items()}
    def pip(python,names):
        budget.run([str(python),'-B','-m','pip','install','--no-index','--no-deps','--no-cache-dir',
            *(str(paths[n]) for n in names)],timeout=120,log=work/'dependencies.log',env=env)
    pip(python,BUILD)
    destination=work/'reference-built';destination.mkdir()
    build_env=dict(env,CMAKE_BUILD_PARALLEL_LEVEL='1',MAX_JOBS='1')
    budget.before('prepare')
    budget.run([str(python),'-B','-m','pip','wheel','--no-index','--no-deps','--no-build-isolation',
        '--no-cache-dir','--wheel-dir',str(destination),str(paths['hnswlib'])],
        timeout=300,log=work/'reference-build.log',env=build_env)
    generated=list(destination.glob('hnswlib-0.8.0-*.whl'))
    require(len(generated)==1,'reference wheel output differs')
    original=paths['hnswlib'];paths['hnswlib']=generated[0]
    atomic_json(work/'reference-build-receipt.json',dict(source=records['hnswlib'],source_sha256=sha(original),
        wheel_sha256=sha(generated[0]),build_dependencies={n:records[n] for n in BUILD},
        build_parameters=dict(compiler_options='unmodified upstream defaults',parallel_jobs=1),
        source_modified=False,product_build=False))
    return paths,records


def install_set(name, python, names, paths, records, work, budget, env, root):
    budget.run([str(python),'-B','-m','pip','install','--no-index','--no-deps','--no-cache-dir',
        *(str(paths[n]) for n in names)],timeout=120,log=work/'dependencies.log',env=env)
    budget.run([str(python),'-B','-m','pip','check'],timeout=30,log=work/'dependencies.log',env=env)
    path=work/'identities'/f'{name}-dependencies.json'
    record=dict(schema='alopex.hnsw-dependencies/v1',wheels=[dict(name=n,version=records[n]['version'],
        path=str(paths[n]),sha256=sha(paths[n])) for n in names],modules={})
    atomic_json(path,record)
    code="""import importlib.util,json,pathlib,sys
p=pathlib.Path(sys.argv[1]);r=json.loads(p.read_bytes());prefix=pathlib.Path(sys.prefix).resolve()
for module in json.loads(sys.argv[2]):
 spec=importlib.util.find_spec(module);assert spec and spec.origin
 r['modules'][module]=pathlib.Path(spec.origin).resolve().relative_to(prefix).as_posix()
p.write_text(json.dumps(r))
"""
    budget.run([str(python),'-B','-c',code,str(path),json.dumps([MODULES[n] for n in names])],
        timeout=30,log=work/'dependencies.log',env=env)
    check_env=dict(env,PYTHONPATH=str(root/'scripts/ops'))
    budget.run([str(python),'-B',str(root/'scripts/ops/hnsw_search_identity.py'),str(path),sha(path)],
        timeout=90,log=work/'dependencies.log',env=check_env)
    return path
