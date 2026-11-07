"""Two independent three-row transfers from published wheels to a candidate."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import urllib.request


def run(root, work, temporary, candidate, budget, env):
    from common import atomic_json, private_dir, require
    def digest(path):
        with path.open('rb') as stream: return hashlib.file_digest(stream, 'sha256').hexdigest()
    def command(args, label):
        budget.run(list(map(str,args)), timeout=120, log=work/(label+'.log'), env=env)
    def environment(name):
        venv=temporary/name
        command([sys.executable,'-m','venv',venv],name+'-venv')
        python=venv/'bin/python'
        command([python,'-m','pip','install','--no-cache-dir','--only-binary=:all:',
                 'numpy==2.2.1','pyarrow==26.0.0'],name+'-dependencies')
        return python
    probe=work/'identity-probe.py'
    probe.write_text('''import importlib.metadata,importlib.util,json,pathlib,sys
import alopex,alopex._alopex as native
spec=importlib.util.spec_from_file_location('qualification',sys.argv[1]);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
assert importlib.metadata.version('alopex')==sys.argv[3]
identity=module.installed_identity(pathlib.Path(sys.argv[2]),pathlib.Path(alopex.__file__),pathlib.Path(native.__file__),pathlib.Path(sys.prefix))
pathlib.Path(sys.argv[4]).write_text(json.dumps({'version':sys.argv[3],'modules':identity})+'\\n')
''')
    def identity(python,wheel,version,name):
        output=work/(name+'.json')
        command([python,'-I',probe,root/'scripts/release/verify_python_parquet.py',wheel,version,output],name)
        return json.loads(output.read_text())
    current=environment('candidate')
    command([current,'-m','pip','install','--no-index','--no-deps',candidate],'candidate-install')
    candidate_identity=identity(current,candidate,'0.8.17','candidate-before')
    cases=[]
    script=root/'scripts/compatibility/vector_parquet_legacy.py'
    for version in ('0.8.15','0.8.16'):
        label='published-'+version
        previous=environment(label)
        wheel_dir=private_dir(work/(label+'-wheel'))
        command([previous,'-m','pip','download','--no-cache-dir','--only-binary=:all:','--no-deps',
                 '--dest',wheel_dir,'alopex=='+version],label+'-download')
        wheels=list(wheel_dir.glob('*.whl'))
        require(len(wheels)==1 and 0<wheels[0].stat().st_size<32_000_000,'IDENTITY')
        old_wheel=wheels[0]
        with urllib.request.urlopen('https://pypi.org/pypi/alopex/'+version+'/json',timeout=30) as response:
            raw=response.read(2_000_001)
        require(len(raw)<=2_000_000,'IDENTITY')
        metadata=json.loads(raw)
        selected=[item for item in metadata['urls'] if item['filename']==old_wheel.name]
        require(metadata['info']['version']==version and len(selected)==1
                and selected[0]['packagetype']=='bdist_wheel' and not selected[0]['yanked']
                and selected[0]['digests']['sha256']==digest(old_wheel)
                and selected[0]['size']==old_wheel.stat().st_size,'IDENTITY')
        atomic_json(work/(label+'-pypi.json'),dict(version=version,file=selected[0]))
        command([previous,'-m','pip','install','--no-index','--no-deps',old_wheel],label+'-install')
        old_identity=identity(previous,old_wheel,version,label+'-before')
        transfer=work/(label+'-transfer')
        command([previous,'-I',script,'export','--directory',transfer,'--expected-version',version],label+'-export')
        require(not (transfer/'old.db').exists(),'CLEANUP')
        command([current,'-I',script,'import','--directory',transfer,'--expected-version','0.8.17'],label+'-import')
        require(not (transfer/'new.db').exists(),'CLEANUP')
        require(identity(previous,old_wheel,version,label+'-after')==old_identity,'IDENTITY')
        exported=json.loads((transfer/'export.json').read_text())
        imported=json.loads((transfer/'import.json').read_text())
        require(exported['outcome']==imported['outcome']=='passed'
                and exported['rows']==imported['rows'] and len(imported['rows'])==3
                and exported['parquet_sha256']==imported['parquet_sha256']==digest(transfer/'vectors.parquet'), 'QUALIFICATION')
        cases.append(dict(published_version=version,status='passed',rows=3,
            published_wheel_sha256=digest(old_wheel),parquet_sha256=exported['parquet_sha256']))
    require(identity(current,candidate,'0.8.17','candidate-after')==candidate_identity,'IDENTITY')
    return dict(schema='alopex.vector-parquet-legacy-runtime/v1',status='passed',cases=cases,
                candidate_version='0.8.17',candidate_wheel_sha256=digest(candidate),
                workload_source_sha256=digest(script))
