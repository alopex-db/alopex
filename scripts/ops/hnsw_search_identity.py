"""Match imported packages to locked wheels without depending on package RECORD."""
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path, PurePosixPath
import sys
import zipfile

from hnsw_search_contract import require


def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def verify_identity(path, expected):
    require(sha(path)==expected,'dependency identity document differs')
    record=json.loads(path.read_bytes())
    require(record['schema']=='alopex.hnsw-dependencies/v1','dependency schema differs')
    entries=record['wheels'];prefix=Path(sys.prefix).resolve();roots={}
    require(entries and len({e['name'] for e in entries})==len(entries),'duplicate dependency')
    for entry in entries:
        wheel=Path(entry['path']);require(sha(wheel)==entry['sha256'],'dependency wheel differs')
        distribution=importlib.metadata.distribution(entry['name'])
        require(distribution.version==entry['version'],'dependency version differs')
        with zipfile.ZipFile(wheel) as archive:
            infos=archive.infolist()
            require(len(infos)==len({i.filename for i in infos}) and
                sum(i.file_size for i in infos)<512_000_000,'dependency archive bounds exceeded')
            for info in infos:
                name=PurePosixPath(info.filename)
                require(not name.is_absolute() and '..' not in name.parts,'dependency path escapes')
                if info.is_dir() or name.parts[0].endswith('.dist-info'):continue
                # pip rewrites console scripts; they are neither imported nor executed.
                if name.parts[0].endswith('.data'):
                    require(len(name.parts)>2 and name.parts[1]=='scripts','unsupported wheel relocation')
                    continue
                installed=Path(distribution.locate_file(info.filename)).resolve()
                require(installed.is_relative_to(prefix) and installed.is_file()
                    and sha(installed)==hashlib.sha256(archive.read(info)).hexdigest(),'installed dependency differs')
                root=Path(distribution.locate_file(name.parts[0])).resolve()
                roots.setdefault(root,set()).add(installed.relative_to(root).as_posix())
    for root,names in roots.items():
        actual={'.'} if root.is_file() else {p.relative_to(root).as_posix() for p in root.rglob('*')
            if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc'}
        require(actual==names,'unexpected installed dependency file')
    for name,relative in record['modules'].items():
        module=importlib.util.find_spec(name)
        require(module is not None and module.origin
            and Path(module.origin).resolve()==(prefix/relative).resolve(),'imported dependency differs')
    return record


if __name__=='__main__':
    record=verify_identity(Path(sys.argv[1]),sys.argv[2])
    print(json.dumps(dict(status='verified',wheels=len(record['wheels']))))
