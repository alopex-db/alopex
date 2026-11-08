"""Verify pinned SQLite native wheel and installed package files."""
import hashlib,importlib.metadata,importlib.util,json,sys,zipfile
from pathlib import Path,PurePosixPath
from dataframe_phase_worker import sha

def reference_identity(path, expected):
    assert sha(path) == expected
    record = json.loads(path.read_bytes())
    assert record['schema'] == 'alopex.reference-wheels/v1' and record['engine'] == 'sqlite'
    assert {entry['name'] for entry in record['wheels']} == {'pysqlite3-binary'}
    assert len(record['wheels']) == 1
    prefix = Path(sys.prefix).resolve()
    packages = {}
    for entry in record['wheels']:
        assert entry['version'] == '0.5.4'
        wheel_path = Path(entry['path'])
        assert sha(wheel_path) == entry['sha256']
        distribution = importlib.metadata.distribution(entry['name'])
        assert distribution.version == entry['version']
        with zipfile.ZipFile(wheel_path) as archive:
            infos = archive.infolist()
            assert len(infos) == len({i.filename for i in infos})
            assert sum(i.file_size for i in infos) <= 256_000_000
            for info in infos:
                name = PurePosixPath(info.filename)
                assert not name.is_absolute() and '..' not in name.parts
                if info.is_dir() or name.parts[0].endswith('.dist-info'):
                    continue
                assert not name.parts[0].endswith('.data')
                installed = Path(distribution.locate_file(info.filename)).resolve()
                assert installed.is_relative_to(prefix) and installed.is_file()
                assert sha(installed) == hashlib.sha256(archive.read(info)).hexdigest()
                package_root = Path(distribution.locate_file(name.parts[0])).resolve()
                packages.setdefault(package_root,set()).add(installed.relative_to(package_root).as_posix())
    for root, names in packages.items():
        actual = {p.relative_to(root).as_posix() for p in root.rglob('*')
            if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc'}
        assert actual == names
    module = importlib.util.find_spec('pysqlite3')
    assert module is not None and module.origin
    assert Path(module.origin).resolve() == Path(importlib.metadata.distribution('pysqlite3-binary').locate_file('pysqlite3/__init__.py')).resolve()
    return record
