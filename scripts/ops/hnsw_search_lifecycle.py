"""Parent-owned cell scratch directories, removed after supervised exit."""
import re
import shutil


def cell_temp(work, cell_id):
    if not re.fullmatch(r'trial[0-2]-(published|before|candidate|hnswlib|faiss-hnsw)', cell_id):
        raise RuntimeError('invalid cell scratch identity')
    parent = work / 'tmp'
    path = parent / cell_id
    if parent.is_symlink() or not parent.is_dir() or path.is_symlink():
        raise RuntimeError('invalid cell scratch directory')
    return path


def prepare_cell_temp(work, cell_id):
    path = cell_temp(work, cell_id)
    path.mkdir(mode=0o700)
    return path


def cleanup_cell_temp(work, cell_id, supervision):
    if (supervision.get('reaped') is not True
            or type(supervision.get('returncode')) is not int
            or supervision['returncode'] != 0
            or 'failure' not in supervision or supervision['failure'] is not None):
        raise RuntimeError('cell scratch cleanup requires successful wait')
    path = cell_temp(work, cell_id)
    shutil.rmtree(path)
    if path.exists():
        raise RuntimeError('cell scratch cleanup incomplete')
    return dict(cell=cell_id, removed_after_wait=True)
