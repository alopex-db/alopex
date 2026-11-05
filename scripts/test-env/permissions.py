"""Private generated trees: configure extracted inputs, audit runtime outputs."""
import os
from pathlib import Path
import stat

from common import Failure, require


def private_tree(root, *, configure=False, external_targets=()):
    require(not Path(root).is_symlink() and Path(root).is_dir(), "POLICY")
    root = Path(root).resolve(strict=True)
    allowed = {Path(path).resolve(strict=True) for path in external_targets}
    rows = []
    def check(condition, path, detail):
        if not condition:
            raise Failure("POLICY", str(path.relative_to(root)) + ": " + detail)
    for path in [root, *sorted(root.rglob("*"))]:
        info = path.lstat()
        check(info.st_uid == os.getuid() and info.st_gid == os.getgid(), path, "owner/group mismatch")
        row = {"path": str(path.relative_to(root)), "uid": info.st_uid, "gid": info.st_gid}
        if stat.S_ISLNK(info.st_mode):
            target = path.resolve(strict=True)
            check(target.is_relative_to(root) or
                  (path.parent == root / "bin" and target in allowed), path, "undeclared link target")
            row.update(kind="symlink", target=os.readlink(path))
        else:
            check(stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode) or
                  stat.S_ISSOCK(info.st_mode), path, "unsupported file type")
            mode = 0o700 if (stat.S_ISDIR(info.st_mode) or stat.S_ISSOCK(info.st_mode) or
                             info.st_mode & 0o111) else 0o600
            if configure:
                check(not stat.S_ISSOCK(info.st_mode), path, "runtime socket cannot be normalized")
                path.chmod(mode)
            actual = stat.S_IMODE(path.lstat().st_mode)
            check(actual == mode, path, f"mode {oct(actual)} differs from {oct(mode)}")
            row.update(kind="directory" if path.is_dir() else "socket" if stat.S_ISSOCK(info.st_mode) else "file",
                       mode=oct(mode))
        rows.append(row)
    return rows
