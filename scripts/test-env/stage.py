"""Initialize private named volumes before the non-root runtime starts."""
import grp
import os
from pathlib import Path
import pwd

from common import read_json, require


def main():
    require(os.getuid() == 0, "POLICY")
    uid, gid = pwd.getpwnam("alopex").pw_uid, grp.getgrnam("alopex").gr_gid
    root = Path("/input")
    manifest = read_json(root / "assets.json")
    executable = {item["path"] for item in manifest["files"] if item["role"] == "server"}
    for path in [root, *root.rglob("*")]:
        require(not path.is_symlink(), "ARCHIVE")
        os.chown(path, 0, gid)
        path.chmod(0o750 if path.is_dir() else 0o550 if str(path.relative_to(root)) in executable else 0o640)
    work = Path("/work")
    os.chown(work, uid, gid)
    work.chmod(0o700)


if __name__ == "__main__":
    main()
