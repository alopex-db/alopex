"""Private, bounded Alopex allocation inventory for one selected hosted job."""

from __future__ import annotations

import os
import hashlib
import re
from pathlib import Path
import stat
import time
from dataclasses import dataclass

STOP_BYTES = 40_000_000_000
CEILING_BYTES = 50_000_000_000
MAX_ENTRIES = 200_000
MAX_SECONDS = 90
MAX_MOUNT_BYTES = 4_000_000
MAX_MOUNTS = 4096
SCRATCH_NAME = "alopex-nim-parser-v0.8.17-contract-0.28.0-x86_64-unknown-linux-gnu"
TOOLCHAIN_SYSTEM_PREFIXES = (
    Path("/usr/share/rust"),
    Path("/usr/local/bin"),
    Path("/usr/bin"),
)


def admitted_cargo_bin_symlink(
    root: Path,
    path: Path,
    info: os.stat_result,
    home: Path,
    pools: dict[int, int],
) -> tuple[Path, os.stat_result] | None:
    """Admit only direct regular-file toolchain symlinks inside ~/.cargo/bin."""
    cargo_root = home / ".cargo"
    cargo_bin = cargo_root / "bin"
    if root != cargo_root or path.parent != cargo_bin:
        return None
    if info.st_uid not in (0, os.geteuid()) or info.st_dev not in pools or info.st_blocks < 0:
        return None
    try:
        bin_info = cargo_bin.lstat()
    except OSError:
        return None
    if not stat.S_ISDIR(bin_info.st_mode) or stat.S_ISLNK(bin_info.st_mode) or bin_info.st_uid != os.geteuid():
        return None
    try:
        target = path.resolve(strict=True)
        tinfo = target.lstat()
    except (OSError, RuntimeError):
        return None
    if (not stat.S_ISREG(tinfo.st_mode) or tinfo.st_blocks < 0 or
            tinfo.st_uid not in (0, os.geteuid()) or (tinfo.st_mode & 0o022)):
        return None
    user_prefixes = (cargo_root, home / ".rustup")
    in_user = any(target != prefix and target.is_relative_to(prefix)
                  for prefix in user_prefixes)
    in_system = any(target != prefix and target.is_relative_to(prefix)
                    for prefix in TOOLCHAIN_SYSTEM_PREFIXES)
    if in_user and tinfo.st_dev in pools:
        return target, tinfo
    if in_system and (tinfo.st_dev in pools or tinfo.st_uid == 0):
        return target, tinfo
    return None


def require(value):
    if not value:
        raise ValueError("STORAGE")


@dataclass(frozen=True)
class Manifest:
    workspace: Path
    temporary: Path
    home: Path
    task_parent: Path | None
    home_parent: Path | None
    roots: tuple[Path, ...]
    pools: tuple[Path, ...]
    scratch: Path
    anchors: tuple[tuple[Path, tuple[int, int, int, int]], ...] = ()
    mounts: tuple[tuple, ...] = ()


def _mount_text() -> str:
    with open("/proc/self/mountinfo", encoding="utf-8") as source:
        value = source.read(MAX_MOUNT_BYTES + 1)
    require(len(value.encode("utf-8")) <= MAX_MOUNT_BYTES)
    return value


def _mount_path(value: str) -> Path:
    require(type(value) is str)
    require(not re.search(r"\\(?![0-7]{3})", value))
    decoded = re.sub(r"\\([0-7]{3})",
                     lambda found: chr(int(found.group(1), 8)), value)
    require(decoded.startswith("/") and "\x00" not in decoded and
            (decoded == "/" or (not decoded.endswith("/") and
             all(part not in ("", ".", "..") for part in decoded.split("/")[1:]))))
    return Path(decoded)


def parse_mountinfo(value: str) -> tuple[tuple, ...]:
    require(type(value) is str and len(value.encode("utf-8")) <= MAX_MOUNT_BYTES)
    lines = value.splitlines()
    require(1 <= len(lines) <= MAX_MOUNTS)
    rows = []
    ids, points = set(), set()
    for line in lines:
        require(len(line) <= 8192 and line.count(" - ") == 1)
        before, after = line.split(" - ", 1)
        fields, tail = before.split(), after.split()
        require(len(fields) >= 6 and len(tail) >= 3 and
                fields[0].isdecimal() and fields[1].isdecimal() and
                re.fullmatch(r"[0-9]+:[0-9]+", fields[2]) is not None)
        mount_id, parent_id = int(fields[0]), int(fields[1])
        root, point = _mount_path(fields[3]), _mount_path(fields[4])
        require(mount_id > 0 and parent_id > 0 and mount_id not in ids)
        ids.add(mount_id)
        points.add(point)
        major, minor = (int(part) for part in fields[2].split(":"))
        rows.append((mount_id, parent_id, major, minor, str(root), str(point),
                     tuple(fields[5:]), tuple(tail)))
    require(Path("/") in points)
    return tuple(rows)


def mount_topology(anchors: tuple, roots: tuple[Path, ...],
                   value: str) -> tuple[tuple, ...]:
    rows = parse_mountinfo(value)
    grouped: dict[Path, list[tuple]] = {}
    for row in rows:
        grouped.setdefault(Path(row[5]), []).append(row)

    def visible(point: Path) -> tuple:
        stack = grouped[point]
        by_id = {row[0]: row for row in stack}
        bases = [row for row in stack if row[1] not in by_id or row[1] == row[0]]
        require(len(bases) == 1)
        ordered = [bases[0]]
        while len(ordered) < len(stack):
            children = [row for row in stack if row[1] == ordered[-1][0] and
                        row[0] != ordered[-1][0]]
            require(len(children) == 1 and children[0] not in ordered)
            ordered.append(children[0])
        return ordered[-1]

    admitted = {path for path, _ in anchors} | set(roots)
    relevant = set()
    for path, _ in anchors:
        covering = [point for point in grouped if path == point or
                    path.is_relative_to(point)]
        require(bool(covering))
        point = max(covering, key=lambda item: len(item.parts))
        selected = visible(point)
        info = path.stat()
        require((selected[2], selected[3]) ==
                (os.major(info.st_dev), os.minor(info.st_dev)))
        relevant.update(grouped[point])
    for root in roots:
        if not root.exists() and not root.is_symlink():
            continue
        covering = [point for point in grouped if root == point or
                    root.is_relative_to(point)]
        require(bool(covering))
        point = max(covering, key=lambda item: len(item.parts))
        visible(point)
        relevant.update(grouped[point])
        for point, stack in grouped.items():
            if point != root and point.is_relative_to(root):
                require(point in admitted)
                visible(point)
                relevant.update(stack)
    return tuple(sorted(relevant))


def _check_mounts(manifest: Manifest) -> None:
    require(bool(manifest.mounts) and
            mount_topology(manifest.anchors, manifest.roots, _mount_text()) ==
            manifest.mounts)


def _directory(path: Path) -> None:
    require(path.is_absolute() and path.is_dir() and not path.is_symlink()
            and path.resolve(strict=True) == path
            and path.stat().st_uid == os.geteuid())


def _scratch_parent(path: Path) -> None:
    require(path.is_absolute() and path.is_dir() and not path.is_symlink()
            and path.resolve(strict=True) == path)
    info = path.lstat()
    mode = stat.S_IMODE(info.st_mode)
    require(stat.S_ISDIR(info.st_mode) and
            ((info.st_uid == 0 and mode == 0o1777) or
             (info.st_uid == os.geteuid() and mode == 0o700)))


def _root_identity(path: Path, *, scratch_parent: bool = False) -> tuple[int, int, int, int]:
    (_scratch_parent if scratch_parent else _directory)(path)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode))
    return info.st_dev, info.st_ino, info.st_uid, info.st_mode


def _check_anchors(manifest: Manifest) -> None:
    require(bool(manifest.anchors))
    for path, expected in manifest.anchors:
        require(_root_identity(path, scratch_parent=path == manifest.scratch.parent) == expected)


def root_seal(manifest: Manifest) -> str:
    """Private precheckout-to-postcheckout root identity, without path output."""
    _check_anchors(manifest)
    _check_mounts(manifest)
    values = (tuple((str(path), identity) for path, identity in manifest.anchors),
              manifest.mounts)
    return hashlib.sha256(repr(values).encode("utf-8")).hexdigest()


def make_manifest(workspace: Path, temporary: Path, *,
                  task_parent: Path | None = None,
                  home_parent: Path | None = None,
                  home: Path | None = None,
                  scratch: Path | None = None) -> Manifest:
    home = Path.home() if home is None else home
    scratch = Path("/tmp") / SCRATCH_NAME if scratch is None else scratch
    for path in (workspace, temporary, home, workspace.parent):
        _directory(path)
    _scratch_parent(scratch.parent)
    require(not any(path.exists() or path.is_symlink() for path in
                    (scratch, Path(str(scratch) + ".lock"))))
    require((task_parent is None) == (home_parent is None))
    if task_parent is not None:
        for path in (task_parent, home_parent):
            _directory(path)
            require(stat.S_IMODE(path.stat().st_mode) == 0o700)
        require(task_parent != home_parent and
                task_parent not in home_parent.parents and
                home_parent not in task_parent.parents and
                workspace not in task_parent.parents and
                workspace not in home_parent.parents)
    local = home / ".local"
    if local.exists() or local.is_symlink():
        _directory(local)
        installs = tuple(sorted(path for path in local.iterdir()
                                if path.name.startswith("alopex-")))
    else:
        installs = ()
    roots = (workspace.parent, temporary, home / ".cargo", home / ".cache",
             home / ".rustup", *installs)
    if task_parent is not None:
        roots += (task_parent, home_parent)
    roots += (scratch, Path(str(scratch) + ".lock"))
    pools = (workspace.parent, temporary, home, scratch.parent)
    if task_parent is not None:
        pools += (task_parent, home_parent)
    fixed = (workspace, workspace.parent, temporary, home, scratch.parent)
    if task_parent is not None:
        fixed += (task_parent, home_parent)
    if local.exists():
        fixed += (local,)
    fixed += tuple(path for path in (home / ".cargo", home / ".cache",
                                    home / ".rustup", *installs) if path.exists())
    anchors = tuple((path, _root_identity(path, scratch_parent=path == scratch.parent))
                    for path in dict.fromkeys(fixed))
    mounts = mount_topology(anchors, roots, _mount_text())
    return Manifest(workspace, temporary, home, task_parent, home_parent,
                    roots, pools, scratch, anchors, mounts)


def snapshot(manifest: Manifest) -> tuple[int, dict[int, int]]:
    require(type(manifest) is Manifest)
    _check_anchors(manifest)
    _check_mounts(manifest)
    deadline = time.monotonic() + MAX_SECONDS
    seen: set[tuple[int, int]] = set()
    total = entries = 0
    pools: dict[int, int] = {}
    for path in manifest.pools:
        (_scratch_parent if path == manifest.scratch.parent else _directory)(path)
        fs = os.statvfs(path)
        require(fs.f_frsize > 0 and 0 <= fs.f_bavail <= fs.f_bfree <= fs.f_blocks)
        device = path.stat().st_dev
        free = fs.f_bavail * fs.f_frsize
        pools[device] = min(pools.get(device, free), free)
    for root in manifest.roots:
        if not root.exists() and not root.is_symlink():
            require(root in (manifest.home / ".cargo", manifest.home / ".cache",
                             manifest.home / ".rustup", manifest.scratch,
                             Path(str(manifest.scratch) + ".lock")))
            continue
        pending = [(root, 0)]
        while pending:
            require(time.monotonic() <= deadline and entries < MAX_ENTRIES)
            path, depth = pending.pop()
            require(depth <= 64)
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                admitted = admitted_cargo_bin_symlink(
                    root, path, info, manifest.home, pools)
                require(admitted is not None)
                link_identity = (info.st_dev, info.st_ino)
                if link_identity not in seen:
                    seen.add(link_identity)
                    entries += 1
                    total += info.st_blocks * 512
                    require(total < CEILING_BYTES)
                _, tinfo = admitted
                if tinfo.st_dev in pools:
                    target_identity = (tinfo.st_dev, tinfo.st_ino)
                    if target_identity not in seen:
                        require(entries < MAX_ENTRIES)
                        seen.add(target_identity)
                        entries += 1
                        total += tinfo.st_blocks * 512
                        require(total < CEILING_BYTES)
                continue
            require(info.st_uid == os.geteuid() and
                    info.st_dev in pools and info.st_blocks >= 0)
            if path in (manifest.scratch, Path(str(manifest.scratch) + ".lock")):
                require(stat.S_ISDIR(info.st_mode) and
                        stat.S_IMODE(info.st_mode) == 0o700)
            identity = (info.st_dev, info.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            entries += 1
            require(stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
            total += info.st_blocks * 512
            require(total < CEILING_BYTES)
            if stat.S_ISDIR(info.st_mode):
                with os.scandir(path) as children:
                    pending.extend((Path(child.path), depth + 1)
                                   for child in children)
    _check_anchors(manifest)
    _check_mounts(manifest)
    return total, pools
