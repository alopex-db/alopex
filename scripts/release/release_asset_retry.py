#!/usr/bin/env python3
"""Attach release assets without replacing an existing asset's bytes."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import resource
import subprocess
import tempfile


def gh(*args, max_file_size=None):
    def limit_file_size():
        resource.setrlimit(resource.RLIMIT_FSIZE, (max_file_size, max_file_size))

    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True,
        preexec_fn=limit_file_size if max_file_size is not None else None,
        timeout=600,
    ).stdout


def assets(repo, tag):
    release = json.loads(gh("api", f"repos/{repo}/releases/tags/{tag}"))
    release_id = release["id"]
    result = {}
    for page in range(1, 101):
        batch = json.loads(gh("api", f"repos/{repo}/releases/{release_id}/assets?per_page=100&page={page}"))
        for asset in batch:
            name = asset["name"]
            if name in result:
                raise ValueError(f"duplicate release asset: {name}")
            result[name] = asset
        if len(batch) < 100:
            return result
    raise ValueError("release asset pagination exceeded 100 pages")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_asset(repo, tag, path, asset):
    size = path.stat().st_size
    if asset["size"] != size:
        raise ValueError(f"release asset differs in size: {path.name}")
    expected = sha256(path)
    digest = asset.get("digest")
    if digest is not None:
        if digest != f"sha256:{expected}":
            raise ValueError(f"release asset differs in digest: {path.name}")
        return
    # Older release assets may lack GitHub's digest field. The size match bounds
    # the download to the already staged distribution's size.
    with tempfile.TemporaryDirectory(prefix="primary-asset-") as directory:
        gh("release", "download", tag, "--repo", repo,
           "--pattern", path.name, "--dir", directory,
           max_file_size=size)
        downloaded = Path(directory) / path.name
        if not downloaded.is_file() or downloaded.stat().st_size != size or sha256(downloaded) != expected:
            raise ValueError(f"release asset differs in bytes: {path.name}")


def upload(repo, tag, directory):
    parts = repo.split("/")
    if (len(parts) != 2 or any(re.fullmatch(r"[A-Za-z0-9_.-]+", part) is None or part in (".", "..") for part in parts)
            or re.fullmatch(r"[A-Za-z0-9._-]+", tag) is None or tag in (".", "..")):
        raise ValueError("invalid release repository or tag")
    entries = list(directory.iterdir())
    if any(path.is_symlink() for path in entries):
        raise ValueError("release assets must not be symlinks")
    paths = sorted(path for path in entries if path.is_file())
    if not paths or any(path.stat().st_size == 0 for path in paths):
        raise ValueError("release assets must be nonempty regular files")
    remote = assets(repo, tag)
    # Finish the entire preflight before changing the release.
    for path in paths:
        if path.name in remote:
            verify_asset(repo, tag, path, remote[path.name])
    for path in paths:
        if path.name in remote:
            continue
        try:
            gh("release", "upload", tag, str(path), "--repo", repo)
        except subprocess.CalledProcessError:
            # A concurrent uploader may have won the name. Accept only its
            # identical bytes; otherwise preserve the original failure.
            current = assets(repo, tag).get(path.name)
            if current is None:
                raise
            verify_asset(repo, tag, path, current)
    final = assets(repo, tag)
    for path in paths:
        if path.name not in final:
            raise ValueError(f"release asset missing after upload: {path.name}")
        verify_asset(repo, tag, path, final[path.name])


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--repo", required=True)
    cli.add_argument("--tag", required=True)
    cli.add_argument("--directory", required=True, type=Path)
    args = cli.parse_args()
    upload(args.repo, args.tag, args.directory)
