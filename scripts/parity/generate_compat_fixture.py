#!/usr/bin/env python3
"""Generate a content-addressed S2-c data-directory fixture with an old CLI.

The caller must build ``alopex`` from the immutable source tag named by the
arguments.  This script verifies the binary version, runs corpus 01-07, checks
99_verify.sql against the canonical expected result, and records every input
and output digest.  It never overwrites an existing fixture.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PARITY = ROOT / "scripts/parity"
sys.path.insert(0, str(PARITY))

from runner import normalize, surfaces  # noqa: E402


SCHEMA = "alopex-compat-fixture/v1"
SHA40 = re.compile(r"^[0-9a-f]{40}$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_records(paths: list[Path], *, relative_to: Path) -> list[dict[str, str]]:
    return [
        {
            "path": path.relative_to(relative_to).as_posix(),
            "sha256": sha256(path),
        }
        for path in sorted(paths)
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alopex-binary", required=True, type=Path)
    parser.add_argument("--source-version", required=True)
    parser.add_argument("--source-tag", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = args.alopex_binary.resolve()
    output = args.output.resolve()
    if not binary.is_file():
        raise SystemExit(f"alopex binary is not a regular file: {binary}")
    if not SHA40.fullmatch(args.source_sha):
        raise SystemExit("--source-sha must be a full lowercase 40-hex SHA")
    if args.source_tag != f"v{args.source_version}":
        raise SystemExit("--source-tag must equal v<source-version>")
    if output.exists():
        raise SystemExit(f"refusing to overwrite existing fixture: {output}")

    version = subprocess.run(
        [str(binary), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if version != f"alopex {args.source_version}":
        raise SystemExit(
            f"binary version mismatch: expected alopex {args.source_version}, got {version!r}"
        )

    corpus_dir = PARITY / "corpus"
    expected_source = PARITY / "expected/99_verify.json"
    corpus_files = surfaces.corpus_files(corpus_dir)
    corpus_statements = surfaces.load_statements(corpus_files)
    verify_statements = surfaces.load_statements([surfaces.verify_sql_path(corpus_dir)])
    expected_entries = normalize.load_statements_file(expected_source)

    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}-", dir=output.parent))
    try:
        data_dir = staging / "data"
        data_dir.mkdir()
        os.environ[surfaces.BINARY_SOURCE_ENV] = surfaces.BINARY_SOURCE_RELEASED
        cli = surfaces.CliSurface(binary, ROOT)
        cli.run_statements(corpus_statements, data_dir=data_dir)
        actual_entries = normalize.normalize_records(
            cli.run_statements(verify_statements, data_dir=data_dir)
        )
        if actual_entries != expected_entries:
            raise SystemExit(
                "generated fixture does not satisfy expected/99_verify.json:\n"
                + normalize.canonical_json(
                    {"expected": expected_entries, "actual": actual_entries}
                )
            )

        expected = staging / "expected.json"
        shutil.copyfile(expected_source, expected)
        data_files = [path for path in data_dir.rglob("*") if path.is_file()]
        data_records = file_records(data_files, relative_to=data_dir)
        archive = staging / "data.tar.gz"
        subprocess.run(
            [
                "tar",
                "--sparse",
                "--sort=name",
                "--mtime=@0",
                "--owner=0",
                "--group=0",
                "--numeric-owner",
                "-czf",
                str(archive),
                "-C",
                str(staging),
                "data",
            ],
            check=True,
        )
        provenance = {
            "schema": SCHEMA,
            "source": {
                "version": args.source_version,
                "tag": args.source_tag,
                "peeled_sha": args.source_sha,
                "binary_sha256": sha256(binary),
            },
            "generator": "scripts/parity/generate_compat_fixture.py",
            "corpus": file_records(corpus_files, relative_to=ROOT),
            "expected_sha256": sha256(expected),
            "archive": {"path": archive.name, "sha256": sha256(archive)},
            "data": data_records,
        }
        (staging / "provenance.json").write_text(
            json.dumps(provenance, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        shutil.rmtree(data_dir)
        staging.rename(output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    print(f"generated {output} from {args.source_tag} ({args.source_sha})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
