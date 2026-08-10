#!/usr/bin/env python3
"""Replace a freshly-built parser payload with the reviewed vendor bytes."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def archive_bytes(members: dict[str, bytes]) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w", format=tarfile.GNU_FORMAT) as tar:
            for name in sorted(members):
                info = tarfile.TarInfo(name=name)
                info.size = len(members[name])
                info.mode = 0o644
                info.mtime = 0
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                tar.addfile(info, io.BytesIO(members[name]))
    return raw.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    record = json.loads(args.record.read_text())
    library_data = args.library.read_bytes()
    record["library"] = {
        "path": record["library"]["path"],
        "sha256": digest(library_data),
        "size": len(library_data),
    }
    identity = {
        "alopex_version": record["alopex_version"],
        "builder": record["builder"],
        "contract_version": record["contract_version"],
        "library": record["library"],
        "packages": record["packages"],
        "packing": record["packing"],
        "parser_source": record["parser_source"],
        "registry_metadata": record["registry_metadata"],
        "schema": "alopex-parser-build-identity-v1",
        "target": record["target"],
    }
    identity_data = canonical(identity)
    record["build_identity"] = {
        "path": record["build_identity"]["path"],
        "sha256": digest(identity_data),
        "size": len(identity_data),
    }
    payload = archive_bytes({
        record["build_identity"]["path"]: identity_data,
        record["library"]["path"]: library_data,
    })
    record["archive"] = {
        "filename": record["archive"]["filename"],
        "sha256": digest(payload),
        "size": len(payload),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.record.write_bytes(canonical(record))
    (args.output_dir / record["archive"]["filename"]).write_bytes(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
