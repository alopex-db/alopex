"""One tiny public overflow compatibility phase per isolated Python process.

Run legacy-write, upgrade, legacy-reject, reopen in order. The first/third
phases require a published wheel; the other phases require the checkout's
native module hash. See README.md for resource bounds and the exact scope.
"""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path

import alopex
import alopex._alopex as native


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def files(db):
    paths = [db]
    sidecar = Path(str(db) + ".d")
    if sidecar.exists():
        paths.extend(sorted(sidecar.rglob("*")))
    result = {}
    for path in paths:
        if path.is_file() and path.name != ".alopex.lock":
            assert path.stat().st_size <= 1_048_576, "tiny fixture unexpectedly grew"
            result[str(path.relative_to(db.parent))] = {
                "bytes": path.stat().st_size, "sha256": digest(path)}
    return result


def options(db):
    return alopex.WalOverflowOptions(
        payload_bytes=256, spool_bytes=512, directory_bytes=4096,
        replay_bytes=4096, key_bytes=32, value_bytes=65, fragment_bytes=32,
        wal_segment_bytes=92, wal_segments=1, inline_bytes=128,
        activation_bytes=4096, container_fence_bytes=max(4096, db.stat().st_size))


def read(db, upgraded):
    with db.begin(alopex.TxnMode.READ_ONLY) as tx:
        assert tx.get(b"old") == b"ok"
        assert tx.get(b"new") == (b"x" * 65 if upgraded else None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["legacy-write", "upgrade", "legacy-reject", "reopen"])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--legacy-version", default="0.8.16")
    parser.add_argument("--candidate-native-sha256")
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    native_path = Path(native.__file__).resolve()
    native_sha = digest(native_path)
    legacy = args.phase.startswith("legacy-")
    if legacy:
        assert importlib.metadata.version("alopex") == args.legacy_version
    else:
        assert args.candidate_native_sha256 == native_sha, "candidate binary mismatch"
    result = {"phase": args.phase, "native": str(native_path), "native_sha256": native_sha,
              "legacy_version": args.legacy_version if legacy else None}
    path = args.db.resolve()
    if args.phase == "legacy-write":
        assert not path.exists() and not Path(str(path) + ".d").exists()
        db = alopex.Database.open(str(path))
        tx = db.begin(alopex.TxnMode.READ_WRITE)
        tx.put(b"old", b"ok")
        tx.commit()
        read(db, False)
        db.close()
    elif args.phase == "upgrade":
        receipt = json.loads((args.evidence / "legacy-write.json").read_text())
        assert files(path) == receipt["files"], "old bytes changed before candidate open"
        db = alopex.Database.open(str(path), wal_overflow=options(path))
        read(db, False)
        tx = db.begin(alopex.TxnMode.READ_WRITE)
        tx.put(b"new", b"x" * 65)
        tx.commit()
        read(db, True)
        db.close()
    elif args.phase == "legacy-reject":
        before = files(path)
        try:
            db = alopex.Database.open(str(path))
        except alopex.AlopexError as error:
            # Require an explicit incompatible-version failure, not an arbitrary error.
            assert "incompatible version:" in str(error).lower(), str(error)
            result["error"] = str(error)
            result["code"] = getattr(error, "code", None)
        else:
            db.close()
            raise AssertionError("legacy reader accepted the fenced format")
        assert files(path) == before, "legacy refusal changed database bytes"
    else:
        db = alopex.Database.open(str(path), wal_overflow=options(path))
        read(db, True)
        db.close()
    result["files"] = files(path)
    result["result"] = "pass"
    (args.evidence / (args.phase + ".json")).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
