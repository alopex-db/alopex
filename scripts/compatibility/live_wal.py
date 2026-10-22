"""Manual six-process live-WAL acceptance; published 0.8.16 -> source candidate.

Build the two core-only clients first (README.md). No load/performance loop,
parser override, wheel, or normal-CI job is needed. Preserve receipts before
removing the fixture/binaries. Python 3.11+ is required for lockfile provenance.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib

ROOT = Path(__file__).resolve().parent
MAX_BYTES = 65_536


def identity(path):
    with path.open("rb") as stream:
        return {"bytes": path.stat().st_size,
                "sha256": hashlib.file_digest(stream, "sha256").hexdigest()}


def files(directory):
    result = {}
    for path in sorted(directory.rglob("*")):
        assert not path.is_symlink(), f"unexpected symlink: {path}"
        if path.is_file() and path.name != ".alopex.lock":
            assert path.stat().st_size <= MAX_BYTES, "fixture file exceeded bound"
            result[str(path.relative_to(directory))] = identity(path)
    assert sum(item["bytes"] for item in result.values()) <= MAX_BYTES
    return result


def wal_version(directory):
    data = (directory / "lsm.wal").read_bytes()
    assert len(data) == 112, "expected section20 + segment28 + payload64"
    assert data[20:24] == b"AWAL"
    return int.from_bytes(data[24:26], "little")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    legacy, candidate = args.legacy.resolve(), args.candidate.resolve()
    assert legacy != candidate
    args.output.mkdir(parents=True, exist_ok=False)
    data = args.output / "store"  # Plain directory; no closed container substitute.
    lock = tomllib.loads((ROOT / "wal-v0816/Cargo.lock").read_text())
    old_core, = [p for p in lock["package"] if p["name"] == "alopex-core"]
    assert old_core["version"] == "0.8.16"
    assert old_core["source"] == "registry+https://github.com/rust-lang/crates.io-index"
    assert len(old_core["checksum"]) == 64
    report = {
        "contract": "live WAL recovery; no mid-publication kill/power-loss claim",
        "legacy_crate": old_core,
        "binaries": {str(p): identity(p) for p in (legacy, candidate)},
        "source": {str(p.relative_to(ROOT)): identity(p) for p in (
            ROOT / "live_wal.rs", ROOT / "live_wal.py",
            ROOT / "wal-v0816/Cargo.toml", ROOT / "wal-v0816/Cargo.lock",
            ROOT / "wal-current/Cargo.toml", ROOT / "wal-current/Cargo.lock")},
        "checkout_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "phases": [],
    }
    try:
        phases = [(legacy, "legacy-write"), (candidate, "current-read"),
                  (candidate, "upgrade"), (legacy, "legacy-reject"),
                  (candidate, "reopen"), (candidate, "reopen")]
        for index, (binary, phase) in enumerate(phases):
            before = files(data)
            run = subprocess.run([str(binary), phase, str(data.resolve())],
                                 capture_output=True, text=True, timeout=30)
            (args.output / f"{index}-{phase}.log").write_text(run.stdout + run.stderr)
            assert run.returncode == 0, f"{phase} failed; see phase log"
            receipt = json.loads(run.stdout)
            assert receipt["phase"] == phase and receipt["result"] == "pass"
            after = files(data)
            version = wal_version(data)
            assert version == (2 if index < 2 else 3)
            if phase in ("legacy-write", "current-read"):
                assert list(after) == ["lsm.wal"], "expected WAL only, no SST/checkpoint"
            if phase in ("current-read", "upgrade"):
                assert receipt["last_lsn"] == report["phases"][0]["last_lsn"]
            if phase in ("current-read", "legacy-reject"):
                assert before == after, f"{phase} changed storage bytes"
            if phase == "reopen":
                assert receipt["last_lsn"] > report["phases"][0]["last_lsn"]
                assert receipt["checkpoint_lsn"] is not None
                assert receipt["checkpoint_lsn"] < receipt["last_lsn"]
                if index == 5:
                    assert receipt["last_lsn"] == report["phases"][4]["last_lsn"]
                    assert after == report["phases"][4]["files_after"]
            receipt.update(files_before=before, files_after=after, wal_version=version)
            report["phases"].append(receipt)
        report["result"] = "pass"
    except BaseException as error:
        report.update(result="fail", error=repr(error))
        raise
    finally:
        (args.output / "receipt.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"result": report["result"], "processes": len(report["phases"]),
                      "receipt": str(args.output / "receipt.json")}))


if __name__ == "__main__":
    main()
