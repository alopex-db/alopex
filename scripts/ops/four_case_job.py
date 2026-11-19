"""Private workflow handoff for the #712 four-case same-job diagnostic.

Inputs are two independently reviewed SHA-256/base64 pairs. No final pins are
embedded here, and no phase dispatches or claims performance qualification.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tarfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
from four_case_diagnostic import controller, entrypoint, stage_pair  # noqa: E402

LOCK_DIR = "alopex-712-lock"
MATRIX_DIR = "alopex-712-matrix"
CERT = REPO / ".github/ops/native-evidence-cert.pem"
CERT_SHA256 = "d33a612dab2f76986e0dc6ba0ba45be15265ceccd2d56b07c14f9f696259f6b2"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MAX_FILES_PER_PAIR = 64
MAX_RECEIPT_FILES = 16
MAX_PRIVATE_FILES = 12 * MAX_FILES_PER_PAIR + MAX_RECEIPT_FILES
MAX_PRIVATE_BYTES = 200_000_000


def require(value, code):
    if not value:
        raise ValueError(code)


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _input(raw, expected, limit):
    require(type(raw) is str and type(expected) is str and
            HEX64.fullmatch(expected) and 0 < len(raw) <= 4 * limit // 3 + 4,
            "MISSING_PIN")
    try:
        data = base64.b64decode(raw, validate=True)
    except (ValueError, base64.binascii.Error) as error:
        raise ValueError("MISSING_PIN") from error
    require(0 < len(data) <= limit and
            base64.b64encode(data).decode() == raw and _digest(data) == expected,
            "MISSING_PIN")
    return data


def _runner_temp(environment):
    raw = environment.get("RUNNER_TEMP")
    require(type(raw) is str and Path(raw).is_absolute(), "PREREQUISITE")
    path = Path(raw)
    require(path.is_dir() and not path.is_symlink() and
            path.resolve(strict=True) == path, "PREREQUISITE")
    return path


def _write_private(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def install_inputs(environment):
    """Validate both independent pins before installing a fixed private lock."""
    root = _runner_temp(environment)
    names = ("ALOPEX_FOUR_CASE_LOCK_SHA256", "ALOPEX_FOUR_CASE_LOCK_B64",
             "ALOPEX_FOUR_CASE_MANIFEST_SHA256", "ALOPEX_FOUR_CASE_MANIFEST_B64")
    require(all(type(environment.get(name)) is str for name in names) and
            sum(len(environment[name]) for name in names) <= 65_535,
            "MISSING_PIN")
    lock_sha = environment.get("ALOPEX_FOUR_CASE_LOCK_SHA256")
    manifest_sha = environment.get("ALOPEX_FOUR_CASE_MANIFEST_SHA256")
    lock_raw = _input(environment.get("ALOPEX_FOUR_CASE_LOCK_B64"), lock_sha, 16_384)
    manifest_raw = _input(environment.get("ALOPEX_FOUR_CASE_MANIFEST_B64"),
                          manifest_sha, 65_536)
    directory = root / LOCK_DIR
    directory.mkdir(mode=0o700)
    require(stat.S_IMODE(directory.stat().st_mode) == 0o700 and
            directory.stat().st_uid == os.geteuid(), "PRIVATE_MANIFEST")
    lock_path = directory / "lock.json"
    manifest_path = directory / "manifest.json"
    _write_private(lock_path, lock_raw)
    _write_private(manifest_path, manifest_raw)
    lock = entrypoint.load_reviewed_lock(lock_path, lock_sha)
    manifest = controller.load_manifest(manifest_path, manifest_sha)
    require(manifest["lock_sha256"] == lock_sha and
            all(manifest[key] == lock["pin"][key]
                for key in ("source_commit", "source_tree", "assets_digest")),
            "MISSING_PIN")
    return manifest


def run_job(environment):
    """Attest the current #804 run and stage the reviewed 12-pair matrix."""
    root = _runner_temp(environment)
    manifest_path = root / LOCK_DIR / "manifest.json"
    manifest = controller.load_manifest(
        manifest_path, environment.get("ALOPEX_FOUR_CASE_MANIFEST_SHA256"))
    lock = entrypoint.load_reviewed_lock(root / LOCK_DIR / "lock.json",
                                         environment.get("ALOPEX_FOUR_CASE_LOCK_SHA256"))
    require(manifest["lock_sha256"] == environment["ALOPEX_FOUR_CASE_LOCK_SHA256"],
            "MISSING_PIN")
    operations = entrypoint.controller_operations(REPO, lock)
    from four_case_diagnostic import docs_stage
    launch = stage_pair.make_stage_pair(manifest, REPO, root, operations)
    docs_ready = False

    def staged(config, output, proof, receipt):
        nonlocal docs_ready
        if not docs_ready:
            docs_stage.stage_docs(root / "alopex-804-candidate", root,
                                  pin=lock["pin"], docs_files=manifest["docs_files"])
            docs_ready = True
        return launch(config, output, proof, receipt)

    return controller.run_current_job(
        manifest_path, environment["ALOPEX_FOUR_CASE_MANIFEST_SHA256"],
        repo=REPO, runner_temp=root, environment=environment,
        output=root / MATRIX_DIR, operations=operations, stage_pair=staged)


def _capacity_before_seal(root, additional_bytes):
    sys.path.insert(0, str(REPO / "scripts/test-env"))
    from common import read_json
    from capacity import Budget
    profile = read_json(root / "alopex-804-qualification/profile.json", private=True)
    storage = profile["storage"]
    used = Budget(storage).used()
    require(used + additional_bytes + storage["reserve_bytes"] <
            storage["stop_bytes"] == 40_000_000_000, "CAPACITY")
    for path in (root, REPO):
        vfs = os.statvfs(path)
        require(vfs.f_bavail * vfs.f_frsize > additional_bytes,
                "CAPACITY")
    return storage


def _matrix_outcome(private, environment):
    """Reduce the controller receipt to fixed public status for this attempt."""
    fallback = {"matrix_status": "failed", "matrix_error_code": "INCOMPLETE"}
    path = private / "receipt.public.json"
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and
                    stat.S_IMODE(info.st_mode) == 0o600 and
                    0 < info.st_size <= 4096, "RECEIPT_IDENTITY")
            raw = os.read(fd, 4097)
            require(len(raw) == info.st_size, "RECEIPT_IDENTITY")
        finally:
            os.close(fd)
        receipt = json.loads(raw,
                             object_pairs_hook=entrypoint.acquire.acquisition._unique_pairs)
        require(type(receipt) is dict and
                receipt.get("schema") == controller.SCHEMA and
                receipt.get("performance_qualification") is False and
                receipt.get("run_attempt") == environment.get("GITHUB_RUN_ATTEMPT"),
                "RECEIPT_IDENTITY")
        if receipt.get("status") == "passed":
            require(receipt.get("job_id") == environment.get("GITHUB_RUN_ID") and
                    type(receipt.get("qualification_binding_sha256")) is str and
                    HEX64.fullmatch(receipt["qualification_binding_sha256"]),
                    "RECEIPT_IDENTITY")
            controller.verify_public_receipt(
                private, job_id=environment["GITHUB_RUN_ID"],
                run_attempt=environment["GITHUB_RUN_ATTEMPT"],
                binding_sha256=receipt["qualification_binding_sha256"])
            return {"matrix_status": "passed", "matrix_error_code": None}
        allowed = {"MISSING_PIN", "PRIVATE_MANIFEST", "SAME_JOB", "QUALIFICATION",
                   "QUALIFICATION_BINDING", "POLICY", "CLEANUP", "IDENTITY",
                   "OPERATOR_IDENTITY", "OUTPUT_STATE", "PAIR_STAGE", "PAIR_FAILED"}
        if receipt.get("status") == "failed" and receipt.get("error_code") in allowed:
            return {"matrix_status": "failed",
                    "matrix_error_code": receipt["error_code"]}
    except (OSError, ValueError, UnicodeError, KeyError, TypeError):
        pass
    return fallback


def seal_job(environment):
    """Encrypt bounded private output; export only CMS and a fixed public status."""
    root = _runner_temp(environment)
    private = root / MATRIX_DIR
    export = REPO / "native-sql-four-case-export"
    export.mkdir(mode=0o700)
    require(export.stat().st_uid == os.geteuid() and
            stat.S_IMODE(export.stat().st_mode) == 0o700 and
            private.is_dir() and not private.is_symlink() and
            private.stat().st_uid == os.geteuid() and
            stat.S_IMODE(private.stat().st_mode) == 0o700 and
            CERT.is_file() and _digest(CERT.read_bytes()) == CERT_SHA256,
            "EVIDENCE_BOUND")
    members = sorted(path for path in private.rglob("*") if path.is_file())
    require(members and len(members) <= MAX_PRIVATE_FILES and
            all(not path.is_symlink() and path.stat().st_uid == os.geteuid() and
                stat.S_IMODE(path.stat().st_mode) == 0o600
                for path in members) and
            sum(path.stat().st_size for path in members) <= MAX_PRIVATE_BYTES,
            "EVIDENCE_BOUND")
    for path in private.rglob("*"):
        require(not path.is_symlink(), "EVIDENCE_BOUND")
    archive = private / "evidence.tar.gz"
    pending = export / ".private-evidence.pending.cms"
    ciphertext = export / "private-evidence.cms"
    require(not archive.exists() and not pending.exists() and
            not ciphertext.exists(), "EVIDENCE_STATE")
    source_bytes = sum(path.stat().st_size for path in members)
    storage = _capacity_before_seal(root, 2 * source_bytes + 10_000_000)
    try:
        with tarfile.open(archive, "w:gz") as bundle:
            for path in members:
                bundle.add(path, arcname=str(path.relative_to(private)), recursive=False)
        require(0 < archive.stat().st_size <= MAX_PRIVATE_BYTES, "EVIDENCE_BOUND")
        require(_capacity_before_seal(root, archive.stat().st_size +
                                      source_bytes + 10_000_000) == storage,
                "CAPACITY")
        result = subprocess.run(
            ["openssl", "cms", "-encrypt", "-binary", "-aes256", "-outform",
             "DER", "-in", str(archive), "-out", str(pending), str(CERT)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, timeout=60, check=False)
        require(result.returncode == 0 and pending.is_file() and
                0 < pending.stat().st_size <= 205_000_000, "SEAL_FAILED")
        require(_capacity_before_seal(root, 10_000_000) == storage, "CAPACITY")
        pending.replace(ciphertext)
    finally:
        archive.unlink(missing_ok=True)
        pending.unlink(missing_ok=True)
    public = {"schema": "alopex.sql-four-case-export/v1", "status": "sealed",
              "performance_qualification": False,
              "ciphertext_sha256": _digest(ciphertext.read_bytes())}
    public.update(_matrix_outcome(private, environment))
    _write_private(export / "public.json",
                   (json.dumps(public, sort_keys=True) + "\n").encode())
    return public


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("install", "run", "seal"))
    args = parser.parse_args(argv)
    try:
        result = {"install": install_inputs, "run": run_job,
                  "seal": seal_job}[args.phase](dict(os.environ))
        print(json.dumps({"status": "passed" if args.phase != "seal" else "sealed",
                          "performance_qualification": False}, sort_keys=True))
        return 0
    except BaseException as error:
        code = error.args[0] if type(error) is ValueError and error.args else "INTERNAL"
        print(json.dumps({"status": "failed", "error_code": code if code in {
            "MISSING_PIN", "PRIVATE_MANIFEST", "PREREQUISITE", "EVIDENCE_BOUND",
            "EVIDENCE_STATE", "SEAL_FAILED"} else "INTERNAL",
                          "performance_qualification": False}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
