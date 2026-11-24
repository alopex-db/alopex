"""Public workflow handoff for the #712 four-case same-job diagnostic.

Inputs are two independently reviewed SHA-256/base64 pairs. No final pins are
embedded here, and no phase dispatches or claims performance qualification.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
from four_case_diagnostic import controller, entrypoint, stage_pair  # noqa: E402

LOCK_DIR = "alopex-712-lock"
MATRIX_DIR = "alopex-712-matrix"
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
MAX_FILES_PER_PAIR = 64
MAX_RECEIPT_FILES = 16
MAX_PRIVATE_FILES = 12 * MAX_FILES_PER_PAIR + MAX_RECEIPT_FILES
MAX_PRIVATE_BYTES = 200_000_000
MAX_PUBLIC_BYTES = 3_000_000  # 24 bounded operation cells, 12 stage cells, timings and metadata.


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

    def complete(runs, reviewed_lock, reviewed_manifest, proof, receipt, job_id):
        from four_case_diagnostic import evidence_assembly
        _capacity_before_seal(root, MAX_PUBLIC_BYTES + 10_000_000)
        evidence_assembly.assemble_public_timing(
            runs, root / MATRIX_DIR / "public-timing")
        evidence_assembly.assemble_files(
            runs, lock=reviewed_lock, manifest=reviewed_manifest,
            proof=proof, receipt=receipt, job_id=job_id)

    return controller.run_current_job(
        manifest_path, environment["ALOPEX_FOUR_CASE_MANIFEST_SHA256"],
        repo=REPO, runner_temp=root, environment=environment,
        output=root / MATRIX_DIR, operations=operations, stage_pair=staged,
        on_complete=complete)


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
    fallback = {"matrix_status": "failed", "matrix_error_code": "INCOMPLETE",
                "completed_pairs": None}
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
            accepted = controller.verify_public_receipt(
                private, job_id=environment["GITHUB_RUN_ID"],
                run_attempt=environment["GITHUB_RUN_ATTEMPT"],
                binding_sha256=receipt["qualification_binding_sha256"])
            require(type(accepted.get("source_commit")) is str and
                    re.fullmatch(r"[0-9a-f]{40}", accepted["source_commit"]) and
                    type(accepted.get("assets_digest")) is str and
                    HEX64.fullmatch(accepted["assets_digest"]), "RECEIPT_IDENTITY")
            lock = entrypoint.load_reviewed_lock(
                private.parent / LOCK_DIR / "lock.json",
                environment["ALOPEX_FOUR_CASE_LOCK_SHA256"])
            pin = lock["pin"]
            require(accepted["source_commit"] == pin["source_commit"] and
                    accepted["assets_digest"] == pin["assets_digest"],
                    "RECEIPT_IDENTITY")
            return {"matrix_status": "passed", "matrix_error_code": None,
                    "completed_pairs": 12,
                    "source_commit": accepted["source_commit"],
                    "source_tree": pin["source_tree"],
                    "artifact_id": pin["artifact_id"],
                    "artifact_sha256": pin["artifact_sha256"],
                    "server_sha256": pin["server_sha256"],
                    "wheel_sha256": pin["wheel_sha256"],
                    "assets_digest": accepted["assets_digest"]}
        allowed = {"MISSING_PIN", "PRIVATE_MANIFEST", "SAME_JOB", "QUALIFICATION",
                   "QUALIFICATION_BINDING", "POLICY", "CLEANUP", "IDENTITY",
                   "OPERATOR_IDENTITY", "OUTPUT_STATE", "PAIR_STAGE", "PAIR_FAILED"}
        if receipt.get("status") == "failed" and receipt.get("error_code") in allowed:
            verified = controller.verify_failed_receipt(
                private, run_attempt=environment["GITHUB_RUN_ATTEMPT"])
            return {"matrix_status": "failed",
                    "matrix_error_code": verified["error_code"],
                    "completed_pairs": verified["pair_count"]}
    except (OSError, ValueError, UnicodeError, KeyError, TypeError):
        pass
    return fallback


def _verify_timing_raw_join(private, measurement, environment):
    """Rejoin every public row to collected observations at upload time."""
    from four_case_diagnostic import adapter, log_parser, public_attribution, supervisor

    job_id = environment.get("GITHUB_RUN_ID")
    require(type(job_id) is str and job_id.isdecimal(), "PUBLIC_OUTPUT")
    cells = {(case, trial, mode): [] for case in adapter.CASES
             for trial in range(3) for mode in ("on", "off")}
    for row in measurement["timings"]:
        cells[(row["case"], row["trial"], row["mode"])].append(row)
    operations = {(case, trial, mode): [] for case in adapter.CASES
                  for trial in range(3) for mode in ("on", "off")}
    stages = {(case, trial): [] for case in adapter.CASES for trial in range(3)}
    for row in measurement["operations"]:
        operations[(row["case"], row["trial"], row["mode"])].append(row)
    for row in measurement["stages"]:
        stages[(row["case"], row["trial"])].append(row)
    for case in adapter.CASES:
        for trial in range(3):
            collected = (private / f"{case}-{trial}" / "owned-runs" /
                         f"f712-{job_id}-{case}-{trial}" / "collected")
            directories = (private / f"{case}-{trial}",
                           private / f"{case}-{trial}" / "owned-runs",
                           collected.parent, collected, collected / "on",
                           collected / "off")
            for directory in directories:
                try:
                    info = directory.lstat()
                except OSError as error:
                    raise ValueError("PUBLIC_OUTPUT") from error
                require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.geteuid() and
                        stat.S_IMODE(info.st_mode) == 0o700, "PUBLIC_OUTPUT")
            clients = {}
            for mode in ("on", "off"):
                raw, raw_bytes = supervisor._ndjson(
                    collected / mode / "raw.ndjson", supervisor.RAW_BYTES, 21)
                require(all(type(row) is dict and
                            type(row.get("seconds")) in (int, float) and
                            math.isfinite(row["seconds"]) and row["seconds"] > 0
                            for row in raw), "PUBLIC_OUTPUT")
                clients[mode], _ = supervisor._ndjson(
                    collected / mode / "client.ndjson", supervisor.CLIENT_BYTES,
                    adapter.expected_client_records(case, mode))
                measured = {row.get("sample"): row for row in clients[mode]
                            if type(row) is dict and row.get("phase") == "measured"}
                require(set(measured) == set(range(21)), "PUBLIC_OUTPUT")
                cell = supervisor._json_file(collected / mode / "measurement.json",
                                             supervisor.RESULT_BYTES)
                timing = cells[(case, trial, mode)]
                require(len(timing) == len(raw) == 21 and
                        all(measured[index].get("pair_ordinal") ==
                            raw[index].get("pair_ordinal") and
                            type(measured[index].get("outer_ns")) is int and
                            measured[index]["outer_ns"] == round(
                                raw[index].get("seconds") * 1_000_000_000)
                            for index in range(21)) and
                        cell.get("raw_sha256") == _digest(raw_bytes) and
                        cell.get("raw_seconds") == [row.get("seconds") for row in raw] and
                        cell.get("raw_ordinals") ==
                        [row.get("pair_ordinal") for row in raw] and
                        all((item["sample"], item["pair_ordinal"],
                             item["elapsed_seconds"]) ==
                            (index, raw[index].get("pair_ordinal"),
                             raw[index].get("seconds"))
                            for index, item in enumerate(timing)), "PUBLIC_OUTPUT")
                require(operations[(case, trial, mode)] == [
                    public_attribution.operation_record(
                        case, trial, mode, index, row["pair_ordinal"], row["seconds"],
                        measured[index] if mode == "on" else None, row["resources"])
                    for index, row in enumerate(raw)], "PUBLIC_OUTPUT")
            events = log_parser.parse_server_timing(
                collected / "server.log", case, trial, clients["on"], clients["off"])
            require(stages[(case, trial)] == public_attribution.stage_records(
                case, trial, clients["on"], events), "PUBLIC_OUTPUT")


def _public_measurement(private):
    """Load only the three fixed public inventories with their declared units."""
    from four_case_diagnostic import public_attribution, public_timing

    timings = public_timing.load_timing_inventory(private / "public-timing")
    return {"schema": "alopex.sql-four-case-measurement/v2",
            "units": {"elapsed_seconds": "seconds", "client_phase": "nanoseconds",
                      "server_stage": "nanoseconds", "cpu": "seconds",
                      "rss": "bytes", "io": "bytes"},
            "server_resource_aggregation": "sum_of_observed_process_counters",
            "timings": timings,
            "paired": public_timing.summarize_timing_records(timings),
            "operations": public_attribution.load_operation_inventory(
                private / "public-operations"),
            "stages": public_attribution.load_stage_inventory(private / "public-stages")}


def prepare_public_export(environment):
    """Export only a bounded, canonical public measurement or fixed failure."""
    root = _runner_temp(environment)
    private = root / MATRIX_DIR
    export = REPO / "native-sql-four-case-export"
    export.mkdir(mode=0o700, exist_ok=True)
    require(not export.is_symlink() and export.stat().st_uid == os.geteuid() and
            stat.S_IMODE(export.stat().st_mode) == 0o700 and
            private.is_dir() and not private.is_symlink() and
            private.stat().st_uid == os.geteuid() and
            stat.S_IMODE(private.stat().st_mode) == 0o700,
            "EVIDENCE_BOUND")
    require(not list(export.iterdir()), "EVIDENCE_STATE")
    members = sorted(path for path in private.rglob("*") if path.is_file())
    require(members and len(members) <= MAX_PRIVATE_FILES and
            all(not path.is_symlink() and path.stat().st_uid == os.geteuid() and
                stat.S_IMODE(path.stat().st_mode) == 0o600
                for path in members) and
            sum(path.stat().st_size for path in members) <= MAX_PRIVATE_BYTES,
            "EVIDENCE_BOUND")
    for path in private.rglob("*"):
        require(not path.is_symlink(), "EVIDENCE_BOUND")
    matrix = _matrix_outcome(private, environment)
    if matrix["matrix_status"] == "passed":
        measurement = _public_measurement(private)
        public = {"schema": "alopex.sql-four-case-export/v4", "status": "prepared",
                  "performance_qualification": False,
                  "measurement": measurement, **matrix}
    else:
        public = {"schema": "alopex.sql-four-case-export/v4", "status": "prepared",
                  "performance_qualification": False,
                  "measurement": None, **matrix}
    body = (json.dumps(public, sort_keys=True, allow_nan=False) + "\n").encode()
    require(0 < len(body) <= MAX_PUBLIC_BYTES, "PUBLIC_OUTPUT")
    _capacity_before_seal(root, len(body) + 1_000_000)
    _write_private(export / "public.json", body)
    return public


def verify_public_export(environment):
    """Rebuild the public allowlist before any workflow artifact upload."""
    root = _runner_temp(environment)
    private = root / MATRIX_DIR
    export = REPO / "native-sql-four-case-export"
    require(export.is_dir() and not export.is_symlink() and
            export.stat().st_uid == os.geteuid() and
            stat.S_IMODE(export.stat().st_mode) == 0o700 and
            {path.name for path in export.iterdir()} == {"public.json"},
            "PUBLIC_OUTPUT")
    path = export / "public.json"
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and
                stat.S_IMODE(info.st_mode) == 0o600 and
                0 < info.st_size <= MAX_PUBLIC_BYTES, "PUBLIC_OUTPUT")
        body = stream.read(MAX_PUBLIC_BYTES + 1)
        require(len(body) == info.st_size, "PUBLIC_OUTPUT")
    try:
        public = json.loads(body,
                            object_pairs_hook=entrypoint.acquire.acquisition._unique_pairs)
    except (ValueError, UnicodeError) as error:
        raise ValueError("PUBLIC_OUTPUT") from error
    require(type(public) is dict and
            body == (json.dumps(public, sort_keys=True, allow_nan=False) + "\n").encode(),
            "PUBLIC_OUTPUT")
    matrix = _matrix_outcome(private, environment)
    if matrix["matrix_status"] == "passed":
        measurement = _public_measurement(private)
        _verify_timing_raw_join(private, measurement, environment)
        expected = {"schema": "alopex.sql-four-case-export/v4", "status": "prepared",
                    "performance_qualification": False,
                    "measurement": measurement,
                    **matrix}
    else:
        expected = {"schema": "alopex.sql-four-case-export/v4", "status": "prepared",
                    "performance_qualification": False,
                    "measurement": None, **matrix}
    require(public == expected, "PUBLIC_OUTPUT")
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("install", "run", "export", "verify-public"))
    args = parser.parse_args(argv)
    try:
        {"install": install_inputs, "run": run_job,
         "export": prepare_public_export,
         "verify-public": verify_public_export}[args.phase](dict(os.environ))
        print(json.dumps({"status": "prepared" if args.phase == "export" else "passed",
                          "performance_qualification": False}, sort_keys=True))
        return 0
    except BaseException as error:
        code = error.args[0] if type(error) is ValueError and error.args else "INTERNAL"
        print(json.dumps({"status": "failed", "error_code": code if code in {
            "MISSING_PIN", "PRIVATE_MANIFEST", "PREREQUISITE", "EVIDENCE_BOUND",
            "EVIDENCE_STATE", "PUBLIC_OUTPUT", "CAPACITY"} else "INTERNAL",
                          "performance_qualification": False}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
