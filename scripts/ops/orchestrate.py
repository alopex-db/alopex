"""Task-private #712 12-cell orchestrator; no dispatch or security configuration.

Production hooks must adapt the frozen same-job #804 qualifier, image/profile,
cell supervisor and Budget. Missing pins or qualification prevent execution.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import time

from audit import audit
from common import ERRORS, Failure
from diagnostic import CASES, CLIENT_RECORDS_PER_CELL, cell_order

SOURCE_COMMIT = "1951a3fdfe086ea6e8bc537c16766f91f619396f"
SOURCE_TREE = "b55feda6be535a3e2168835a5ad9c88c0b669c68"
QUALIFICATION_CASES = ("E01", "E02", "E03", "E04", "E05", "E06", "E07",
                       "S01", "S02", "S03", "F01", "F02", "R01", "P01")
ORACLE = {"aggregate": "9526c7a7b4705e368f15fcc97fe25199d4cb2472b331236b560383d20c9807de",
          "ordered": "04ecfc3159f09b1f84e104801fd8c7bfa98469cd91072a47d60baf0b049c962f"}
FILES = {"manifest.json": 16_384, "cells.ndjson": 131_072, "client.ndjson": 2_000_000,
         "server.ndjson": 5_000_000, "join-audit.json": 131_072,
         "paired-summary.json": 16_384}
MAX_CLIENT = 12 * CLIENT_RECORDS_PER_CELL
MAX_SERVER = 20_000
CELL_SECONDS = 420
CASE_SECONDS = 300
ADDITIONAL_BYTES = 4_000_000_000
RESERVE_BYTES = 1_000_000_000
STOP_BYTES = 40_000_000_000


def require(value, code):
    if not value:
        raise ValueError(code)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def digest(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def commit(value):
    return type(value) is str and len(value) == 40 and all(c in "0123456789abcdef" for c in value)


def plan():
    return [{"cell_id": f"{case}-{trial}-{mode}", "case": case, "trial": trial,
             "mode": mode, "case_seconds": CASE_SECONDS, "run_seconds": CELL_SECONDS}
            for case in CASES for trial in range(3) for mode in cell_order(trial)]


def validate_dispatch(pin, receipt, *, job_id, operator_dir):
    """Validate output of frozen #804 same-job qualification, not a prior run."""
    required = {"source_commit", "source_tree", "artifact_id", "artifact_sha256",
                "assets_digest", "server_sha256", "wheel_sha256", "controller_commit",
                "operator_sha256"}
    require(type(pin) is dict and set(pin) == required, "MISSING_PIN")
    require(pin["source_commit"] == SOURCE_COMMIT and pin["source_tree"] == SOURCE_TREE,
            "SOURCE_IDENTITY")
    require(type(pin["artifact_id"]) is int and pin["artifact_id"] > 0 and
            all(digest(pin[name]) for name in ("artifact_sha256", "assets_digest",
                                               "server_sha256", "wheel_sha256")), "MISSING_ASSET")
    require(commit(pin["controller_commit"]), "MISSING_CONTROLLER_PIN")
    required_operators = ("diagnostic_worker.py", "diagnostic_measure.py", "diagnostic.py",
                          "diagnostic_supervisor.py", "same_job_1951.py",
                          "attestor_1951.py", "live_wrapper.py", "audit.py",
                          "orchestrate.py", "evidence.py")
    require(type(pin["operator_sha256"]) is dict and
            set(pin["operator_sha256"]) == set(required_operators) and
            all(sha256(Path(operator_dir) / name) == pin["operator_sha256"][name]
                for name in required_operators), "OPERATOR_IDENTITY")
    required_receipt = {"job_id", "source_commit", "source_tree", "artifact_id",
                        "artifact_sha256", "assets_digest", "server_sha256", "wheel_sha256",
                        "controller_commit", "qualification_status", "acceptance_cases",
                        "image_digest", "binding_sha256", "security_profile_digest"}
    require(type(receipt) is dict and set(receipt) == required_receipt, "QUALIFICATION")
    require(receipt["job_id"] == job_id and receipt["qualification_status"] == "passed" and
            type(receipt["acceptance_cases"]) is list and
            set(receipt["acceptance_cases"]) == set(QUALIFICATION_CASES) and
            len(receipt["acceptance_cases"]) == len(QUALIFICATION_CASES),
            "QUALIFICATION")
    require(all(receipt[name] == pin[name] for name in
                ("source_commit", "source_tree", "artifact_id", "artifact_sha256",
                 "assets_digest", "server_sha256", "wheel_sha256", "controller_commit")) and
            all(digest(receipt[name]) for name in
                ("image_digest", "binding_sha256", "security_profile_digest")), "QUALIFICATION_BINDING")
    return {"source_commit": SOURCE_COMMIT, "source_tree": SOURCE_TREE,
            "artifact_id": pin["artifact_id"], "artifact_sha256": pin["artifact_sha256"],
            "assets_digest": pin["assets_digest"], "server_sha256": pin["server_sha256"],
            "wheel_sha256": pin["wheel_sha256"], "controller_commit": pin["controller_commit"],
            "image_digest": receipt["image_digest"], "binding_sha256": receipt["binding_sha256"],
            "security_profile_digest": receipt["security_profile_digest"]}


def _write_json(path, value, limit):
    data = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    require(len(data) <= limit, "EVIDENCE_BOUND")
    with path.open("xb") as stream:
        stream.write(data)
    path.chmod(0o600)


def _write_ndjson(path, values, limit):
    data = b"".join((json.dumps(value, sort_keys=True, separators=(",", ":"),
                                allow_nan=False) + "\n").encode() for value in values)
    require(len(data) <= limit, "EVIDENCE_BOUND")
    with path.open("xb") as stream:
        stream.write(data)
    path.chmod(0o600)


def write_evidence(root, binding, cells, clients, servers, report, cleanup_proof):
    """Exact six-file plaintext allowlist; caller seals it before deletion."""
    root = Path(root)
    require(root.is_dir() and not root.is_symlink() and
            root.stat().st_mode & 0o077 == 0 and not any(root.iterdir()), "EVIDENCE_STATE")
    require(len(cells) == 12 and len(clients) <= MAX_CLIENT and len(servers) <= MAX_SERVER,
            "EVIDENCE_BOUND")
    require(cleanup_proof is True, "CLEANUP")
    manifest = {"schema": "alopex.sql-diagnostic-private/v1", "binding": binding,
                "cell_count": len(cells), "client_count": len(clients),
                "server_count": len(servers), "cleanup_verified": True,
                "performance_qualification": False}
    _write_json(root / "manifest.json", manifest, FILES["manifest.json"])
    _write_ndjson(root / "cells.ndjson", cells, FILES["cells.ndjson"])
    _write_ndjson(root / "client.ndjson", clients, FILES["client.ndjson"])
    _write_ndjson(root / "server.ndjson", servers, FILES["server.ndjson"])
    join = {key: value for key, value in report.items() if key != "paired"}
    # Per-UUID residuals are bounded here; they remain private.
    _write_json(root / "join-audit.json", join, FILES["join-audit.json"])
    _write_json(root / "paired-summary.json", report["paired"], FILES["paired-summary.json"])
    require({p.name for p in root.iterdir()} == set(FILES), "EVIDENCE_ALLOWLIST")
    return {name: {"sha256": sha256(root / name), "size": (root / name).stat().st_size}
            for name in FILES}


class DiagnosticFailure(ValueError):
    """Keeps independent primary, cleanup and evidence errors for private review."""

    def __init__(self, primary_error=None, cleanup_error=None, evidence_error=None):
        self.primary_error = primary_error
        self.cleanup_error = cleanup_error
        self.evidence_error = evidence_error
        super().__init__("DIAGNOSTIC_FAILED")


def _error_identity(error):
    if error is None:
        return None
    value = str(error)
    code = (error.code if isinstance(error, Failure) and error.code in ERRORS else
            value if type(error) is ValueError and value.isupper() and
            value.replace("_", "").isalpha() else "INTERNAL")
    return {"type": type(error).__name__, "code": code}


def _preserve_failure(root, phase, primary, cleanup):
    """A bounded, owner-only fixed-field failure record, even before qualification."""
    root = Path(root)
    if not root.exists():
        root.mkdir(mode=0o700, parents=True)
    require(root.is_dir() and not root.is_symlink() and root.stat().st_mode & 0o077 == 0,
            "EVIDENCE_STATE")
    path = root / "failure.private.json"
    _write_json(path, {"schema": "alopex.sql-diagnostic-failure/v1", "status": "failed",
                       "phase": phase, "primary": _error_identity(primary),
                       "cleanup": _error_identity(cleanup),
                       "performance_qualification": False}, 4_096)


def orchestrate(pin, *, job_id, operator_dir, evidence_dir, hooks,
                job_limit_seconds, clock=time.monotonic):
    """Hooks: qualify, used_bytes, run_cell, cleanup, seal. All are required.

    `run_cell` must use the frozen #804 profile/supervisor and return one
    independently verified cell summary, its client rows and filtered server
    rows. The hook owns per-cell process stop/wait and raw resource capture.
    """
    cells, clients, servers = [], [], []
    phase = "qualification"
    primary = cleanup_error = None
    binding = report = None
    initial = None
    try:
        require(set(hooks) == {"qualify", "used_bytes", "run_cell", "cleanup", "seal"}, "HOOKS")
        require(type(job_limit_seconds) in (int, float) and math.isfinite(job_limit_seconds) and
                0 < job_limit_seconds <= 86_400, "JOB_SAFETY_LIMIT")
        started = clock()
        receipt = hooks["qualify"](pin, job_id)
        binding = validate_dispatch(pin, receipt, job_id=job_id, operator_dir=operator_dir)
        phase = "capacity"
        initial = hooks["used_bytes"]()
        require(type(initial) is int and initial >= 0 and
                initial + ADDITIONAL_BYTES + RESERVE_BYTES < STOP_BYTES, "CAPACITY")
        phase = "cells"
        for cell in plan():
            # Independent job safety limit. No admission/acceptance semantics
            # are inferred while the #712 deadline contract is unsettled.
            require(clock() - started < job_limit_seconds, "JOB_SAFETY_LIMIT")
            used = hooks["used_bytes"]()
            require(type(used) is int and used < STOP_BYTES and
                    used - initial <= ADDITIONAL_BYTES, "CAPACITY")
            cell_started = clock()
            returned = hooks["run_cell"](cell, binding)
            require(clock() - cell_started <= CELL_SECONDS and
                    clock() - started <= job_limit_seconds, "JOB_SAFETY_LIMIT")
            require(type(returned) is tuple and len(returned) == 3, "CELL")
            result, client_rows, server_rows = returned
            require(type(result) is dict and
                    (result.get("case"), result.get("trial"), result.get("mode")) ==
                    (cell["case"], cell["trial"], cell["mode"]), "CELL")
            cells.append(result)
            clients.extend(client_rows)
            servers.extend(server_rows)
            require(len(clients) <= MAX_CLIENT and len(servers) <= MAX_SERVER, "EVIDENCE_BOUND")
        phase = "audit"
        report = audit(cells, clients, servers, expected_oracle=ORACLE)
        require(clock() - started <= job_limit_seconds, "JOB_SAFETY_LIMIT")
    except BaseException as error:
        primary = error
    finally:
        try:
            require(hooks["cleanup"]() is True, "CLEANUP")
        except BaseException as error:
            cleanup_error = error
    if primary is not None or cleanup_error is not None:
        evidence_error = None
        try:
            _preserve_failure(evidence_dir, phase, primary, cleanup_error)
        except BaseException as error:
            evidence_error = error
        raise DiagnosticFailure(primary, cleanup_error, evidence_error) from primary
    try:
        phase = "final_capacity"
        final = hooks["used_bytes"]()
        require(type(final) is int and final >= 0 and final - initial <= ADDITIONAL_BYTES and
                final + RESERVE_BYTES < STOP_BYTES, "CAPACITY")
        phase = "evidence"
        hashes = write_evidence(evidence_dir, binding, cells, clients, servers, report, True)
        phase = "encryption"
        sealed = hooks["seal"](Path(evidence_dir), hashes)
        require(type(sealed) is dict and digest(sealed.get("sha256")) and
                type(sealed.get("size")) is int and sealed["size"] > 0 and
                sealed.get("verified") is True, "ENCRYPTION")
    except BaseException as error:
        evidence_error = None
        try:
            _preserve_failure(evidence_dir, phase, error, None)
        except BaseException as preservation_error:
            evidence_error = preservation_error
        raise DiagnosticFailure(error, None, evidence_error) from error
    return {"status": "passed", "performance_qualification": False,
            "measurement_cells": 12, "measured_uuids": report["measured_uuids"],
            "measured_server_events": report["measured_server_events"],
            "ciphertext_sha256": sealed["sha256"], "ciphertext_size": sealed["size"]}
