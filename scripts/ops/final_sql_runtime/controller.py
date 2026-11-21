"""Entry point for a source-pinned, same-job final SQL run.

The caller supplies transfer/profile selection, storage admission, and an
independent receipt authenticator. Missing integration is an error, never a
synthetic collection or a performance acceptance decision.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path
import time

from . import audit_bridge, container, matrix, operation, same_job


def _require(ok, code):
    if not ok:
        raise ValueError(code)


def _reviewed(subjects, manifests, audit_pins, operators, declared_seconds):
    _require(all(type(value) is dict and set(value) == set(matrix.SUBJECTS)
                 for value in (subjects, manifests, audit_pins)), "SUBJECT_INVENTORY")
    _require(subjects["postgresql"].get("published_client_pin") == subjects["published"] and
             manifests["postgresql"] == manifests["published"], "REFERENCE_CLIENT")
    for subject in matrix.SUBJECTS:
        pin, audit_pin, manifest = subjects[subject], audit_pins[subject], manifests[subject]
        _require(type(pin) is dict and isinstance(manifest, Path) and
                 manifest.is_absolute() and manifest.is_file() and not manifest.is_symlink() and
                 type(audit_pin) is dict and
                 all(audit_pin.get(key) == pin.get(key) for key in ("source", "assets_digest")) and
                 all(matrix._hex(audit_pin.get(key), 64) for key in
                     ("assets_digest", "resource_digest", "worker_sha256", "harness_sha256")),
                 "REVIEWED_PIN")
        _require(pin.get("subject") == subject and
                 pin.get("product_subject") == ("published" if subject == "postgresql" else subject) and
                 type(pin.get("source")) is dict and
                 set(pin["source"]) == {"commit", "tree", "profile"} and
                 matrix._hex(pin["source"]["commit"], 40) and
                 matrix._hex(pin["source"]["tree"], 40) and
                 pin["source"]["profile"] == "release" and
                 matrix._hex(pin.get("assets_digest"), 64) and
                 type(pin.get("platform")) is str and pin["platform"] and
                 type(pin.get("files")) is list and pin["files"] and
                 all(type(item) is dict and
                     set(item) == {"role", "path", "size", "sha256"} and
                     type(item["role"]) is str and type(item["path"]) is str and
                     type(item["size"]) is int and item["size"] >= 0 and
                     matrix._hex(item["sha256"], 64) for item in pin["files"]) and
                 {item["role"] for item in pin["files"]}.issuperset(
                     {"server", "wheel", "receipt"}) and
                 hashlib.sha256(manifest.read_bytes()).hexdigest() == pin["assets_digest"],
                 "REVIEWED_PIN")
        if subject == "postgresql":
            _require(matrix._hex(pin.get("reference_image_digest"), 64),
                     "REFERENCE_IMAGE_PIN")
    _require(set(operators) == {"measurement_controller", "measurement_worker"} and
             all(type(item) is dict and isinstance(item.get("path"), Path) and
                 item["path"].is_absolute() and item["path"].is_file() and
                 not item["path"].is_symlink() and
                 matrix._hex(item.get("sha256"), 64) for item in operators.values()),
             "OPERATOR_PIN")
    _require(all(hashlib.sha256(item["path"].read_bytes()).hexdigest() == item["sha256"]
                 for item in operators.values()), "OPERATOR_BYTES")
    _require(type(declared_seconds) is dict and
             set(declared_seconds) == {"preflight", "cell"} and
             all(type(bound) in (int, float) and math.isfinite(bound) and
                 0 < bound <= operation.RUN_SECONDS for bound in declared_seconds.values()) and
             4 * declared_seconds["preflight"] + 48 * declared_seconds["cell"] <=
             operation.MATRIX_SECONDS, "PHASE_BOUNDS")


def run(*, lock_path, expected, repo, runner_temp, environment,
        subjects, product_manifests, audit_pins, docs_archive, product_root,
        product_files, harness_path, operators, destination_for, profile_for,
        admit_storage, capacity, independent_evidence, declared_seconds,
        clock=time.monotonic):
    """Execute and independently audit, returning false acceptance throughout.

    ``independent_evidence(result)`` must obtain private collections and
    independently authenticated receipts after owned cleanup. It returns a
    mapping with ``collections``, ``receipts``, ``preflight_collections``,
    ``preflight_receipts`` and ``authenticate_receipt``. This controller does
    not construct receipts from runtime reports.
    """
    _require(type(expected) is dict and set(expected) ==
             {"lock_sha256", "source_commit", "source_tree", "controller_commit",
              "operator_sha256"}, "MISSING_PIN")
    _require(all(callable(callback) for callback in
                 (destination_for, profile_for, admit_storage, capacity,
                  independent_evidence, clock)), "MISSING_INTEGRATION")
    # Authentication precedes every SQL copy or harness creation.
    qualification = same_job.attest_live(
        lock_path=lock_path, expected=expected, repo=repo,
        runner_temp=runner_temp, environment=environment)
    _reviewed(subjects, product_manifests, audit_pins, operators, declared_seconds)
    _require(all(isinstance(path, Path) and path.is_absolute() for path in
                 (docs_archive, product_root, harness_path)), "INPUT_PATH")
    _require(type(product_files) is dict and
             set(product_files) == {"parity_v0811_measure.py", "parity_v0811_gate.py",
                                    "fixtures/sql-v0.8.11-curated.json"}, "PRODUCT_CLOSURE")
    # Admission covers both harness staging and the complete declared schedule.
    required = 4 * declared_seconds["preflight"] + 48 * declared_seconds["cell"]
    _require(admit_storage(required) is True, "STORAGE_ADMISSION")
    harness_record = matrix.compose_harness(docs_archive, product_root,
                                            product_files, harness_path)
    callbacks = container.ContainerCallbacks(profile_for, clock=clock)
    destinations = set()

    def unique_destination(kind, row):
        destination = destination_for(kind, row)
        _require(isinstance(destination, Path) and destination.is_absolute() and
                 not destination.exists() and destination not in destinations,
                 "STAGING_DESTINATION")
        destinations.add(destination)
        return destination

    result = operation.run(
        qualification=qualification, subjects=subjects,
        product_manifests=product_manifests, destination_for=unique_destination,
        harness=harness_path, harness_record=harness_record, operators=operators,
        prepare=matrix.prepare_cell, execute=callbacks.execute,
        collect=callbacks.collect, cleanup=callbacks.cleanup,
        capacity=capacity, clock=clock, declared_seconds=declared_seconds)
    _require(result["summary"].get("performance_acceptance") is False and
             result["evidence"].get("performance_acceptance") is False,
             "OPERATION_ACCEPTANCE")
    evidence = independent_evidence(result)
    _require(type(evidence) is dict and set(evidence) ==
             {"collections", "receipts", "preflight_collections",
              "preflight_receipts", "authenticate_receipt"} and
             callable(evidence["authenticate_receipt"]), "MISSING_AUDIT_EVIDENCE")
    audited = audit_bridge.audit_completed(
        result, evidence["collections"], evidence["receipts"], audit_pins,
        evidence["authenticate_receipt"],
        preflight_collections=evidence["preflight_collections"],
        preflight_receipts=evidence["preflight_receipts"])
    _require(audited.get("performance_acceptance") is False, "AUDIT_ACCEPTANCE")
    return {"qualification": qualification, "operation": result,
            "audit": audited, "performance_acceptance": False}
