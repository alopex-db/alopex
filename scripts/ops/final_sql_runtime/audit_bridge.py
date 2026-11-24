"""Offline binding of completed runtime cells to the independent SQL audit.

The caller supplies 48 private collections and independently authenticated
receipts. Each cell receipt names the coordinate, proves owned cleanup was
deleted, and pins SHA-256 of measurement.json, raw.ndjson, resources.json,
worker.py, and the
archived SQL harness worker. Four preflight receipts each pin the full ordered
result JSON. ``authenticate_receipt`` must verify that provenance; this
module cannot infer it from operation summaries or self-declared file hashes.
No execution, acceptance, or publication occurs here.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.ops import final_sql_matrix as independent
from . import matrix as runtime_matrix
from . import worker

FILES = ("measurement", "raw", "resources", "worker", "harness")


def require(condition, code):
    if not condition:
        raise ValueError(code)


def _key(item):
    require(type(item) is dict, "RECORD_SHAPE")
    return item.get("case"), item.get("trial"), item.get("subject")


def _inventory(items, code):
    require(type(items) is list and len(items) == 48, code)
    keys = [_key(item) for item in items]
    expected = [(row["case"], row["trial"], row["subject"])
                for row in runtime_matrix.plan()]
    require(keys == expected and len(set(keys)) == 48, code)


def _read(path, *, allow_bytes=False):
    if allow_bytes:
        require(type(path) is bytes and 0 < len(path) < 4_000_000,
                "PRIVATE_FILE")
        return path
    require(isinstance(path, Path) and path.is_absolute() and not path.is_symlink()
            and path.is_file(), "PRIVATE_FILE")
    return path.read_bytes()


def audit_completed(operation_result, collections, receipts, pins, authenticate_receipt,
                    *, preflight_collections, preflight_receipts):
    """Audit exact 48/1008 data without elevating it to performance acceptance.

    ``authenticate_receipt`` is a caller-owned, source-pinned independent
    verifier. It must return literal ``True`` only for an authentic receipt;
    file digests and cleanup statements in an unauthenticated receipt are not
    evidence. The receipt's ``sha256`` maps all five FILES to their exact bytes.
    A collection maps private files to absolute Paths and harness to verified
    archived bytes. Neither may be generated
    from the runtime operation summary.
    """
    require(type(operation_result) is dict and
            type(operation_result.get("evidence")) is dict and
            type(operation_result.get("summary")) is dict and
            operation_result["summary"].get("performance_acceptance") is False and
            operation_result["evidence"].get("performance_acceptance") is False,
            "INCOMPLETE_OPERATION")
    operation_cells = operation_result["evidence"].get("cells")
    _inventory(operation_cells, "OPERATION_COVERAGE")
    _inventory(collections, "COLLECTION_COVERAGE")
    _inventory(receipts, "RECEIPT_COVERAGE")
    require(callable(authenticate_receipt), "RECEIPT_AUTHENTICATOR")
    require(type(pins) is dict and set(pins) == set(independent.SUBJECTS), "SUBJECT_PINS")
    preflights = operation_result["evidence"].get("preflights")
    require(type(preflights) is list and len(preflights) == 4 and
            [p.get("subject") for p in preflights if type(p) is dict] ==
            list(independent.SUBJECTS) and
            all(p.get("result_checksum") == worker.result_digest("select-all")
                for p in preflights), "PREFLIGHT_COVERAGE")
    require(type(preflight_collections) is list and
            type(preflight_receipts) is list and
            len(preflight_collections) == len(preflight_receipts) == 4 and
            all(type(item) is dict for item in
                preflight_collections + preflight_receipts), "PREFLIGHT_FILES")
    preflight_paths = set()
    for operated, collected, receipt in zip(preflights, preflight_collections,
                                            preflight_receipts):
        subject = operated["subject"]
        pin = pins[subject]
        require(collected.get("subject") == receipt.get("subject") == subject and
                receipt.get("source") == pin.get("source") and
                receipt.get("assets_digest") == pin.get("assets_digest") and
                receipt.get("cleanup") == "deleted" and
                authenticate_receipt(receipt) is True, "PREFLIGHT_AUTHENTICATION")
        path = collected.get("result")
        require(isinstance(path, Path) and path not in preflight_paths,
                "PREFLIGHT_DUPLICATE_FILE")
        preflight_paths.add(path)
        data = _read(path)
        require(hashlib.sha256(data).hexdigest() == receipt.get("result_sha256"),
                "PREFLIGHT_DIGEST")
        try:
            exact = json.loads(data)
        except (UnicodeError, ValueError) as error:
            raise ValueError("PREFLIGHT_JSON") from error
        require(type(exact) is dict and set(exact) == {"rows", "result"} and
                worker.validate_exact("select-all", exact["result"], exact["rows"]) ==
                operated["result_checksum"], "PREFLIGHT_RESULT")
    require(operation_result["summary"] == runtime_matrix.summarize(operation_cells),
            "OPERATION_SUMMARY")
    public_rows = operation_result["evidence"].get("public_operations")
    require(type(public_rows) is list and len(public_rows) == 1008,
            "PUBLIC_COVERAGE")
    cells = []
    resource_digests = {}
    seen_private = set()
    for operated, collection, receipt in zip(operation_cells, collections, receipts):
        key = _key(operated)
        require(_key(collection) == key and _key(receipt) == key,
                "COORDINATE_SUBSTITUTION")
        require(operated.get("status") == "passed" and
                operated.get("id") == f"sql-{key[0]}-{key[1]}-{key[2]}" and
                operated.get("engine") == ("postgresql" if key[2] == "postgresql"
                                           else "alopex-server"), "OPERATION_STATUS")
        require(receipt.get("cleanup") == "deleted" and
                authenticate_receipt(receipt) is True, "UNAUTHENTICATED_RECEIPT")
        pin = pins[key[2]]
        require(receipt.get("source") == pin.get("source") and
                receipt.get("assets_digest") == pin.get("assets_digest"),
                "RECEIPT_SUBJECT_IDENTITY")
        hashes = receipt.get("sha256")
        require(type(hashes) is dict and set(hashes) == set(FILES) and
                all(type(value) is str and len(value) == 64 and
                    all(char in "0123456789abcdef" for char in value)
                    for value in hashes.values()), "RECEIPT_DIGESTS")
        paths = collection.get("files")
        require(type(paths) is dict and set(paths) == set(FILES), "COLLECTION_FILES")
        data = {}
        for name in FILES:
            path = paths[name]
            if name in ("measurement", "raw", "resources"):
                require(path not in seen_private, "DUPLICATE_PRIVATE_FILE")
                seen_private.add(path)
            data[name] = _read(path, allow_bytes=name == "harness")
            require(hashlib.sha256(data[name]).hexdigest() == hashes[name],
                    "STALE_OR_SUBSTITUTED_FILE")
        try:
            measurement = json.loads(data["measurement"])
        except (UnicodeError, ValueError) as error:
            raise ValueError("MEASUREMENT_JSON") from error
        latency = measurement.get("latency_p50_ms") if type(measurement) is dict else None
        require(type(measurement) is dict and
                operated.get("result_checksum") == measurement.get("result_sha256") and
                type(operated.get("trial_median_seconds")) in (int, float) and
                type(latency) in (int, float) and
                operated["trial_median_seconds"] == latency / 1000,
                "OPERATION_COLLECTION_BINDING")
        try:
            private_rows = [json.loads(line) for line in data["raw"].splitlines()]
        except (UnicodeError, ValueError) as error:
            raise ValueError("PUBLIC_PRIVATE_MISMATCH") from error
        offset = len(cells) * 21
        require(len(private_rows) == 21 and all(
            public_rows[offset + sample] == worker.public_record(operated, sample,
                private_rows[sample]["seconds"], private_rows[sample]["resources"])
            for sample in range(21)), "PUBLIC_PRIVATE_MISMATCH")
        cells.append({"case": key[0], "trial": key[1], "subject": key[2],
                      "engine": operated["engine"], "status": "passed",
                      "cleanup": "deleted", "source": pin["source"],
                      "assets_digest": pin["assets_digest"],
                      "measurement": measurement, "raw_bytes": data["raw"],
                      "worker_bytes": data["worker"], "harness_bytes": data["harness"],
                      "trial_median_seconds": operated["trial_median_seconds"]})
        resource_digests[key] = hashes["resources"]
    result = independent.audit_matrix(cells, pins,
                                      resource_digests=resource_digests)
    require(result.get("verified_cells") == 48 and
            result.get("verified_samples") == 1008 and
            result.get("performance_acceptance") is False,
            "AUDIT_CONTRACT")
    result["public_sha256"] = hashlib.sha256(json.dumps(
        public_rows, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return result
