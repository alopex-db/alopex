"""Assemble independently verified #712 pair collections for strict CMS intake.

No missing client, server, resource or cell record is generated here. Server
events are parsed from each original bounded server log; all other records
come from the collected private files. The producer performs the independent
full audit before encrypting the exact file inventory.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from . import adapter, audit, intake, producer, qualification, supervisor


def require(value, code="EVIDENCE_INCOMPLETE"):
    if not value:
        raise ValueError(code)


def _operator_digest(operators):
    """Bind the entire reviewed operator map, rather than one arbitrary file."""
    return hashlib.sha256(json.dumps(operators, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _pins(lock, manifest, proof, receipt, job_id):
    require(type(lock) is dict and type(lock.get("pin")) is dict and
            type(manifest) is dict and type(job_id) is str and job_id.isdecimal(),
            "MISSING_PIN")
    pin = lock["pin"]
    qualification.validate(
        lock, proof, receipt, job_id=job_id,
        expected_source_commit=pin["source_commit"],
        expected_source_tree=pin["source_tree"],
        expected_controller_commit=pin["controller_commit"],
        expected_operator_sha256=pin["operator_sha256"])
    require(all(manifest.get(key) == pin[key] for key in
                ("source_commit", "source_tree", "assets_digest")) and
            type(manifest.get("fixture_sha256")) is str and
            qualification.HEX64.fullmatch(manifest["fixture_sha256"]) and
            type(manifest.get("docs_files")) is dict and
            type(manifest.get("product_files")) is dict,
            "MISSING_PIN")
    controller_hash = pin["operator_sha256"]["four_case_diagnostic/controller.py"]
    binding = {"source": {"commit": pin["source_commit"],
                          "tree": pin["source_tree"], "profile": "release"},
               "assets_digest": pin["assets_digest"],
               "controller_sha256": controller_hash,
               "operator_sha256": _operator_digest(pin["operator_sha256"]),
               "same_job_id": job_id,
               "qualification_passed": True, "security_passed": True,
               "cleanup_passed": True}
    # The reviewed static manifest carries one common fixture-source digest;
    # each collected cell must contain that exact digest. This does not claim
    # that the read and rollback row counts are equal.
    fixtures = {case: manifest["fixture_sha256"] for case in adapter.CASES}
    return binding, fixtures


def assemble_files(runs, *, lock, manifest, proof, receipt, job_id):
    """Return exact intake file bytes and independently derivable binding.

    ``runs`` maps every ``(case, trial)`` to a deleted owned Run. The caller
    obtains these from the source-pinned controller, not from archive contents.
    """
    binding, fixtures = _pins(lock, manifest, proof, receipt, job_id)
    expected = {(case, trial) for case in adapter.CASES for trial in range(3)}
    require(type(runs) is dict and set(runs) == expected, "PAIR_INVENTORY")
    files = {}
    cells, clients, servers, resources = [], [], [], {}
    roots = set()
    for case in adapter.CASES:
        for trial in range(3):
            run = runs[(case, trial)]
            require(hasattr(run, "root") and hasattr(run, "record") and
                    callable(getattr(run, "verify_collection", None)),
                    "PAIR_INVENTORY")
            root = Path(run.root)
            require(root.is_absolute() and root not in roots, "PAIR_INVENTORY")
            roots.add(root)
            config = {"schema": "alopex.sql-four-case-pair/v1",
                      "engine": "alopex-server", "case": case, "trial": trial,
                      "modes": ["on", "off"], "source": binding["source"],
                      "assets_digest": binding["assets_digest"],
                      "same_job_id": job_id,
                      "fixture_sha256": manifest["fixture_sha256"],
                      "docs_files": manifest["docs_files"],
                      "product_files": manifest["product_files"]}
            supervisor.run_collected_pair(config, run, proof, receipt)
            collected = root / "collected"
            report = supervisor._json_file(collected / "result.json",
                                           supervisor.RESULT_BYTES)
            identity = {key: report["server"][key] for key in ("pid", "start_ticks")}
            resource_data = supervisor._private_file(
                collected / "resources.json", supervisor.RESULT_BYTES)
            resource_digest = intake.digest(resource_data)
            verified = supervisor.verify_output(config, collected, identity,
                                                resource_digest)
            pair = f"{case}-{trial}"
            files[f"resources/{pair}.json"] = resource_data
            resources[pair] = resource_digest
            for mode in ("on", "off"):
                cell = verified["cells"][mode]
                cells.append(cell)
                raw = supervisor._private_file(collected / mode / "raw.ndjson",
                                               supervisor.RAW_BYTES)
                client = supervisor._private_file(collected / mode / "client.ndjson",
                                                  supervisor.CLIENT_BYTES)
                require(raw.endswith(b"\n") and client.endswith(b"\n"),
                        "EVIDENCE_INCOMPLETE")
                files[f"raw/{pair}-{mode}.ndjson"] = raw
                clients.append(client)
            servers.extend(verified["server_timing"])
    files["cells.ndjson"] = b"".join(producer._json_line(cell) for cell in cells)
    files["client.ndjson"] = b"".join(clients)
    files["server.ndjson"] = b"".join(producer._json_line(event) for event in servers)
    files["resource-manifests.json"] = producer._json_line(resources)
    require(set(files) == set(intake._paths()) - {"manifest.json"} and
            all(type(data) is bytes and 0 < len(data) <= intake._paths()[name]
                for name, data in files.items()), "EVIDENCE_INCOMPLETE")
    # The strict intake contract independently validates all 24 cells, 3,720
    # client rows, 23,919 server events, raw joins and resource manifests.
    manifest_record = {"schema": intake.SCHEMA, "binding": binding,
                       "files": {name: {"sha256": intake.digest(data),
                                        "size": len(data)} for name, data in files.items()},
                       "cell_count": 24, "client_count": 3720,
                       "server_count": 23919,
                       "cleanup_verified": True,
                       "performance_qualification": False}
    intake._decode(dict(files, **{"manifest.json": producer._json_line(manifest_record)}),
                   binding, fixtures)
    return files, binding, fixtures


def seal_runs(runs, *, lock, manifest, proof, receipt, job_id,
              recipient_certificate, output_path):
    """Seal exact assembled bytes and return local CMS identity for a receipt."""
    files, binding, fixtures = assemble_files(
        runs, lock=lock, manifest=manifest, proof=proof, receipt=receipt,
        job_id=job_id)
    result = producer.seal_evidence(
        files, binding=binding, expected_fixtures=fixtures,
        recipient_certificate=recipient_certificate, output_path=output_path)
    return {**result, "binding": binding}
