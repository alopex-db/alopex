"""Offline SQL diagnostic coverage; no Docker or network calls."""
import sys
from pathlib import Path as _BootstrapPath
_OPS = _BootstrapPath(__file__).resolve().parents[1]
sys.path.insert(0, str(_OPS))
sys.path.insert(0, str(_OPS.parent / "test-env"))

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from common import atomic_json
from orchestrate import QUALIFICATION_CASES, SOURCE_COMMIT, SOURCE_TREE, plan, validate_dispatch
from same_job_1951 import (LOCK_SHA256, OPERATORS, SECCOMP_SHA256, derive_receipt,
                           prepare_diagnostic_cell)
from test_orchestrate import HERE, OPERATORS as HASHED_OPERATORS


def h(value):
    return hashlib.sha256(value.encode()).hexdigest()


def fixture():
    pins = {"source_commit": SOURCE_COMMIT, "source_tree": SOURCE_TREE,
            "artifact_id": 123, "artifact_sha256": h("artifact"),
            "assets_digest": h("assets"), "server_sha256": h("server"),
            "wheel_sha256": h("wheel"), "controller_commit": hashlib.sha1(b"controller").hexdigest(),
            "controller_digest": h("controller"), "asset_run_id": 456,
            "asset_run_head": hashlib.sha1(b"asset-run").hexdigest(),
            "harness_commit": hashlib.sha1(b"harness").hexdigest(), "zip_sha256": h("zip"),
            "image_lock_sha256": LOCK_SHA256, "seccomp_sha256": SECCOMP_SHA256,
            "operator_sha256": {name: h((HERE / name).read_text())
                                for name in HASHED_OPERATORS}}
    rows = [{"id": name, "status": "passed", "error_code": None}
            for name in QUALIFICATION_CASES]
    proof = {"job_id": "12345",
             "qualification_public": {"schema": "alopex.hosted-test-env-qualification/v1",
                                      "outcome": "passed", "error_code": None,
                                      "source_commit": SOURCE_COMMIT,
                                      "asset_digest": pins["assets_digest"], "cases": rows},
             "profile": {"run_id": "hosted-804-12345", "assets_digest": pins["assets_digest"],
                         "image_lock_sha256": LOCK_SHA256,
                         "security": {"lsm_kind": "apparmor", "lsm_profile": "docker-default",
                                      "seccomp_sha256": SECCOMP_SHA256},
                         "storage": {"stop_bytes": 40_000_000_000,
                                     "reserve_bytes": 1_000_000_000, "poll_seconds": 10}},
             "acquisition": {"artifact_id": pins["artifact_id"],
                             "zip_sha256": pins["zip_sha256"],
                             "assets_digest": pins["assets_digest"]},
             "product": {"source_commit": SOURCE_COMMIT, "source_tree": SOURCE_TREE,
                         "artifact_sha256": pins["artifact_sha256"],
                         "server_sha256": pins["server_sha256"],
                         "wheel_sha256": pins["wheel_sha256"],
                         "assets_digest": pins["assets_digest"]},
             "runs": {name: {"phase": "deleted", "collection_verified": True,
                              "cleanup_errors": []} for name in ("primary", "repeat", "probe")},
             "acceptance": {"status": "passed", "cases": rows},
             "accepted_public": {"status": "passed", "assets_digest": pins["assets_digest"],
                                 "image_digest": h("image"),
                                 "controller_commit": pins["controller_commit"],
                                 "controller_digest": pins["controller_digest"]},
             "image_digest": h("image"),
             "qualification_public_sha256": h("public"),
             "acceptance_sha256": h("acceptance"),
             "profile_sha256": h("profile"), "temp_snapshot_verified": True}
    return pins, proof


class SameJob1951Tests(unittest.TestCase):
    def test_truthful_current_job_receipt_and_exact_14_cases(self):
        pins, proof = fixture()
        receipt = derive_receipt(pins, job_id="12345", attestor=lambda p, j: proof)
        narrow = {key: pins[key] for key in ("source_commit", "source_tree", "artifact_id",
                                            "artifact_sha256", "assets_digest", "server_sha256",
                                            "wheel_sha256", "controller_commit", "operator_sha256")}
        self.assertEqual(validate_dispatch(narrow, receipt, job_id="12345", operator_dir=HERE)
                         ["binding_sha256"], receipt["binding_sha256"])
        self.assertEqual(set(receipt["acceptance_cases"]), set(QUALIFICATION_CASES))

    def test_old_source_missing_pin_and_prior_job_refused(self):
        for mutate in (lambda p, q: q["qualification_public"].update(source_commit="a4158b873963dd79c7844953d64a8b511ee75fde"),
                       lambda p, q: p.update(assets_digest="0" * 64),
                       lambda p, q: q.update(job_id="prior-job")):
            pins, proof = fixture()
            mutate(pins, proof)
            with self.assertRaises(ValueError):
                derive_receipt(pins, job_id="12345", attestor=lambda p, j: proof)

    def test_security_profile_or_storage_change_refused(self):
        for change in (lambda q: q["profile"]["security"].update(lsm_profile="unconfined"),
                       lambda q: q["profile"]["storage"].update(poll_seconds=30),
                       lambda q: q["runs"]["primary"].update(phase="stopped")):
            pins, proof = fixture()
            change(proof)
            with self.assertRaises(ValueError):
                derive_receipt(pins, job_id="12345", attestor=lambda p, j: proof)

    def test_hashed_four_operator_cell_preparation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pins, proof = fixture()
            source = root / "product"
            source.mkdir()
            files = {}
            for role in ("server", "wheel", "library", "harness", "receipt"):
                path = source / (role + ".bin")
                path.write_text(json.dumps({"source": {"commit": SOURCE_COMMIT,
                                                        "tree": SOURCE_TREE,
                                                        "profile": "release"}})
                                if role == "receipt" else role)
                files[role] = {"role": role, "path": path.name,
                               "sha256": h(path.read_text()), "size": path.stat().st_size}
            pins["server_sha256"] = files["server"]["sha256"]
            pins["wheel_sha256"] = files["wheel"]["sha256"]
            manifest = source / "assets.json"
            manifest.write_text(json.dumps({"source_commit": SOURCE_COMMIT,
                                            "harness_commit": pins["harness_commit"],
                                            "files": list(files.values())}))
            pins["assets_digest"] = h(manifest.read_text())
            harness = root / "combined.tar"
            harness.write_text("harness")
            harness_record = {"sha256": h("harness"), "docs_subdirectory": "docs",
                              "product_subdirectory": "product", "docs_files": {"x": h("x")},
                              "product_files": {"y": h("y")}}
            operators = {role: {"path": str(HERE / source_name),
                                "sha256": pins["operator_sha256"][source_name]}
                         for role, (source_name, _) in OPERATORS.items()}
            verify = lambda path, platform: json.loads(Path(path).read_text())
            output, config = prepare_diagnostic_cell(
                manifest, root / "cell", harness, harness_record, plan()[0], pins,
                operators, verify_assets=verify)
            self.assertEqual(config["schema"], "alopex.sql-diagnostic-supervisor/v1")
            self.assertEqual(config["engine"], "alopex-server")
            self.assertEqual(json.loads((root / "cell/measurement.json").read_text())["engine"],
                             "alopex-server")
            self.assertEqual({item["role"] for item in json.loads(output.read_text())["files"]},
                             {"server", "wheel", "library", "receipt", "harness",
                              "measurement_config", *OPERATORS})
            staged = root / "staged/assets.json"
            atomic_json(staged, json.loads(output.read_text()))
            self.assertEqual(output.read_bytes(), staged.read_bytes())
            self.assertEqual((root / "cell/controller.py").read_bytes(),
                             (HERE / "diagnostic_supervisor.py").read_bytes())
            altered = {role: dict(record) for role, record in operators.items()}
            altered["measurement_worker"]["sha256"] = h("wrong-worker")
            with self.assertRaisesRegex(ValueError, "OPERATOR_IDENTITY"):
                prepare_diagnostic_cell(manifest, root / "cell-bad", harness,
                                        harness_record, plan()[0], pins, altered,
                                        verify_assets=verify)
            self.assertFalse((root / "cell-bad").exists())
            (source / files["receipt"]["path"]).write_text(json.dumps({
                "source": {"commit": "a4158b873963dd79c7844953d64a8b511ee75fde",
                           "tree": SOURCE_TREE, "profile": "release"}}))
            with self.assertRaisesRegex(ValueError, "IDENTITY"):
                prepare_diagnostic_cell(manifest, root / "cell-old", harness,
                                        harness_record, plan()[0], pins, operators,
                                        verify_assets=verify)


if __name__ == "__main__":
    unittest.main()
