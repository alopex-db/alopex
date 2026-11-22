"""Four-subject pins must bind admitted product bytes and the qualified image."""
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from scripts.ops.final_sql_runtime import controller, subject_binding as binding


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def wheel():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as package:
        package.writestr("alopex/module.so", b"fixture")
    return stream.getvalue()


class SubjectBindingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.manifests, self.reviewed = {}, {}
        for subject in binding.PRODUCTS:
            root = self.root / subject / "assets"
            root.mkdir(parents=True, mode=0o700)
            version = binding.VERSIONS[subject]
            source = {"commit": binding.SOURCE_COMMITS[subject],
                      "tree": "b" * 40, "profile": "release"}
            members = {
                "server": ("bin/alopex-server", b"server"),
                "library": ("lib/libalopex_sql_parser.so", b"library"),
                "wheel": (f"wheels/alopex-{version}-cp38-abi3-linux_x86_64.whl", wheel()),
                "harness": ("harness.tar", b"harness"),
            }
            rows = []
            for role, (name, data) in members.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                path.write_bytes(data)
                path.chmod(0o700 if role == "server" else 0o600)
                rows.append({"role": role, "path": name, "size": len(data),
                             "sha256": sha(data)})
            receipt = canonical({"schema": "alopex.performance-build/v1",
                                 "version": version, "source": source,
                                 "server_sha256": rows[0]["sha256"],
                                 "parser_sha256": rows[1]["sha256"],
                                 "python_wheel_sha256": rows[2]["sha256"]})
            path = root / "receipts/build.json"
            path.parent.mkdir(mode=0o700)
            path.write_bytes(receipt)
            path.chmod(0o600)
            rows.insert(3, {"role": "receipt", "path": "receipts/build.json",
                            "size": len(receipt), "sha256": sha(receipt)})
            manifest = canonical({"schema": "alopex.test-env-assets/v1",
                                  "platform": "linux/amd64",
                                  "source_commit": source["commit"],
                                  "harness_commit": "c" * 40,
                                  "expanded_limit_bytes": 268_435_456,
                                  "files": rows})
            path = root / "assets.json"
            path.write_bytes(manifest)
            path.chmod(0o600)
            self.manifests[subject] = path
            self.reviewed[subject] = {"source": source,
                                      "assets_digest": sha(manifest),
                                      "platform": "linux/amd64", "version": version,
                                      "files": [row for row in rows
                                                if row["role"] != "harness"]}

    def bind(self, **changes):
        arguments = {"manifests": self.manifests, "reviewed_pins": self.reviewed,
                     "qualification_args": {"lock_path": self.root / "lock.json",
                         "expected": {}, "repo": self.root, "runner_temp": self.root,
                         "environment": {"GITHUB_RUN_ID": "123",
                                         "GITHUB_RUN_ATTEMPT": "1"}},
                     "selected_image_digest": "d" * 64,
                     "worker_sha256": "e" * 64,
                     "harness_sha256": "f" * 64,
                     "controller_digest": "a" * 64}
        arguments.update(changes)
        with patch.object(binding.same_job, "attest_live", return_value={
                "image_digest": "d" * 64, "job_id": "123", "run_attempt": "1",
                "qualification_cases": sorted(binding.same_job.qualification.CASES)}) as live:
            result = binding.assemble(**arguments)
            live.assert_called_once()
            return result

    def test_four_subjects_reuse_published_client_and_pass_controller_review(self):
        subjects, manifests, audit = self.bind()
        self.assertEqual(set(subjects), {"published", "before", "candidate", "postgresql"})
        self.assertEqual(manifests["postgresql"], manifests["published"])
        self.assertEqual(subjects["postgresql"]["published_client_pin"],
                         subjects["published"])
        self.assertEqual(subjects["postgresql"]["reference_image_digest"], "d" * 64)
        self.assertTrue(all(pin["image_digest"] == "d" * 64 for pin in audit.values()))
        operator = self.root / "operator.py"
        operator.write_bytes(b"pass\n")
        controller._reviewed(subjects, manifests, audit,
                             {name: {"path": operator, "sha256": sha(b"pass\n")}
                              for name in ("measurement_controller", "measurement_worker")},
                             {"preflight": 100, "cell": 100})

    def test_wrong_source_image_and_product_bytes_fail(self):
        wrong = copy.deepcopy(self.reviewed)
        wrong["candidate"]["source"]["commit"] = "f" * 40
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            self.bind(reviewed_pins=wrong)
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            self.bind(selected_image_digest="not-an-image")
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            self.bind(selected_image_digest="e" * 64)
        (self.manifests["published"].parent / "bin/alopex-server").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            self.bind()

    def test_changed_receipt_and_extra_role_fail(self):
        receipt = self.manifests["before"].parent / "receipts/build.json"
        receipt.write_bytes(receipt.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            self.bind()
        receipt.write_bytes(receipt.read_bytes()[:-1])
        manifest_path = self.manifests["before"]
        manifest = json.loads(manifest_path.read_bytes())
        manifest["files"].append({"role": "debug", "path": "debug.log",
                                  "size": 1, "sha256": "a" * 64})
        data = canonical(manifest)
        manifest_path.write_bytes(data)
        reviewed = copy.deepcopy(self.reviewed)
        reviewed["before"]["assets_digest"] = sha(data)
        with self.assertRaisesRegex(ValueError, "SUBJECT_BINDING"):
            self.bind(reviewed_pins=reviewed)


if __name__ == "__main__":
    unittest.main()
