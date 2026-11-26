"""Synthetic, offline public-reference ingress contract tests."""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
import zipfile

from scripts.ops.final_sql_runtime import reference_public as public


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def archive_bytes(files, *, link=None):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name, data in files.items():
            item = zipfile.ZipInfo(name)
            item.create_system = 3
            item.external_attr = ((stat.S_IFLNK | 0o777) if name == link else
                                  (stat.S_IFREG | 0o600)) << 16
            archive.writestr(item, data)
    return output.getvalue()


def wheel_bytes(content=b"safe wheel"):
    return archive_bytes({"alopex/package.py": content})


class PublicReferenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.destination = self.root / "admitted"
        self.destination.mkdir(mode=0o700)
        self.pins, self.runs, self.artifacts, self.wrappers = {}, {}, {}, {}
        self.files = {}
        for index, subject in enumerate(public.SUBJECTS):
            version = public.VERSION[subject]
            source = {"commit": public.SOURCE_COMMIT[subject],
                      "tree": "b" * 40, "profile": "release"}
            payload = {
                "assets/bin/alopex-server": b"safe server",
                "assets/lib/libalopex_sql_parser.so": b"safe library",
                "assets/wheels/alopex-" + version + "-cp38-abi3-linux_x86_64.whl": wheel_bytes(),
                "assets/harness.tar": b"safe harness",
            }
            product = []
            for role, name in (("server", "bin/alopex-server"),
                               ("library", "lib/libalopex_sql_parser.so"),
                               ("wheel", "wheels/alopex-" + version +
                                "-cp38-abi3-linux_x86_64.whl"),
                               ("harness", "harness.tar")):
                data = payload["assets/" + name]
                product.append({"role": role, "path": name, "size": len(data),
                                "sha256": sha(data)})
            receipt = encode({"schema": "alopex.performance-build/v1", "source": source,
                              "version": version,
                              "server_sha256": product[0]["sha256"],
                              "parser_sha256": product[1]["sha256"],
                              "python_wheel_sha256": product[2]["sha256"]})
            payload["assets/receipts/build.json"] = receipt
            product.append({"role": "receipt", "path": "receipts/build.json",
                            "size": len(receipt), "sha256": sha(receipt)})
            manifest = encode({"schema": "alopex.test-env-assets/v1",
                               "platform": "linux/amd64", "source_commit": source["commit"],
                               "harness_commit": "c" * 40,
                               "expanded_limit_bytes": public.MAX_EXPANDED,
                               "files": product})
            payload["assets/assets.json"] = manifest
            result = encode({"schema": "alopex.native-assets/v1", "status": "passed",
                             "source": source, "assets_digest": sha(manifest),
                             "performance_qualification": False})
            payload["result.json"] = result
            self.files[subject] = payload
            raw = archive_bytes(payload)
            path = self.root / f"{subject}.zip"
            path.write_bytes(raw)
            path.chmod(0o600)
            self.wrappers[subject] = path
            run_id, artifact_id = 100 + index, 200 + index
            self.pins[subject] = {"run_id": run_id, "head_sha": "d" * 40,
                "repository_id": 13, "workflow_id": 17, "artifact_id": artifact_id,
                "artifact_name": "native-product-assets", "zip_sha256": sha(raw),
                "zip_size_bytes": len(raw), "manifest_sha256": sha(manifest),
                "result_sha256": sha(result), "source_commit": source["commit"],
                "source_tree": source["tree"], "harness_commit": "c" * 40,
                "version": version, "platform": "linux/amd64", "files": product}
            self.runs[subject] = {"id": run_id, "head_sha": "d" * 40,
                "repository": {"id": 13}, "workflow_id": 17, "run_attempt": 1,
                "event": "workflow_dispatch", "status": "completed", "conclusion": "success"}
            self.artifacts[subject] = {"id": artifact_id,
                "name": "native-product-assets", "digest": "sha256:" + sha(raw),
                "size_in_bytes": len(raw), "expired": False,
                "workflow_run": {"id": run_id, "head_sha": "d" * 40}}

    def admit(self):
        return public.admit_public_references(
            pins=self.pins, runs=self.runs, artifacts=self.artifacts,
            wrappers=self.wrappers, destination=self.destination,
            private_roots=("/nonexistent-private-root",))

    def rewrite(self, subject="before", *, link=None):
        raw = archive_bytes(self.files[subject], link=link)
        self.wrappers[subject].write_bytes(raw)
        self.wrappers[subject].chmod(0o600)
        self.pins[subject]["zip_sha256"] = sha(raw)
        self.pins[subject]["zip_size_bytes"] = len(raw)
        self.artifacts[subject]["digest"] = "sha256:" + sha(raw)
        self.artifacts[subject]["size_in_bytes"] = len(raw)

    def test_two_exact_references_extract_owner_only(self):
        manifests = self.admit()
        self.assertEqual(set(manifests), set(public.SUBJECTS))
        for path in manifests.values():
            self.assertTrue(path.is_file())
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.destination / "before/assets/bin/alopex-server").stat().st_mode),
                         0o700)

    def test_wrong_run_artifact_and_unreviewed_zip_fail(self):
        for edit in (
            lambda: self.runs["before"].update(head_sha="e" * 40),
            lambda: self.runs["before"].update(run_attempt=2),
            lambda: self.artifacts["before"].update(id=999),
            lambda: self.artifacts["before"].update(expired=True),
            lambda: self.wrappers["before"].write_bytes(b"changed"),
        ):
            with self.subTest(edit=edit):
                self.setUp()
                edit()
                with self.assertRaises((ValueError, zipfile.BadZipFile)):
                    self.admit()
                self.assertFalse(any(self.destination.iterdir()))

    def test_cms_extra_traversal_and_symlink_fail(self):
        for name, data, link in (
            ("private-evidence.cms", b"private", None),
            ("assets/extra.log", b"private", None),
            ("../outside", b"escape", None),
            ("assets/bin/alopex-server", b"safe server", "assets/bin/alopex-server"),
        ):
            with self.subTest(name=name, link=link):
                self.setUp()
                self.files["before"][name] = data
                self.rewrite(link=link)
                with self.assertRaises(ValueError):
                    self.admit()
                self.assertFalse(any(self.destination.iterdir()))

    def test_changed_product_manifest_or_source_tree_fail(self):
        for edit in (
            lambda: self.pins["before"].update(source_tree="e" * 40),
            lambda: self.files["before"].update({"assets/bin/alopex-server": b"changed"}),
            lambda: self.pins["before"].update(manifest_sha256="f" * 64),
        ):
            with self.subTest(edit=edit):
                self.setUp()
                edit()
                if self.files["before"]["assets/bin/alopex-server"] == b"changed":
                    self.rewrite()
                with self.assertRaises(ValueError):
                    self.admit()
                self.assertFalse(any(self.destination.iterdir()))

    def test_public_privacy_scan_covers_binary_and_decompressed_wheel(self):
        for name, data in (
            ("assets/bin/alopex-server", b"prefix /home/runner/work/private"),
            ("assets/wheels/alopex-0.8.17-cp38-abi3-linux_x86_64.whl",
             wheel_bytes(b"source path+file:///private/source")),
        ):
            with self.subTest(name=name):
                self.setUp()
                self.files["before"][name] = data
                role = "server" if name.endswith("alopex-server") else "wheel"
                item = next(item for item in self.pins["before"]["files"]
                            if item["role"] == role)
                item["size"], item["sha256"] = len(data), sha(data)
                manifest = encode({**json.loads(self.files["before"]["assets/assets.json"]),
                                   "files": self.pins["before"]["files"]})
                self.files["before"]["assets/assets.json"] = manifest
                self.pins["before"]["manifest_sha256"] = sha(manifest)
                result = json.loads(self.files["before"]["result.json"])
                result["assets_digest"] = sha(manifest)
                self.files["before"]["result.json"] = encode(result)
                self.pins["before"]["result_sha256"] = sha(self.files["before"]["result.json"])
                self.rewrite()
                with self.assertRaisesRegex(ValueError, "PUBLIC_PRIVACY"):
                    self.admit()

    def test_private_destination_and_size_bound_fail(self):
        self.destination.chmod(0o755)
        with self.assertRaises(ValueError):
            self.admit()

    def test_wrapper_symlink_and_wrong_source_pin_fail(self):
        link = self.root / "wrapper-link.zip"
        link.symlink_to(self.wrappers["before"])
        self.wrappers["before"] = link
        with self.assertRaises((ValueError, OSError)):
            self.admit()
        self.assertFalse(any(self.destination.iterdir()))
        self.wrappers["before"] = self.root / "before.zip"
        self.pins["before"]["source_commit"] = "f" * 40
        with self.assertRaises(ValueError):
            self.admit()

    def test_same_prefix_commit_and_public_wrapper_mode_fail(self):
        original = self.pins["before"]["source_commit"]
        self.pins["before"]["source_commit"] = original[:8] + "f" * 32
        with self.assertRaisesRegex(ValueError, "PUBLIC_PIN"):
            self.admit()
        self.pins["before"]["source_commit"] = original
        self.wrappers["before"].chmod(0o644)
        with self.assertRaisesRegex(ValueError, "PUBLIC_WRAPPER"):
            self.admit()
        self.destination.chmod(0o700)
        self.pins["before"]["zip_size_bytes"] = public.MAX_ZIP + 1
        with self.assertRaises(ValueError):
            self.admit()


if __name__ == "__main__":
    unittest.main()
