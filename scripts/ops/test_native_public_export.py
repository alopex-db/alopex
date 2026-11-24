"""Public native asset upload boundary checks."""
import io
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import zipfile

from . import native_public_export as public


class PublicExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.export = Path(self.temp.name) / "native-assets-export"
        self.export.mkdir(mode=0o700)
        self.files = {
            "server": "bin/alopex-server",
            "library": "lib/libalopex_sql_parser.so",
            "wheel": "wheels/alopex-0.8.17-cp38-abi3-linux_x86_64.whl",
            "receipt": "receipts/build.json",
            "harness": "harness.tar",
        }

    def write(self, name, data, mode=0o600):
        path = self.export / name
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = path.parent
        while parent != self.export:
            parent.chmod(0o700)
            parent = parent.parent
        path.write_bytes(data)
        path.chmod(mode)
        return path

    def json(self, name, value):
        return self.write(name, (json.dumps(
            value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode())

    def success(self, version="0.8.17"):
        if (self.export / "assets").exists():
            shutil.rmtree(self.export / "assets")
        self.files["wheel"] = f"wheels/alopex-{version}-cp38-abi3-linux_x86_64.whl"
        rows = []
        for role, name in self.files.items():
            if role == "wheel":
                stream = io.BytesIO()
                with zipfile.ZipFile(stream, "w") as package:
                    package.writestr("alopex/module.so", b"fixture")
                data = stream.getvalue()
            elif role == "receipt":
                data = b"pending"
            else:
                data = role.encode()
            path = self.write("assets/" + name, data,
                              0o700 if role == "server" else 0o600)
            rows.append({"role": role, "path": name, "size": len(data),
                         "sha256": public.digest(path)})
        by_role = {row["role"]: row for row in rows}
        receipt = {
            "schema": "alopex.performance-build/v1", "version": version,
            "source": {"commit": "a" * 40, "tree": "c" * 40, "profile": "release"},
            "server_sha256": by_role["server"]["sha256"],
            "python_wheel_sha256": by_role["wheel"]["sha256"],
            "parser_sha256": by_role["library"]["sha256"],
        }
        receipt_path = self.json("assets/" + self.files["receipt"], receipt)
        by_role["receipt"].update(size=receipt_path.stat().st_size,
                                  sha256=public.digest(receipt_path))
        manifest = {
            "schema": "alopex.test-env-assets/v1", "platform": "linux/amd64",
            "source_commit": "a" * 40, "harness_commit": "b" * 40,
            "expanded_limit_bytes": 268_435_456, "files": rows,
        }
        manifest_path = self.json("assets/assets.json", manifest)
        self.json("result.json", {
            "schema": public.SCHEMA, "status": "passed",
            "source": {"commit": "a" * 40, "tree": "c" * 40, "profile": "release"},
            "assets_digest": public.digest(manifest_path),
            "performance_qualification": False,
        })

    def replace_asset(self, role, data):
        name = self.files[role]
        path = self.write("assets/" + name, data,
                          0o700 if role == "server" else 0o600)
        manifest = json.loads((self.export / "assets/assets.json").read_bytes())
        row = next(item for item in manifest["files"] if item["role"] == role)
        row.update(size=len(data), sha256=public.digest(path))
        manifest_path = self.json("assets/assets.json", manifest)
        result = json.loads((self.export / "result.json").read_bytes())
        result["assets_digest"] = public.digest(manifest_path)
        self.json("result.json", result)

    def replace_receipt(self, change):
        receipt = json.loads((self.export / "assets/receipts/build.json").read_bytes())
        change(receipt)
        data = (json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
        self.replace_asset("receipt", data)

    def assert_rejected(self):
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public.verify(self.export, asset_verifier=lambda *_: None)

    def test_supported_versions_bind_receipt_and_exact_wheel(self):
        for version in ("0.8.16", "0.8.17"):
            with self.subTest(version=version):
                self.success(version)
                self.assertEqual(public.verify(self.export, asset_verifier=lambda *_: None),
                                 "passed")

    def test_receipt_version_and_source_mismatches_are_rejected(self):
        changes = (
            lambda receipt: receipt.update(version="0.8.18"),
            lambda receipt: receipt.update(version="0.8.16"),
            lambda receipt: receipt.pop("version"),
            lambda receipt: receipt["source"].update(commit="d" * 40),
            lambda receipt: receipt["source"].update(tree="d" * 40),
            lambda receipt: receipt.pop("source"),
            lambda receipt: receipt.update(server_sha256="d" * 64),
        )
        for change in changes:
            with self.subTest(change=repr(change)):
                self.success()
                self.replace_receipt(change)
                self.assert_rejected()

    def test_noncanonical_or_extra_receipt_and_wheel_are_rejected(self):
        self.success()
        receipt = self.export / "assets/receipts/build.json"
        self.replace_asset("receipt", receipt.read_bytes() + b" ")
        self.assert_rejected()
        self.success()
        receipt = self.export / "assets/receipts/build.json"
        duplicated = receipt.read_text().replace('"version": "0.8.17"',
                                                  '"version": "0.8.17", "version": "0.8.17"')
        self.replace_asset("receipt", duplicated.encode())
        self.assert_rejected()
        self.success()
        self.write("assets/receipts/extra.json", b"{}")
        self.assert_rejected()
        self.success()
        self.write("assets/wheels/alopex-0.8.16-cp38-abi3-linux_x86_64.whl", b"extra")
        self.assert_rejected()

    def test_wheel_filename_must_match_receipt_version(self):
        for version in ("0.8.16", "0.8.18"):
            with self.subTest(version=version):
                self.success()
                original = self.export / "assets" / self.files["wheel"]
                renamed = self.export / f"assets/wheels/alopex-{version}-cp38-abi3-linux_x86_64.whl"
                original.rename(renamed)
                manifest = json.loads((self.export / "assets/assets.json").read_bytes())
                next(row for row in manifest["files"] if row["role"] == "wheel")["path"] = \
                    "wheels/" + renamed.name
                manifest_path = self.json("assets/assets.json", manifest)
                result = json.loads((self.export / "result.json").read_bytes())
                result["assets_digest"] = public.digest(manifest_path)
                self.json("result.json", result)
                self.assert_rejected()

    def test_success_requires_only_fixed_public_assets(self):
        self.success()
        self.assertEqual(public.verify(self.export, asset_verifier=lambda *_: None),
                         "passed")
        self.write("private-evidence.cms", b"private")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public.verify(self.export, asset_verifier=lambda *_: None)

    def test_changed_asset_is_rejected(self):
        self.success()
        self.write("assets/bin/alopex-server", b"altered", 0o700)
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public.verify(self.export, asset_verifier=lambda *_: None)

    def test_failure_contains_only_fixed_result(self):
        self.json("result.json", {"schema": public.SCHEMA, "status": "failed"})
        self.assertEqual(public.verify(self.export), "failed")
        self.write("failure.log", b"local detail")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public.verify(self.export)

    def test_symlink_is_rejected(self):
        self.success()
        extra = self.export / "assets/extra"
        extra.symlink_to(self.export / "result.json")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public.verify(self.export, asset_verifier=lambda *_: None)

    def test_embedded_host_paths_are_rejected(self):
        self.success()
        self.replace_asset("server", b"binary /home/runner/work/_temp/private")
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public.verify(self.export, asset_verifier=lambda *_: None)
        self.success()
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as package:
            package.writestr("alopex/module.so", b"path+file:///home/runner/work/private")
        self.replace_asset("wheel", stream.getvalue())
        with self.assertRaisesRegex(ValueError, "PUBLIC_OUTPUT"):
            public.verify(self.export, asset_verifier=lambda *_: None)


if __name__ == "__main__":
    unittest.main()
