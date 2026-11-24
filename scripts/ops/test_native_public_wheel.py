"""Offline checks for the public wheel SBOM boundary."""
from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from zipfile import ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parent))
from native_public_wheel import normalize_wheel_sbom


COMMIT = "d7818774379b6d451ebdc2742778114f1e53c748"
HISTORICAL = (Path(__file__).resolve().parents[4] / ".tmp" /
              "v0817-682-a415-replacement-run-44829488487.private" / "assets" /
              "wheels" / "alopex-0.8.17-cp38-abi3-linux_x86_64.whl")
SBOM = "alopex-0.8.17.dist-info/sboms/alopex-py.cyclonedx.json"
RECORD = "alopex-0.8.17.dist-info/RECORD"
ROOT = Path("/build/source")
OLD = "path+file:///build/source/crates/alopex-core#0.8.17"


def digest(data):
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


def synthetic(path, *, extra_ref=None, include_sbom=True):
    document = {"bomFormat": "CycloneDX", "specVersion": "1.5",
                "components": [{"bom-ref": OLD, "name": "alopex-core"}],
                "dependencies": [{"ref": OLD, "dependsOn": [OLD]}]}
    if extra_ref:
        document["components"].append({"bom-ref": extra_ref, "name": "another"})
    files = {"alopex/__init__.py": b"pass\n"}
    if include_sbom:
        files[SBOM] = json.dumps(document).encode()
    rows = [[name, digest(data), str(len(data))] for name, data in files.items()]
    rows.append([RECORD, "", ""])
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerows(rows)
    files[RECORD] = output.getvalue().encode()
    with ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)


def verify_record(archive):
    rows = list(csv.reader(io.StringIO(archive.read(RECORD).decode())))
    assert {row[0] for row in rows} == set(archive.namelist())
    for name, hash_value, size in rows:
        if name == RECORD:
            assert (hash_value, size) == ("", "")
        else:
            data = archive.read(name)
            assert (hash_value, size) == (digest(data), str(len(data)))


class WheelSbomTests(unittest.TestCase):
    def test_synthetic_references_and_record(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = Path(directory) / "synthetic.whl"
            synthetic(wheel)
            with ZipFile(wheel) as archive:
                unaffected = archive.read("alopex/__init__.py")
            proof = normalize_wheel_sbom(wheel, ROOT, COMMIT)
            self.assertEqual(proof["replacements"], 3)
            with ZipFile(wheel) as archive:
                document = json.loads(archive.read(SBOM))
                ref = document["components"][0]["bom-ref"]
                self.assertEqual(ref, "https://github.com/alopex-db/alopex/tree/" +
                                 COMMIT + "/crates/alopex-core#0.8.17")
                self.assertEqual(document["dependencies"],
                                 [{"ref": ref, "dependsOn": [ref]}])
                self.assertEqual(archive.read("alopex/__init__.py"), unaffected)
                verify_record(archive)

    def test_rejects_unknown_absolute_ref_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = Path(directory) / "synthetic.whl"
            synthetic(wheel, extra_ref="path+file:///other/source/crates/other#1")
            before = wheel.read_bytes()
            with self.assertRaisesRegex(ValueError, "WHEEL_SBOM_INVALID"):
                normalize_wheel_sbom(wheel, ROOT, COMMIT)
            self.assertEqual(wheel.read_bytes(), before)

    def test_rejects_reference_collision_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = Path(directory) / "synthetic.whl"
            target = "https://github.com/alopex-db/alopex/tree/" + COMMIT + "/crates/alopex-core#0.8.17"
            synthetic(wheel, extra_ref=target)
            before = wheel.read_bytes()
            with self.assertRaisesRegex(ValueError, "WHEEL_SBOM_INVALID"):
                normalize_wheel_sbom(wheel, ROOT, COMMIT)
            self.assertEqual(wheel.read_bytes(), before)

    def test_rejects_missing_sbom_without_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = Path(directory) / "synthetic.whl"
            synthetic(wheel, include_sbom=False)
            before = wheel.read_bytes()
            with self.assertRaisesRegex(ValueError, "WHEEL_SBOM_INVALID"):
                normalize_wheel_sbom(wheel, ROOT, COMMIT)
            self.assertEqual(wheel.read_bytes(), before)

    @unittest.skipUnless(HISTORICAL.is_file(), "retained historical wheel unavailable")
    def test_retained_wheel_has_26_replacements_and_identical_other_members(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = Path(directory) / "historical.whl"
            shutil.copyfile(HISTORICAL, wheel)
            with ZipFile(HISTORICAL) as archive:
                original = {name: archive.read(name) for name in archive.namelist()}
                document = json.loads(original[SBOM])
                local_ref = next(item["bom-ref"] for item in document["components"]
                                 if item.get("bom-ref", "").startswith("path+file://"))
                root = Path(local_ref.split("/crates/", 1)[0].removeprefix("path+file://"))
            proof = normalize_wheel_sbom(wheel, root, COMMIT)
            self.assertEqual(proof["replacements"], 26)
            with ZipFile(wheel) as archive:
                self.assertEqual(set(archive.namelist()), set(original))
                for name in archive.namelist():
                    if name not in (SBOM, RECORD):
                        self.assertEqual(archive.read(name), original[name])
                    self.assertNotIn(str(root).encode(), archive.read(name))
                verify_record(archive)


if __name__ == "__main__":
    unittest.main()
