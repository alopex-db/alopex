"""Offline negative checks for owned SQL collection attestation."""
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from scripts.ops.final_sql_runtime import collection_attestor as attestor


def write_private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value if type(value) is bytes else json.dumps(value).encode())
    path.chmod(0o600)


class Docker:
    def __init__(self):
        self.survivor = None
        self.unrecorded = False

    def inspect(self, item):
        return self.survivor if self.survivor == item else None

    def call(self, *args):
        return "extra-resource" if self.unrecorded else ""


class CollectionAttestorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "run"
        self.root.mkdir()
        self.run_id = attestor.container.owned_run_id("preflight", attestor.matrix.plan()[0])
        self.assets = "a" * 64
        self.image = "b" * 64
        self.controller = "c" * 40
        self.digest = "d" * 64
        self.docker = Docker()
        self.resources = [
            {"kind": "container", "name": "alopex-test-" + self.run_id, "id": "container-id"},
            {"kind": "volume", "name": "alopex-test-" + self.run_id + "-input", "id": "input@now"},
            {"kind": "volume", "name": "alopex-test-" + self.run_id + "-work", "id": "work@now"},
        ]
        self.record = {"schema": "alopex.test-env-run/v1", "run_id": self.run_id,
                       "phase": "deleted", "ownership_token": "f" * 32,
                       "resources": self.resources, "expected_cases": [],
                       "identities": {"assets_digest": self.assets,
                                      "image_digest": self.image,
                                      "controller_commit": self.controller,
                                      "controller_digest": self.digest}}
        write_private(self.root / "run.json", self.record)
        report = b'{"status":"passed"}'
        write_private(self.root / "collected/result.json", report)
        write_private(self.root / "collection.json", {"result.json": hashlib.sha256(report).hexdigest()})

    def check(self):
        return attestor._run(self.root, self.run_id, self.assets, self.image,
                             self.controller, self.digest, self.docker)

    def test_phase_prefixed_ids_are_unique_across_52_runs(self):
        runs = attestor.expected_runs()
        ids = [attestor.container.owned_run_id(kind, row) for kind, row in runs]
        self.assertEqual((len(runs), len(set(ids))), (52, 52))
        self.assertNotEqual(ids[0], ids[4])

    def test_owned_deleted_collection_is_accepted(self):
        self.assertEqual(self.check()["run_id"], self.run_id)

    def test_mutated_record_manifest_and_extra_file_fail(self):
        mutations = [
            lambda: self.record.update(phase="collected"),
            lambda: self.record["resources"].pop(),
            lambda: self.record["identities"].update(image_digest="0" * 64),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                original = json.loads(json.dumps(self.record))
                mutate()
                write_private(self.root / "run.json", self.record)
                with self.assertRaises(ValueError):
                    self.check()
                self.record = original
        write_private(self.root / "run.json", self.record)
        write_private(self.root / "collected/extra.json", {})
        with self.assertRaisesRegex(ValueError, "COLLECTION_MANIFEST"):
            self.check()

    def test_surviving_and_unrecorded_resources_fail(self):
        self.docker.survivor = self.resources[0]
        with self.assertRaisesRegex(ValueError, "SURVIVING_RESOURCE"):
            self.check()
        self.docker.survivor = None
        self.docker.unrecorded = True
        with self.assertRaisesRegex(ValueError, "SURVIVING_RESOURCE"):
            self.check()

    def test_unprivate_file_fails(self):
        (self.root / "run.json").chmod(0o644)
        with self.assertRaisesRegex(ValueError, "PRIVATE_FILE"):
            self.check()

    def test_harness_member_is_distinct_from_archive_bytes(self):
        path = self.root / "harness.tar"
        member = b"archived SQL worker\n"
        with tarfile.open(path, "w") as archive:
            info = tarfile.TarInfo(attestor.HARNESS_MEMBER)
            info.size = len(member)
            archive.addfile(info, io.BytesIO(member))
        path.chmod(0o600)
        self.assertEqual(attestor._harness_bytes(path), member)
        self.assertNotEqual(attestor.sha(attestor.private_file(path)),
                            attestor.sha(member))


if __name__ == "__main__":
    unittest.main()
