"""Offline collected-run and namespace boundary tests; no Docker calls."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from . import container_pair_entrypoint as inside
from . import supervisor
from .test_qualification import fixture, JOB


class FakeRun:
    def __init__(self, root, record):
        self.root, self.record = root, record
        self.verified = False

    def verify_collection(self):
        self.verified = True


class ContainerPairTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.collected = self.root / "collected"
        self.collected.mkdir(mode=0o700)
        for mode in ("on", "off"):
            (self.collected / mode).mkdir(mode=0o700)
        _, self.proof, self.receipt = fixture()
        self.config = {
            "schema": "alopex.sql-four-case-pair/v1", "engine": "alopex-server",
            "case": "select-all", "trial": 0, "modes": ["on", "off"],
            "source": {"commit": self.receipt["source_commit"],
                       "tree": self.receipt["source_tree"], "profile": "release"},
            "assets_digest": self.receipt["assets_digest"],
            "same_job_id": JOB, "fixture_sha256": "a" * 64}
        self.server = {"pid": 201, "start_ticks": 301,
                       "pid_namespace": "pid:[1234]", "net_namespace": "net:[5678]"}
        self.client = {"pid": 202, "start_ticks": 302,
                       "pid_namespace": "pid:[1234]", "net_namespace": "net:[5678]"}
        self.url = "http://127.0.0.1:8080"
        self.resource = {"schema": "alopex.sql-resource-servers/v1",
                         "servers": {"alopex-server": {"pid": 201, "start_ticks": 301}}}
        self.resource_bytes = (json.dumps(self.resource) + "\n").encode()
        self.write("resources.json", self.resource_bytes)
        self.write("subject.json", (json.dumps({"source": self.config["source"],
            "server": {"pid": 201, "start_ticks": 301, "url": self.url}}) + "\n").encode())
        self.report = inside.paired_report(
            self.config, server=self.server, client=self.client, url=self.url,
            image_digest=self.proof["image_digest"],
            profile_sha256=self.proof["profile_sha256"],
            resource_manifest_sha256=supervisor.digest(self.resource_bytes),
            cleanup={"server_reaped": True, "worker_reaped": True,
                     "process_groups_empty": True})
        self.write("result.json", (json.dumps(self.report) + "\n").encode())
        self.run = FakeRun(self.root, {"phase": "deleted", "identities": {
            "image_digest": self.proof["image_digest"],
            "assets_digest": self.config["assets_digest"],
            "source": self.config["source"], "job_id": JOB}})

    def write(self, name, data):
        path = self.collected / name
        path.write_bytes(data)
        path.chmod(0o600)

    def test_collected_run_requires_verified_evidence_and_clean_owner(self):
        with patch.object(supervisor, "verify_output", return_value={"server_timing": []}) as output:
            observed = supervisor.run_collected_pair(
                self.config, self.run, self.proof, self.receipt)
        self.assertTrue(self.run.verified)
        self.assertTrue(observed["owned_resources_cleaned"])
        output.assert_called_once()

    def test_rejects_cleanup_or_namespace_drift(self):
        self.run.record["phase"] = "collected"
        with self.assertRaisesRegex(ValueError, "CLEANUP"):
            supervisor.run_collected_pair(self.config, self.run, self.proof, self.receipt)
        self.run.record["phase"] = "deleted"
        self.report["client"]["pid_namespace"] = "pid:[9999]"
        self.write("result.json", (json.dumps(self.report) + "\n").encode())
        with self.assertRaisesRegex(ValueError, "PAIR_IDENTITY"):
            supervisor.run_collected_pair(self.config, self.run, self.proof, self.receipt)

    def test_rejects_resource_and_image_drift(self):
        self.write("resources.json", b"{}\n")
        with self.assertRaisesRegex(ValueError, "RESOURCE_IDENTITY"):
            supervisor.run_collected_pair(self.config, self.run, self.proof, self.receipt)
        self.write("resources.json", self.resource_bytes)
        self.report["image_digest"] = "0" * 64
        self.write("result.json", (json.dumps(self.report) + "\n").encode())
        with self.assertRaisesRegex(ValueError, "PAIR_FAILED"):
            supervisor.run_collected_pair(self.config, self.run, self.proof, self.receipt)

    def test_entrypoint_rejects_external_or_changed_namespace(self):
        changed = dict(self.client, net_namespace="net:[9999]")
        with self.assertRaisesRegex(ValueError, "PAIR_IDENTITY"):
            inside.validate_process_pair(self.server, changed, self.url)
        with self.assertRaisesRegex(ValueError, "PAIR_IDENTITY"):
            inside.validate_process_pair(self.server, self.client, "http://0.0.0.0:8080")
        with self.assertRaisesRegex(ValueError, "MISSING_PIN"):
            inside.main()


if __name__ == "__main__":
    unittest.main()
