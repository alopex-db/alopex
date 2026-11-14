"""Synthetic negative checks for the disposable resolver's prestart boundary."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from capture_signed_indexes import (inspect_resolver, ensure_base_image,
                                    collect_resolver_results, BASE)
from common import Failure
from lifecycle import Run


class FakeDocker:
    def __init__(self, live):
        self.live = live

    def call(self, *args):
        assert args == ("container", "inspect", "resolver-id")
        return json.dumps([self.live])


class ResolverPolicyTests(unittest.TestCase):
    def setUp(self):
        self.profile = {"limits": {"memory_bytes": 1_610_612_736, "pids": 256},
                        "cpu": {"server": [0], "client": [1]},
                        "security": {"seccomp_profile": "/private/seccomp.json"}}
        self.live = {"Mounts": [], "HostConfig": {
            "Privileged": False, "NetworkMode": "bridge", "IpcMode": "private",
            "PidMode": "", "Binds": None, "Devices": [], "PortBindings": {},
            "CapDrop": ["ALL"], "CapAdd": ["CHOWN", "DAC_OVERRIDE", "FOWNER",
                                           "SETUID", "SETGID", "AUDIT_WRITE"],
            "Memory": 1_610_612_736, "MemorySwap": 1_610_612_736,
            "PidsLimit": 256, "Init": True, "CpusetCpus": "0,1",
            "LogConfig": {"Type": "local", "Config": {
                "max-size": "1m", "max-file": "1", "compress": "false"}},
            "SecurityOpt": ["apparmor=docker-default", "seccomp={\"default\":true}"]},
            "Config": {"User": "root", "Image": BASE, "Entrypoint": ["/bin/sh"],
                       "Cmd": ["/resolution/resolve.sh"], "Labels": {
                           "org.alopex.test-env.owner": "envctl",
                           "org.alopex.test-env.token": "token"}}}

    def inspect(self, live=None):
        with patch("capture_signed_indexes.read_json", return_value={"default": True}):
            return inspect_resolver(FakeDocker(live or self.live), "resolver-id",
                                    self.profile, "token")

    def test_accepts_exact_existing_resolver_policy(self):
        self.assertEqual(self.inspect(), self.live)

    def test_rejects_added_mount_and_capability(self):
        for field, value in (("Mounts", [{"Destination": "/host"}]),):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.live)
                changed[field] = value
                with self.assertRaises(Failure):
                    self.inspect(changed)
        changed = copy.deepcopy(self.live)
        changed["HostConfig"]["CapAdd"].append("SYS_ADMIN")
        with self.assertRaises(Failure):
            self.inspect(changed)

    def test_rejects_network_or_policy_drift(self):
        for key, value in (("NetworkMode", "host"), ("CpusetCpus", "0"),
                           ("PidsLimit", 512), ("MemorySwap", -1)):
            with self.subTest(key=key):
                changed = copy.deepcopy(self.live)
                changed["HostConfig"][key] = value
                with self.assertRaises(Failure):
                    self.inspect(changed)
        changed = copy.deepcopy(self.live)
        changed["HostConfig"]["SecurityOpt"] = ["apparmor=unconfined",
                                                    "seccomp={\"default\":true}"]
        with self.assertRaises(Failure):
            self.inspect(changed)


class CaptureLifecycleTests(unittest.TestCase):
    def test_exact_cached_digest_skips_pull_and_missing_digest_pulls_once(self):
        expected = json.dumps([{"Os": "linux", "Architecture": "amd64",
                                "RepoDigests": [BASE]}])

        class ImageDocker:
            prefix = ["docker", "--context", "selected"]

            def __init__(self, missing):
                self.missing = missing
                self.calls = []

            def call(self, *args, missing=False):
                self.calls.append(args)
                if self.missing and len(self.calls) == 1:
                    return None
                return expected

        class Monitor:
            def __init__(self):
                self.commands = []

            def run(self, command, **kwargs):
                self.commands.append(command)

        with tempfile.TemporaryDirectory() as temp:
            run = type("RunRoot", (), {"root": Path(temp)})()
            for missing, pulls in ((False, 0), (True, 1)):
                docker, monitor = ImageDocker(missing), Monitor()
                ensure_base_image(docker, monitor, {"limits": {"run_seconds": 900}}, run)
                self.assertEqual(len(monitor.commands), pulls)
                self.assertEqual(len(docker.calls), pulls + 1)

    def test_failed_resolver_collects_owned_raw_apt_and_gpgv_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Run(Path(temp), "synthetic").create({})
            run.intent("container", "owned")
            run.resource("container", "owned", "container-id")
            run.transition("running")
            run.transition("stopped")

            class Docker:
                prefix = ["docker", "--context", "selected"]

                def __init__(self, valid=True):
                    self.valid = valid

                def inspect(self, item):
                    return {"id": "container-id", "running": False,
                            "labels": {"org.alopex.test-env.owner": "envctl",
                                       "org.alopex.test-env.run": "synthetic",
                                       "org.alopex.test-env.token":
                                       run.record["ownership_token"] if self.valid else "wrong"}}

            class Monitor:
                def run(self, command, **kwargs):
                    destination = Path(command[-1])
                    (destination / "apt-raw.txt").write_bytes(b"failed APT raw bytes")
                    (destination / "noble_InRelease.gpgv-stderr").write_bytes(
                        b"failed signature raw bytes")

            with self.assertRaises(Failure):
                collect_resolver_results(run, Docker(False), Monitor(), "container-id",
                                         {"case_seconds": 60}, 100)
            self.assertEqual(run.record["phase"], "stopped")
            collected = collect_resolver_results(run, Docker(), Monitor(), "container-id",
                                                  {"case_seconds": 60}, 100)
            run.verify_collection()
            self.assertEqual((collected / "apt-raw.txt").read_bytes(), b"failed APT raw bytes")
            self.assertEqual((collected / "noble_InRelease.gpgv-stderr").read_bytes(),
                             b"failed signature raw bytes")
            self.assertEqual(json.loads((collected / "result.json").read_text()), {
                "capture": "failed", "resolver_exit_code": 100,
                "container_id": "container-id"})


if __name__ == "__main__":
    unittest.main()
