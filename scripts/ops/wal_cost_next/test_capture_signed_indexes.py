"""Synthetic negative checks for the disposable resolver's prestart boundary."""
import copy
import json
import unittest
from unittest.mock import patch

from capture_signed_indexes import inspect_resolver, BASE
from common import Failure


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


if __name__ == "__main__":
    unittest.main()
