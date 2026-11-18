"""Synthetic checks for a narrower, owned offline dpkg container contract."""
import copy
import hashlib
import json
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parent))
from offline_closure_lifecycle import create_arguments, inspect_offline_container
from resolve_builder_lock import BASE, LockError

IDENTITY = "a" * 64
IMAGE_ID = "sha256:" + "b" * 64
RUN_ID = "hosted-804-123-trace-lock"
TOKEN = "c" * 32


class OfflineLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        policy = Path(self.temp.name) / "seccomp.json"
        policy.write_text("{}\n")
        self.profile = {"run_id": "hosted-804-123", "platform": "linux/amd64",
                        "execution_mode": "native", "docker_context": "selected",
                        "cpu": {"server": [1], "client": [2]},
                        "limits": {"memory_bytes": 1_610_612_736,
                                   "swap_bytes": 0, "pids": 256},
                        "security": {"lsm_kind": "apparmor",
                                     "lsm_profile": "docker-default",
                                     "seccomp_profile": str(policy),
                                     "seccomp_sha256": hashlib.sha256(
                                         policy.read_bytes()).hexdigest()}}
        self.live = {"Id": IDENTITY, "Image": IMAGE_ID, "Mounts": [],
                     "State": {"Running": True, "Paused": False},
                     "HostConfig": {"Privileged": False, "NetworkMode": "none",
                                    "IpcMode": "private", "PidMode": "",
                                    "ReadonlyRootfs": True, "Binds": None,
                                    "Tmpfs": None, "VolumesFrom": None,
                                    "Devices": [], "PortBindings": {},
                                    "CapDrop": ["ALL"], "CapAdd": [],
                                    "Memory": 1_610_612_736,
                                    "MemorySwap": 1_610_612_736,
                                    "PidsLimit": 256, "Init": True,
                                    "CpusetCpus": "1,2",
                                    "LogConfig": {"Type": "local", "Config": {
                                        "max-size": "1m", "max-file": "1",
                                        "compress": "false"}},
                                    "SecurityOpt": ["seccomp={}",
                                                    "apparmor=docker-default"]},
                     "Config": {"User": "65534:65534", "Image": BASE,
                                "Entrypoint": ["/bin/sh"],
                                "Cmd": ["-c", "sleep 900"],
                                "Labels": {"org.alopex.test-env.owner": "envctl",
                                           "org.alopex.test-env.run": RUN_ID,
                                           "org.alopex.test-env.token": TOKEN}}}
        self.image = {"Id": IMAGE_ID, "Os": "linux", "Architecture": "amd64",
                      "RepoDigests": [BASE]}
        self.docker = Mock()
        self.docker.profile = self.profile
        self.docker.prefix = ["docker", "--context", "selected"]
        self.docker.policy.return_value = ["--security-opt", "seccomp=" +
                                           self.profile["security"]["seccomp_profile"],
                                           "--security-opt", "apparmor=docker-default"]
        self.docker.call.side_effect = self.call

    def call(self, kind, action, _identity):
        return json.dumps([self.live if kind == "container" else self.image])

    def inspect(self, *, running=True):
        return inspect_offline_container(self.docker, IDENTITY, self.profile,
                                         RUN_ID, TOKEN, running=running)

    def test_create_request_and_positive_inspection(self):
        args = create_arguments(self.docker, self.profile, RUN_ID, TOKEN)
        self.assertEqual(args[:3], ["create", "--name", "alopex-test-" + RUN_ID])
        self.assertIn("--network=none", args)
        self.assertIn("--read-only", args)
        self.assertEqual(args[args.index("--user") + 1], "65534:65534")
        self.assertEqual(args[args.index("--cap-drop=ALL")], "--cap-drop=ALL")
        self.assertNotIn("--cap-add", args)
        self.assertNotIn("--mount", args)
        self.assertEqual(args[-4:], ["/bin/sh", BASE, "-c", "sleep 900"])
        self.assertEqual(self.inspect()["Id"], IDENTITY)

    def test_refuses_authority_or_identity_drift(self):
        cases = [
            ("HostConfig", "NetworkMode", "bridge"),
            ("HostConfig", "ReadonlyRootfs", False),
            ("HostConfig", "CapAdd", ["SYS_PTRACE"]),
            ("HostConfig", "Tmpfs", {"/tmp": "rw"}),
            ("HostConfig", "VolumesFrom", ["other"]),
            ("HostConfig", "Privileged", True),
            ("Config", "User", "root"),
            ("Config", "Labels", {}),
            ("State", "Running", False),
        ]
        for section, key, value in cases:
            with self.subTest(section=section, key=key):
                self.live = copy.deepcopy(self.live)
                original = self.live[section][key]
                self.live[section][key] = value
                with self.assertRaisesRegex(LockError, "offline container policy drift"):
                    self.inspect()
                self.live[section][key] = original
        self.image["RepoDigests"] = []
        with self.assertRaisesRegex(LockError, "selected base resolver unavailable"):
            self.inspect()

    def test_refuses_changed_profile_before_create(self):
        for field, value in (("execution_mode", "emulated"),
                             ("platform", "linux/arm64")):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.profile)
                changed[field] = value
                self.docker.profile = changed
                with self.assertRaisesRegex(LockError, "offline profile identity"):
                    create_arguments(self.docker, changed, RUN_ID, TOKEN)
                self.docker.profile = self.profile
        changed = copy.deepcopy(self.profile)
        changed["limits"]["memory_bytes"] = 0
        self.docker.profile = changed
        with self.assertRaisesRegex(LockError, "offline resource profile"):
            create_arguments(self.docker, changed, RUN_ID, TOKEN)

    def test_rejects_different_docker_context_before_create_or_inspect(self):
        self.docker.prefix = ["docker", "--context", "other"]
        with self.assertRaisesRegex(LockError, "offline Docker context drift"):
            create_arguments(self.docker, self.profile, RUN_ID, TOKEN)
        with self.assertRaisesRegex(LockError, "offline Docker context drift"):
            self.inspect()
        self.docker.call.assert_not_called()

    def test_uses_selected_profile_limits_without_fixed_product_values(self):
        selected = copy.deepcopy(self.profile)
        selected["limits"].update(memory_bytes=1_000_000_000, swap_bytes=0, pids=64)
        self.docker.profile = selected
        args = create_arguments(self.docker, selected, RUN_ID, TOKEN)
        self.assertEqual(args[args.index("--memory") + 1], "1000000000")
        self.assertEqual(args[args.index("--pids-limit") + 1], "64")
        self.live["HostConfig"].update(Memory=1_000_000_000,
                                       MemorySwap=1_000_000_000, PidsLimit=64)
        self.assertEqual(inspect_offline_container(
            self.docker, IDENTITY, selected, RUN_ID, TOKEN, running=True)["Id"],
            IDENTITY)


if __name__ == "__main__":
    unittest.main()
