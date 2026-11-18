"""Synthetic checks for a narrower, owned offline dpkg container contract."""
import copy
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from offline_closure_lifecycle import (create_arguments, inspect_offline_container,
                                       run_candidate_lifecycle, captured_inputs)
from candidate_closure import SCHEMA
from lifecycle import Run
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
                                   "swap_bytes": 0, "pids": 256,
                                   "run_seconds": 900},
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


class OwnedLifecycleTests(unittest.TestCase):
    def setUp(self):
        OfflineLifecycleTests.setUp(self)
        self.profile["private_output"] = str(Path(self.temp.name) / "runs")
        self.profile["storage"] = {"reserve_bytes": 1_000_000_000,
                                   "stop_bytes": 40_000_000_000}
        self.docker.profile = self.profile
        self.live["State"]["Running"] = False
        self.live["Config"]["Labels"]["org.alopex.test-env.token"] = None
        self.removed = False
        self.started = False
        self.stopped = False
        self.monitor = Mock(cleanup_mode=False)
        self.monitor.failure = None
        self.budget = Mock()
        self.budget.used.return_value = 23_000_000_000
        output = Path(self.temp.name) / "signed-index-export"
        output.mkdir()
        (output / "private-evidence.cms").write_bytes(b"selected ciphertext")
        self.signed_receipt = {"schema": "alopex.signed-index-capture/v1",
                               "case": "SIGNED_INDEX_CAPTURE", "outcome": "passed",
                               "error_code": None,
                               "capture_run_id": self.profile["run_id"] + "-trace-resolve",
                               "proof_sha256": "a" * 64,
                               "evidence_sha256": hashlib.sha256(
                                   b"selected ciphertext").hexdigest()}
        (output / "public.json").write_text(json.dumps(self.signed_receipt))

    def call(self, kind, action, *rest):
        if kind == "image" and action == "inspect":
            return json.dumps([self.image])
        if kind == "create":
            self.live["Id"] = IDENTITY
            labels = [part for part in (kind, action, *rest)
                      if isinstance(part, str) and
                      part.startswith("org.alopex.test-env.token=")]
            self.live["Config"]["Labels"]["org.alopex.test-env.token"] = \
                labels[0].split("=", 1)[1]
            return IDENTITY
        if kind == "start":
            self.live["State"]["Running"] = True
            self.started = True
            return IDENTITY
        if kind == "container" and action == "inspect":
            return json.dumps([self.live])
        raise AssertionError("unexpected Docker call")

    def owned_inspect(self, item):
        if self.removed:
            return None
        return {"kind": "container", "name": item["name"], "id": IDENTITY,
                "labels": self.live["Config"]["Labels"],
                "running": self.live["State"]["Running"]}

    def owned_stop(self, identity):
        self.assertEqual(identity, IDENTITY)
        self.live["State"]["Running"] = False
        self.stopped = True

    def owned_remove(self, item):
        self.assertFalse(self.live["State"]["Running"])
        self.removed = True

    def execute(self, candidate=None, failure=None):
        self.docker.prerequisites.return_value = None
        self.docker.inspect.side_effect = self.owned_inspect
        self.docker.stop.side_effect = self.owned_stop
        self.docker.remove.side_effect = self.owned_remove
        self.docker.call.side_effect = self.call
        result = candidate or {"schema": SCHEMA, "approval": "unapproved",
                               "formal_lock": False}
        behavior = failure if failure is not None else result
        with patch("offline_closure_lifecycle.captured_inputs",
                   return_value=({}, {}, "a" * 64, {}, "", "b" * 64)), \
             patch("offline_closure_lifecycle.candidate_closure",
                   side_effect=behavior if isinstance(behavior, BaseException) else None,
                   return_value=None if isinstance(behavior, BaseException) else behavior), \
             patch.dict(os.environ, {"GITHUB_WORKSPACE": self.temp.name}):
            return run_candidate_lifecycle(
                self.profile, {"baseline_bytes": 22_000_000_000},
                Path(self.temp.name), None, self.signed_receipt,
                admission=lambda *_: (self.budget, None),
                monitor_factory=lambda *_: self.monitor,
                docker_factory=lambda *_: self.docker)

    def test_success_stops_collects_and_removes_owned_container(self):
        result = self.execute()
        self.assertEqual(result["schema"], SCHEMA)
        self.assertTrue(self.started and self.stopped and self.removed)
        self.monitor.start.assert_called_once()
        self.monitor.stop.assert_called_once()
        run = Path(self.profile["private_output"]) / RUN_ID
        self.assertEqual(json.loads((run / "run.json").read_text())["phase"], "deleted")
        self.assertEqual(json.loads((run / "collected/result.json").read_text())["status"],
                         "pending_monitor")
        self.assertEqual(json.loads((run / "final-result.private.json").read_text())["status"],
                         "candidate")

    def test_parser_failure_still_cleans_and_preserves_failure(self):
        with self.assertRaisesRegex(LockError, "closure rejected"):
            self.execute(failure=LockError("closure rejected"))
        self.assertTrue(self.stopped and self.removed)
        run = Path(self.profile["private_output"]) / RUN_ID
        self.assertEqual(json.loads((run / "run.json").read_text())["phase"], "deleted")
        self.assertEqual(json.loads((run / "collected/result.json").read_text())["status"],
                         "failed")
        self.assertEqual(json.loads((run / "final-result.private.json").read_text())["status"],
                         "failed")

    def test_only_package_closure_failure_is_diagnostic(self):
        with self.assertRaisesRegex(LockError, "unsatisfied Depends"):
            self.execute(failure=LockError("unsatisfied Depends: libx"))
        run = Path(self.profile["private_output"]) / RUN_ID
        final = json.loads((run / "final-result.private.json").read_text())
        self.assertEqual(final["error_code"], "CLOSURE_MISMATCH")
        self.assertEqual(json.loads((run / "run.json").read_text())["phase"], "deleted")

    def test_proof_error_is_not_diagnostic(self):
        with self.assertRaisesRegex(LockError, "candidate proof digest"):
            self.execute(failure=LockError("candidate proof digest"))
        run = Path(self.profile["private_output"]) / RUN_ID
        final = json.loads((run / "final-result.private.json").read_text())
        self.assertNotEqual(final["error_code"], "CLOSURE_MISMATCH")

    def test_owner_drift_blocks_removal_and_retains_cleanup_error(self):
        original = self.owned_inspect
        def drift(item):
            result = original(item)
            result["labels"] = {**result["labels"],
                                "org.alopex.test-env.token": "wrong"}
            return result
        self.owned_inspect = drift
        with self.assertRaisesRegex(LockError, "offline cleanup failed"):
            self.execute()
        self.assertFalse(self.removed)
        run = Path(self.profile["private_output"]) / RUN_ID
        self.assertTrue((run / "cleanup-error.private.json").exists())

    def test_stale_sealed_receipt_stops_before_container_create(self):
        self.signed_receipt["proof_sha256"] = "0" * 64
        with self.assertRaisesRegex(LockError, "signed receipt identity"):
            self.execute()
        self.assertFalse(self.started)
        self.monitor.start.assert_not_called()

    def test_rejects_wrong_schema_run_or_ciphertext_before_create(self):
        public = Path(self.temp.name) / "signed-index-export/public.json"
        cms = Path(self.temp.name) / "signed-index-export/private-evidence.cms"
        for field, value in (("schema", "other/v1"),
                             ("case", "OTHER"),
                             ("capture_run_id", "other-run")):
            with self.subTest(field=field):
                original = self.signed_receipt[field]
                self.signed_receipt[field] = value
                public.write_text(json.dumps(self.signed_receipt))
                with self.assertRaisesRegex(LockError, "signed receipt identity"):
                    self.execute()
                self.signed_receipt[field] = original
        public.write_text(json.dumps(self.signed_receipt))
        cms.write_bytes(b"different ciphertext")
        with self.assertRaisesRegex(LockError, "signed receipt identity"):
            self.execute()
        self.monitor.start.assert_not_called()

    def test_async_monitor_failure_cannot_be_candidate_success(self):
        self.monitor.failure = LockError("asynchronous capacity failure")
        with self.assertRaisesRegex(LockError, "asynchronous capacity failure"):
            self.execute()
        run = Path(self.profile["private_output"]) / RUN_ID
        self.assertTrue(self.stopped and self.removed)
        self.assertEqual(json.loads((run / "final-result.private.json").read_text())["status"],
                         "failed")
        self.assertFalse((run / "collected/candidate.private.json").exists())

    def test_monitor_failure_at_stop_overrides_pending_candidate(self):
        self.monitor.stop.side_effect = lambda: setattr(
            self.monitor, "failure", LockError("late capacity failure"))
        with self.assertRaisesRegex(LockError, "late capacity failure"):
            self.execute()
        run = Path(self.profile["private_output"]) / RUN_ID
        self.assertEqual(json.loads((run / "collected/result.json").read_text())["status"],
                         "pending_monitor")
        self.assertEqual(json.loads((run / "final-result.private.json").read_text())["status"],
                         "failed")

    def test_capture_inputs_stay_inside_verified_collection(self):
        capture_id = self.profile["run_id"] + "-trace-resolve"
        run = Run(self.profile["private_output"], capture_id).create({
            "base": BASE, "resolver_sha256": "b" * 64})
        run.transition("stopped")
        def collect(staging):
            (staging / "result.json").write_text("{}\n")
            (staging / "capture.status").write_text("capture-complete\n")
            (staging / "apt-uris.txt").write_text("test\n")
            (staging / "packages").mkdir()
            (staging / "packages" / "index").write_bytes(b"Package: test\n")
        run.collect(collect)
        run.clean(lambda _: None, lambda _: None)
        proof = run.root / "proof"
        proof.mkdir(mode=0o700)
        (proof / "base.json").write_text("{}\n")
        auth = {"indexes": [{"name": "index-1"}],
                "resolver_sha256": "b" * 64, "signature_verified": True}
        (proof / "authentication.json").write_text(json.dumps(auth) + "\n")
        proof_hash = hashlib.sha256(json.dumps(
            auth, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        (proof / "proof.sha256").write_text(proof_hash + "\n")
        index_path = run.root / "collected" / "packages" / "index"
        (proof / "indexes.json").write_text(json.dumps({
            "index-1": str(index_path)}) + "\n")
        self.assertEqual(captured_inputs(self.profile)[3],
                         {"index-1": b"Package: test\n"})
        (proof / "indexes.json").write_text(json.dumps({
            "index-1": str(proof / "base.json")}) + "\n")
        with self.assertRaisesRegex(LockError, "signed index path"):
            captured_inputs(self.profile)
        (proof / "proof.sha256").write_text("0" * 64 + "\n")
        with self.assertRaisesRegex(LockError, "signed proof digest"):
            captured_inputs(self.profile)


if __name__ == "__main__":
    unittest.main()
