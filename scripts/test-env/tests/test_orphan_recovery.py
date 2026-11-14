"""Offline model of a CLI timeout followed by delayed daemon publication."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))

from common import Failure
import envctl
from envctl import recover
from lifecycle import Run


class DelayedDaemon:
    def __init__(self, run):
        self.run = run
        self.live = None
        self.stops = []
        self.removes = []

    def publish(self, *, token=None, name="owned", identity="immutable", running=True):
        self.live = {"kind": "container", "name": name, "id": identity,
                     "labels": {"org.alopex.test-env.owner": "envctl",
                                "org.alopex.test-env.run": self.run.run_id,
                                "org.alopex.test-env.token": token or self.run.record["ownership_token"]},
                     "running": running}

    def inspect(self, item):
        return None if self.live is None else dict(self.live)

    def stop(self, identity):
        self.stops.append(identity)
        if self.live and self.live["id"] == identity:
            self.live["running"] = False

    def remove(self, item):
        self.removes.append(item["id"])
        self.live = None


class Recovery(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.run = Run(self.temporary.name, "case").create({})
        self.run.intent("container", "owned")
        self.daemon = DelayedDaemon(self.run)

    def collect(self):
        self.run.collect(lambda path: (path / "result.json").write_text(json.dumps({"status": "failed"})))

    def test_absent_intent_stays_unresolved_and_late_create_is_adopted(self):
        with self.assertRaises(Failure) as caught:
            recover(self.run, self.daemon)
        self.assertEqual(caught.exception.code, "CLEANUP")
        self.assertEqual(self.run.load().record["phase"], "preparing")
        self.daemon.publish()
        recover(self.run, self.daemon)
        self.assertEqual(self.run.load().record["resources"][0]["id"], "immutable")
        self.assertEqual(self.daemon.stops, ["immutable"])
        self.collect()
        self.run.clean(self.daemon.inspect, self.daemon.remove)
        self.assertEqual(self.run.load().record["phase"], "deleted")

    def test_unbound_intent_cannot_be_deleted_after_collection(self):
        self.run.transition("stopped")
        self.collect()
        with self.assertRaises(Failure):
            self.run.clean(self.daemon.inspect, self.daemon.remove)
        self.assertEqual(self.run.load().record["phase"], "collected")

    def test_wrong_token_and_reused_name_do_not_stop(self):
        for changes in ({"token": "foreign"}, {"name": "other"}):
            with self.subTest(changes=changes):
                self.daemon.publish(**changes)
                with self.assertRaises(Failure):
                    recover(self.run, self.daemon)
                self.assertEqual(self.daemon.stops, [])
                self.assertIsNone(self.run.load().record["resources"][0]["id"])

    def test_wrong_kind_does_not_bind_or_stop(self):
        self.daemon.publish()
        self.daemon.live["kind"] = "volume"
        with self.assertRaises(Failure):
            recover(self.run, self.daemon)
        self.assertEqual(self.daemon.stops, [])
        self.assertIsNone(self.run.load().record["resources"][0]["id"])

    def test_reused_id_and_running_object_prevent_delete(self):
        self.daemon.publish(running=False)
        recover(self.run, self.daemon)
        self.collect()
        for changes in ({"identity": "replacement", "running": False},
                        {"identity": "immutable", "running": True}):
            with self.subTest(changes=changes):
                self.daemon.publish(**changes)
                with self.assertRaises(Failure):
                    self.run.clean(self.daemon.inspect, self.daemon.remove)
                self.assertEqual(self.daemon.removes, [])

    def test_collection_failure_prevents_delete(self):
        self.daemon.publish(running=False)
        recover(self.run, self.daemon)
        with self.assertRaises(OSError):
            self.run.collect(lambda path: (_ for _ in ()).throw(OSError("copy")))
        with self.assertRaises(Failure):
            self.run.clean(self.daemon.inspect, self.daemon.remove)
        self.assertEqual(self.daemon.removes, [])

    def test_late_reappearance_after_remove_prevents_deleted(self):
        self.daemon.publish(running=False)
        recover(self.run, self.daemon)
        self.collect()
        def remove_then_reappear(item):
            self.daemon.remove(item)
            self.daemon.publish(identity="replacement", running=False)
        with self.assertRaises(Failure):
            self.run.clean(self.daemon.inspect, remove_then_reappear)
        self.assertEqual(self.run.load().record["phase"], "collected")

    def test_stage_create_timeout_keeps_original_and_intent(self):
        class TimedOutDocker:
            prefix = ["docker", "--context", "pinned"]
            stopped = False
            def policy(self):
                return []
            def call(self, *args):
                raise Failure("TIMEOUT", "create")
            def stop(self, name):
                self.stopped = True
        profile = {"assets": "offline", "platform": "linux/amd64",
                   "limits": {"memory_bytes": 1024, "pids": 2, "case_seconds": 1}}
        docker = TimedOutDocker()
        with patch.object(envctl, "verify_assets", return_value={"files": []}), \
             patch.object(envctl, "runtime_request", return_value={}):
            with self.assertRaises(Failure) as caught:
                envctl.stage(profile, docker, None, self.run, "image", "input", "work", [])
        self.assertEqual(caught.exception.code, "TIMEOUT")
        self.assertFalse(docker.stopped)
        self.assertIsNone(self.run.load().record["resources"][-1]["id"])
        self.assertFalse((self.run.root / "bundle").exists())

    def test_stage_timeout_absent_then_late_owned_helper_recovers(self):
        run = Run(self.temporary.name, "stage").create({})
        helper = "alopex-stage-stage"
        class StageDocker:
            prefix = ["docker", "--context", "pinned"]
            live = None
            stopped = []
            removed = []
            def policy(self):
                return []
            def call(self, *args):
                raise Failure("TIMEOUT", "stage create")
            def inspect(self, item):
                return None if self.live is None else dict(self.live)
            def stop(self, identity):
                self.stopped.append(identity)
                self.live["running"] = False
            def remove(self, item):
                self.removed.append(item["id"])
                self.live = None
        docker = StageDocker()
        profile = {"assets": "offline", "platform": "linux/amd64",
                   "limits": {"memory_bytes": 1024, "pids": 2, "case_seconds": 1}}
        with patch.object(envctl, "verify_assets", return_value={"files": []}), \
             patch.object(envctl, "runtime_request", return_value={}):
            with self.assertRaises(Failure) as caught:
                envctl.stage(profile, docker, None, run, "image", "input", "work", [])
        self.assertEqual(caught.exception.code, "TIMEOUT")
        self.assertEqual(docker.stopped, [])
        with self.assertRaises(Failure) as caught:
            recover(run, docker)
        self.assertEqual(caught.exception.code, "CLEANUP")
        docker.live = {"kind": "container", "name": helper, "id": "late-helper",
                       "running": True,
                       "labels": {"org.alopex.test-env.owner": "envctl",
                                  "org.alopex.test-env.run": run.run_id,
                                  "org.alopex.test-env.token": run.record["ownership_token"]}}
        recover(run, docker)
        self.assertEqual(run.record["resources"][0]["id"], "late-helper")
        self.assertEqual(docker.stopped, ["late-helper"])
        run.collect(lambda path: (path / "result.json").write_text(json.dumps({"status": "failed"})))
        run.clean(docker.inspect, docker.remove)
        self.assertEqual(run.record["phase"], "deleted")

    def test_docker_cli_timeout_is_categorized_without_claiming_settlement(self):
        import subprocess
        from docker_backend import Docker
        with patch("docker_backend.subprocess.run", side_effect=subprocess.TimeoutExpired(["docker"], 30)):
            with self.assertRaises(Failure) as caught:
                Docker({"docker_context": "pinned"}).call("create")
        self.assertEqual(caught.exception.code, "TIMEOUT")

    def test_primary_create_timeout_preserves_failure_then_adopts_late_object(self):
        class PrimaryDocker:
            def __init__(self):
                self.run = None
                self.live = {}
                self.stops = []
                self.removes = []

            def create_volume(self, run, suffix):
                self.run = run
                name = "alopex-test-" + run.run_id + "-" + suffix
                run.intent("volume", name)
                identity = name + "@created"
                run.resource("volume", name, identity)
                self.live[name] = self.item("volume", name, identity)
                return name

            def item(self, kind, name, identity, running=False):
                return {"kind": kind, "name": name, "id": identity, "running": running,
                        "labels": {"org.alopex.test-env.owner": "envctl",
                                   "org.alopex.test-env.run": self.run.run_id,
                                   "org.alopex.test-env.token": self.run.record["ownership_token"]}}

            def arguments(self, *args):
                return ["create"]

            def call(self, *args):
                raise Failure("TIMEOUT", "primary create")

            def inspect(self, item):
                value = self.live.get(item["name"])
                return None if value is None else dict(value)

            def stop(self, identity):
                self.stops.append(identity)
                for value in self.live.values():
                    if value["id"] == identity:
                        value["running"] = False

            def remove(self, item):
                self.removes.append(item["id"])
                del self.live[item["name"]]

        docker = PrimaryDocker()
        profile = {"private_output": self.temporary.name, "run_id": "primary",
                   "assets": "offline", "platform": "linux/amd64"}
        class Budget:
            def before(self, operation):
                pass
        with patch.object(envctl, "verified_image", return_value="sha256:image"), \
             patch.object(envctl, "verify_assets", return_value={"files": [{"role": "harness", "sha256": "h"}]}), \
             patch.object(envctl, "digest", return_value="assets"), \
             patch.object(envctl, "source_identity", return_value={}), \
             patch.object(envctl, "stage"):
            with self.assertRaises(Failure) as caught:
                envctl.execute(profile, docker, Budget(), [])
        self.assertEqual(caught.exception.code, "TIMEOUT")
        run = Run(self.temporary.name, "primary").load()
        self.assertEqual(json.loads((run.root / "failure.json").read_text())["error_code"], "TIMEOUT")
        self.assertEqual(run.record["phase"], "preparing")
        self.assertIsNone(run.record["resources"][-1]["id"])
        self.assertEqual(docker.stops, [])
        with self.assertRaises(Failure):
            recover(run, docker)
        self.assertEqual(run.record["phase"], "preparing")
        name = "alopex-test-primary"
        docker.live[name] = docker.item("container", name, "late-id", running=True)
        recover(run, docker)
        self.assertEqual(run.record["resources"][-1]["id"], "late-id")
        self.assertEqual(docker.stops, ["late-id"])
        run.collect(lambda path: (path / "result.json").write_text(json.dumps({"status": "failed"})))
        run.clean(docker.inspect, docker.remove)
        self.assertEqual(run.record["phase"], "deleted")


if __name__ == "__main__":
    unittest.main()
