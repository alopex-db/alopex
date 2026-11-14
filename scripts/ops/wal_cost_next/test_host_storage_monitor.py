"""Bounded monitor cancellation without Docker or a host storage scan."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import host_storage_monitor as monitored
from common import Failure
from envctl import recover
from lifecycle import Run


class HostMonitorTests(unittest.TestCase):
    def monitor(self):
        storage = {"reserve_bytes": 1, "stop_bytes": 40_000_000_000}
        result = monitored.HostMonitor(Path("/unused/profile.json"), storage, 100, 250)
        samples = 0

        def sample():
            nonlocal samples
            samples += 1
            if samples > 1:
                raise Failure("CAPACITY", "synthetic cap trip")
            return 100

        result.sample = sample
        return result

    def test_blocked_docker_call_is_cancelled_on_monitor_trip(self):
        monitor = self.monitor()
        with patch.object(monitored, "SAMPLE_SECONDS", 0.05):
            monitor.start()
            docker = monitored.MonitoredDocker({"docker_context": "unused"}, monitor)
            docker.prefix = [sys.executable, "-c", "import time; time.sleep(30)"]
            with self.assertRaises(Failure) as raised:
                docker.call("image", "inspect", "synthetic")
            self.assertEqual(raised.exception.code, "CAPACITY")
            monitor.cleanup_mode = True
            monitor.stop()

    def test_full_host_sample_rejects_over_budget_value(self):
        monitor = monitored.HostMonitor(
            Path("/unused/profile.json"),
            {"reserve_bytes": 1, "stop_bytes": 40_000_000_000}, 100, 250)

        class SampleChild:
            returncode = 0

            def communicate(self, timeout):
                self.timeout = timeout
                return b"400\n", b""

        with patch.object(monitored.subprocess, "Popen", return_value=SampleChild()):
            with self.assertRaises(Failure) as raised:
                monitor.sample()
        self.assertEqual(raised.exception.code, "CAPACITY")
        self.assertEqual(raised.exception.detail, "host_growth")

    def test_stage_growth_has_distinct_fixed_capacity_gate(self):
        monitor = monitored.HostMonitor(
            Path("/unused/profile.json"),
            {"reserve_bytes": 1, "stop_bytes": 40_000_000_000}, 100, 500)
        monitor.stage_baseline, monitor.stage_limit = 150, 200

        class SampleChild:
            returncode = 0

            def communicate(self, timeout):
                return b"400\n", b""

        with patch.object(monitored.subprocess, "Popen", return_value=SampleChild()):
            with self.assertRaises(Failure) as raised:
                monitor.sample()
        self.assertEqual((raised.exception.code, raised.exception.detail),
                         ("CAPACITY", "capture_stage_growth"))

    def test_image_inspect_only_treats_exact_missing_error_as_cache_miss(self):
        monitor = self.monitor()
        docker = monitored.MonitoredDocker({"docker_context": "selected"}, monitor)
        with patch.object(monitor, "run", return_value=(1, "", "permission denied")):
            with self.assertRaises(Failure):
                docker.call("image", "inspect", "pinned-digest", missing=True)
        with patch.object(monitor, "run", return_value=(1, "", "No such image: pinned-digest")):
            self.assertIsNone(docker.call("image", "inspect", "pinned-digest", missing=True))

    def test_nonzero_resolver_exit_is_returned_for_raw_collection(self):
        monitor = self.monitor()
        with tempfile.TemporaryDirectory() as temp:
            code = monitor.run([sys.executable, "-c", "raise SystemExit(17)"],
                               timeout=2, log=Path(temp) / "resolver.log",
                               allow_nonzero=True)
        self.assertEqual(code, 17)

    def test_proof_is_cancelled_and_cleanup_can_run_after_trip(self):
        monitor = self.monitor()
        with tempfile.TemporaryDirectory() as temp, \
             patch.object(monitored, "SAMPLE_SECONDS", 0.05):
            monitor.start()
            with self.assertRaises(Failure) as raised:
                monitor.run([sys.executable, "-c", "import time; time.sleep(30)"],
                            timeout=5, log=Path(temp) / "proof.log")
            self.assertEqual(raised.exception.code, "CAPACITY")
            monitor.cleanup_mode = True
            result = monitor.run([sys.executable, "-c", "print('recovered')"],
                                 timeout=2, capture=True)
            self.assertEqual(result, "recovered")
            run = Run(Path(temp), "synthetic").create({})
            run.intent("container", "owned")
            run.resource("container", "owned", "container-id")
            run.transition("running")

            class OwnedDocker:
                running = True
                present = True

                def inspect(self, item):
                    if not self.present:
                        return None
                    return {"kind": item["kind"], "name": item["name"],
                            "id": "container-id", "running": self.running,
                            "labels": {"org.alopex.test-env.owner": "envctl",
                                       "org.alopex.test-env.run": "synthetic",
                                       "org.alopex.test-env.token":
                                       run.record["ownership_token"]}}

                def stop(self, identity):
                    monitor.run([sys.executable, "-c", "pass"], timeout=2, capture=True)
                    self.running = False

                def remove(self, item):
                    monitor.run([sys.executable, "-c", "pass"], timeout=2, capture=True)
                    self.present = False

            docker = OwnedDocker()
            recover(run, docker)
            self.assertEqual(run.record["phase"], "stopped")
            self.assertFalse(docker.running)
            run.collect(lambda staging: (staging / "result.json").write_text("{}"))
            run.clean(docker.inspect, docker.remove)
            self.assertEqual(run.record["phase"], "deleted")
            self.assertFalse(docker.present)
            monitor.stop()


if __name__ == "__main__":
    unittest.main()
