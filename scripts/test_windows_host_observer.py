"""Integration contract for the real Windows observer used by WSL verification."""
import time
import unittest

from host_resources import HostMonitor


class WindowsObserverTests(unittest.TestCase):
    def test_open_request_pipe_survives_idle_and_eof_stops_observer(self):
        monitor = HostMonitor()
        try:
            first = monitor.read()
            # This idle fixture crosses the former observer self-exit boundary.
            time.sleep(11)
            self.assertIsNone(monitor.process.poll(), "observer exited while its request pipe remained open")
            second = monitor.read()
            self.assertGreater(second["request_id"], first["request_id"])
            self.assertGreater(second["observed_elapsed_ms"], first["observed_elapsed_ms"] + 10000)
            self.assertLessEqual(second["observation_age_seconds"], 5)
        finally:
            process = monitor.process
            monitor.close()
            if process is not None:
                self.assertEqual(process.poll(), 0, "observer did not exit cleanly after EOF")


if __name__ == "__main__":
    unittest.main()
