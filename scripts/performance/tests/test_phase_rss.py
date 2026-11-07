import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from phase_rss import PhaseRss, PhaseRssError, parse_status, start_ticks


def stat(ticks=123):
    return "41 (child ) with spaces) S " + " ".join(["0"] * 18 + [str(ticks)] + ["0"] * 3)


@unittest.skipUnless(os.open in os.supports_dir_fd and hasattr(os, "O_NOFOLLOW"),
                     "POSIX directory descriptors required")
class PhaseRssTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.process = self.root / "41"
        self.process.mkdir()
        (self.process / "stat").write_text(stat())
        self.status(10, 12)
        (self.process / "clear_refs").write_text("")

    def status(self, rss, peak):
        (self.process / "status").write_text(f"Name: child\nVmRSS: {rss} kB\nVmHWM: {peak} kB\n")

    def observer(self):
        result = PhaseRss(41, self.root)
        self.addCleanup(result.close)
        return result

    def test_stat_identity_handles_parentheses_and_spaces(self):
        self.assertEqual(start_ticks(stat()), 123)
        for invalid in ("", "41 (child) S 0", stat("oops")):
            with self.subTest(invalid=invalid), self.assertRaises(PhaseRssError):
                start_ticks(invalid)

    def test_counter_units_and_missing_data_are_not_guessed(self):
        self.assertEqual(parse_status("VmRSS: 1 kB\nVmHWM: 2 kB\n"),
                         {"rss_bytes": 1024, "high_water_bytes": 2048})
        for invalid in ("", "VmRSS: 1 kB", "VmRSS: -1 kB\nVmHWM: 2 kB",
                        "VmRSS: 1 MB\nVmHWM: 2 kB", "VmRSS: 3 kB\nVmHWM: 2 kB",
                        "VmRSS: 1 kB\nVmHWM: 2 kB\nVmHWM: 2 kB"):
            with self.subTest(invalid=invalid), self.assertRaises(PhaseRssError):
                parse_status(invalid)

    def test_reset_and_interval_report_keep_both_boundary_values(self):
        observer = self.observer()
        observer.begin("insert")
        self.assertEqual((self.process / "clear_refs").read_bytes(), b"5\n")
        self.status(14, 20)
        result = observer.finish("insert")
        self.assertEqual(result["after_reset"]["rss_bytes"], 10240)
        self.assertEqual(result["after"]["rss_bytes"], 14336)
        self.assertEqual(result["kernel_reported_peak_rss_bytes"], 20480)
        self.assertIsNone(observer.active)

    def test_interval_order_is_checked(self):
        observer = self.observer()
        with self.assertRaises(PhaseRssError):
            observer.finish("insert")
        observer.begin("insert")
        for action in (lambda: observer.begin("next"), lambda: observer.finish("next")):
            with self.assertRaises(PhaseRssError):
                action()

    def test_pid_reuse_is_rejected(self):
        observer = self.observer()
        observer.begin("insert")
        (self.process / "stat").write_text(stat(124))
        with self.assertRaises(PhaseRssError):
            observer.finish("insert")

    def test_open_directory_does_not_follow_replaced_pid_path(self):
        observer = self.observer()
        self.process.rename(self.root / "retired")
        self.process.mkdir()
        (self.process / "stat").write_text(stat(999))
        (self.process / "clear_refs").write_text("unchanged")
        observer.begin("insert")
        self.assertEqual((self.process / "clear_refs").read_text(), "unchanged")
        self.assertEqual((self.root / "retired" / "clear_refs").read_text(), "5\n")

    def test_missing_or_unwritable_reset_fails_before_phase_starts(self):
        observer = self.observer()
        (self.process / "clear_refs").unlink()
        with self.assertRaises(PhaseRssError):
            observer.begin("insert")
        self.assertIsNone(observer.active)

    def test_short_reset_write_is_rejected(self):
        observer = self.observer()
        with patch("phase_rss.os.write", return_value=1), self.assertRaises(PhaseRssError):
            observer.begin("insert")
        self.assertIsNone(observer.active)

    def test_decreasing_high_water_mark_is_missing_evidence(self):
        observer = self.observer()
        observer.begin("insert")
        self.status(10, 11)
        with self.assertRaises(PhaseRssError):
            observer.finish("insert")

    def test_counter_disappearance_is_not_zero(self):
        observer = self.observer()
        observer.begin("insert")
        (self.process / "status").unlink()
        with self.assertRaises(PhaseRssError):
            observer.finish("insert")

    def test_close_is_idempotent_and_disables_reads(self):
        observer = self.observer()
        observer.close()
        observer.close()
        with self.assertRaises(PhaseRssError):
            observer.snapshot()

    def test_other_uid_and_symlink_are_rejected(self):
        with patch("phase_rss.os.getuid", return_value=os.getuid() + 1), self.assertRaises(PhaseRssError):
            self.observer()
        self.process.rename(self.root / "retired")
        self.process.symlink_to(self.root / "retired")
        with self.assertRaises(PhaseRssError):
            self.observer()


if __name__ == "__main__":
    unittest.main()
