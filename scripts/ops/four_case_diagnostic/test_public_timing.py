"""Focused public timing boundary and arithmetic tests."""
import json
import math
import os
from pathlib import Path
import tempfile
import unittest

from . import adapter, public_timing


class PublicTimingTests(unittest.TestCase):
    def make_inventory(self, root):
        for case_index, case in enumerate(adapter.CASES):
            for trial in range(3):
                with (public_timing.TimingCellWriter(root, case, trial, "on") as on,
                      public_timing.TimingCellWriter(root, case, trial, "off") as off):
                    writers = {"on": on, "off": off}
                    for sample in range(21):
                        for position, mode in enumerate(adapter.pair_order(trial, sample)):
                            off_seconds = .1 + .01 * case_index + .001 * trial + .0001 * sample
                            seconds = off_seconds + (.001 * (trial + 1) +
                                                     .0001 * (sample - 10) if mode == "on" else 0)
                            writers[mode].append(sample, 2 * sample + position, seconds)
        return public_timing.load_timing_inventory(root)

    def test_full_inventory_and_numeric_recomputation(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as root:
            records = self.make_inventory(root)
            self.assertEqual(len(records), 504)
            self.assertEqual(len(list(Path(root).iterdir())), 24)
            paired = public_timing.summarize_timing_records(records)
            self.assertEqual(public_timing.validate_timing_summary(records, paired), paired)
            for case in adapter.CASES:
                result = paired[case]
                for actual, expected in zip(result["delta_ms"], (1, 2, 3)):
                    self.assertAlmostEqual(actual, expected)
                self.assertAlmostEqual(result["D_ms"], 2)
                self.assertAlmostEqual(result["MAD_ms"], 1)
                self.assertAlmostEqual(result["trial_U_ms"][0],
                                       1 + 2 * 1.4826 * .5 / math.sqrt(21))
                self.assertAlmostEqual(result["U_ms"], 2 + 2 * 1.4826)
                case_index = adapter.CASES.index(case)
                self.assertAlmostEqual(result["bound_ms"],
                                       .05 * (100 + 10 * case_index + 1 + 1))
                self.assertTrue(result["small_overhead"])

    def test_reject_incorrect_published_arithmetic(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as root:
            records = self.make_inventory(root)
            paired = public_timing.summarize_timing_records(records)
            paired[adapter.CASES[0]]["U_ms"] += 1
            with self.assertRaisesRegex(ValueError, "timing_summary_arithmetic"):
                public_timing.validate_timing_summary(records, paired)

    def test_reject_unknown_raw_path_and_duplicate_json_key(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as root:
            self.make_inventory(root)
            path = Path(root) / public_timing.cell_filename(adapter.CASES[0], 0, "on")
            content = path.read_text()
            lines = content.splitlines()
            first = json.loads(lines[0])
            first["local_path"] = "/private/tmp/raw"
            lines[0] = json.dumps(first, separators=(",", ":"))
            path.write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "timing_fields"):
                public_timing.load_timing_inventory(root)
            lines[0] = content.splitlines()[0].replace('"schema":', '"schema":"extra","schema":', 1)
            path.write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "timing_duplicate_field"):
                public_timing.load_timing_inventory(root)

    def test_reject_missing_duplicate_and_nonfinite(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as root:
            records = self.make_inventory(root)
            with self.assertRaisesRegex(ValueError, "timing_inventory"):
                public_timing.summarize_timing_records(records[:-1])
            duplicated = records.copy()
            duplicated[1] = records[0]
            with self.assertRaises(ValueError):
                public_timing.summarize_timing_records(duplicated)
            nonfinite = [dict(record) for record in records]
            nonfinite[0]["elapsed_seconds"] = float("nan")
            with self.assertRaisesRegex(ValueError, "timing_duration"):
                public_timing.summarize_timing_records(nonfinite)
            nonfinite[0]["elapsed_seconds"] = -1
            with self.assertRaisesRegex(ValueError, "timing_duration"):
                public_timing.summarize_timing_records(nonfinite)
            nonfinite[0]["elapsed_seconds"] = 1e308
            with self.assertRaisesRegex(ValueError, "timing_summary_arithmetic"):
                public_timing.summarize_timing_records(nonfinite)

    def test_reject_wrong_order_and_extra_file(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as root:
            records = self.make_inventory(root)
            out_of_order = records.copy()
            out_of_order[0], out_of_order[1] = out_of_order[1], out_of_order[0]
            with self.assertRaises(ValueError):
                public_timing.summarize_timing_records(out_of_order)
            (Path(root) / "raw.jsonl").write_text("private")
            with self.assertRaisesRegex(ValueError, "timing_file_inventory"):
                public_timing.load_timing_inventory(root)

    def test_writer_rejects_duplicate_sample_and_invalid_identifiers(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as root:
            with self.assertRaisesRegex(ValueError, "timing_case"):
                public_timing.TimingCellWriter(root, "../private", 0, "on")
            with public_timing.TimingCellWriter(root, adapter.CASES[0], 0, "off") as writer:
                writer.append(0, 0, 0.0)
                with self.assertRaisesRegex(ValueError, "timing_order"):
                    writer.append(0, 0, 0.0)
                with self.assertRaisesRegex(ValueError, "timing_pair_order"):
                    writer.append(1, 2, 0.0)
                with self.assertRaisesRegex(ValueError, "timing_duration"):
                    writer.append(1, 3, float("inf"))

    def test_root_symlink_and_existing_file_are_rejected(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as parent:
            root = Path(parent) / "timing"
            root.mkdir(mode=0o700)
            linked = Path(parent) / "linked"
            linked.symlink_to(root)
            with self.assertRaisesRegex(ValueError, "timing_directory"):
                public_timing.TimingCellWriter(linked, adapter.CASES[0], 0, "on")
            filename = public_timing.cell_filename(adapter.CASES[0], 0, "on")
            target = root / filename
            target.write_text("preserve")
            with self.assertRaises(FileExistsError):
                public_timing.TimingCellWriter(root, adapter.CASES[0], 0, "on")
            self.assertEqual(target.read_text(), "preserve")

    def test_hardlink_and_oversized_cell_are_rejected(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as root:
            self.make_inventory(root)
            path = Path(root) / public_timing.cell_filename(adapter.CASES[0], 0, "on")
            outside = Path(root).with_name(Path(root).name + "-linked-cell")
            try:
                os.link(path, outside)
                with self.assertRaisesRegex(ValueError, "timing_file"):
                    public_timing.load_timing_inventory(root)
            finally:
                outside.unlink(missing_ok=True)
            path.write_bytes(b"x" * (public_timing.MAX_CELL_BYTES + 1))
            with self.assertRaisesRegex(ValueError, "timing_file"):
                public_timing.load_timing_inventory(root)


if __name__ == "__main__":
    unittest.main()
