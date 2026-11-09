"""Fixed synthetic fixtures; never reads a host's procfs."""

import unittest
from unittest.mock import patch

import wal_mapping_diagnostic as diagnostic


HEADER_FILE = "00400000-00401000 r-xp 00000000 08:01 42 /fixture/native module.so"
HEADER_FILE_2 = "00401000-00402000 r--p 00001000 08:30d 42 /fixture/native module.so"
HEADER_ANON = "70000000-70001000 rw-p 00000000 00:00 0"
HEADER_SPECIAL = "7fff0000-7fff1000 r-xp 00000000 00:00 0 [vdso]"


def block(header, rss, pss):
    return f"{header}\nSize: 4 kB\nRss: {rss} kB\nPss: {pss} kB\nVmFlags: rd ex\n"


class SmapsParserTests(unittest.TestCase):
    def test_groups_exact_pathnames_and_anonymous_mappings(self):
        raw = "".join((
            block(HEADER_FILE, 4, 2),
            block(HEADER_FILE_2, 4, 3),
            block(HEADER_ANON, 4, 4),
            block(HEADER_SPECIAL, 4, 1),
        ))
        result = diagnostic.parse_smaps(raw)
        self.assertEqual(result["totals"], {"Rss": 16 * 1024, "Pss": 10 * 1024})
        self.assertEqual(len(result["mappings"]), 4)
        groups = {(row["category"], row["pathname"]): row["bytes"]
                  for row in result["groups"]}
        self.assertEqual(groups[("file", "/fixture/native module.so")],
                         {"Rss": 8 * 1024, "Pss": 5 * 1024, "count": 2})
        self.assertEqual(groups[("anonymous", None)]["Pss"], 4 * 1024)
        self.assertEqual(groups[("special", "[vdso]")]["Pss"], 1024)

    def test_missing_duplicate_and_invalid_metrics_fail(self):
        good = block(HEADER_FILE, 4, 2)
        for raw in (
            good.replace("Pss: 2 kB\n", ""),
            good.replace("Size: 4 kB\n", ""),
            good.replace("VmFlags: rd ex\n", ""),
            good.replace("Rss: 4 kB\n", "Rss: 4 kB\nRss: 4 kB\n"),
            good.replace("Pss: 2 kB", "Pss: unknown kB"),
            good.replace("Pss: 2 kB", "Pss: -1 kB"),
            good.replace("Pss: 2 kB", "Pss: 5 kB"),
            good + HEADER_ANON + "\nRss: 4 kB\n",
        ):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                diagnostic.parse_smaps(raw)

    def test_invalid_header_and_input_fail(self):
        for raw in ("", "Rss: 4 kB\n", "bad header\n" + block(HEADER_FILE, 4, 2),
                    block(HEADER_FILE.replace("00401000", "00400000"), 4, 2),
                    block(HEADER_FILE, 4, 2) + "not a smaps field\n"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                diagnostic.parse_smaps(raw)

    def test_self_read_propagates_permission_and_missing_procfs(self):
        for error in (PermissionError("denied"), FileNotFoundError("missing")):
            with patch.object(diagnostic._SELF_SMAPS.__class__, "read_text", side_effect=error):
                with self.assertRaises(type(error)):
                    diagnostic.read_self_smaps()

    def test_boundary_observer_retains_calibration_and_rejects_partial_order(self):
        raw_values = [block(HEADER_FILE, 4, pss) for pss in (1, 2, 2, 3, 3, 4)]
        snapshots = [dict(diagnostic.parse_smaps(raw), raw_smaps=raw) for raw in raw_values]
        observer = diagnostic.BoundaryObserver(read=iter(snapshots).__next__)
        with self.assertRaises(ValueError):
            observer.capture("after_open")
        for phase in diagnostic.BOUNDARIES:
            observer.capture(phase)
        report = observer.report()
        self.assertEqual(report["calibration_delta_bytes"], {"Rss": 0, "Pss": 1024})
        self.assertIs(diagnostic.validate_report(report), report)
        report["rows"][3]["totals"]["Pss"] += 1024
        with self.assertRaises(ValueError):
            diagnostic.validate_report(report)


if __name__ == "__main__":
    unittest.main()
