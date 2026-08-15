from __future__ import annotations

import unittest

from .report import EXIT_MISMATCH, EXIT_OK, Report


class StrictReportTests(unittest.TestCase):
    def test_skip_is_allowed_for_interactive_runs(self) -> None:
        report = Report()
        report.skip("s2c", "fixture", "not provisioned")

        self.assertEqual(report.exit_code(), EXIT_OK)

    def test_skip_fails_require_all_runs_and_rendered_exit_agrees(self) -> None:
        report = Report()
        report.skip("s2c", "fixture", "not provisioned")

        self.assertEqual(report.exit_code(require_all=True), EXIT_MISMATCH)
        self.assertIn("-> exit 1", report.render(require_all=True))


if __name__ == "__main__":
    unittest.main()
