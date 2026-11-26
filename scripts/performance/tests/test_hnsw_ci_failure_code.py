"""The CI diagnostic exposes only allowlisted module groups."""

import unittest

from scripts.performance.hnsw_ci_failure_code import classify, UNKNOWN


class FailureCodeTests(unittest.TestCase):
    def test_first_allowlisted_failure_and_phase_rss(self):
        log = ("ERROR: failed_case (test_issue681_manual_storage.StorageTests.failed_case)\n"
               "FAIL: other (test_phase_rss.PhaseRssTests.other)\n")
        self.assertEqual(classify(log, {"test_issue681_manual_storage"}),
                         "TEST_ISSUE681_MANUAL_STORAGE")
        self.assertEqual(classify("FAIL: other (test_phase_rss.PhaseRssTests.other)", set()),
                         "TEST_PHASE_RSS")
        self.assertEqual(classify(
            "ERROR: test_phase_rss (test_issue681_manual_storage.StorageTests.test_phase_rss)",
            {"test_issue681_manual_storage"}), "TEST_ISSUE681_MANUAL_STORAGE")
        self.assertEqual(classify(
            "ERROR: test_issue681_manual_storage "
            "(unittest.loader._FailedTest.test_issue681_manual_storage)",
            {"test_issue681_manual_storage"}), "TEST_ISSUE681_MANUAL_STORAGE")

    def test_unknown_and_traceback_text_do_not_become_codes(self):
        self.assertEqual(classify("ERROR: failed (test_issue681_secret.Other.failed)",
                                  {"test_issue681_manual_storage"}), UNKNOWN)
        self.assertEqual(classify("/private/path/test_issue681_manual_storage.py\n"
                                  "Traceback: secret token", {"test_issue681_manual_storage"}),
                         UNKNOWN)
        self.assertEqual(classify("ERROR: _FailedTest.test_other", set()), UNKNOWN)
        self.assertEqual(classify(
            "ERROR: test_issue681_manual_storage (test_other.SomeCase.failed)",
            {"test_issue681_manual_storage"}), UNKNOWN)


if __name__ == "__main__":
    unittest.main()
