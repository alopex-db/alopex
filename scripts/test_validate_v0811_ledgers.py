import unittest

from scripts.validate_v0811_ledgers import LEDGERS, validate


class LedgerContractTests(unittest.TestCase):
    def test_all_ledgers_have_unique_evidenced_entries(self):
        errors = [error for path in LEDGERS for error in validate(path)]
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
