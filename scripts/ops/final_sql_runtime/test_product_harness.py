"""Candidate SQL helper files are pinned independently of the checkout."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.ops.final_sql_runtime import product_harness as product


class ProductHarnessTests(unittest.TestCase):
    OLD_COMMIT = "d7818774379b6d451ebdc2742778114f1e53c748"
    OLD_TREE = "82e8b7b3925376d58b60f90e0e03343e3cea905d"

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.checkout = Path(temporary.name).resolve() / "checkout"
        self.root = self.checkout / "scripts/performance"
        self.root.mkdir(parents=True)
        (self.root / "fixtures").mkdir()
        self.data = {name: name.encode() for name in product.FILES}
        self.spec = {name: (len(data), hashlib.sha256(data).hexdigest())
                     for name, data in self.data.items()}
        for name, data in self.data.items():
            (self.root / name).write_bytes(data)

    def verify(self, head=product.COMMIT, tree=product.TREE):
        with patch.object(product, "FILES", self.spec), patch.object(
            product, "_git", side_effect=lambda _checkout, expression:
                head if expression == "HEAD" else tree):
            return product.verify(self.checkout)

    def test_exact_closure_and_fixed_hash_shapes(self):
        self.assertTrue(all(size > 0 and len(digest) == 64 for size, digest
                            in product.FILES.values()))
        root, records = self.verify()
        self.assertEqual(root, self.root)
        self.assertEqual(set(records), set(product.FILES))

    def test_wrong_source_or_changed_bytes_fail(self):
        self.assertEqual(product.COMMIT, "426418099c4337e73f5ced4444215a640898ed5a")
        self.assertEqual(product.TREE, "84f2b38c5a1e0aa9becd1527a2de9cb5356e4b93")
        with self.assertRaisesRegex(ValueError, "PRODUCT_HARNESS"):
            self.verify(head=self.OLD_COMMIT)
        with self.assertRaisesRegex(ValueError, "PRODUCT_HARNESS"):
            self.verify(tree=self.OLD_TREE)
        with self.assertRaisesRegex(ValueError, "PRODUCT_HARNESS"):
            self.verify(head="a" * 40)
        with self.assertRaisesRegex(ValueError, "PRODUCT_HARNESS"):
            self.verify(tree="b" * 40)
        (self.root / "parity_v0811_measure.py").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "PRODUCT_HARNESS"):
            self.verify()

    def test_symlinked_fixture_rejected(self):
        path = self.root / "fixtures/sql-v0.8.11-curated.json"
        path.unlink()
        path.symlink_to(self.root / "parity_v0811_measure.py")
        with self.assertRaisesRegex(ValueError, "PRODUCT_HARNESS"):
            self.verify()

    def test_symlinked_source_directory_rejected(self):
        scripts = self.checkout / "scripts"
        moved = self.checkout / "moved-scripts"
        scripts.rename(moved)
        scripts.symlink_to(moved, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "PRODUCT_HARNESS"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
