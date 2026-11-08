import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from wal_cost_worker import sha


class Digest(unittest.TestCase):
    def test_streamed_digest_matches_file_without_whole_file_read(self):
        content=b'bounded-digest-fixture\x00'*19
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'native-fixture';path.write_bytes(content)
            with patch.object(Path,'read_bytes',side_effect=AssertionError('whole-file allocation')):
                self.assertEqual(sha(path),hashlib.sha256(content).hexdigest())

    def test_empty_and_missing_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'empty';path.touch()
            self.assertEqual(sha(path),hashlib.sha256(b'').hexdigest())
            with self.assertRaises(FileNotFoundError):sha(path.with_name('missing'))


if __name__=='__main__':unittest.main()
