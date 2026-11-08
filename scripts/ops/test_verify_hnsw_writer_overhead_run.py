import json
import itertools
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import verify_hnsw_writer_overhead_run as verifier


class CleanupTests(unittest.TestCase):
    def test_all_decrypt_archive_audit_and_close_paths_remove_plaintext(self):
        for failure, explicit in itertools.product(
                ('decrypt_1', 'decrypt_2', 'timeout', 'archive', 'audit', 'close', None), (False, True)):
            with self.subTest(failure=failure, explicit=explicit), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                destination = root / 'collected' if explicit else root
                destination.mkdir(exist_ok=True)
                options = {'evidence_directory': destination} if explicit else {}
                (root / 'result.json').write_text('{}')
                evidence = Mock()
                calls = []
                def decrypt(command, **kwargs):
                    output = Path(command[command.index('-out') + 1])
                    self.assertEqual(output.parent.parent, destination)
                    self.assertEqual(Path(command[command.index('-inkey') + 1]), destination / 'native-evidence.key')
                    output.write_text(json.dumps({'synthetic': 'private'}))
                    calls.append(output)
                    if failure == 'timeout': raise subprocess.TimeoutExpired('synthetic', 30)
                    return SimpleNamespace(returncode=int(failure == 'decrypt_' + str(len(calls))))
                def open_archive(path):
                    self.assertTrue(path.exists())
                    if failure == 'archive': raise ValueError('archive failed')
                    return evidence
                if failure == 'close': evidence.close.side_effect = ValueError('close failed')
                with patch.object(verifier, 'BASE', root), \
                     patch.object(verifier.subprocess, 'run', side_effect=decrypt), \
                     patch.object(verifier, 'Evidence', side_effect=open_archive), \
                     patch.object(verifier, 'audit', return_value={'status': 'verified'},
                                  side_effect=ValueError('audit failed') if failure == 'audit' else None):
                    if failure is None:
                        self.assertEqual(verifier.decrypt_and_audit(root, {}, 'controller', **options), {'status': 'verified'})
                    else:
                        with self.assertRaises(Exception): verifier.decrypt_and_audit(root, {}, 'controller', **options)
                self.assertTrue(calls)
                self.assertTrue(all(not path.exists() for path in calls))
                self.assertEqual(list(destination.glob('overhead-audit-*')), [])
                if failure in ('audit', 'close', None): evidence.close.assert_called_once()


if __name__ == '__main__': unittest.main()
