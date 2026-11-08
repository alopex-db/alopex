import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import hnsw_search_phase_venv as subject


class VenvTests(unittest.TestCase):
    def test_retained_bundled_wheels_match_and_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, records = subject.bundled_wheels(root)
            self.assertEqual(subject.bundled_wheels(root), (directory, records))
            for row in records:
                self.assertEqual(subject.digest((directory / row['filename']).read_bytes()), row['sha256'])
            (directory / records[0]['filename']).write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'changed'):
                subject.bundled_wheels(root)

    def test_existing_or_unowned_venv_is_rejected_before_command(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'venvs').mkdir()
            (root / 'venvs/existing').mkdir()
            budget = Mock()
            for target in (root / 'venvs/existing', root / 'outside'):
                with self.assertRaises(ValueError):
                    subject.create_venv(target, root, budget, {}, root / 'install.log')
            budget.run.assert_not_called()

    def test_bootstrap_is_offline_and_receipt_binds_installer(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'venvs').mkdir()
            (root / 'identities').mkdir()
            target = root / 'venvs/example'
            calls = []
            def run(command, **kwargs):
                calls.append(command)
                env = kwargs['env']
                self.assertNotIn('PIP_TARGET', env)
                self.assertNotIn('PYTHONPATH', env)
                self.assertEqual(env['PIP_CONFIG_FILE'], os.devnull)
                if '--without-pip' in command:
                    target.mkdir()
                elif subject.BOOTSTRAP in command:
                    self.assertTrue(all(Path(p).is_file() for p in command[command.index(subject.BOOTSTRAP) + 1:]))
                    self.assertIn("'--no-compile'", subject.BOOTSTRAP)
                    self.assertIn("'--no-index'", subject.BOOTSTRAP)
                elif subject.VERIFY in command:
                    records = json.loads(command[-2])
                    Path(command[-1]).write_text(json.dumps(dict(
                        installed={r['name']: r['version'] for r in records},
                        no_bytecode=True, isolated_prefix=True)))
                else:
                    self.fail('Unexpected bootstrap command')
            budget = Mock()
            budget.run.side_effect = run
            subject.create_venv(target, root, budget,
                dict(PIP_TARGET='untrusted', PYTHONPATH='untrusted'), root / 'install.log')
            receipt = json.loads((root / 'identities/venv-example.json').read_bytes())
            self.assertEqual(len(calls), 3)
            self.assertTrue(receipt['no_bytecode'] and receipt['isolated_prefix'])
            self.assertEqual(receipt['helper_sha256'], subject.digest(Path(subject.__file__).read_bytes()))
            self.assertTrue((root / 'bootstrap-wheels').is_dir())


if __name__ == '__main__': unittest.main()
