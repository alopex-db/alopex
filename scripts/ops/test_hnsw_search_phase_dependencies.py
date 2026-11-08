import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import hnsw_search_phase_dependencies as subject


class PhaseDependenciesTests(unittest.TestCase):
    def exercise(self, kind):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'venvs').mkdir()
            records = {name: dict(kind=kind, filename=name + '.whl') for name in subject.PREPARE}
            lock = {name: dict(files=[record]) for name, record in records.items()}
            # Reference packages must not be selected, fetched or built.
            lock['hnswlib'] = dict(files=[dict(kind='sdist', filename='unused.tar.gz')])
            lock['faiss-cpu'] = dict(files=[dict(kind='wheel', filename='unused.whl')])
            def run(command, **kwargs):
                if '-c' in command:
                    selected_lock = json.loads(Path(command[-2]).read_bytes())
                    self.assertEqual(set(selected_lock), set(subject.PREPARE))
                    Path(command[-1]).write_text(json.dumps(records))
            budget = Mock()
            budget.run.side_effect = run
            with patch.object(subject, 'download', side_effect=lambda record, *args: record['filename']) as download:
                if kind != 'wheel':
                    with self.assertRaises(ValueError): subject.install(lock, root, budget, {}, root)
                    download.assert_not_called()
                else:
                    paths, selected = subject.install(lock, root, budget, {}, root)
                    self.assertEqual(set(paths), set(subject.PREPARE))
                    self.assertEqual(selected, records)
                    self.assertEqual(download.call_count, len(subject.PREPARE))
                self.assertEqual(budget.run.call_count, 2)
                self.assertTrue(all('wheel' not in call.args[0] for call in budget.run.call_args_list))

    def test_only_locked_input_wheels_are_downloaded(self): self.exercise('wheel')

    def test_source_build_substitution_is_rejected(self): self.exercise('sdist')
