import json
from pathlib import Path
import tempfile
import unittest
import sys
import types
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
            with patch.object(subject, 'create_venv') as create, patch.object(subject, 'download', side_effect=lambda record, *args: record['filename']) as download:
                if kind != 'bdist_wheel':
                    with self.assertRaises(ValueError): subject.install(lock, root, budget, {}, root)
                    download.assert_not_called()
                else:
                    paths, selected = subject.install(lock, root, budget, {}, root)
                    self.assertEqual(set(paths), set(subject.PREPARE))
                    self.assertEqual(selected, records)
                    self.assertEqual(download.call_count, len(subject.PREPARE))
                self.assertEqual(budget.run.call_count, 1)
                create.assert_called_once()
                self.assertTrue(all('wheel' not in call.args[0] for call in budget.run.call_args_list))

    def test_only_locked_input_wheels_are_downloaded(self): self.exercise('bdist_wheel')

    def test_source_build_substitution_is_rejected(self): self.exercise('sdist')


class LockedSelectorTests(unittest.TestCase):
    def select(self, lock):
        with tempfile.TemporaryDirectory() as temporary:
            source, output = Path(temporary) / 'lock.json', Path(temporary) / 'selected.json'
            source.write_text(json.dumps(lock))
            tags = types.ModuleType('pip._vendor.packaging.tags')
            tags.sys_tags = lambda: ['supported-test-tag']
            utils = types.ModuleType('pip._vendor.packaging.utils')
            utils.parse_wheel_filename = lambda name: (None, None, None, {'supported-test-tag'})
            with patch.dict(sys.modules, {tags.__name__: tags, utils.__name__: utils}), \
                 patch.object(sys, 'argv', ['selector', str(source), str(output)]):
                exec(subject.SELECTOR_CODE, {})
            return json.loads(output.read_bytes())

    def test_actual_locked_package_types_are_selected(self):
        lock = json.loads(Path(__file__).with_name('hnsw_search.lock.json').read_bytes())
        inputs = {name: lock[name] for name in subject.PREPARE}
        result = self.select(inputs)
        self.assertEqual(set(result), set(subject.PREPARE))
        for name, row in result.items():
            self.assertIn(row, inputs[name]['files'])
            self.assertEqual(row['kind'], 'bdist_wheel')

    def test_selector_rejects_source_only_package(self):
        with self.assertRaisesRegex(AssertionError, 'unavailable'):
            self.select({'input': {'files': [{'kind': 'sdist', 'filename': 'input.tar.gz'}]}})
