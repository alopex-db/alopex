import copy
import json
import unittest

import hnsw_completion_contract as contract
import hosted_hnsw_completion_stage as control
import test_hnsw_stage_contract as legacy
import test_hosted_hnsw_stage as legacy_control


def configuration(rows=1000, finish='commit'):
    value = legacy_control.config(rows)
    value.update(schema='alopex.hnsw-completion-stage-input/v1', finish=finish)
    return value


class CompletionTests(unittest.TestCase):
    def fixture(self, finish='commit', subject='candidate'):
        args = legacy.StageTests().fixture(subject)
        args[0]['finish'] = finish
        args[3]['finish'] = finish
        args[2][-3]['phase'] = args[2][-2]['phase'] = finish
        args[2][-2]['data']['elapsed_ns'] = 321
        args[2][-1]['data'].update(finish=finish, result_validated=True)
        args[1]['phase_kernel_rss'][-1]['phase'] = finish
        return args

    def test_all_four_subject_mode_combinations_keep_completion_separate(self):
        for finish in ('commit', 'rollback'):
            for subject in ('before', 'candidate'):
                result = contract.verify_sample(*self.fixture(finish, subject))
                self.assertEqual(result['elapsed_ns'][finish], 321)
                self.assertEqual(len(result['elapsed_ns']), 8)
                self.assertEqual(len(result['kernel_peak_rss_bytes']), 9)
                self.assertEqual(result['finish'], finish)
                self.assertIs(result['result_validated'], True)

    def test_legacy_protocol_wrong_mode_or_unverified_results_never_pass(self):
        faults = ('identity', 'phase', 'rss_phase', 'missing_time', 'zero_time',
                  'bool_time', 'missing_validation', 'false_validation',
                  'integer_validation', 'complete_mode', 'missing_complete')
        for finish in ('commit', 'rollback'):
            for fault in faults:
                args = self.fixture(finish)
                if fault == 'identity': args[0]['finish'] = None
                if fault == 'phase': args[2][-2]['phase'] = 'warm'
                if fault == 'rss_phase': args[1]['phase_kernel_rss'][-1]['phase'] = 'warm'
                if fault == 'missing_time': args[2][-2]['data'].pop('elapsed_ns')
                if fault == 'zero_time': args[2][-2]['data']['elapsed_ns'] = 0
                if fault == 'bool_time': args[2][-2]['data']['elapsed_ns'] = True
                if fault == 'missing_validation': args[2][-1]['data'].pop('result_validated')
                if fault == 'false_validation': args[2][-1]['data']['result_validated'] = False
                if fault == 'integer_validation': args[2][-1]['data']['result_validated'] = 1
                if fault == 'complete_mode': args[2][-1]['data']['finish'] = 'other'
                if fault == 'missing_complete': args[2].pop()
                with self.subTest(finish=finish, fault=fault), self.assertRaises((ValueError, KeyError)):
                    contract.verify_sample(*args)

    def test_previous_cache_source_and_rss_contracts_remain_required(self):
        for fault in ('binary', 'cache', 'rss', 'storage'):
            args = self.fixture()
            if fault == 'binary': args[0]['binary_sha256'] = 'other'
            if fault == 'cache': args[2][7]['data']['stats']['misses'] = 2
            if fault == 'rss': args[1]['phase_kernel_rss'][-1]['kernel_reported_peak_rss_bytes'] = 0
            if fault == 'storage': args[1]['final_inventory']['allocated_bytes'] = 0
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                contract.verify_sample(*args)

    def test_mode_bound_plans_keep_five_alternating_pairs(self):
        ids = set()
        for size in (1000, 10000, 100000):
            for finish in ('commit', 'rollback'):
                value = configuration(size, finish)
                control.validate_config(value)
                cells = contract.plan(size, finish)
                self.assertEqual(len(cells), 10)
                self.assertEqual({c['finish'] for c in cells}, {finish})
                for sample in range(5):
                    self.assertEqual([c['subject'] for c in cells[2*sample:2*sample+2]],
                                     ['before', 'candidate'] if sample % 2 == 0 else ['candidate', 'before'])
                for cell in cells:
                    self.assertNotIn(cell['id'], ids)
                    ids.add(cell['id'])
        for invalid in (None, '', 'legacy', True):
            with self.assertRaises(ValueError): control.validate_config(configuration(finish=invalid))

    def test_first_failure_stops_cases_without_replacement_samples(self):
        called, saved = [], []
        def execute(cell):
            called.append(cell)
            if len(called) == 2: raise ValueError('completion result differs')
            return {'verified': True}
        result = control.run_samples(configuration(), execute,
                                     lambda rows: saved.append(copy.deepcopy(rows)), clock=lambda: 0)
        self.assertEqual(len(called), 2)
        self.assertEqual([r['status'] for r in result], ['passed', 'failed'] + ['not_run'] * 8)
        self.assertEqual(saved[-1], result)
        self.assertIn('completion result differs', result[1]['error'])

    def test_legacy_or_different_mode_cannot_authorize_next_scale(self):
        config = configuration(10000)
        for role in ('before', 'candidate'):
            config['subjects'][role]['assets_digest'] = role
        result = dict(schema='alopex.hnsw-completion-stage/v1', status='passed',
                      finish='commit', stage_rows=1000, completed_samples=10,
                      owned_resources_cleaned=True, assets={'before': 'before', 'candidate': 'candidate'})
        control.verify_previous_stage(result, config)
        for key, value in (('schema', 'alopex.hnsw-stage/v1'), ('finish', 'rollback'),
                           ('stage_rows', 10000), ('completed_samples', 9),
                           ('owned_resources_cleaned', False), ('assets', {}), ('status', 'failed')):
            changed = dict(result, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                control.verify_previous_stage(changed, config)


class CompletionAssetTests(legacy.AssetTests):
    # Inherited tests still exercise the original asset boundary separately.
    def setUp(self):
        super().setUp()
        self.original_receipt = json.loads((self.assets / 'receipt').read_text())

    def set_receipt(self, receipt):
        data = json.dumps(receipt).encode()
        (self.assets / 'receipt').write_bytes(data)
        item = next(i for i in self.manifest['files'] if i['role'] == 'receipt')
        item.update(size=len(data), sha256=contract.sha(data))
        self.save()

    def test_completion_smoke_required_even_with_recomputed_manifest(self):
        with self.assertRaises(ValueError): contract.verify_assets(self.root, self.expected)
        receipt = copy.deepcopy(self.original_receipt)
        receipt['completion_smoke'] = {
            mode: dict(mode=mode, status='passed', result_validated=True, rows=2,
                       dimensions=4, phases=9, events=19, elapsed_ns=100)
            for mode in ('commit', 'rollback')}
        self.set_receipt(receipt)
        self.assertEqual(len(contract.verify_assets(self.root, self.expected)), 4)
        for fault in ('missing_mode', 'unverified', 'wrong_mode', 'invalid_time'):
            changed = copy.deepcopy(receipt)
            if fault == 'missing_mode': changed['completion_smoke'].pop('commit')
            if fault == 'unverified': changed['completion_smoke']['commit']['result_validated'] = False
            if fault == 'wrong_mode': changed['completion_smoke']['commit']['mode'] = 'rollback'
            if fault == 'invalid_time': changed['completion_smoke']['commit']['elapsed_ns'] = True
            self.set_receipt(changed)
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                contract.verify_assets(self.root, self.expected)


if __name__ == '__main__': unittest.main()
