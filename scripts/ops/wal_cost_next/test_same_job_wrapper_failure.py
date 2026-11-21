"""Fixed private diagnostics for pre-comparator capacity failures."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from common import Failure
from same_job_wrapper import seal_wrapper_failure, run_closure_candidate
from lifecycle import Run
from resolve_builder_lock import LockError


class WrapperFailureTests(unittest.TestCase):
    def test_closure_mismatch_requires_deleted_run_and_final_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / 'private'
            private.mkdir()
            profile = {'private_output': str(private / 'runs'), 'run_id': 'hosted-804-42'}
            signed = {'proof_sha256': 'a' * 64}
            run = Run(profile['private_output'], 'hosted-804-42-trace-lock').create({})
            run.transition('stopped')
            run.collect(lambda staging: (staging / 'result.json').write_text('{}'))
            run.transition('deleted')
            (run.root / 'final-result.private.json').write_text(json.dumps({
                'schema': 'alopex.trace-closure-final/v1', 'status': 'failed',
                'proof_sha256': 'a' * 64, 'error_code': 'CLOSURE_MISMATCH'}))
            with patch('offline_closure_lifecycle.run_candidate_lifecycle',
                       side_effect=LockError('unsatisfied Depends: x')):
                result = run_closure_candidate(profile, {}, private, object(), signed)
            self.assertEqual(result['status'], 'mismatch')

    def test_closure_guard_failure_cannot_be_diagnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / 'private'
            private.mkdir()
            profile = {'private_output': str(private / 'runs'), 'run_id': 'hosted-804-42'}
            with patch('offline_closure_lifecycle.run_candidate_lifecycle',
                       side_effect=LockError('offline host capacity')):
                with self.assertRaisesRegex(RuntimeError, 'closure guard failure'):
                    run_closure_candidate(profile, {}, private, object(),
                                          {'proof_sha256': 'a' * 64})
            receipt = json.loads((private / 'closure-phase.private.json').read_text())
            self.assertEqual(receipt['status'], 'guard_failed')

    def test_records_fixed_capture_gate_without_exposing_detail(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "alopex-804-qualification"
            private.mkdir(mode=0o700)
            error = Failure("CAPACITY", "host_growth")
            error.wrapper_phase = "capture_signed_indexes"
            error.capture_stage = "capture_resolver"
            with patch.dict(os.environ, {"RUNNER_TEMP": directory, "GITHUB_RUN_ID": "42"}):
                seal_wrapper_failure(error)
            receipt = json.loads((private / "wrapper-result.private.json").read_text())
            self.assertEqual(receipt["run_id"], "42")
            self.assertEqual(receipt["wrapper_phase"], "capture_signed_indexes")
            self.assertEqual(receipt["capture_stage"], "capture_resolver")
            self.assertEqual(receipt["capacity_gate"], "host_growth")
            self.assertEqual(receipt["original_error"], "Failure('CAPACITY')")

    def test_unknown_detail_is_not_copied_into_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory) / "alopex-804-qualification"
            private.mkdir(mode=0o700)
            error = Failure("CAPACITY", "private-path-or-untrusted-text")
            error.capture_stage = "private-path-or-untrusted-text"
            with patch.dict(os.environ, {"RUNNER_TEMP": directory, "GITHUB_RUN_ID": "42"}):
                seal_wrapper_failure(error)
            receipt = json.loads((private / "wrapper-result.private.json").read_text())
            self.assertIsNone(receipt["capacity_gate"])
            self.assertIsNone(receipt["capture_stage"])
            self.assertNotIn("private-path", json.dumps(receipt))


if __name__ == "__main__":
    unittest.main()
