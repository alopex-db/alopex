import unittest
from common import Failure
from private_failure import error_record

class Contracts(unittest.TestCase):
    def test_measurement_cause_and_detail_are_preserved(self):
        try:
            try:raise FileNotFoundError('PRIVATE_DIAGNOSTIC_SENTINEL')
            except OSError as error:raise Failure('MEASUREMENT',str(error)) from error
        except Failure as error:record=error_record(error)
        self.assertEqual([c['type'] for c in record['causes']],['Failure','FileNotFoundError'])
        self.assertIn('PRIVATE_DIAGNOSTIC_SENTINEL',record['causes'][0]['detail'])
        self.assertIn('FileNotFoundError',record['traceback'])

    def test_cleanup_failure_does_not_replace_primary(self):
        error=Failure('CAPACITY','primary');error.cleanup_errors=[dict(operation='reap',detail='secondary')]
        record=error_record(error)
        self.assertEqual(record['causes'][0]['code'],'CAPACITY')
        self.assertEqual(record['cleanup_errors'],error.cleanup_errors)
