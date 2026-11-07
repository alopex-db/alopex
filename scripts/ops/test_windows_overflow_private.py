import tempfile
from pathlib import Path
import unittest
from windows_overflow_private import PHASES, manifest, phase_success


class PrivateAcceptanceTests(unittest.TestCase):
    def test_every_phase_including_cleanup_must_succeed(self):
        phases = dict.fromkeys(PHASES, 0)
        self.assertTrue(phase_success(phases))
        for phase in PHASES:
            missing = dict(phases); del missing[phase]
            failed = dict(phases, **{phase:1})
            self.assertFalse(phase_success(missing))
            self.assertFalse(phase_success(failed))
        self.assertFalse(phase_success(dict(phases, extra=0)))
        self.assertFalse(phase_success(dict(phases, verify=False)))

    def test_manifest_uses_relative_names_and_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'case.json').write_bytes(b'{}')
            result = manifest(root)
            self.assertEqual(set(result), {'case.json'})
            self.assertEqual(result['case.json']['bytes'], 2)
            self.assertEqual(len(result['case.json']['sha256']), 64)

    def test_manifest_rejects_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'case.json').write_bytes(b'{}')
            (root/'link').symlink_to(root/'case.json')
            with self.assertRaises(RuntimeError): manifest(root)


if __name__ == '__main__': unittest.main()
