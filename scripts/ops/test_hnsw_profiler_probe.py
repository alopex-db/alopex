import unittest
from pathlib import Path
from unittest.mock import patch
import errno
import hnsw_profiler_probe as probe

class Contract(unittest.TestCase):
    def test_owned_user_only_command(self):
        command=probe.record_command('/tool/perf','/owned/data','/tool/python')
        options=command[:command.index('--')]
        self.assertEqual(options[options.index('-e')+1],'cpu-clock:u')
        self.assertTrue({'--no-inherit','--per-thread','--no-buildid-cache'}.issubset(options))
        self.assertFalse({'-a','-C','-p','-t','-g','--call-graph','--intr-regs','sudo'}.intersection(options))
        self.assertEqual(command[command.index('--')+1],'/tool/python')
    def test_samples_must_be_nonzero_addresses(self):
        self.assertEqual(probe.sample_count('  7fa0\n 1c20\n'),2)
        for raw in ('','0\n','[unknown]','hostname 7fa0','7fa0\nnot-an-address'):
            with self.assertRaises(ValueError): probe.sample_count(raw)
    def test_export_rejects_unbounded_reason(self):
        with self.assertRaises(ValueError): probe.public_result('failed','private-host',0,'a',{},'b','c')
        result=probe.public_result('unavailable','permission_denied',0,'a',{},'b','c')
        self.assertFalse(result['performance_acceptance'])
        self.assertEqual(set(result),{'schema','status','reason','samples','controller','sources',
            'evidence_sha256','encrypted_sha256','work_removed','performance_acceptance'})
    def test_file_capabilities_rejected(self):
        from types import SimpleNamespace
        with patch.object(Path,'resolve',return_value=Path('/perf')), patch.object(Path,'stat',
                return_value=SimpleNamespace(st_mode=0o100755,st_uid=0)), patch.object(probe.os,'getxattr',return_value=b'caps',create=True):
            with self.assertRaisesRegex(ValueError,'file_capabilities'): probe.safe_executable('/perf')
    def test_setuid_or_writable_tool_rejected(self):
        from types import SimpleNamespace
        for mode in (0o104755,0o102755,0o100777):
            with patch.object(Path,'resolve',return_value=Path('/perf')), patch.object(Path,'stat',
                    return_value=SimpleNamespace(st_mode=mode,st_uid=0)):
                with self.assertRaisesRegex(ValueError,'unsafe_executable'): probe.safe_executable('/perf')

if __name__=='__main__': unittest.main()
