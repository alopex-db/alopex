import copy
import unittest
from wal_sync_contract import verify, CASES


class TraceContract(unittest.TestCase):
    def fixture(self, case='kv-reference_append'):
        stage=case.split('-',1)[1]
        suffix={'spool':'overflow/a.tmp','directory':'overflow','reference_append':'lsm.wal'}[stage]
        call='fdatasync' if stage=='reference_append' else 'fsync'
        ordinal={'spool':1,'directory':2,'reference_append':1}[stage]
        paths=['overflow/a.tmp','overflow','lsm.wal']
        lines=['write(2</log>, "ALOPEX_COMMIT_BEGIN\\n", 20) = 20']
        for path in paths:lines.append(f'{"fdatasync" if path=="lsm.wal" else "fsync"}(3</test/baseline.alopex.d/{path}>) = 0')
        lines.append('write(2</log>, "ALOPEX_COMMIT_END\\n", 18) = 18')
        good='\n'.join(lines)
        bad=good.replace('baseline.alopex','fault.alopex').replace(f'{suffix}>) = 0',f'{suffix}>) = -1 EIO (Input/output error) (INJECTED)')
        sources={'scripts/compatibility/'+n:'source' for n in ('sync_syscall.py','publication_python.py','spool_os_write.py')}
        report=dict(status='passed',native_sha256='native',source_sha256={n.rsplit('/',1)[1]:v for n,v in sources.items()},
            cases=[dict(case=case,status='passed',fixture_bytes=200,reopen_cycles=2,replay='complete' if stage=='reference_append' else 'prior-only',
                        syscall=call,ordinal=ordinal,target_suffix=suffix)])
        return report,case,dict(native_sha256='native'),sources,good,bad

    def test_all_six_boundaries(self):
        for case in CASES:verify(*self.fixture(case))

    def test_duplicate_wal_sync_is_rejected(self):
        args=list(self.fixture());args[4]=args[4].replace('fdatasync(3', 'fdatasync(4</test/baseline.alopex.d/lsm.wal>) = 0\nfdatasync(3')
        with self.assertRaises(AssertionError):verify(*args)

    def test_wrong_ordinal_or_recovery_or_native_is_rejected(self):
        for key,value in [('ordinal',2),('replay','prior-only'),('reopen_cycles',1),('fixture_bytes',1048577)]:
            args=list(self.fixture());args[0]['cases'][0][key]=value
            with self.subTest(key=key),self.assertRaises(AssertionError):verify(*args)
        args=list(self.fixture());args[0]['native_sha256']='wrong'
        with self.assertRaises(AssertionError):verify(*args)

    def test_missing_or_outside_commit_injection_is_rejected(self):
        for text in ('-1 EIO (Input/output error)', '0'):
            args=list(self.fixture());args[5]=args[5].replace('-1 EIO (Input/output error) (INJECTED)',text)
            with self.assertRaises(AssertionError):verify(*args)
        args=list(self.fixture());lines=args[5].splitlines();lines[0],lines[-2]=lines[-2],lines[0];args[5]='\n'.join(lines)
        with self.assertRaises(AssertionError):verify(*args)


if __name__=='__main__':unittest.main()
