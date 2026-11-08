import ast
import copy
from pathlib import Path
import unittest

from wal_commit_diagnostic import memory, observe
from test_wal_commit_diagnostic import raw


# Load only the audit's pure check; the decryption CLI is not needed to reject
# tampering and this keeps contract tests free of local archive dependencies.
tree=ast.parse(Path(__file__).with_name('verify_wal_diagnostic.py').read_text())
definition=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='verify_observation')
namespace={'memory':memory}
exec(compile(ast.Module(body=[definition],type_ignores=[]),'auditor','exec'),namespace)
verify=namespace['verify_observation']


class Audit(unittest.TestCase):
    def observation(self):
        snapshots=iter([raw(),raw(24)])
        _,report=observe(lambda:None,lambda fn:(fn(),42),read_memory=lambda:next(snapshots),write=lambda fd,b:len(b))
        return report

    def test_rebuilds_from_raw(self):
        self.assertEqual(verify(self.observation(),42)['status_bytes']['RssAnon'],4096)

    def test_summary_tampering_is_rejected(self):
        for group in ('before','after','delta_bytes'):
            report=self.observation();report[group]['status_bytes']['RssAnon']+=1
            with self.assertRaises(AssertionError):verify(report,42)

    def test_raw_tampering_is_rejected(self):
        report=self.observation();report['raw_after']=raw(28)
        with self.assertRaises(AssertionError):verify(report,42)

    def test_timer_schema_or_acceptance_tampering_is_rejected(self):
        for key,value in [('timer_nanoseconds',43),('schema','unknown'),('performance_acceptance',True)]:
            report=self.observation();report[key]=value
            with self.assertRaises(AssertionError):verify(report,42)


if __name__=='__main__':unittest.main()
