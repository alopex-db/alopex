import unittest
import hnsw_timer_syscall_probe as probe
class Contract(unittest.TestCase):
 def test_clock_only_no_archive_or_descendants(self):
  cmd=probe.record_command('/tool/gprofng','/owned/clock.er','/tool/python')
  opts=cmd[3:cmd.index('/tool/python')]
  self.assertEqual(opts[:2],['-p','10'])
  for flag in ('-F','-a','-S','-j','-s','-H','-i'):
   self.assertEqual(opts[opts.index(flag)+1],'off')
  self.assertNotIn('-h',opts)
  self.assertNotIn('sudo',cmd)
 def test_profile_requires_one_positive_total(self):
  self.assertEqual(probe.sampled_cpu('Name CPU\n <Total> 0.310\n function 0.100\n'),0.31)
  for raw in ('','<Total> 0','<Total> nan','<Total> 4.0','<Total> 0.2\n<Total> 0.1','0.2 function'):
   with self.assertRaises(ValueError):probe.sampled_cpu(raw)
 def test_public_result_has_no_machine_fields(self):
  r=probe.public_result('supported','profile_observed',0.3,'a',{},'b','c')
  self.assertEqual(set(r),{'schema','status','reason','observed_cpu_seconds','controller','sources',
   'evidence_sha256','encrypted_sha256','work_removed','performance_acceptance'})
  self.assertFalse(r['performance_acceptance'])
if __name__=='__main__':unittest.main()
