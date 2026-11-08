import io
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from build_gprofng_timer_probe import source_members, freeze_measure, workload
from patch_gprofng_timer import patch

class SourceBoundaryTests(unittest.TestCase):
    def archive(self, name, kind=tarfile.REGTYPE):
        raw=io.BytesIO()
        with tarfile.open(fileobj=raw,mode='w') as output:
            member=tarfile.TarInfo(name);member.type=kind
            output.addfile(member,io.BytesIO())
        raw.seek(0)
        return tarfile.open(fileobj=raw)

    def test_parent_and_absolute_paths_rejected(self):
        for name in ('binutils-2.42/../escape','/binutils-2.42/gprofng/file','other/gprofng/file'):
            with self.subTest(name=name), self.archive(name) as archive:
                with self.assertRaisesRegex(ValueError,'archive_path'):source_members(archive)

    def test_links_and_special_files_rejected(self):
        for kind in (tarfile.SYMTYPE,tarfile.LNKTYPE,tarfile.FIFOTYPE):
            with self.subTest(kind=kind),self.archive('binutils-2.42/gprofng/escape',kind) as archive:
                with self.assertRaisesRegex(ValueError,'archive_type'):source_members(archive)

    def test_incomplete_archive_is_not_build_input(self):
        with self.archive('binutils-2.42/gprofng/one.c') as archive:
            with self.assertRaisesRegex(ValueError,'source_inventory'):source_members(archive)

    def test_patch_rejects_wrong_upstream(self):
        with self.assertRaisesRegex(AssertionError,'Unexpected upstream'):patch(b'not the pinned source')

    def test_probe_code_is_compilable(self):
        compile(workload(),'<collector-workload>','exec')

    @unittest.skipUnless(sys.platform=='linux','Linux process-group validation')
    def test_owned_build_is_stopped_during_measure_and_resumed_after_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            counter=Path(temporary)/'counter'
            code='import pathlib,sys,time\np=pathlib.Path(sys.argv[1]);i=0\nwhile True:\n i+=1;p.write_text(str(i));time.sleep(.01)'
            child=subprocess.Popen([sys.executable,'-I','-B','-c',code,str(counter)],start_new_session=True)
            try:
                deadline=time.monotonic()+2
                while not counter.exists():
                    self.assertLess(time.monotonic(),deadline);time.sleep(.01)
                class Measure:
                    def check(inner):
                        before=counter.read_bytes();time.sleep(.05)
                        self.assertEqual(counter.read_bytes(),before)
                        raise ValueError('injected_measurement_failure')
                with self.assertRaisesRegex(ValueError,'injected_measurement_failure'):freeze_measure(child,Measure())
                before=counter.read_bytes();time.sleep(.05)
                self.assertNotEqual(counter.read_bytes(),before)
            finally:
                os.killpg(child.pid,signal.SIGTERM);child.wait(timeout=2)

if __name__=='__main__':unittest.main()
