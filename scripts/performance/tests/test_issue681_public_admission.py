"""Fixed admission math and reversible affinity; no host qualification claim."""

from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public_admission as admission


def stat(values):
    return "\n".join(
        f"cpu{number} {busy} 0 0 {idle} 0 0 0 0"
        for number, (busy, idle) in values.items())


class AdmissionTests(unittest.TestCase):
    def test_cgroup_quota_and_effective_cpuset_are_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "cgroup.controllers").write_text("cpu cpuset\n")
            (root / "cpu.max").write_text("max 100000\n")
            (root / "cpuset.cpus.effective").write_text("2-5\n")
            (root / "cgroup.procs").write_text("")
            child = root / "selected"
            child.mkdir()
            (child / "cpuset.cpus.effective").write_text("2-5\n")
            (child / "cgroup.procs").write_text("123\n")
            (child / "cpu.max").write_text("400000 100000\n")
            (child / "cpu.stat").write_text("usage_usec 100\nnr_throttled 0\n")
            self.assertEqual(admission.cgroup_allocation(
                root, "/selected", (2, 3, 4, 5), pid=123), ("/selected", 0))
            (child / "cpuset.cpus.effective").unlink()
            self.assertEqual(admission.cgroup_allocation(
                root, "/hidden/selected", (2, 3, 4, 5), pid=123),
                ("/selected", 0))
            (root / "cpuset.cpus.effective").write_text("2-4\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
                admission.cgroup_allocation(root, "/hidden/selected",
                                            (2, 3, 4, 5), pid=123)
            (root / "cpuset.cpus.effective").write_text("2-5\n")
            (child / "cpu.max").write_text("300000 100000\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
                admission.cgroup_allocation(root, "/selected", (2, 3, 4, 5),
                                            pid=123)
            (child / "cpu.max").write_text("max 100000\n")
            (root / "cpu.max").write_text("200000 100000\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
                admission.cgroup_allocation(root, "/selected", (2, 3, 4, 5),
                                            pid=123)
            (root / "cpu.max").write_text("max 100000\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
                admission.cgroup_allocation(root, "/hidden/selected",
                                            (2, 3, 4, 5), pid=999)
            (root / "cgroup.procs").write_text("123\n")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
                admission.cgroup_allocation(root, "/hidden/selected",
                                            (2, 3, 4, 5), pid=123)

    def test_case_completion_rejects_throttling(self):
        with tempfile.TemporaryDirectory() as tmp:
            profile = {"schema": admission.SCHEMA,
                       "runner_kind": "selected_linux",
                       "cpu_count": 4, "additional_bytes": 1_000}
            gate = admission.Admission(profile, roots=(Path(tmp),),
                                       log_dir=Path(tmp))
            gate.pending = (1_000, 0, "before", 0)
            with mock.patch.object(gate, "_allocation", return_value=1):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
                    gate.complete_case(1_000, 0, "before")

    def test_each_selected_cpu_must_be_idle(self):
        before = stat({2: (100, 100), 3: (100, 100)})
        passing = stat({2: (100, 200), 3: (100, 200)})
        busy = stat({2: (100, 200), 3: (102, 198)})
        self.assertEqual(admission.quiet(before, passing, (2, 3)), 10_000)
        with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
            admission.quiet(before, busy, (2, 3))
        with self.assertRaisesRegex(ValueError, "^PUBLIC_ADMIT$"):
            admission.quiet(before, stat({2: (100, 200)}), (2, 3))
        self.assertEqual(admission.eligible_cpus(before, busy, (2, 3)), (2,))

    def test_affinity_restored_after_private_admission(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp) / "admission"
            log_dir.mkdir(mode=0o700)
            profile = {"schema": admission.SCHEMA,
                       "runner_kind": "selected_linux",
                       "cpu_count": 4, "additional_bytes": 1_000_000_000}
            state = {"affinity": {2, 3, 4, 5, 6}}
            def get_affinity(pid):
                return state["affinity"].copy()
            def set_affinity(pid, value):
                state["affinity"] = set(value)
            with mock.patch.object(admission.sys, "platform", "linux"), \
                    mock.patch.object(admission.platform, "machine",
                                      return_value="x86_64"), \
                    mock.patch.object(admission.os, "geteuid", return_value=os.geteuid()), \
                    mock.patch.dict(admission.os.environ,
                                    {"RUNNER_KIND": "selected_linux"}), \
                    mock.patch.object(admission.os, "sched_getaffinity",
                                      side_effect=get_affinity, create=True), \
                    mock.patch.object(admission.os, "sched_setaffinity",
                                      side_effect=set_affinity, create=True), \
                    mock.patch.object(admission, "child_counter_probe"), \
                    mock.patch.object(admission, "current_allocation",
                                      return_value=("/", 0)), \
                    mock.patch.object(admission.Admission, "_select_cpus",
                                      return_value=(2, 3, 4, 5)), \
                    mock.patch.object(admission.Admission, "_capacity",
                                      return_value=1_000), \
                    mock.patch.object(admission.Admission, "_quiet_window",
                                      return_value=10_000):
                with admission.Admission(profile, roots=(Path(tmp),),
                                         log_dir=log_dir) as admitted:
                    self.assertEqual(state["affinity"], {2, 3, 4, 5})
                    self.assertTrue(admitted.admit_stage(1_000))
                    self.assertTrue(admitted.admit_case(1_000, 0, "before"))
                    self.assertTrue(admitted.complete_case(1_000, 0, "before"))
                self.assertEqual(state["affinity"], {2, 3, 4, 5, 6})
            self.assertEqual(len((log_dir / "admission.jsonl").read_text().splitlines()),
                             4)


if __name__ == "__main__":
    unittest.main()
