import copy
import io
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (Failure, atomic_json, digest, inspect_archive, public_result, read_json,
                    host_lock, verify_assets, elf_signature)
from capacity import Budget, allocated_bytes
from lifecycle import Run
from observe import delta
from docker_backend import Docker


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="alopex-test-env-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)


class ExecutionMode(unittest.TestCase):
    def check(self, mode, selected, daemon):
        profile = {"docker_context": "test", "platform": selected, "execution_mode": mode}
        docker = Docker(profile)
        with patch.object(docker, "call", side_effect=[
                json.dumps([{"Endpoints": {"docker": {"Host": "unix:///test.sock"}}}]), daemon]):
            docker.prerequisites()

    def test_native_container_must_match_daemon(self):
        with self.assertRaises(Failure):
            self.check("native", "linux/target-a", "linux/target-b")
        self.check("native", "linux/target-a", "linux/target-a")

    def test_full_system_emulation_can_have_matching_guest_and_container(self):
        self.check("emulated", "linux/target-a", "linux/target-a")

    def test_container_emulation_can_differ_from_daemon(self):
        self.check("emulated", "linux/target-a", "linux/target-b")


class ImageInventory(unittest.TestCase):
    def verify(self, inventory):
        import runpy
        with patch("pathlib.Path.read_text", return_value=json.dumps({"packages": {"base": "1"}})), \
             patch("subprocess.check_output", side_effect=[inventory, "postgres (PostgreSQL) 16.14"]), \
             patch.object(sys, "version_info", (3, 12, 0)):
            runpy.run_path(str(Path(__file__).resolve().parents[1] / "image_verify.py"))

    def test_exact_inventory_is_accepted(self):
        self.verify("base\t1\n")

    def test_unlocked_package_is_rejected(self):
        with self.assertRaises(Failure):
            self.verify("base\t1\nextra\t1\n")

    def test_wrong_version_is_rejected(self):
        with self.assertRaises(Failure):
            self.verify("base\t2\n")


class ImageBuild(Fixture):
    def test_successful_build_retains_controller_identity_after_copying_dependencies(self):
        from unittest.mock import Mock
        import envctl
        cache = self.root / "cache"
        (cache / "deps").mkdir(parents=True)
        package = cache / "deps/tiny.deb"
        package.write_bytes(b"fixture")
        lock = self.root / "lock.json"
        lock.write_text("{}")
        profile = {"cache": str(cache), "image_lock": str(lock),
                   "platform": "linux/test", "limits": {"run_seconds": 1}}
        source = {"controller_commit": "a" * 40, "controller_digest": "b" * 64}
        variant = {"files": [{"path": "deps/tiny.deb", "size": 7,
                              "sha256": digest(package)}]}
        docker, budget = Mock(), Mock()
        docker.prefix = ["docker"]
        budget.run.side_effect = lambda *args, **kwargs: (cache / "image.id").write_text("sha256:" + "c" * 64)
        with patch.object(envctl, "image_lock", return_value=({"base": "locked"}, variant)), \
             patch.object(envctl, "source_identity", return_value=source):
            envctl.build(profile, docker, budget)
        result = read_json(cache / "image.json")
        self.assertEqual(result["controller_commit"], source["controller_commit"])
        self.assertEqual(result["controller_digest"], source["controller_digest"])
        self.assertFalse((cache / "context").exists())


class Collection(Fixture):
    def run_record(self):
        run = Run(self.root, "test").create({})
        self.token = run.record["ownership_token"]
        run.resource("container", "owned", "original")
        run.transition("running")
        run.transition("stopped")
        return run

    def copier(self, destination):
        atomic_json(destination / "result.json", {"status": "failed", "cases": []})

    def inspect(self, item):
        return {"id": "original", "labels": {"org.alopex.test-env.run": "test",
                 "org.alopex.test-env.owner": "envctl",
                 "org.alopex.test-env.token": self.token}, "running": False}

    def test_uncollected_run_cannot_be_deleted(self):
        run = self.run_record()
        calls = []
        with self.assertRaises(Failure):
            run.clean(self.inspect, calls.append)
        self.assertEqual(calls, [])

    def copy_failure(self, code):
        from unittest.mock import Mock
        from envctl import collect
        run = self.run_record()
        run.record["resources"][0]["name"] = "alopex-test-test"
        run.save()
        docker, budget = Mock(), Mock()
        docker.prefix = ["docker"]
        docker.inspect.side_effect = self.inspect
        budget.run.side_effect = Failure(code, "original diagnostic")
        with self.assertRaises(Failure) as caught:
            collect({"private_output": str(self.root), "limits": {"case_seconds": 1}},
                    docker, budget, run.run_id)
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(caught.exception.detail, "original diagnostic")
        self.assertEqual(read_json(run.root / "collect-copy-error.json")["error_code"], code)
        self.assertEqual(run.load().record["phase"], "stopped")
        docker.remove.assert_not_called()

    def test_collection_preserves_measurement_failure_and_original_data(self):
        self.copy_failure("MEASUREMENT")

    def test_collection_preserves_capacity_failure_and_original_data(self):
        self.copy_failure("CAPACITY")

    def test_collection_preserves_timeout_and_original_data(self):
        self.copy_failure("TIMEOUT")

    def partial_startup(self, failed):
        from unittest.mock import Mock
        from envctl import collect
        run = self.run_record()
        run.record["resources"][0]["name"] = "alopex-test-test"
        run.save()
        if failed:
            atomic_json(run.root / "failure.json", {"error_code": "INTERRUPTED"})
        docker, budget = Mock(), Mock()
        docker.prefix = ["docker"]
        docker.inspect.side_effect = self.inspect
        # docker cp succeeds but the interrupted supervisor produced no report.
        action = lambda: collect({"private_output": str(self.root), "limits": {"case_seconds": 1}},
                                 docker, budget, run.run_id)
        if failed:
            action()
            run.load().verify_collection()
            result = read_json(run.root / "collected/result.json")
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["controller_error"], "INTERRUPTED")
            self.assertEqual(result["error_code"], "INCOMPLETE")
            self.assertEqual(result["cases"], [])
        else:
            with self.assertRaises(Failure) as caught:
                action()
            self.assertEqual(caught.exception.code, "COLLECT")
            self.assertEqual(run.load().record["phase"], "stopped")
        docker.remove.assert_not_called()

    def test_empty_startup_results_remain_collectable_after_recorded_interrupt(self):
        self.partial_startup(True)

    def test_missing_report_without_recorded_failure_is_not_fabricated(self):
        self.partial_startup(False)

    def test_copy_failure_preserves_run_for_retry(self):
        run = self.run_record()
        with self.assertRaises(OSError):
            run.collect(lambda destination: (_ for _ in ()).throw(OSError("copy failed")))
        self.assertEqual(Run(self.root, "test").load().record["phase"], "stopped")
        run.collect(self.copier)
        self.assertEqual(run.record["phase"], "collected")

    def test_tampered_collection_prevents_deletion(self):
        run = self.run_record()
        run.collect(self.copier)
        (run.root / "collected/result.json").write_text("changed")
        calls = []
        with self.assertRaises(Failure):
            run.clean(self.inspect, calls.append)
        self.assertEqual(calls, [])

    def test_reused_name_is_not_deleted(self):
        run = self.run_record()
        run.collect(self.copier)
        with self.assertRaises(Failure):
            run.clean(lambda item: dict(self.inspect(item), id="replacement"), self.fail)

    def test_foreign_run_is_not_deleted(self):
        run = self.run_record()
        run.collect(self.copier)
        with self.assertRaises(Failure):
            run.clean(lambda item: {"id": "original", "labels": {}, "running": False}, self.fail)

    def test_running_container_is_not_deleted(self):
        run = self.run_record()
        run.collect(self.copier)
        with self.assertRaises(Failure):
            run.clean(lambda item: dict(self.inspect(item), running=True), self.fail)

    def test_cleanup_is_idempotent_and_collect_does_not_recopy(self):
        run = self.run_record()
        run.collect(self.copier)
        run.collect(lambda path: self.fail("must not recopy"))
        calls = []
        run.clean(self.inspect, calls.append)
        run.clean(lambda item: self.fail("already cleaned"), calls.append)
        self.assertEqual(len(calls), 1)

    def test_symlink_from_container_is_rejected(self):
        run = self.run_record()
        def copy(destination):
            (destination / "result.json").symlink_to(self.root / "test/run.json")
        with self.assertRaises(Failure):
            run.collect(copy)
        self.assertEqual(run.record["phase"], "stopped")

    def test_interrupted_create_is_recovered_by_persisted_token(self):
        from envctl import recover
        from unittest.mock import Mock
        run = Run(self.root, "test").create({})
        self.token = run.record["ownership_token"]
        run.intent("container", "owned")
        docker = Mock()
        docker.inspect.side_effect = self.inspect
        recover(run, docker)
        self.assertEqual(run.record["resources"][0]["id"], "original")
        self.assertEqual(run.record["phase"], "stopped")
        docker.stop.assert_called_once_with("original")

    def test_interrupted_create_does_not_adopt_a_foreign_token(self):
        from envctl import recover
        from unittest.mock import Mock
        run = Run(self.root, "test").create({})
        self.token = "another-run-token"
        run.intent("container", "owned")
        docker = Mock()
        docker.inspect.side_effect = self.inspect
        with self.assertRaises(Failure):
            recover(run, docker)
        docker.stop.assert_not_called()


class Capacity(Fixture):
    def config(self):
        return {"roots": [str(self.root)], "stop_bytes": 100,
                "reserve_bytes": 10, "planned_bytes": {"build": 20}, "poll_seconds": 1}

    def test_overlapping_roots_and_hardlinks_are_not_double_counted(self):
        source = self.root / "file"
        source.write_bytes(b"x" * 4096)
        os.link(source, self.root / "linked")
        self.assertEqual(allocated_bytes([self.root]), allocated_bytes([self.root, source]))

    def test_dangling_link_counts_link_allocation_without_inventing_target_bytes(self):
        link = self.root / "dangling"
        link.symlink_to(self.root / "absent")
        expected = (self.root.lstat().st_blocks + link.lstat().st_blocks) * 512
        self.assertEqual(allocated_bytes([self.root]), expected)

    def test_external_symlink_target_is_counted_once(self):
        directory = self.root / "tracked"
        directory.mkdir()
        target = self.root / "payload"
        target.write_bytes(b"x" * 4096)
        link = directory / "alias"
        link.symlink_to(target)
        expected = sum(p.lstat().st_blocks * 512 for p in (directory, target, link))
        self.assertEqual(allocated_bytes([directory, target]), expected)

    def test_missing_root_is_not_zero(self):
        with self.assertRaises(Failure):
            allocated_bytes([self.root / "absent"])

    def test_measurement_error_is_not_zero(self):
        with self.assertRaises(Failure) as caught:
            Budget(self.config(), lambda roots: (_ for _ in ()).throw(PermissionError())).before("build")
        self.assertEqual(caught.exception.code, "MEASUREMENT")

    def test_preflight_reserves_planned_and_cleanup_capacity(self):
        with self.assertRaises(Failure):
            Budget(self.config(), lambda roots: 70).before("build")

    def test_growth_above_plan_stops(self):
        values = iter([10, 31])
        budget = Budget(self.config(), lambda roots: next(values))
        budget.before("build")
        with self.assertRaises(Failure):
            budget.check()

    def test_failed_preflight_never_launches_a_process(self):
        budget = Budget(self.config(), lambda roots: 95)
        with patch("capacity.subprocess.Popen") as launch:
            with self.assertRaises(Failure):
                budget.before("build")
                budget.run(["unused"], timeout=1, log=self.root / "unused")
            launch.assert_not_called()

    def test_runtime_measurement_error_cancels_owned_operation(self):
        budget = Budget(self.config(), lambda roots: 10)
        budget.before("build")
        with patch.object(budget, "check", side_effect=[10, Failure("MEASUREMENT")]), \
             patch("capacity.subprocess.Popen") as launch, \
             patch("capacity.os.killpg") as kill:
            import subprocess
            child = launch.return_value
            child.poll.return_value = None
            child.wait.side_effect = [subprocess.TimeoutExpired("owned", 1), 0]
            child.pid = 1
            cancelled = []
            with self.assertRaises(Failure):
                budget.run(["owned"], timeout=2, log=self.root / "log", cancel=lambda: cancelled.append(True))
            self.assertEqual(cancelled, [True])
            kill.assert_called_once()

    def test_cancel_failure_preserves_original_measurement_error_and_reaps_command(self):
        budget = Budget(self.config(), lambda roots: 10)
        budget.before("build")
        original = Failure("MEASUREMENT", "counter unavailable")
        with patch.object(budget, "check", side_effect=[10, original]), \
             patch("capacity.subprocess.Popen") as launch, \
             patch("capacity.os.killpg") as kill:
            import subprocess
            child = launch.return_value
            child.poll.return_value = None
            child.wait.side_effect = [subprocess.TimeoutExpired("owned", 1), 0]
            child.pid = 1
            def cancel():
                raise Failure("CLEANUP", "daemon stop failed")
            with self.assertRaises(Failure) as caught:
                budget.run(["owned"], timeout=2, log=self.root / "log", cancel=cancel)
            self.assertIs(caught.exception, original)
            self.assertEqual(original.detail, "counter unavailable")
            self.assertEqual(original.cleanup_errors[0]["operation"], "cancel_owned_operation")
            self.assertEqual(original.cleanup_errors[0]["error_code"], "CLEANUP")
            kill.assert_called_once()
            self.assertEqual(child.wait.call_count, 2)

    def test_reap_failure_also_preserves_timeout(self):
        budget = Budget(self.config(), lambda roots: 10)
        budget.before("build")
        original = Failure("TIMEOUT")
        with patch.object(budget, "check", side_effect=[10, original]), \
             patch("capacity.subprocess.Popen") as launch, \
             patch("capacity.os.killpg", side_effect=PermissionError()):
            import subprocess
            child = launch.return_value
            child.poll.return_value = None
            child.wait.side_effect = subprocess.TimeoutExpired("owned", 1)
            child.pid = 1
            with self.assertRaises(Failure) as caught:
                budget.run(["owned"], timeout=2, log=self.root / "log")
            self.assertIs(caught.exception, original)
            self.assertEqual(original.cleanup_errors[0]["operation"], "reap_command")


class Archives(Fixture):
    def test_parent_escape_is_rejected(self):
        path = self.root / "bundle.zip"
        with zipfile.ZipFile(path, "w") as z:
            z.writestr("../escape", "x")
        with self.assertRaises(Failure):
            inspect_archive(path, 100)

    def test_symlink_is_rejected(self):
        path = self.root / "bundle.tar"
        with tarfile.open(path, "w") as archive:
            item = tarfile.TarInfo("link")
            item.type = tarfile.SYMTYPE
            item.linkname = "outside"
            archive.addfile(item)
        with self.assertRaises(Failure):
            inspect_archive(path, 100)

    def test_duplicate_archive_entries_are_rejected(self):
        path = self.root / "bundle.tar"
        with tarfile.open(path, "w") as archive:
            for _ in range(2):
                item = tarfile.TarInfo("same")
                item.size = 1
                archive.addfile(item, io.BytesIO(b"x"))
        with self.assertRaises(Failure):
            inspect_archive(path, 100)

    def test_expansion_bound_is_checked_before_extracting(self):
        path = self.root / "bundle.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("data", "x" * 100)
        with self.assertRaises(Failure):
            inspect_archive(path, 99)
        self.assertEqual(inspect_archive(path, 100), 100)


class Assets(Fixture):
    def bundle(self):
        (self.root / "server").write_bytes(b"tiny synthetic server")
        with zipfile.ZipFile(self.root / "client.whl", "w") as archive:
            archive.writestr("client.py", "pass\n")
        with tarfile.open(self.root / "harness.tar", "w") as archive:
            item = tarfile.TarInfo("sql_resources.py")
            item.size = 5
            archive.addfile(item, io.BytesIO(b"pass\n"))
        receipt = {"source_commit": "a" * 40, "platform": "linux/fixture",
                   "artifacts": {"server": digest(self.root / "server"),
                                 "client.whl": digest(self.root / "client.whl")}}
        atomic_json(self.root / "receipt.json", receipt)
        files = []
        for role, name in (("server", "server"), ("wheel", "client.whl"),
                           ("receipt", "receipt.json"), ("harness", "harness.tar")):
            path = self.root / name
            files.append({"role": role, "path": name, "size": path.stat().st_size, "sha256": digest(path)})
        manifest = {"schema": "alopex.test-env-assets/v1", "platform": "linux/fixture",
                    "source_commit": "a" * 40, "harness_commit": "b" * 40,
                    "expanded_limit_bytes": 1024, "files": files}
        atomic_json(self.root / "assets.json", manifest)
        return manifest

    def test_verified_bundle_retains_original_receipt(self):
        self.bundle()
        before = (self.root / "receipt.json").read_bytes()
        verify_assets(self.root / "assets.json", "linux/fixture")
        self.assertEqual((self.root / "receipt.json").read_bytes(), before)

    def test_modified_server_is_rejected_before_execution(self):
        self.bundle()
        (self.root / "server").write_bytes(b"different")
        with self.assertRaises(Failure):
            verify_assets(self.root / "assets.json", "linux/fixture")

    def test_matching_hash_does_not_hide_wrong_receipt_identity(self):
        manifest = self.bundle()
        receipt = read_json(self.root / "receipt.json")
        receipt["source_commit"] = "c" * 40
        atomic_json(self.root / "receipt.json", receipt)
        entry = next(item for item in manifest["files"] if item["role"] == "receipt")
        entry.update(size=(self.root / "receipt.json").stat().st_size, sha256=digest(self.root / "receipt.json"))
        atomic_json(self.root / "assets.json", manifest)
        with self.assertRaises(Failure):
            verify_assets(self.root / "assets.json", "linux/fixture")

    def test_platform_mismatch_is_rejected(self):
        self.bundle()
        with self.assertRaises(Failure):
            verify_assets(self.root / "assets.json", "linux/different")

    def performance_bundle(self, name="libalopex_sql_parser.so", expected=None, explicit=None):
        manifest = self.bundle()
        path = self.root / name
        path.write_bytes(b"tiny parser")
        manifest["files"].append({"role": "library", "path": name,
                                  "size": path.stat().st_size, "sha256": digest(path)})
        receipt = {"schema": "alopex.performance-build/v1", "source": {"commit": "a" * 40},
                   "server_sha256": digest(self.root / "server"),
                   "python_wheel_sha256": digest(self.root / "client.whl"),
                   "parser_sha256": expected or digest(path)}
        if explicit is not None:
            receipt["runtime_libraries"] = {name: explicit}
        atomic_json(self.root / "receipt.json", receipt)
        item = next(item for item in manifest["files"] if item["role"] == "receipt")
        item.update(size=(self.root / "receipt.json").stat().st_size, sha256=digest(self.root / "receipt.json"))
        atomic_json(self.root / "assets.json", manifest)

    def test_original_parser_receipt_is_accepted_without_rewriting(self):
        self.performance_bundle()
        before = (self.root / "receipt.json").read_bytes()
        verify_assets(self.root / "assets.json", "linux/fixture")
        self.assertEqual((self.root / "receipt.json").read_bytes(), before)

    def test_parser_receipt_does_not_authorize_different_bytes(self):
        self.performance_bundle(expected="d" * 64)
        with self.assertRaises(Failure):
            verify_assets(self.root / "assets.json", "linux/fixture")

    def test_parser_receipt_does_not_authorize_an_unrelated_library(self):
        self.performance_bundle(name="another.so")
        with self.assertRaises(Failure):
            verify_assets(self.root / "assets.json", "linux/fixture")

    def test_parser_field_cannot_override_explicit_library_binding(self):
        self.performance_bundle(explicit="d" * 64)
        with self.assertRaises(Failure):
            verify_assets(self.root / "assets.json", "linux/fixture")

    def test_input_symlink_escape_is_rejected(self):
        self.bundle()
        (self.root / "server").unlink()
        (self.root / "server").symlink_to(self.root / "receipt.json")
        with self.assertRaises(Failure):
            verify_assets(self.root / "assets.json", "linux/fixture")

    def test_execution_format_signature_uses_artifact_bytes(self):
        path = self.root / "binary"
        header = bytearray(20)
        header[:4] = b"\x7fELF"
        header[4:6] = bytes((2, 1))
        header[16:20] = bytes((2, 0, 17, 0))
        path.write_bytes(header)
        self.assertEqual(elf_signature(path), (2, 1, 17))
        header[18] = 18
        path.write_bytes(header)
        self.assertNotEqual(elf_signature(path), (2, 1, 17))
        path.write_bytes(b"not an ELF executable")
        with self.assertRaises(Failure):
            elf_signature(path)


class Accounting(unittest.TestCase):
    def row(self):
        return {"pid": 1, "start_ticks": 10, "exe_sha256": "a" * 64, "threads": [1],
                "children": [], "rss_bytes": 4096, "user_ticks": 2, "system_ticks": 3,
                "reaped_user_ticks": 0, "reaped_system_ticks": 0,
                "read_bytes": 0, "write_bytes": 4096, "cancelled_write_bytes": 0}

    def test_stable_process_cpu_io_delta_and_rss_are_separate(self):
        before = self.row()
        after = dict(before, write_bytes=8192, user_ticks=4)
        actual = delta({1: before}, {1: after}, 100)[0]
        self.assertEqual(actual["write_bytes"], 4096)
        self.assertEqual(actual["cpu_seconds"], 0.02)
        self.assertNotIn("peak_rss", actual)

    def test_child_exit_and_reap_cannot_be_double_counted(self):
        before = {1: self.row()}
        for changed in (dict(self.row(), reaped_user_ticks=1), dict(self.row(), start_ticks=20),
                        dict(self.row(), children=[2]), dict(self.row(), threads=[1, 2])):
            with self.subTest(changed=changed), self.assertRaises(Failure):
                delta(before, {1: changed}, 100)
        with self.assertRaises(Failure):
            delta(before, {}, 100)

    def test_missing_counters_are_not_zero_filled(self):
        after = self.row()
        del after["write_bytes"]
        with self.assertRaises(KeyError):
            delta({1: self.row()}, {1: after}, 100)

    def test_child_that_exits_between_snapshots_invalidates_the_interval(self):
        from observe import Observation
        meter = Observation(1, 4)
        meter.before = {"allocation": "100", "server": {1: self.row()}, "client": {2: self.row()}}
        after = dict(meter.before, allocation="101")
        with patch.object(meter, "snapshot", return_value=after), self.assertRaises(Failure):
            meter.end()

    def test_missing_postgresql_backend_cannot_pass_a_complete_snapshot(self):
        from observe import Observation
        server = {1: self.row(), 3: dict(self.row(), pid=3)}
        meter = Observation(1, 4, required_backend=3)
        with patch('observe.Path.read_text', return_value='100'), \
             patch('observe.os.getpid', return_value=2), \
             patch('observe.process', return_value=dict(self.row(), pid=2)), \
             patch('observe.tree', return_value=server):
            self.assertEqual(set(meter.snapshot()['server']), {1, 3})
            # The postmaster remains observable, but losing the connection's
            # backend must invalidate the cell even with stable PID allocation.
            del server[3]
            with self.assertRaises(Failure) as caught:
                meter.snapshot()
            self.assertEqual(caught.exception.code, 'MEASUREMENT')

    def test_remote_sql_dictionary_rows_keep_values_and_nulls(self):
        from verify_sql import rows
        self.assertEqual(rows([{"id": 1, "value": "one"}, {"id": 2, "value": None}]),
                         [(1, "one"), (2, None)])

    def test_harness_result_retains_process_rss_and_byte_counters(self):
        from unittest.mock import Mock
        from observe import Observation
        harness = Mock()
        harness.end.return_value = {"status": "observed"}
        meter = Observation(1, 4, harness=harness)
        meter.harness_before = None
        meter.before = {"allocation": "100", "server": {1: self.row()}, "client": {2: self.row()}}
        after = {"allocation": "100", "server": {1: self.row()},
                 "client": {2: dict(self.row(), rss_bytes=8192, read_bytes=4096)}}
        with patch.object(meter, "snapshot", return_value=after):
            result = meter.end()
        self.assertEqual(result["client"][0]["rss_after_bytes"], 8192)
        self.assertEqual(result["client"][0]["read_bytes"], 4096)
        self.assertEqual(result["server"][0]["rss_before_bytes"], 4096)
        self.assertEqual(result["qualified_harness"], {"status": "observed"})


class Privacy(Fixture):
    def test_export_drops_free_form_diagnostics(self):
        secret = "synthetic-host /private/synthetic/path total-memory=987654321"
        actual = public_result({"image_digest": "a" * 64, "detail": secret,
            "cases": [{"id": "E01", "status": "failed", "error_code": "POLICY", "exception": secret}]})
        self.assertNotIn(secret, json.dumps(actual))
        self.assertNotIn("exception", actual["cases"][0])

    def test_error_code_cannot_smuggle_free_text(self):
        with self.assertRaises(Failure):
            public_result({"cases": [{"id": "E01", "status": "failed", "error_code": "secret"}]})

    def test_atomic_result_permissions(self):
        path = self.root / "private/result.json"
        atomic_json(path, {"ok": True})
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)

    def test_empty_results_cannot_export_a_success(self):
        self.assertEqual(public_result({"status": "passed", "cases": []})["status"], "failed")

    def test_unknown_cli_argument_does_not_print_its_value(self):
        import contextlib
        from envctl import main
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream), self.assertRaises(SystemExit) as caught:
            main(["doctor", "--profile", "synthetic-private-path", "--secret-synthetic-value"])
        self.assertEqual(caught.exception.code, 64)
        self.assertNotIn("synthetic", stream.getvalue())
        self.assertEqual(json.loads(stream.getvalue())["error_code"], "INPUT")

    def test_private_log_failure_preserves_status_without_traceback_or_private_text(self):
        import contextlib
        import envctl
        from unittest.mock import Mock
        profile = {"private_output": str(self.root), "run_id": "privacy", "storage": {}}
        budget = Mock()
        budget.before.side_effect = Failure("MEASUREMENT", "synthetic-private-counter")
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(envctl, "load_profile", return_value=profile), \
             patch.object(envctl, "host_lock", return_value=contextlib.nullcontext()), \
             patch.object(envctl, "Docker"), patch.object(envctl, "Budget", return_value=budget), \
             patch.object(envctl, "atomic_json", side_effect=OSError("synthetic-private-log-path")), \
             contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = envctl.main(["doctor", "--profile", "synthetic-profile"])
        self.assertEqual(code, 78)
        self.assertEqual(json.loads(stdout.getvalue()), {"status": "failed", "error_code": "MEASUREMENT"})
        self.assertEqual(stderr.getvalue(), "")
        self.assertNotIn("synthetic", stdout.getvalue())

    def test_host_lock_prevents_two_profiles_from_building(self):
        with host_lock(self.root / "lock"):
            with self.assertRaises(Failure):
                with host_lock(self.root / "lock"):
                    self.fail("must not acquire twice")


if __name__ == "__main__":
    unittest.main()
