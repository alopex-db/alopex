"""The hosted input connector retains one guard across every acquisition."""

from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_acquire_all as acquire
import issue681_hosted_storage as storage


class Guard:
    def __init__(self, events=None):
        self.calls = 0
        self.events = events

    def __call__(self):
        self.calls += 1
        if self.events is not None:
            self.events.append("guard")


class HostedAcquireAllTest(unittest.TestCase):
    def patch(self, obj, name, **kwargs):
        patcher = mock.patch.object(obj, name, **kwargs)
        value = patcher.start()
        self.addCleanup(patcher.stop)
        return value

    def layout(self, root):
        root = root.resolve()
        workspace = root / "checkout"
        task_parent = root / "task-parent"
        home = root / "home"
        home_parent = home / "cargo-parent"
        for path in (workspace, task_parent, home, home_parent):
            path.mkdir(mode=0o700)
        self.patch(acquire.Path, "home", return_value=home)
        return workspace, task_parent, home_parent

    def components(self, *, fail=None):
        events = []
        guard = Guard(events)
        self.patch(acquire.capacity, "HostStorageGuard", new=Guard)

        def source(workspace, parent, *, manifest):
            events.append("sources")
            self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o700)
            return {"guard": guard, "sources": {
                "before": parent / "before", "candidate": parent / "candidate"},
                "git_binary": Path("/usr/bin/git"), "git_env": {"HOME": "sealed"}}

        def step(name, result):
            def call(*args):
                events.append(name)
                self.assertIn(guard, args)
                if name == fail:
                    raise OSError("private machine detail")
                return result
            return call

        self.patch(acquire.source, "acquire_sources", side_effect=source)
        self.patch(acquire.nim_archive, "fetch_archive",
                   side_effect=step("nim-fetch", Path("/private/nim.tar.xz")))
        self.patch(acquire.nim_archive, "extract_archive",
                   side_effect=step("nim-extract", Path("/private/nim-extracted")))
        self.patch(acquire.parser_tools, "verify_tools",
                   side_effect=step("tools", {"nim": Path("/private/nim"),
                                               "nimble": Path("/private/nimble")}))
        self.patch(acquire.parser_seed, "acquire_seed",
                   side_effect=step("seed", Path("/private/seed")))
        self.patch(acquire.parser_build, "build_parser",
                   side_effect=step("parser", {"parser_record": Path("/private/record")}))
        self.patch(acquire.vendor_fetch, "fetch_vendor",
                   side_effect=step("vendor-fetch", Path("/private/vendor.zip")))
        self.patch(acquire.vendor_consume, "install_vendor",
                   side_effect=step("vendor-install", {"cargo_home": Path("/private/cargo")}))
        return events, guard

    def manifest(self, workspace, task_parent, home_parent):
        home = home_parent.parent
        return storage.Manifest(workspace, task_parent, home, task_parent,
                                home_parent, (), (), task_parent / "scratch")

    def test_exact_order_and_shared_pretransfer_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_parent = self.layout(Path(tmp))
            events, guard = self.components()
            result = acquire.acquire_all(
                workspace, task_parent, home_parent,
                manifest=self.manifest(workspace, task_parent, home_parent))
            self.assertEqual(events, [
                "sources", "guard", "nim-fetch", "guard", "nim-extract",
                "tools", "guard", "seed", "guard", "parser", "guard",
                "vendor-fetch", "guard", "vendor-install", "guard"])
            self.assertIs(result["guard"], guard)
            self.assertGreaterEqual(guard.calls, 7)
            self.assertEqual(result["sources"]["candidate"].parent,
                             result["task"] / "sources")
            self.assertEqual(result["cargo_home"].parent, home_parent)
            self.assertEqual(stat.S_IMODE(result["task"].stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(result["cargo_home"].stat().st_mode),
                             0o700)

    def test_later_failure_removes_both_owned_trees_with_fixed_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_parent = self.layout(Path(tmp))
            events, _ = self.components(fail="vendor-install")
            with self.assertRaisesRegex(ValueError, "^PUBLIC_ACQUIRE$"):
                acquire.acquire_all(
                    workspace, task_parent, home_parent,
                    manifest=self.manifest(workspace, task_parent, home_parent))
            self.assertEqual(events[-1], "vendor-install")
            self.assertEqual(list(task_parent.iterdir()), [])
            self.assertEqual(list(home_parent.iterdir()), [])

    def test_overlap_and_nonprivate_parent_rejected_before_source(self):
        for problem in ("overlap", "mode"):
            with self.subTest(problem=problem), tempfile.TemporaryDirectory() as tmp:
                workspace, task_parent, home_parent = self.layout(Path(tmp))
                events, _ = self.components()
                if problem == "overlap":
                    workspace = task_parent
                else:
                    task_parent.chmod(0o755)
                with self.assertRaisesRegex(ValueError, "^PUBLIC_ACQUIRE$"):
                    acquire.acquire_all(
                        workspace, task_parent, home_parent,
                        manifest=self.manifest(workspace, task_parent,
                                               home_parent))
                self.assertEqual(events, [])

    def test_on_substage_reports_allowlisted_detail_without_private_leaks(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace, task_parent, home_parent = self.layout(Path(tmp))
            self.components()
            recorded = []

            def fail_cargo(_sources, _cargo_home, _scratch, _guard, *, timeout, diag):
                diag["code"] = "FETCH_NORMALIZE_TIMEOUT"
                raise ValueError("PUBLIC_VENDOR")

            with mock.patch.object(acquire.cargo_fetch, "fetch_and_stage_vendor",
                                   side_effect=fail_cargo):
                with self.assertRaisesRegex(ValueError, "^PUBLIC_ACQUIRE$"):
                    acquire.acquire_all(
                        workspace, task_parent, home_parent,
                        manifest=self.manifest(workspace, task_parent, home_parent),
                        vendor_mode="cargo_vendor_locked",
                        on_substage=lambda *row: recorded.append(row),
                        stage_budget_seconds=120)
            self.assertEqual([r[:2] for r in recorded[:-1]], [
                ("sources", "passed"),
                ("nim_fetch", "passed"),
                ("nim_extract", "passed"),
                ("parser_tools", "passed"),
                ("parser_seed", "passed"),
                ("parser_build", "passed"),
            ])
            self.assertEqual(recorded[-1][:3],
                             ("cargo_vendor", "failed", "FETCH_NORMALIZE_TIMEOUT"))


if __name__ == "__main__":
    unittest.main()
