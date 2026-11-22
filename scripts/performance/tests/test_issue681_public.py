"""Public #681 collector boundary; synthetic child only, no database workload."""

import hashlib
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_public as public
import issue681_public_audit as audit


class Counter:
    def __init__(self, pid):
        self.active = None

    def begin(self, phase):
        assert self.active is None
        self.active = phase

    def finish(self, phase):
        assert self.active == phase
        self.active = None
        before = {"rss_bytes": 1024, "high_water_bytes": 1024}
        after = {"rss_bytes": 1536, "high_water_bytes": 2048}
        return {"phase": phase, "before_reset": before, "after_reset": before,
                "after": after, "kernel_reported_peak_rss_bytes": 2048,
                "kernel_high_water_decreased": False,
                "aggregation": "maximum of reset and end kernel high-water readings",
                "scope": "child process between phase handshakes, including end-event serialization",
                "counter": "Linux VmHWM after clear_refs=5; kernel RSS accounting accuracy applies"}

    def close(self):
        pass


def events(*, finish="rollback", malformed=False, secret=False,
           validated=True):
    result = []
    for phase in (*public.PHASES, finish):
        result.append({"phase": phase, "data": {"event": "start"}})
        data = {"event": "end"}
        if phase != "warm":
            data["elapsed_ns"] = 12345
        if phase in ("cold_transaction_1", "cold_transaction_2", "warm",
                     "first_insert", "second_insert"):
            data["stats"] = {"hits": 2, "misses": 1, "indexes": [
                {"name": "private/path", "last_access_ns": 123}]}
        if phase == "warm":
            data["graph_bytes"] = 123456
        if secret:
            data["local_path"] = "/private/host/unpublishable"
        result.append({"phase": phase, "data": data})
    result.append({"phase": "complete", "data": {
        "finish": finish, "result_validated": validated,
        "stats": {"hits": 2, "misses": 1}}})
    if malformed:
        result[3]["phase"] = "unknown/private/path"
    return result


class PublicCollectorTests(unittest.TestCase):
    def test_inventory_includes_prior_cases_and_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            current = root / "current"
            previous = root / "previous"
            assets = root / "assets"
            for path in (current, previous, assets):
                path.mkdir(mode=0o700)
            with mock.patch.object(public.storage, "inventory",
                                   return_value={"allocated_bytes": 1_000}) as inventory:
                public._inventory(current, (previous, assets))
            inventory.assert_called_once_with([current, previous, assets])

    def measure(self, *, malformed=False, secret=False, stderr_secret=False,
                capacity_error=False, counter_error=False, finish="rollback",
                validated=True, tamper_private=None, exit_delay=0):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "fixture"
            payload = [json.dumps(row, separators=(",", ":")) for row in
                       events(finish=finish, malformed=malformed, secret=secret,
                              validated=validated)]
            binary.write_text(
                f"#!{sys.executable}\nimport sys,time\nlines={payload!r}\n"
                + ("print('/private/host/stderr',file=sys.stderr)\n" if stderr_secret else "")
                + "for line in lines:\n print(line,flush=True)\n"
                  " if sys.stdin.readline().strip() != 'continue': sys.exit(9)\n"
                + f"time.sleep({exit_delay!r})\n")
            binary.chmod(0o700)
            output = root / "export"
            output.mkdir(mode=0o700)
            private = root / "private"
            private.mkdir(mode=0o700)
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            inventory = (mock.Mock(side_effect=OSError("/private/host/capacity"))
                         if capacity_error else mock.Mock(return_value={
                             "allocated_bytes": 1000}))
            counter = (mock.Mock(side_effect=OSError("/private/host/proc"))
                       if counter_error else Counter)
            with mock.patch.object(public.sys, "platform", "linux"), \
                    mock.patch.object(public.storage, "inventory", inventory):
                result = public.run_case(
                    binary=binary, directory=output, private_dir=private,
                    source_commit="a" * 40,
                    binary_sha256=digest, rows=1000, dimensions=384, trial=0,
                    finish=finish, timeout=5, rss_limit=1_000_000,
                    counter_factory=counter)
            content = (output / "public.json").read_bytes()
            self.assertEqual(list(output.iterdir()), [output / "public.json"])
            self.assertEqual(content, public._canonical(result))
            self.assertEqual((output / "public.json").stat().st_mode & 0o777, 0o600)
            self.assertNotIn(b"/private/host", content)
            if result["status"] == "measured":
                self.assertEqual(list(private.iterdir()), [private / "audit.jsonl"])
                self.assertEqual(len((private / "audit.jsonl").read_bytes().splitlines()), 20)
                if tamper_private is not None:
                    lines = [json.loads(line) for line in
                             (private / "audit.jsonl").read_bytes().splitlines()]
                    if tamper_private == "elapsed":
                        lines[1]["event"]["data"]["elapsed_ns"] += 1
                    elif tamper_private == "peak":
                        lines[1]["observation"]["kernel_reported_peak_rss_bytes"] += 1
                    elif tamper_private == "receipt":
                        lines[-1]["receipt"]["runner_sha256"] = "0" * 64
                    (private / "audit.jsonl").write_bytes(b"".join(
                        public._canonical(line) for line in lines))
                pin = {"source_commit": "a" * 40, "source_tree": "1" * 40,
                       "binary_sha256": digest, "assets_digest": "2" * 64,
                       "example_sha256": "3" * 64, "parser_sha256": "4" * 64,
                       "parser_record_sha256": "5" * 64,
                       "cargo_lock_sha256": "6" * 64,
                       "rust_version": "1.96.0", "version": "0.8.17",
                       "profile": "release", "platform": "linux/amd64",
                       "parser_contract": "0.28.0"}
                if tamper_private is None:
                    self.assertEqual(audit.audit_case(
                        output, private, binary, pin, 1000, 0, finish), result)
                else:
                    with self.assertRaisesRegex(ValueError, "^PRIVATE_AUDIT$"):
                        audit.audit_case(output, private, binary, pin, 1000, 0, finish)
            return result

    def test_collection_constructs_only_typed_public_rows(self):
        result = self.measure(secret=True, stderr_secret=True)
        self.assertEqual(result["status"], "measured")
        self.assertEqual(len(result["phases"]), 9)
        self.assertEqual(result["phases"][3]["cache_hits"], 2)
        self.assertEqual(result["phases"][4]["elapsed_ns"], None)
        self.assertEqual(set(result["phases"][3]), {
            "phase", "elapsed_ns", "kernel_peak_rss_bytes",
            "cache_hits", "cache_misses", "graph_bytes"})
        self.assertEqual(result["phases"][4]["graph_bytes"], 123456)

    def test_validated_child_may_exit_after_five_seconds(self):
        self.assertEqual(self.measure(exit_delay=6)["status"], "measured")

    def test_child_over_post_finish_grace_is_stopped(self):
        with mock.patch.object(public, "POST_FINISH_SECONDS", 1):
            self.assertEqual(self.measure(exit_delay=3)["status"], "failed")

    def test_commit_mode_requires_validated_completion(self):
        result = self.measure(finish="commit", stderr_secret=True)
        self.assertEqual(result["status"], "measured")
        self.assertEqual(result["finish"], "commit")
        self.assertTrue(result["result_validated"])
        self.assertEqual(result["phases"][-1]["phase"], "commit")
        self.assertEqual(result["phases"][-1]["elapsed_ns"], 12345)

    def test_unvalidated_completion_is_not_measured(self):
        result = self.measure(finish="commit", validated=False)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error_code"], "PUBLIC_PROTOCOL")
        self.assertEqual(result["completed_phases"], 9)

    def test_private_event_counter_and_receipt_tamper_fail_audit(self):
        for tamper in ("elapsed", "peak", "receipt"):
            with self.subTest(tamper=tamper):
                self.measure(tamper_private=tamper)

    def test_invalid_event_fails_with_fixed_code_only(self):
        result = self.measure(malformed=True, stderr_secret=True)
        self.assertEqual(result, {"schema": public.SCHEMA, "status": "failed",
                                  "error_code": "PUBLIC_PROTOCOL", "completed_phases": 1})

    def test_capacity_failure_emits_only_fixed_status(self):
        result = self.measure(capacity_error=True)
        self.assertEqual(result, {"schema": public.SCHEMA, "status": "failed",
                                  "error_code": "PUBLIC_CAPACITY", "completed_phases": 0})

    def test_counter_failure_emits_only_fixed_status(self):
        result = self.measure(counter_error=True)
        self.assertEqual(result, {"schema": public.SCHEMA, "status": "failed",
                                  "error_code": "PUBLIC_COUNTER", "completed_phases": 0})

    def test_unbounded_elapsed_is_rejected(self):
        with self.assertRaises(public.PublicFailure):
            public._record("seed", {"event": "end", "elapsed_ns": 10**30},
                           {"phase": "seed", "kernel_reported_peak_rss_bytes": 1024})

    def test_cli_parse_error_exposes_only_fixed_code(self):
        output = io.StringIO()
        errors = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = public.main(["--unknown=/private/host/path"])
        self.assertEqual(code, 1)
        self.assertEqual(output.getvalue(), "PUBLIC_INPUT\n")
        self.assertEqual(errors.getvalue(), "")

    def test_failed_public_write_removes_partial_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            with mock.patch.object(public.os, "fsync", side_effect=OSError("private/path")):
                with self.assertRaises(public.PublicFailure):
                    public._write(directory, {"schema": public.SCHEMA})
            self.assertEqual(list(directory.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
