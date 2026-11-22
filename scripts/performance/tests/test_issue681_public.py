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


class Counter:
    def __init__(self, pid):
        self.active = None

    def begin(self, phase):
        assert self.active is None
        self.active = phase

    def finish(self, phase):
        assert self.active == phase
        self.active = None
        return {"phase": phase, "kernel_reported_peak_rss_bytes": 2048}

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
    def measure(self, *, malformed=False, secret=False, stderr_secret=False,
                capacity_error=False, counter_error=False, finish="rollback",
                validated=True):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "fixture"
            payload = [json.dumps(row, separators=(",", ":")) for row in
                       events(finish=finish, malformed=malformed, secret=secret,
                              validated=validated)]
            binary.write_text(
                f"#!{sys.executable}\nimport sys\nlines={payload!r}\n"
                + ("print('/private/host/stderr',file=sys.stderr)\n" if stderr_secret else "")
                + "for line in lines:\n print(line,flush=True)\n"
                  " if sys.stdin.readline().strip() != 'continue': sys.exit(9)\n")
            binary.chmod(0o700)
            output = root / "export"
            output.mkdir(mode=0o700)
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            inventory = (mock.Mock(side_effect=OSError("/private/host/capacity"))
                         if capacity_error else mock.Mock(return_value={
                             "allocated_bytes": 1000}))
            counter = (mock.Mock(side_effect=OSError("/private/host/proc"))
                       if counter_error else Counter)
            with mock.patch.object(public.sys, "platform", "linux"), \
                    mock.patch.object(public.storage, "inventory", inventory):
                result = public.run_case(
                    binary=binary, directory=output, source_commit="a" * 40,
                    binary_sha256=digest, rows=1000, dimensions=384, trial=0,
                    finish=finish, timeout=5, rss_limit=1_000_000,
                    counter_factory=counter)
            content = (output / "public.json").read_bytes()
            self.assertEqual(list(output.iterdir()), [output / "public.json"])
            self.assertEqual(content, public._canonical(result))
            self.assertEqual((output / "public.json").stat().st_mode & 0o777, 0o600)
            self.assertNotIn(b"/private/host", content)
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
