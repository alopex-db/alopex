import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import native_begin_supervisor as subject


CONFIG = {"rows": 1000, "indexes": 2}
DIGEST = "a" * 64


def events(spec):
    if spec is None:
        return [dict(event="seeded", extension_sha256=DIGEST, **CONFIG, all_rows_unchanged=True)]
    result = [dict(event="ready", extension_sha256=DIGEST, **CONFIG,
                   mode=spec["mode"], operation=spec["operation"], initial_cache={"entries": 0})]
    for index in range(spec["samples"]):
        snapshot = dict(rss_bytes=1024, allocated_bytes=4096, logical_bytes=4096)
        explicit = spec["mode"] != "auto"
        query = spec["operation"] == "query"
        result.append(dict(event="sample", iteration=index, first_after_open=index == 0,
                           begin_ns=10 if explicit else None, rollback_ns=12 if explicit else None,
                           query_ns=11 if explicit and query else None,
                           auto_total_ns=20 if not explicit else None,
                           before=snapshot, after_finish=snapshot,
                           after_begin=snapshot if explicit else None,
                           after_query=snapshot if query else None,
                           cache_before={"entries": 2 if index else 0, "hits": index},
                           cache_after={"entries": 2, "hits": index + 1}))
    result.append(dict(event="complete", all_rows_unchanged=True,
                       peak_before_validation_bytes=1024, peak_after_validation_bytes=2048))
    return result


class BeginSupervisorTests(unittest.TestCase):
    def test_all_phase_variants_and_extra_first_begin_readers(self):
        self.assertEqual(len(subject.probes(0)), 5)
        self.assertEqual(len(subject.probes(100)), 5)
        self.assertEqual(len(subject.probes(1000)), 7)
        for spec in [None, *subject.probes(1000)]:
            subject.verify_events(events(spec), CONFIG, DIGEST, spec)
        for rows in (True, -1, 1, 1001):
            with self.assertRaises(ValueError): subject.probes(rows)

    def test_incomplete_or_reordered_samples_fail(self):
        spec = subject.probes(1000)[0]
        for mutation in (lambda e: e.pop(), lambda e: e.pop(2),
                         lambda e: e[2].update(iteration=0),
                         lambda e: e[-1].update(all_rows_unchanged=False)):
            value = events(spec)
            mutation(value)
            with self.assertRaises(ValueError): subject.verify_events(value, CONFIG, DIGEST, spec)

    def test_fixture_extension_and_mode_mismatch_fail(self):
        spec = subject.probes(1000)[0]
        for change in ({"rows": 100}, {"indexes": 1}, {"extension_sha256": "b" * 64},
                       {"mode": "auto"}, {"initial_cache": {"entries": 1}}):
            value = events(spec)
            value[0].update(change)
            with self.assertRaises(ValueError): subject.verify_events(value, CONFIG, DIGEST, spec)

    def test_missing_nonfinite_and_wrong_phase_observations_fail(self):
        spec = subject.probes(1000)[0]
        for change in ({"begin_ns": None}, {"begin_ns": True}, {"begin_ns": -1},
                       {"query_ns": 1}, {"auto_total_ns": 1}):
            value = events(spec)
            value[1].update(change)
            with self.assertRaises(ValueError): subject.verify_events(value, CONFIG, DIGEST, spec)
        for key, bad in (("rss_bytes", float("nan")), ("rss_bytes", 0),
                         ("rss_bytes", 2 * 1024**3), ("allocated_bytes", -1)):
            value = events(spec)
            value[1]["before"][key] = bad
            with self.assertRaises(ValueError): subject.verify_events(value, CONFIG, DIGEST, spec)

    def test_warm_cache_hit_required(self):
        spec = subject.probes(1000)[1]
        for key, change in (("cache_before", {"entries": 1, "hits": 1}),
                            ("cache_after", {"entries": 2, "hits": 1})):
            value = events(spec)
            value[2][key] = change
            with self.assertRaises(ValueError): subject.verify_events(value, CONFIG, DIGEST, spec)

    def test_wheel_extension_inventory_and_bytes(self):
        import hashlib
        with tempfile.TemporaryDirectory() as temporary:
            wheel = Path(temporary) / "input.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("alopex/_alopex.abi3.so", b"fixture")
            self.assertEqual(subject.extension_digest(wheel), hashlib.sha256(b"fixture").hexdigest())
            with zipfile.ZipFile(wheel, "a") as archive:
                archive.writestr("alopex/_alopex.other.so", b"other")
            with self.assertRaises(ValueError): subject.extension_digest(wheel)

    def test_process_failure_stops_fixture_and_preserves_prior_results(self):
        saved, invoked = [], []
        with tempfile.TemporaryDirectory() as temporary:
            def execute(index, spec):
                invoked.append(index)
                if index == 2:
                    raise InterruptedError("fixture interruption")
                raw = Path(temporary) / str(index)
                raw.write_text("\n".join(json.dumps(event) for event in events(spec)))
                return raw
            with self.assertRaises(InterruptedError):
                subject.run_fixture(CONFIG, execute, lambda rows: saved.append(copy.deepcopy(rows)), DIGEST)
        self.assertEqual(invoked, [0, 1, 2])
        self.assertEqual([row["status"] for row in saved[-1]], ["passed", "passed", "failed"])
        self.assertIn("fixture interruption", saved[-1][-1]["error"])

    def test_success_requires_all_processes_and_hashes(self):
        saved = []
        with tempfile.TemporaryDirectory() as temporary:
            def execute(index, spec):
                raw = Path(temporary) / str(index)
                raw.write_text("\n".join(json.dumps(event) for event in events(spec)))
                return raw
            records = subject.run_fixture(CONFIG, execute, lambda rows: saved.append(copy.deepcopy(rows)), DIGEST)
        self.assertEqual(len(records), 8)
        self.assertTrue(all(row["status"] == "passed" and len(row["raw_sha256"]) == 64 for row in records))
        self.assertEqual(records, saved[-1])


if __name__ == "__main__":
    unittest.main()
