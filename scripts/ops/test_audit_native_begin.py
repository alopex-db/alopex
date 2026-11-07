import copy
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import audit_native_begin as audit
from native_begin_matrix import plan
from test_native_begin_supervisor import DIGEST, events


class BeginAuditTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = dict(image={"image_record": {"id": "sha256:" + "f" * 64}}, subjects={})
        for subject, code in (("before", "a"), ("candidate", "b")):
            self.config["subjects"][subject] = dict(source_commit=code * 40, source_tree=code * 40, extension_sha256=DIGEST)
        self.data = {}
        self.rows = []
        for cell in plan():
            prefix = "results/" + cell["id"] + "/"
            files, processes = {}, []
            for index, spec in enumerate([None, *audit.expected_specs(cell["rows"])]):
                raw_events = events(spec)
                raw_events[0].update(rows=cell["rows"], indexes=cell["indexes"])
                if spec is not None:
                    for event in raw_events[1:-1]:
                        if event["iteration"]: event["cache_before"]["entries"] = cell["indexes"]
                        # Explicit synthetic improvement, with no timing randomness.
                        if event["begin_ns"] is not None:
                            event["begin_ns"] = 100 if cell["subject"] == "before" else 80
                            event["after_begin"] = dict(event["after_begin"], rss_bytes=2048 if cell["subject"] == "before" else 1536)
                raw = "\n".join(json.dumps(event) for event in raw_events).encode()
                files[f"cell/process-{index}.ndjson"] = raw
                processes.append(dict(ordinal=index, spec=spec, status="passed", raw_sha256=audit.sha(raw)))
            files["cell/processes.json"] = json.dumps(processes).encode()
            code = "a" if cell["subject"] == "before" else "b"
            source = dict(commit=code * 40, tree=code * 40, profile="release")
            row = dict(cell, status="passed", source=source, assets_digest="asset", extension_sha256=DIGEST,
                       processes=len(processes), processes_sha256=audit.sha(files["cell/processes.json"]))
            report = dict(schema="alopex.native-begin-measurement/v1", status="passed", source=source,
                          rows=cell["rows"], indexes=cell["indexes"], trial=cell["trial"],
                          assets_digest="asset", extension_sha256=DIGEST, worker_error=None,
                          finalization_error=None, processes_sha256=row["processes_sha256"])
            files["result.json"] = json.dumps(report).encode()
            for name, value in files.items(): self.data[prefix + "collected/" + name] = value
            self.data[prefix + "collection.json"] = json.dumps({name: audit.sha(value) for name, value in files.items()}).encode()
            self.data[prefix + "run.json"] = json.dumps(dict(phase="deleted", identities={"assets_digest": "asset"})).encode()
            self.rows.append(row)
        self.data["begin-plan.json"] = json.dumps(dict(cells=plan(), configuration=self.config, image="f" * 64)).encode()
        self.data["begin-results.json"] = json.dumps(self.rows).encode()
        summary = dict(schema="alopex.native-begin-matrix/v1", complete=True, performance_acceptance=False,
                       cells=[dict(cell, status="passed") for cell in plan()])
        summary_file = self.root / "begin-comparison.json"
        summary_file.write_text(json.dumps(summary))
        self.public = dict(schema="alopex.native-begin-matrix/v1", status="passed", matrix_complete=True,
                           performance_qualification=False, owned_resources_cleaned=True, measurement_cells=54,
                           completed_cells=54, image_digest="f" * 64, comparison_sha256=audit.sha(summary_file.read_bytes()))

    def check(self):
        (self.root / "result.json").write_text(json.dumps(self.public))
        archive = self.root / "evidence.tar"
        with tarfile.open(archive, "w") as output:
            for name, data in self.data.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                output.addfile(member, io.BytesIO(data))
        return audit.audit(self.root, archive, self.config)

    def test_complete_matrix_is_independently_recomputed(self):
        result = self.check()
        self.assertEqual(result["verified_cells"], 54)
        self.assertFalse(result["performance_acceptance"])
        comparisons = result["comparisons"]
        self.assertTrue(all(not row["comparison"]["improvement_proven"] for row in comparisons if row["rows"] == 0))
        begins = [row for row in comparisons if row["rows"] and row["metric"] == "begin_ns"]
        self.assertTrue(begins and all(row["comparison"]["improvement_proven"] for row in begins))

    def test_missing_cell_rejected(self):
        self.data["begin-results.json"] = json.dumps(self.rows[:-1]).encode()
        with self.assertRaises(ValueError): self.check()

    def test_raw_tamper_rejected(self):
        key = next(key for key in self.data if key.endswith("process-1.ndjson"))
        self.data[key] += b"\n{}"
        with self.assertRaises(ValueError): self.check()

    def test_wrong_source_rejected(self):
        self.config["subjects"]["before"]["source_commit"] = "c" * 40
        self.data["begin-plan.json"] = json.dumps(dict(cells=plan(), configuration=self.config, image="f" * 64)).encode()
        with self.assertRaises(ValueError): self.check()

    def test_cleanup_or_qualification_claim_rejected(self):
        for key, value in (("owned_resources_cleaned", False), ("performance_qualification", True)):
            original = self.public[key]
            self.public[key] = value
            with self.assertRaises(ValueError): self.check()
            self.public[key] = original

    def test_predeclared_time_and_memory_thresholds(self):
        self.assertTrue(audit.compare([100] * 3, [90] * 3, "begin_ns", 1000)["improvement_proven"])
        self.assertFalse(audit.compare([100] * 3, [91] * 3, "begin_ns", 1000)["improvement_proven"])
        self.assertFalse(audit.compare([70, 100, 130], [90] * 3, "begin_ns", 1000)["improvement_proven"])
        self.assertFalse(audit.compare([100] * 3, [50] * 3, "begin_ns", 0)["improvement_proven"])
        self.assertTrue(audit.compare([1024] * 3, [512] * 3, "begin_retained_rss_bytes", 1000)["improvement_proven"])
        self.assertFalse(audit.compare([0] * 3, [-512] * 3, "begin_retained_rss_bytes", 1000)["improvement_proven"])
        self.assertTrue(audit.compare([100] * 3, [120] * 3, "begin_ns", 1000)["increased"])

    def test_missing_cache_and_phase_observations_rejected(self):
        spec = audit.expected_specs(1000)[1]
        for change in ("cache", "phase", "rss"):
            value = events(spec)
            if change == "cache": value[2]["cache_before"]["entries"] = 1
            if change == "phase": value[1]["begin_ns"] = None
            if change == "rss": value[1]["after_begin"]["rss_bytes"] = float("nan")
            with self.assertRaises(ValueError): audit.extract(value, spec, dict(rows=1000, indexes=2), DIGEST)


if __name__ == "__main__":
    unittest.main()
