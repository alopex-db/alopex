import copy
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import audit_native_empty_query_prefault as audit
from native_empty_query_prefault_matrix import plan
from test_native_begin_supervisor import DIGEST, events as base_events


def events(spec):
    output = base_events(spec)
    if spec is None: return output
    output[0].update(diagnostic_schema="alopex.begin-observer-comparison/v1", observer_mode=spec["observer_mode"])
    groups={g:{k:0 for k in audit.FIELDS} for g in audit.GROUPS}
    page=dict(page_size=4096,before=groups,after=groups,extension_pages=[dict(
        file_offset=0,permissions='r-xp',mapped_pages=1,present_before=1,present_after=1,
        newly_present=[],no_longer_present=[])])
    output[0]['page_observer_control']=copy.deepcopy(page) if spec['observer_mode']!='plain' else None
    for index, sample in enumerate(output[1:-1]):
        sample['prefault_pages'] = copy.deepcopy(page) if spec['observer_mode']=='prefault' and index==0 else None
        sample['prefault_ns'] = 10 if spec['observer_mode']=='prefault' and index==0 else None
        sample['query_pages']=copy.deepcopy(page) if spec['observer_mode']!='plain' else None
        names = {"auto": "auto_total_ns"} if spec["mode"] == "auto" else {"begin": "begin_ns", "rollback": "rollback_ns"}
        if spec["mode"] != "auto" and spec["operation"] == "query": names["query"] = "query_ns"
        sample["observer_mode"] = spec["observer_mode"]
        sample["diagnostic_phases"] = {phase: (observation(sample[key]) if spec["observer_mode"]!="plain" else {"wall_ns":sample[key]}) for phase, key in names.items()}
    output[-1]["query_plan"] = [{"plan": "HnswSearch"}]
    return output


def observation(wall):
    return dict(wall_ns=wall, process_cpu_ns=40, caller_cpu_ns=30,
                counters={name: 0 for name in ("ru_minflt", "ru_majflt", "ru_nvcsw", "ru_nivcsw", "ru_inblock", "ru_oublock")},
                io_bytes={name: 0 for name in ("read_bytes", "write_bytes", "cancelled_write_bytes")})


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
            for index, spec in enumerate([None, *audit.expected_specs(cell["rows"], cell["indexes"], cell["trial"], cell["subject"])]):
                raw_events = events(spec)
                raw_events[0].update(rows=cell["rows"], indexes=cell["indexes"])
                if spec is not None: raw_events[-1]["query_plan"] = [{"plan": "HnswSearch" if cell["indexes"] else "ExactKnnScan"}]
                if spec is not None:
                    for event in raw_events[1:-1]:
                        if event["iteration"]: event["cache_before"]["entries"] = cell["indexes"]
                        # Explicit synthetic improvement, with no timing randomness.
                        if event["begin_ns"] is not None:
                            event["begin_ns"] = 100 if cell["subject"] == "before" else 80
                            event["diagnostic_phases"]["begin"]["wall_ns"] = event["begin_ns"]
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
        summary = dict(schema="alopex.native-empty-query-prefault/v1", complete=True, performance_acceptance=False,
                       cells=[dict(cell, status="passed") for cell in plan()])
        summary_file = self.root / "begin-comparison.json"
        summary_file.write_text(json.dumps(summary))
        self.public = dict(schema="alopex.native-empty-query-prefault/v1", status="passed", matrix_complete=True,
                           performance_qualification=False, owned_resources_cleaned=True, measurement_cells=6,
                           completed_cells=6, image_digest="f" * 64, comparison_sha256=audit.sha(summary_file.read_bytes()))

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
        self.assertEqual(result["verified_cells"], 6)
        self.assertFalse(result["performance_acceptance"])
        comparisons = result["comparisons"]
        self.assertTrue(all(row["comparison"]["performance_acceptance"] is False for row in comparisons))
        cpu = [row for row in comparisons if row["metric"] == "query.process_cpu_ns"]
        self.assertEqual(len(cpu), 4)
        self.assertEqual(len(result['page_observations']), 12)
        self.assertTrue(result['prefault_effects'])
        self.assertTrue(all(r['comparison']['performance_acceptance'] is False for r in result['prefault_effects']))
        self.assertTrue(all(row["trials"] == {"before": [40]*3, "candidate": [40]*3} for row in cpu))
        controls = [row for row in comparisons if row["cache_state"] == "observer_control"]
        self.assertFalse(controls)
        self.assertTrue(result["observer_effects"])
        self.assertTrue(all(r["comparison"]["difference"]==0 for r in result["observer_effects"]))

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

    def test_diagnostic_summary_never_qualifies_or_divides_zero(self):
        result = audit.compare([0]*3, [10]*3, "counter", 1000)
        self.assertIsNone(result["ratio"])
        self.assertFalse(result["performance_acceptance"])
        self.assertEqual(result["difference"], 10)

    def test_missing_diagnostics_rejected_independently(self):
        spec = audit.expected_specs(0, 2)[1]
        for mutate in [lambda e: e[0].__setitem__("timer_controls", []),
                       lambda e: e[1]["diagnostic_phases"]["query"]["counters"].pop("ru_nvcsw"),
                       lambda e: e[1]["diagnostic_phases"]["query"].__setitem__("caller_cpu_ns", -1),
                       lambda e: e[-1].pop("query_plan")]:
            value = events(spec)
            value[0].update(rows=0, indexes=2)
            for sample in value[1:-1]: sample["cache_before"]["entries"] = 2
            audit.extract(value, spec, dict(rows=0, indexes=2), DIGEST)
            mutate(value)
            with self.assertRaises(ValueError): audit.extract(value, spec, dict(rows=0, indexes=2), DIGEST)

    def test_missing_cache_and_phase_observations_rejected(self):
        spec = audit.expected_specs(0, 2)[1]
        for change in ("cache", "phase", "rss"):
            value = events(spec)
            value[0].update(rows=0, indexes=2)
            value[-1]["query_plan"] = [{"plan": "HnswSearch"}]
            for sample in value[1:-1]: sample["cache_before"]["entries"] = 2
            audit.extract(value, spec, dict(rows=0, indexes=2), DIGEST)
            if change == "cache": value[2]["cache_before"]["entries"] = 0
            if change == "phase": value[1]["begin_ns"] = None
            if change == "rss": value[1]["after_begin"]["rss_bytes"] = float("nan")
            with self.assertRaises(ValueError): audit.extract(value, spec, dict(rows=0, indexes=2), DIGEST)


if __name__ == "__main__":
    unittest.main()
