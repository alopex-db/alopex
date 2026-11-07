import copy
import unittest

import native_sql_measure_worker as worker


class CellAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.config = {"case": "aggregate", "engine": "alopex-server", "trial": 0}
        self.result = {"case": "aggregate", "engine": "alopex-server", "server_client": True,
                       "fixture_rows": 34250, "warmups": 3, "samples": 21,
                       "result_sha256": "a" * 64, "latency_p50_ms": 1.0}
        process = dict.fromkeys(worker.COUNTERS, 0)
        process.update(pid=2, start_ticks=5)
        observation = {"schema": "alopex.sql-operation-resources/v1", "status": "observed",
                       "coverage": "stable_pid_namespace_allocation", "manifest_sha256": "b" * 64,
                       "client": dict(process, pid=3), "server_processes": [process]}
        self.raw = [{"sample": i, "seconds": .001, "resources": copy.deepcopy(observation)} for i in range(21)]

    def verify(self):
        worker.verify_cell(self.result, self.raw, self.config, "b" * 64)

    def test_complete_observed_cell_accepts_real_zero_counters(self):
        self.verify()

    def test_partial_sample_set_cannot_pass(self):
        self.raw.pop()
        with self.assertRaises(ValueError): self.verify()

    def test_missing_io_cannot_be_substituted_by_zero(self):
        del self.raw[7]["resources"]["server_processes"][0]["write_bytes"]
        with self.assertRaises(KeyError): self.verify()

    def test_missing_resource_cell_fails(self):
        del self.raw[7]["resources"]
        with self.assertRaises(KeyError): self.verify()

    def test_bad_measurements_and_identity_fail(self):
        for field, value in (("status", "failed"), ("coverage", "endpoint_only"),
                             ("manifest_sha256", "c" * 64)):
            with self.subTest(field=field):
                previous = self.raw[0]["resources"][field]
                self.raw[0]["resources"][field] = value
                with self.assertRaises(ValueError): self.verify()
                self.raw[0]["resources"][field] = previous

    def test_counter_and_latency_nonfinite_values_fail(self):
        for value in (float("nan"), float("inf"), -1, True):
            with self.subTest(value=value):
                self.raw[0]["resources"]["client"]["read_bytes"] = value
                with self.assertRaises(ValueError): self.verify()
        self.raw[0]["resources"]["client"]["read_bytes"] = 0
        for value in (float("nan"), float("inf"), -1, 0, True):
            self.raw[0]["seconds"] = value
            with self.assertRaises(ValueError): self.verify()

    def test_server_and_client_cannot_be_double_counted(self):
        self.raw[0]["resources"]["client"]["pid"] = 2
        with self.assertRaises(ValueError): self.verify()

    def test_duplicate_server_rows_fail(self):
        servers = self.raw[0]["resources"]["server_processes"]
        servers.append(copy.deepcopy(servers[0]))
        with self.assertRaises(ValueError): self.verify()

    def test_changed_fixture_or_warmup_fails(self):
        for field, value in (("fixture_rows", 2), ("warmups", 0), ("latency_p50_ms", .5)):
            with self.subTest(field=field):
                old = self.result[field]; self.result[field] = value
                with self.assertRaises(ValueError): self.verify()
                self.result[field] = old

    def test_reordered_samples_fail(self):
        self.raw[2], self.raw[3] = self.raw[3], self.raw[2]
        with self.assertRaises(ValueError): self.verify()

    def test_random_get_keeps_sample_unit_of_100_requests(self):
        self.config["case"] = self.result["case"] = "random-get-hit"
        self.result.update(fixture_rows=10000, input_sha256="c" * 64, operations_per_sample=100)
        for row in self.raw:
            row["index"] = row.pop("sample"); row["phase"] = "measure"
        self.verify()
        self.result["operations_per_sample"] = 1
        with self.assertRaises(ValueError): self.verify()

    def test_known_excluded_case_is_not_admitted(self):
        self.config["case"] = self.result["case"] = "drop-sequence"
        with self.assertRaises(ValueError): self.verify()


if __name__ == "__main__":
    unittest.main()
