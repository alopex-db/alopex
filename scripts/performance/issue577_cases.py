"""Run one frozen engine/shape/policy case per process and preserve each result."""
import hashlib
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import subprocess
import sys


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as output:
        json.dump(value, output, indent=2)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


@dataclass
class CaseRunner:
    engine: str
    binary: Path
    profile_path: Path
    fixtures: Path

    def run(self, case, limit, target, execute):
        profile = json.loads(self.profile_path.read_text())
        fixture_hash = digest(self.fixtures / (case["id"] + ".parquet"))
        policy = "none" if limit is None else str(limit)
        command = ["rtk", "proxy", "taskset", "-c", "0", str(self.binary), self.engine,
                   str(self.profile_path), case["id"], str(self.fixtures), policy, str(target)]
        execute(command)
        row = json.loads(target.read_text())
        expected = {"schema": "alopex.parquet-comparison-result/v1", "engine": self.engine,
                    "case": case["id"], "memory_limit_bytes": limit,
                    "fixture_sha256": fixture_hash, "profile_sha256": digest(self.profile_path),
                    "rows": case["rows"], "columns": case["columns"],
                    "values_verified": True, "cpu_affinity": [0], "thread_count": 1,
                    "warmup_runs": 3, "measurement_runs": 21,
                    "cache_state": "warm", "phase": "public_sql_owned_rows",
                    "build_profile": "release"}
        assert all(row.get(key) == value for key, value in expected.items())
        samples = row["latency_samples_ms"]
        assert len(samples) == 21 and all(math.isfinite(x) and x > 0 for x in samples)
        ordered = sorted(samples)
        assert row["latency_p50_ms"] == ordered[10]
        assert row["latency_p95_ms"] == ordered[19]
        assert all(math.isfinite(row[key]) and row[key] > 0 for key in profile["metrics"])
        return {"file": target.name, "sha256": digest(target)}


def main():
    engine, binary, profile_name, fixtures_name, output_name = sys.argv[1:]
    assert engine in ("before", "after", "duckdb")
    profile_path, fixtures = Path(profile_name).resolve(), Path(fixtures_name).resolve()
    binary, output = Path(binary).resolve(), Path(output_name).resolve()
    profile = json.loads(profile_path.read_text())
    assert profile["cpu_affinity"] == [0]
    output.mkdir(parents=True, exist_ok=False)
    manifest = {"owner": "parent Codex", "engine": engine,
                "engine_revision": profile[engine] if engine != "duckdb" else profile["reference"]["revision"],
                "binary_sha256": digest(binary), "profile_sha256": digest(profile_path),
                "status": "incomplete", "results": []}
    manifest_path = output / "manifest.json"
    save(manifest_path, manifest)
    runner = CaseRunner(engine, binary, profile_path, fixtures)

    def execute(command):
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL)

    try:
        for case in profile["cases"]:
            for limit in profile["memory_limits_bytes"]:
                policy = "none" if limit is None else str(limit)
                target = output / (case["id"] + "-" + policy + ".json")
                manifest["results"].append(runner.run(case, limit, target, execute))
                save(manifest_path, manifest)
        assert len(manifest["results"]) == len(profile["cases"]) * len(profile["memory_limits_bytes"])
        assert digest(binary) == manifest["binary_sha256"]
        assert digest(profile_path) == manifest["profile_sha256"]
        manifest["status"] = "engine_cases_measured"
    except BaseException as error:
        manifest.update(status="failed", error=type(error).__name__)
        raise
    finally:
        save(manifest_path, manifest)


if __name__ == "__main__":
    main()
