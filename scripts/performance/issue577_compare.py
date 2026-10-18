"""Compare only complete, digest-bound real measurements from the frozen matrix."""
import csv
import hashlib
import json
import math
from pathlib import Path
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compare(profile_path, directories):
    profile = json.loads(profile_path.read_text())
    profile_hash = digest(profile_path)
    rows, errors, indexed = [], [], {}
    for engine, directory in zip(("before", "after", "duckdb"), directories):
        try:
            manifest = json.loads((directory / "manifest.json").read_text())
            revision = profile[engine] if engine != "duckdb" else profile["reference"]["revision"]
            assert manifest["engine"] == engine and manifest["engine_revision"] == revision
            assert manifest["profile_sha256"] == profile_hash
            assert manifest["status"] == "engine_cases_measured"
            files = manifest["results"]
            assert len(files) == len(profile["cases"]) * len(profile["memory_limits_bytes"])
            assert len({x["file"] for x in files}) == len(files)
        except (OSError, ValueError, KeyError, AssertionError, TypeError):
            files = []
        for entry in files:
            label = entry.get("file") if isinstance(entry, dict) else "<invalid>"
            try:
                name = entry["file"]
                assert Path(name).name == name
                path = directory / name
                assert digest(path) == entry["sha256"]
                row = json.loads(path.read_text())
                case = next(x for x in profile["cases"] if x["id"] == row["case"])
                policy = row["memory_limit_bytes"]
                assert policy in profile["memory_limits_bytes"]
                expected = {"schema": "alopex.parquet-comparison-result/v1", "engine": engine,
                            "profile_sha256": profile_hash, "rows": case["rows"],
                            "columns": case["columns"], "cpu_affinity": [0], "thread_count": 1,
                            "phase": "public_sql_owned_rows", "build_profile": "release",
                            "cache_state": "warm", "warmup_runs": 3, "measurement_runs": 21,
                            "values_verified": True}
                assert all(row.get(k) == v for k, v in expected.items())
                assert len(row["fixture_sha256"]) == 64
                samples = sorted(row["latency_samples_ms"])
                assert len(samples) == 21 and all(type(x) in (int, float) and math.isfinite(x) and x > 0 for x in samples)
                assert row["latency_p50_ms"] == samples[10] and row["latency_p95_ms"] == samples[19]
                assert all(type(row[k]) in (int, float) and math.isfinite(row[k]) and row[k] > 0 for k in profile["metrics"])
                assert math.isclose(row["rows_per_second"], case["rows"] * 1000 / samples[10])
                key = (engine, case["id"], policy)
                assert key not in indexed
                indexed[key] = row
                rows.append({k: row[k] for k in ["engine", "case", "memory_limit_bytes", "fixture_sha256", *profile["metrics"]]})
            except (OSError, ValueError, KeyError, AssertionError, StopIteration, TypeError):
                errors.append(f"invalid measurement: {engine}/{label}")
    ratios = []
    for case in profile["cases"]:
        for policy in profile["memory_limits_bytes"]:
            cells = [indexed.get((engine, case["id"], policy)) for engine in ("before", "after", "duckdb")]
            for engine, row in zip(("before", "after", "duckdb"), cells):
                if row is None:
                    errors.append(f"missing cell: {engine}/{case['id']}/{policy}")
            if any(row is None for row in cells):
                continue
            if len({row["fixture_sha256"] for row in cells}) != 1:
                errors.append(f"fixture mismatch: {case['id']}/{policy}")
                continue
            before, after, reference = cells
            primary = profile["primary_metric"]
            for peer, row in (("before", before), ("reference", reference)):
                ratio = after[primary] / row[primary]
                limit = profile[f"max_after_{peer}_latency_ratio"]
                ratios.append({"case": case["id"], "memory_limit_bytes": policy, "peer": peer, "ratio": ratio})
                if ratio > limit:
                    errors.append(f"latency exceeds {peer}: {case['id']}/{policy}: {ratio:.6f} > {limit}")
    return {"schema": "alopex.parquet-comparison-report/v1", "profile_sha256": profile_hash,
            "passed": not errors, "required_cells": len(profile["cases"]) * len(profile["memory_limits_bytes"]) * 3, "measured_cells": len(rows),
            "rows": rows, "latency_ratios": ratios, "errors": errors}


def main():
    profile, before, after, reference, prefix = map(Path, sys.argv[1:])
    report = compare(profile, [before, after, reference])
    prefix.parent.mkdir(parents=True, exist_ok=True)
    with prefix.with_suffix(".json").open("x") as output:
        json.dump(report, output, indent=2)
    keys = ["engine", "case", "memory_limit_bytes", "fixture_sha256", "latency_p50_ms", "latency_p95_ms", "rows_per_second", "peak_rss_bytes"]
    with prefix.with_suffix(".csv").open("x", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(report["rows"])
    with prefix.with_suffix(".md").open("x") as output:
        output.write("| Engine | Case | Budget B | p50 ms | p95 ms | Rows/s | Peak RSS B |\n|---|---|---:|---:|---:|---:|---:|\n")
        for row in report["rows"]:
            values = [row[k] for k in keys if k != "fixture_sha256"]
            output.write("| " + " | ".join(map(str, values)) + " |\n")
        output.write("\n" + "\n".join(report["errors"]) + "\n")
    print(json.dumps({k: report[k] for k in ("passed", "required_cells", "measured_cells", "errors")}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
