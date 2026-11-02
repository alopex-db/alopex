"""Probe Polars entrypoints and compare lazy scans without DataFrame construction."""
import argparse
import csv
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import resource
import statistics
import subprocess
import sys
import time

from dataframe_phases_v0816 import Operations, assert_equal, inputs, oracle, _hash

CASES = ("csv-collect", "parquet-collect", "csv-batches", "parquet-batches",
         "parquet-numeric-collect", "parquet-numeric-batches")
DEFAULT_CASES = tuple(case for case in CASES if case not in ("parquet-collect", "parquet-batches"))
PROBES = ("module-select", "series-constructor", "series-arithmetic", "lazy-constructor",
          "module-scan-csv", "module-scan-parquet")
ENGINES = ("alopex", "polars")


def fixtures(directory, rows):
    directory.mkdir(parents=True, exist_ok=False)
    columns = inputs(rows)
    with (directory / "rows.csv").open("x", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("id", "label"))
        writer.writerows(zip(columns["id"], columns["label"]))
    import polars as pl
    pl.DataFrame({"id": columns["id"], "label": columns["label"]}).write_parquet(directory / "rows.parquet")


def probe(engine, case, directory, rows):
    api = Operations(engine, rows).api
    try:
        if case == "module-select":
            actual = api.select(api.lit(2).add(1).alias("value")).to_dict(as_series=False)
            expected = {"value": [3]}
        elif case == "series-constructor":
            actual = api.Series("id", [0, 1, None]).to_list()
            expected = [0, 1, None]
        elif case == "series-arithmetic":
            # Probe arithmetic independently of the unavailable direct constructor.
            series = api.DataFrame({"id": [0, 1, None]}).to_dict()["id"]
            actual = (series + 1).to_list()
            expected = [1, 2, None]
        elif case == "lazy-constructor":
            actual = api.LazyFrame({"id": [0, 1, None]}).collect().to_dict(as_series=False)
            expected = {"id": [0, 1, None]}
        elif case in ("module-scan-csv", "module-scan-parquet"):
            extension = "csv" if case.endswith("csv") else "parquet"
            actual = getattr(api, f"scan_{extension}")(str(directory / f"rows.{extension}")).collect().to_dict(as_series=False)
            source = inputs(rows)
            expected = {"id": source["id"], "label": source["label"]}
        else:
            raise ValueError(f"Unknown probe: {case}")
    except (AttributeError, TypeError, NotImplementedError) as error:
        return {"engine": engine, "case": case, "status": "capability-gap",
                "exception": type(error).__name__, "message": str(error)}
    assert_equal(actual, expected)
    return {"engine": engine, "case": case, "status": "supported", "result_sha256": _hash(expected)}


def measure(engine, case, directory, raw_path, rows=34250, warmups=3, runs=21):
    if case not in CASES or rows <= 0 or warmups < 0 or runs <= 0:
        raise ValueError("Valid case, positive rows/runs and nonnegative warmups required")
    operations = Operations(engine, rows)
    extension, *_, mode = case.split("-")
    fixture = directory / f"rows.{extension}"
    # Both workers warm the identical file outside timing, without constructing a frame.
    input_hash = hashlib.sha256(fixture.read_bytes()).hexdigest()
    scan_owner = operations.api.LazyFrame if engine == "alopex" else operations.api
    scan = getattr(scan_owner, f"scan_{extension}")
    expected = oracle(inputs(rows))["result"]
    numeric_only = "-numeric-" in case
    if numeric_only:
        expected.pop("joined")
    samples = []
    with raw_path.open("x") as raw:
        for index in range(warmups + runs):
            started = time.perf_counter()
            source = operations.next_plan(scan(str(fixture)))
            plan = source.select(*operations.expressions()[:-1]) if numeric_only else operations.select_plan(source)
            if mode == "collect":
                batches = [plan.collect()]
            elif engine == "alopex":
                with plan.collect_batches(chunk_size=8192) as stream:
                    batches = list(stream)
            else:
                batches = list(plan.collect_batches(chunk_size=8192))
            elapsed = time.perf_counter() - started
            actual = {name: [] for name in expected}
            for batch in batches:
                values = batch.to_dict(as_series=False)
                assert list(values) == list(expected), "Batch column order mismatch"
                for name, column in values.items():
                    actual[name].extend(column)
            assert_equal(actual, expected)
            if index >= warmups:
                record = {"phase": "measure", "index": index - warmups, "seconds": elapsed,
                          "validated": True, "output_rows": len(actual["add"])}
                raw.write(json.dumps(record, allow_nan=False) + "\n")
                raw.flush()
                samples.append(elapsed)
            # Release materialized results outside the next timed interval.
            batches = actual = values = batch = plan = source = None
    return {"engine": engine, "case": case, "fixture_rows": rows, "output_rows": rows,
            "warmups": warmups, "samples": runs,
            "latency_p50_ms": statistics.median(samples) * 1000,
            "input_sha256": input_hash, "result_sha256": _hash(expected),
            "scan_entrypoint": f"{engine}." + ("LazyFrame." if engine == "alopex" else "") + f"scan_{extension}",
            "timing": "scan + expression plan + file load + collect/all batches; Python conversion and validation excluded",
            "validation": f"every warmup/sample, all {len(expected)} columns, Python scalar types, NULL, column/row order and values"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--engine", choices=ENGINES)
    parser.add_argument("--case", choices=CASES + PROBES)
    args = parser.parse_args()
    os.environ.update({key: "1" for key in ("POLARS_MAX_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS")})
    os.sched_setaffinity(0, {0})
    output, data = args.output.resolve(), args.data.resolve()
    if args.engine:
        if args.case in PROBES:
            value = probe(args.engine, args.case, data, 34250)
        elif args.case in CASES:
            value = measure(args.engine, args.case, data, Path(os.environ["ALOPEX_PERF_RAW_PATH"]))
        else:
            parser.error("Worker requires --case")
        usage = resource.getrusage(resource.RUSAGE_SELF)
        value["process_peak_rss_bytes"] = usage.ru_maxrss * 1024
        value["process_io"] = {"input_blocks": usage.ru_inblock, "output_blocks": usage.ru_oublock}
        print(json.dumps(value, allow_nan=False))
        return
    if args.case in PROBES:
        parser.error("Parent --case selects a measurement case")
    cases = (args.case,) if args.case else DEFAULT_CASES
    output.mkdir(parents=True, exist_ok=True)
    if (output / "environment.json").exists() or any(output.glob("case-*")):
        raise RuntimeError("Use a new output directory; previous evidence is immutable")
    fixtures(data, 34250)
    spec = importlib.util.spec_from_file_location("shared", Path(__file__).with_name("server-sql-isolated-v0816.py"))
    shared = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shared)
    import polars as pl
    source_files = [Path(__file__), Path(__file__).with_name("dataframe_phases_v0816.py"), Path(shared.__file__)]
    shared.save(output / "environment.json", {
        "schema": "alopex.polars-entrypoints/v1", "python": platform.python_version(),
        "platform": platform.platform(), "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "cpu_model": next(line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),
        "versions": {name: importlib.metadata.version(name) for name in ("alopex", "polars", "polars-runtime-32")},
        "polars_thread_pool_size": pl.thread_pool_size(),
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runner_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], text=True).strip(),
        "source_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files},
        "fixture_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in data.iterdir()},
        "installed_files_sha256": {name: {str(p.relative_to(dist.locate_file(""))): hashlib.sha256(p.read_bytes()).hexdigest()
            for f in dist.files if (p := Path(dist.locate_file(f))).is_file() and p.suffix != ".pyc"}
            for name in ("alopex", "polars", "polars-runtime-32") for dist in [importlib.metadata.distribution(name)]}})
    results, probes = [], []
    for case in PROBES + cases:
        for trial in range(1 if case in PROBES else 3):
            for engine in (ENGINES if trial % 2 == 0 else tuple(reversed(ENGINES))):
                name = f"case-{case}-{engine}-trial{trial}"
                command = [sys.executable, "-B", str(Path(__file__).resolve()), "--output", str(output),
                           "--data", str(data), "--engine", engine, "--case", case]
                # shared.run_process inherits os.environ and persists process failures immediately.
                os.environ["ALOPEX_PERF_RAW_PATH"] = str(output / f"{name}.raw.ndjson")
                status = shared.run_process(command, output, name, {"case": case, "engine": engine, "trial": trial})
                row = dict(status)
                if status["exit_code"] == 0:
                    try:
                        row.update(json.loads((output / f"{name}.stdout.txt").read_text()))
                        if case in CASES:
                            raw = [json.loads(line) for line in (output / f"{name}.raw.ndjson").read_text().splitlines()]
                            assert row["samples"] == len(raw) == 21 and row["warmups"] == 3
                            assert all(r["index"] == i and r["validated"] is True and r["output_rows"] == 34250
                                       and math.isfinite(r["seconds"]) and r["seconds"] > 0 for i, r in enumerate(raw))
                            assert math.isclose(row["latency_p50_ms"], statistics.median(r["seconds"] for r in raw) * 1000)
                        shared.save(output / f"{name}.json", row)
                    except (OSError, ValueError, KeyError, TypeError, AssertionError) as error:
                        status.update(exit_code=65, validation_error=type(error).__name__)
                        shared.save(output / f"{name}.status.json", status)
                        row = dict(status)
                (probes if case in PROBES else results).append(row)
                print(json.dumps(status), flush=True)
    summary = shared.relative(results, cases, ENGINES)
    for cell in summary:
        if cell["comparison_status"] != "valid":
            cell["reason"] = "worker_failure_or_result_mismatch"
            continue
        hashes = {r.get("input_sha256") for r in results if r["case"] == cell["case"]}
        if len(hashes) != 1 or None in hashes:
            case = cell["case"]
            cell.clear()
            cell.update(case=case, comparison_status="failed", reason="input_digest_mismatch_or_missing")
    shared.save(output / "capabilities.json", {"probes": probes})
    shared.save(output / "relative.json", {"schema": "alopex.performance-comparison/v1", "subject": "alopex", "reference": "polars", "cases": summary,
                "default_skips": {case: "Known Utf8/LargeUtf8 failure; explicit --case required" for case in CASES if case not in DEFAULT_CASES}})


if __name__ == "__main__":
    main()
