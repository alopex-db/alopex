"""Original Lazy-file measurement with resource observations outside its clocks."""
import hashlib
import json
import statistics
import time
from polars_entrypoints_v0816 import CASES, Operations, assert_equal, inputs, oracle, _hash


def measure(engine, case, directory, raw_path, rows=34250, warmups=3, runs=21, *, observer):
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
            observer.begin(index)
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
            observation = observer.finish(index)
            actual = {name: [] for name in expected}
            for batch in batches:
                values = batch.to_dict(as_series=False)
                assert list(values) == list(expected), "Batch column order mismatch"
                for name, column in values.items():
                    actual[name].extend(column)
            assert_equal(actual, expected)
            if index >= warmups:
                record = {"phase": "measure", "index": index - warmups, "seconds": elapsed,
                          "validated": True, "output_rows": len(actual["add"]), "resources": observation}
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

