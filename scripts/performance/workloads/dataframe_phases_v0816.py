"""Separate Python-list construction, frame computation and Python conversion."""
import copy
import hashlib
import importlib
import importlib.metadata
import json
from pathlib import Path
import statistics
import time

CASES = ("construct", "compute", "convert", "complete")


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def inputs(rows):
    return {"id": list(range(rows)),
            "label": [None if i % 10 == 0 else f"row-{i}" for i in range(rows)],
            "items": [[str(i), str(i + 1)] for i in range(rows)]}


def oracle(columns):
    columns = copy.deepcopy(columns)
    ids, labels, items = columns["id"], columns["label"], columns["items"]
    rows = len(ids)
    result = {"add": [i + 1 for i in ids], "sub": [i - 1 for i in ids],
              "mul": [i * 2 for i in ids], "eq": [i == 0 for i in ids],
              "ge": [i >= 0 for i in ids], "gt": [i > -1 for i in ids],
              "le": [i <= rows for i in ids], "lt": [i < rows + 1 for i in ids],
              "and": [i >= 0 and i < rows for i in ids],
              "or": [i < 0 or i >= 0 for i in ids], "not": [not i < 0 for i in ids],
              "joined": [None if label is None else label + "!" for label in labels]}
    exploded = {"id": [], "label": [], "items": []}
    for i, label, values in zip(ids, labels, items):
        for value in values:
            exploded["id"].append(i)
            exploded["label"].append(label)
            exploded["items"].append(value)
    return {"input": columns, "next_id": {"next_id": [i + 1 for i in ids]},
            "explode": exploded, "concat": columns, "result": result,
            "height": rows, "width": 3, "first_series_name": "id"}


def assert_equal(actual, expected):
    # bool equals 1 in Python; compare recursively so scalar types are evidence.
    assert type(actual) is type(expected), "DataFrame Python value type mismatch"
    if isinstance(expected, dict):
        assert list(actual) == list(expected), "DataFrame column order/schema mismatch"
        for name in expected:
            assert_equal(actual[name], expected[name])
    elif isinstance(expected, list):
        assert len(actual) == len(expected), "DataFrame row count mismatch"
        for a, b in zip(actual, expected):
            assert_equal(a, b)
    else:
        assert actual == expected, "DataFrame value mismatch"


class Operations:
    def __init__(self, engine, rows):
        if engine not in ("alopex", "polars"):
            raise ValueError(f"Unknown engine: {engine}")
        wanted = "0.8.16" if engine == "alopex" else "1.43.2"
        if importlib.metadata.version(engine) != wanted:
            raise RuntimeError(f"Expected {engine} {wanted}")
        self.api = importlib.import_module(engine)
        self.rows = rows

    def frame(self, columns):
        return self.api.DataFrame(columns)

    def next_frame(self, frame):
        return self.next_plan(frame.lazy())

    def next_plan(self, plan):
        col, lit = self.api.col, self.api.lit
        return plan.filter(col("id").ge(lit(0))).with_columns(col("id").add(lit(1)).alias("next_id"))

    def compute(self, frame):
        return self.select_plan(self.next_frame(frame)).collect()

    def select_plan(self, plan):
        return plan.select(*self.expressions())

    def expressions(self):
        col, lit, concat_str = self.api.col, self.api.lit, self.api.concat_str
        return (
            col("id").add(1).alias("add"), col("id").sub(1).alias("sub"),
            col("id").mul(2).alias("mul"), col("id").eq(0).alias("eq"),
            col("id").ge(0).alias("ge"), col("id").gt(-1).alias("gt"),
            col("id").le(self.rows).alias("le"), col("id").lt(self.rows + 1).alias("lt"),
            col("id").ge(0).and_(col("id").lt(self.rows)).alias("and"),
            col("id").lt(0).or_(col("id").ge(0)).alias("or"),
            col("id").lt(0).not_().alias("not"),
            concat_str("label", lit("!"), separator="").alias("joined"))

    def snapshot(self, frame, result=None, converted=None):
        series = frame.to_dict()
        input_values = {name: column.to_list() for name, column in series.items()}
        if converted is None:
            result = self.compute(frame) if result is None else result
            converted = result.to_dict(as_series=False)
        return {"input": input_values,
                "next_id": self.next_frame(frame).select(self.api.col("next_id")).collect().to_dict(as_series=False),
                "explode": frame.explode("items").to_dict(as_series=False),
                "concat": self.api.concat([frame]).to_dict(as_series=False),
                "result": converted, "height": frame.height, "width": frame.width,
                "first_series_name": next(iter(series.values())).name}


def run(engine, case, raw_path, rows=34250, warmups=3, runs=21):
    if case not in CASES or rows <= 0 or warmups < 0 or runs <= 0:
        raise ValueError("Valid phase, positive rows/runs and nonnegative warmups required")
    columns = inputs(rows)
    expected = oracle(columns)
    operations = Operations(engine, rows)
    frame = operations.frame(columns) if case in ("compute", "convert") else None
    result = operations.compute(frame) if case == "convert" else None

    def operation():
        if case == "construct":
            return operations.frame(columns)
        if case == "compute":
            return operations.compute(frame)
        if case == "convert":
            return result.to_dict(as_series=False)
        # Preserve the old timed items-list generation, and force every output.
        complete_columns = {"id": columns["id"], "label": columns["label"],
                            "items": [[str(i), str(i + 1)] for i in range(rows)]}
        return operations.snapshot(operations.frame(complete_columns))

    samples = []
    with Path(raw_path).open("x") as raw:
        for index in range(warmups + runs):
            started = time.perf_counter()
            actual = operation()
            elapsed = time.perf_counter() - started
            if index >= warmups:
                raw.write(json.dumps({"phase": "measure", "index": index - warmups, "seconds": elapsed}, allow_nan=False) + "\n")
                raw.flush()
                samples.append(elapsed)
            if case == "construct":
                actual = operations.snapshot(actual)
            elif case == "compute":
                actual = operations.snapshot(frame, result=actual)
            elif case == "convert":
                actual = operations.snapshot(frame, result=result, converted=actual)
            assert_equal(actual, expected)
            actual = None
    return {"engine": engine, "case": case, "fixture_rows": rows, "warmups": warmups,
            "samples": runs, "latency_p50_ms": statistics.median(samples) * 1000,
            "result_sha256": _hash(expected), "input_sha256": _hash(columns),
            "logical_input_schema": {"id": "integer", "label": "nullable string", "items": "list of string"},
            "filter": "id >= 0 passes all input rows; no selective-filter claim",
            "initial_python_lists": "outside timing for every phase",
            "complete_timing": "regenerates items Python lists inside timing, as legacy; additionally materializes input all columns, next_id, explode all rows, concat all rows, final 12 columns to Python",
            "validation": "every warmup/sample compared with independent Python oracle, including Python types, NULL and order",
            "compute_limit": "next_id is checked outside timing; final select may prune unused next_id computation"}
