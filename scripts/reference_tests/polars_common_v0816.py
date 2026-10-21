"""Differential audit of ordinary Polars workflows; failed contracts exit nonzero."""
import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import importlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

def assert_equal(actual, expected):
    assert type(actual) is type(expected), "Python value type mismatch"
    if isinstance(expected, dict):
        assert list(actual) == list(expected), "Column order mismatch"
        for key in expected:
            assert_equal(actual[key], expected[key])
    elif isinstance(expected, list):
        assert len(actual) == len(expected), "Row count mismatch"
        for a, b in zip(actual, expected):
            assert_equal(a, b)
    else:
        assert actual == expected, "Value mismatch"



def frame(api):
    return api.DataFrame({"id": [1, 2, 3, 4], "group": ["a", "a", "b", "b"],
                          "value": [10, 20, None, 40], "label": ["One", None, "三", "Four"],
                          "items": [["a", "b"], [], None, ["c"]]})


def bulk(api, rows):
    return api.DataFrame({"id": list(range(rows)),
                          "label": [None if i % 10 == 0 else f"row-{i}" for i in range(rows)]})


def roundtrip(api, context, extension):
    result = frame(api).select("id", "label")
    path = str(context["scratch"] / f"roundtrip.{extension}")
    getattr(result, f"write_{extension}")(path)
    return getattr(api, f"read_{extension}")(path)


class Batches:
    def __init__(self, stream, plan=None):
        try:
            self.frames = list(stream)
        finally:
            if hasattr(stream, "close"):
                stream.close()
        self.empty_schema = None
        self.schema_error = None
        if not self.frames:
            try:
                self.empty_schema = [[name, str(dtype)] for name, dtype in plan.collect_schema().items()]
            except (AttributeError, NotImplementedError) as error:
                self.schema_error = type(error).__name__


def cases():
    result = []

    def add(name, family, operation, expected_error=False):
        result.append({"name": name, "family": family, "operation": operation, "expected_error": expected_error})

    add("construct-dict", "construction", lambda p, c: frame(p))
    add("construct-empty", "construction", lambda p, c: p.DataFrame())
    add("construct-empty-column", "construction", lambda p, c: p.DataFrame({"x": []}))
    add("construct-all-null", "construction", lambda p, c: p.DataFrame({"x": [None, None]}))
    add("construct-bool", "construction", lambda p, c: p.DataFrame({"x": [True, False, None]}))
    add("construct-float", "construction", lambda p, c: p.DataFrame({"x": [1.5, None, -2.5]}))
    add("construct-dates", "construction", lambda p, c: p.DataFrame({"x": [date(2024, 2, 29), None]}))
    add("construct-datetimes", "construction", lambda p, c: p.DataFrame({"x": [datetime(2024, 2, 29, 23, 59), None]}))
    add("construct-schema", "construction", lambda p, c: p.DataFrame({"x": [1, None]}, schema={"x": p.Int64}))
    add("construct-series", "construction", lambda p, c: p.Series("x", [1, None, 3]))
    add("construct-lazy", "construction", lambda p, c: p.LazyFrame({"x": [1, None, 3]}).collect())
    add("column-by-name", "construction", lambda p, c: frame(p)["value"])
    add("schema", "construction", lambda p, c: list(frame(p).schema.items()))
    add("module-select", "selection", lambda p, c: p.select(p.lit(2).add(1).alias("x")))
    add("module-select-series", "selection", lambda p, c: p.select(p.Series("x", [1, None, 3]) + 1))
    add("select-all", "selection", lambda p, c: frame(p).lazy().select(p.all()).collect())
    add("literal-dtype", "types-and-shapes", lambda p, c: p.select(p.lit(1, dtype=p.Int64)))
    add("constructor-nonstrict", "construction", lambda p, c: p.DataFrame({"x": [1, 2.5]}, strict=False))
    add("collect-streaming-option", "selection", lambda p, c: frame(p).lazy().collect(engine="streaming"))

    for lazy in (False, True):
        prefix = "lazy" if lazy else "eager"

        def transform(p, action, lazy=lazy):
            source = frame(p).lazy() if lazy else frame(p)
            value = action(p, source)
            return value.collect() if lazy else value

        for name, action in (
            ("select-name", lambda p, f: f.select("id", "label")),
            ("select-list", lambda p, f: f.select([p.col("label"), p.col("id")])),
            ("select-named", lambda p, f: f.select(next=p.col("id").add(1))),
            ("with-columns", lambda p, f: f.with_columns(p.col("value").add(1).alias("value"))),
            ("filter-partial", lambda p, f: f.filter(p.col("id").gt(2))),
            ("filter-all", lambda p, f: f.filter(p.col("id").ge(0))),
            ("filter-empty", lambda p, f: f.filter(p.col("id").lt(0))),
            ("filter-null-predicate", lambda p, f: f.filter(p.col("value").gt(15))),
            ("group-aggregate", lambda p, f: f.group_by("group", maintain_order=True).agg(
                p.col("value").sum().alias("sum"), p.col("value").mean().alias("mean"),
                p.col("value").count().alias("count"))),
        ):
            add(f"{prefix}-{name}", "selection" if "group" not in name else "aggregation",
                lambda p, c, action=action, transform=transform: transform(p, action))

    for name, expression in (
        ("add-method", lambda p: p.col("value").add(1)),
        ("add-operator", lambda p: p.col("value") + 1),
        ("reverse-add", lambda p: 1 + p.col("value")),
        ("column-add", lambda p: p.col("id") + p.col("value")),
        ("sub", lambda p: p.col("value") - 1),
        ("mul", lambda p: p.col("value") * -2),
        ("div", lambda p: p.col("value") / 2),
        ("division-by-zero", lambda p: p.col("value") / 0),
        ("eq", lambda p: p.col("value") == 20),
        ("ne", lambda p: p.col("value") != 20),
        ("and", lambda p: (p.col("value") > 15) & (p.col("id") < 4)),
        ("or", lambda p: (p.col("value") > 15) | (p.col("id") < 4)),
        ("not", lambda p: ~(p.col("value") > 15)),
        ("sum", lambda p: p.col("value").sum()),
        ("mean", lambda p: p.col("value").mean()),
        ("min", lambda p: p.col("value").min()),
        ("max", lambda p: p.col("value").max()),
        ("count", lambda p: p.col("value").count()),
        ("null-count", lambda p: p.col("value").null_count()),
        ("n-unique", lambda p: p.col("group").n_unique()),
    ):
        add("expr-" + name, "expression", lambda p, c, expression=expression:
            frame(p).lazy().select(expression(p).alias("result")).collect())

    for name, action in (
        ("add", lambda s: s + 1), ("sub", lambda s: s - 1),
        ("mul", lambda s: s * 2), ("div", lambda s: s / 2),
        ("compare", lambda s: s > 1), ("sum", lambda s: s.sum()),
        ("mean", lambda s: s.mean()), ("fill-null", lambda s: s.fill_null(0)),
    ):
        add("series-" + name, "series", lambda p, c, action=action: action(p.Series("x", [1, None, 3])))
        add("existing-series-" + name, "series-diagnostic", lambda p, c, action=action:
            action(p.DataFrame({"x": [1, None, 3]}).to_dict()["x"]))

    for name, action in (
        ("sort", lambda p, f: f.sort("value", descending=True, nulls_last=True)),
        ("head", lambda p, f: f.head(2)), ("tail", lambda p, f: f.tail(2)),
        ("slice", lambda p, f: f.slice(1, 2)),
        ("unique", lambda p, f: f.unique(subset="group", keep="first", maintain_order=True)),
        ("drop", lambda p, f: f.drop("items")),
        ("rename", lambda p, f: f.rename({"label": "name"})),
        ("fill-null", lambda p, f: f.fill_null(0)),
        ("drop-nulls", lambda p, f: f.drop_nulls(subset="value")),
        ("null-count", lambda p, f: f.null_count()),
        ("explode", lambda p, f: f.explode("items", empty_as_null=True)),
        ("implode", lambda p, f: f.lazy().select(p.col("id").implode()).collect()),
    ):
        add("frame-" + name, "table-and-nulls", lambda p, c, action=action: action(p, frame(p)))

    for how in ("inner", "left", "right", "full", "semi", "anti"):
        add("join-" + how, "join-concat", lambda p, c, how=how:
            p.DataFrame({"key": [1, 2, 2, None], "left": [10, 20, 30, 40]}).join(
                p.DataFrame({"key": [2, 2, 3, None], "right": [50, 60, 70, 80]}),
                on="key", how=how, maintain_order="left_right", nulls_equal=True))
    add("concat-vertical", "join-concat", lambda p, c: p.concat([frame(p), frame(p)], how="vertical"))
    add("concat-horizontal", "join-concat", lambda p, c: p.concat(
        [p.DataFrame({"a": [1, 2]}), p.DataFrame({"b": [3, 4]})], how="horizontal"))
    add("concat-diagonal", "join-concat", lambda p, c: p.concat(
        [p.DataFrame({"a": [1]}), p.DataFrame({"b": [2]})], how="diagonal"))
    add("group-null-key", "aggregation", lambda p, c: p.DataFrame(
        {"key": ["a", None, "a", None], "value": [1, 2, None, 4]}).lazy().group_by(
            "key", maintain_order=True).agg(p.col("value").sum()).collect())
    add("group-empty", "aggregation", lambda p, c: frame(p).lazy().filter(p.col("id").lt(0)).group_by(
        "group", maintain_order=True).agg(p.col("value").sum()).collect())
    for source, values in (("empty", []), ("all-null", [None, None])):
        for aggregation in ("sum", "mean", "count"):
            add(f"aggregate-{source}-{aggregation}", "aggregation", lambda p, c, values=values, aggregation=aggregation:
                p.DataFrame({"x": values}, schema={"x": p.Int64}).lazy().select(getattr(p.col("x"), aggregation)()).collect())
    add("boolean-null-logic", "expression", lambda p, c: p.DataFrame(
        {"a": [True, False, None, None], "b": [None, None, True, False]}).lazy().select(
            p.col("a").and_(p.col("b")).alias("and"), p.col("a").or_(p.col("b")).alias("or")).collect())
    add("list-null-element", "types-and-shapes", lambda p, c: p.DataFrame(
        {"items": [["a", None], [], None]}).explode("items", empty_as_null=True))

    for name, expression in (
        ("lower", lambda p: p.col("label").str.to_lowercase()),
        ("upper", lambda p: p.col("label").str.to_uppercase()),
        ("contains", lambda p: p.col("label").str.contains("O|三")),
        ("replace", lambda p: p.col("label").str.replace("One", "一")),
        ("split", lambda p: p.col("label").str.split("o")),
        ("length", lambda p: p.col("label").str.len_chars()),
        ("concat", lambda p: p.concat_str("label", p.lit("!"), separator="")),
    ):
        add("string-" + name, "namespaces", lambda p, c, expression=expression:
            frame(p).lazy().select(expression(p).alias("result")).collect())
    for name, expression in (
        ("length", lambda p: p.col("items").list.len()),
        ("contains", lambda p: p.col("items").list.contains("a")),
        ("join", lambda p: p.col("items").list.join("-")),
        ("first", lambda p: p.col("items").list.get(0, null_on_oob=True)),
    ):
        add("list-" + name, "namespaces", lambda p, c, expression=expression:
            frame(p).lazy().select(expression(p).alias("result")).collect())
    add("datetime-parse-year", "namespaces", lambda p, c: p.DataFrame(
        {"ts": ["2024-02-29 23:59:59", None]}).lazy().select(p.col("ts").str.to_datetime().dt.year()).collect())
    add("datetime-format", "namespaces", lambda p, c: p.DataFrame(
        {"ts": [datetime(2024, 2, 29, 23, 59), None]}).lazy().select(p.col("ts").dt.to_string("%Y-%m-%d")).collect())
    add("datetime-timezone", "namespaces", lambda p, c: p.DataFrame(
        {"ts": [datetime(2024, 2, 29, tzinfo=timezone.utc), None]}).lazy().select(p.col("ts").dt.convert_time_zone("Asia/Tokyo")).collect())
    add("cast-float", "types-and-shapes", lambda p, c: frame(p).lazy().select(p.col("value").cast(p.Float64)).collect())
    add("cast-nonstrict", "types-and-shapes", lambda p, c: p.DataFrame({"x": ["1", "bad", None]}).lazy().select(p.col("x").cast(p.Int64, strict=False)).collect())
    add("fill-nan", "types-and-shapes", lambda p, c: p.DataFrame({"x": [1.0, float("nan"), None]}).fill_nan(0))
    add("window-sum", "types-and-shapes", lambda p, c: frame(p).lazy().with_columns(p.col("value").sum().over("group").alias("group_sum")).collect())
    add("rolling-mean", "types-and-shapes", lambda p, c: frame(p).lazy().select(p.col("value").rolling_mean(window_size=2)).collect())
    add("shift", "types-and-shapes", lambda p, c: frame(p).lazy().select(p.col("value").shift(1)).collect())
    add("pivot", "types-and-shapes", lambda p, c: frame(p).select("id", "group", "value").pivot(on="group", index="id", values="value", aggregate_function="sum"))
    add("unpivot", "types-and-shapes", lambda p, c: p.DataFrame({"id": [1, 2], "x": [10, 20], "y": [30, 40]}).unpivot(on=["x", "y"], index="id"))

    for extension in ("csv", "parquet"):
        add("read-" + extension, "io", lambda p, c, extension=extension: getattr(p, "read_" + extension)(str(c[extension])))
        add("scan-" + extension, "io", lambda p, c, extension=extension: getattr(p, "scan_" + extension)(str(c[extension])).filter(p.col("id").gt(2)).select("label", "id").collect())
        add("scan-" + extension + "-strings", "io", lambda p, c, extension=extension: getattr(p, "scan_" + extension)(str(c[extension])).select(p.concat_str("label", p.lit("!"))).collect())
        add("write-read-" + extension, "io", lambda p, c, extension=extension: roundtrip(p, c, extension))
    add("read-csv-options", "io", lambda p, c: p.read_csv(str(c["csv_options"]), separator=";", null_values="NA", schema_overrides={"id": p.Int64}))
    add("read-parquet-projection", "io", lambda p, c: p.read_parquet(str(c["parquet"]), columns=["label", "id"]))
    for rows in (0, 1023, 1024, 1025):
        add(f"collect-{rows}", "batch-boundaries", lambda p, c, rows=rows: bulk(p, rows).lazy().select("label", "id").collect())
        add(f"batches-{rows}", "batch-boundaries", lambda p, c, rows=rows: materialize_batches(bulk(p, rows).lazy().select("label", "id")))
    add("batches-filter-empty-first", "batch-boundaries", lambda p, c:
        materialize_batches(bulk(p, 1025).lazy().filter(p.col("id").ge(1024)).select("label", "id")))
    add("batches-expressions-null", "batch-boundaries", lambda p, c:
        materialize_batches(bulk(p, 1025).lazy().select(p.col("id").add(1), p.concat_str("label", p.lit("!")))))
    add("error-missing-column", "errors", lambda p, c: frame(p).lazy().select(p.col("missing")).collect(), True)
    add("error-column-length", "errors", lambda p, c: p.DataFrame({"a": [1], "b": [1, 2]}), True)
    add("error-strict-cast", "errors", lambda p, c: p.DataFrame({"x": ["bad"]}).lazy().select(p.col("x").cast(p.Int64)).collect(), True)
    assert len({case["name"] for case in result}) == len(result)
    return result


def json_value(value):
    if isinstance(value, (datetime, date)):
        return {"python_type": type(value).__name__, "iso": value.isoformat()}
    if isinstance(value, float) and not math.isfinite(value):
        return {"python_float": "nan" if math.isnan(value) else ("inf" if value > 0 else "-inf")}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if value is None or type(value) in (bool, int, float, str):
        return value
    return {"python_type": type(value).__name__, "value": str(value)}


def observe(value):
    if isinstance(value, Batches):
        observations = [observe(batch) for batch in value.frames]
        if not observations:
            result = {"kind": "DataFrame", "schema": value.empty_schema,
                      "values": [[name, []] for name, _ in value.empty_schema or []]}
            if value.schema_error:
                result["schema_error"] = value.schema_error
            return result
        combined = observations[0]
        for other in observations[1:]:
            assert_equal(other["schema"], combined["schema"])
            assert [x[0] for x in other["values"]] == [x[0] for x in combined["values"]]
            for (_, target), (_, source) in zip(combined["values"], other["values"]):
                target.extend(source)
        return combined
    kind = type(value).__name__
    result = {"kind": kind, "schema": None}
    if kind == "DataFrame":
        result["values"] = [[name, json_value(column)] for name, column in value.to_dict(as_series=False).items()]
        try:
            result["schema"] = [[name, str(dtype)] for name, dtype in value.schema.items()]
        except (AttributeError, NotImplementedError) as error:
            result["schema_error"] = type(error).__name__
    elif kind == "Series":
        result["values"] = json_value(value.to_list())
        try:
            result["schema"] = [[value.name, str(value.dtype)]]
        except (AttributeError, NotImplementedError) as error:
            result["schema_error"] = type(error).__name__
    else:
        result["values"] = json_value(value)
    return result


def compare(reference, subject, expected_error):
    if expected_error and reference["execution"] != "error":
        return {"status": "invalid-reference", "reason": "expected_reference_error_not_observed"}
    if reference["execution"] != "ok" and not expected_error:
        return {"status": "invalid-reference", "reason": "reference_did_not_execute"}
    if expected_error:
        matched = (reference["execution"] == subject["execution"] == "error"
                   and reference["exception"] == subject["exception"])
        return {"status": "passed" if matched else "failed", "reason": "exception_contract"}
    if subject["execution"] != "ok":
        return {"status": "failed", "reason": "execution_error", "exception": subject.get("exception")}
    wanted, actual = reference["observation"], subject["observation"]
    result = {"status": "passed", "kind": "match" if wanted["kind"] == actual["kind"] else "mismatch"}
    try:
        assert_equal(actual["values"], wanted["values"])
        result["values"] = "match"
    except AssertionError:
        result["values"] = "mismatch"
    result["schema"] = "unverified" if "schema_error" in actual or "schema_error" in wanted else (
        "match" if actual["schema"] == wanted["schema"] else "mismatch")
    if any(result[key] != "match" for key in ("kind", "values", "schema")):
        result["status"] = "failed"
    return result


def worker(engine, case, directory, subject_version="0.8.16"):
    wanted = subject_version if engine == "alopex" else "1.43.2"
    if importlib.metadata.version(engine) != wanted:
        raise RuntimeError(f"Expected {engine} {wanted}")
    api = importlib.import_module(engine)
    with tempfile.TemporaryDirectory(prefix="case-", dir=directory) as scratch:
        context = {"csv": directory / "rows.csv", "parquet": directory / "rows.parquet",
                   "csv_options": directory / "options.csv", "scratch": Path(scratch)}
        try:
            value = case["operation"](api, context)
        except Exception as error:
            return {"execution": "error", "exception": type(error).__name__,
                    "message": str(error).replace(str(directory), "<fixture-data>")}
        try:
            return {"execution": "ok", "observation": observe(value)}
        except Exception as error:
            return {"execution": "observation-error", "exception": type(error).__name__,
                    "message": str(error).replace(str(directory), "<fixture-data>")}


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def materialize_batches(plan):
    return Batches(plan.collect_batches(chunk_size=1024), plan)


def run_process(command, output, name, metadata):
    with (output / f"{name}.stdout.txt").open("x") as stdout, (output / f"{name}.stderr.txt").open("x") as stderr:
        try:
            result = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=60)
            exit_code = result.returncode
        except subprocess.TimeoutExpired:
            exit_code = 124
            stderr.write("Case exceeded 60 second execution budget\n")
    status = dict(metadata, exit_code=exit_code)
    save(output / f"{name}.status.json", status)
    return status


def fixtures(data):
    data.mkdir(parents=True, exist_ok=False)
    import polars as pl
    pl.DataFrame({"id": [1, 2, 3, 4], "label": ["One", None, "三", "Four"]}).write_csv(data / "rows.csv")
    pl.read_csv(data / "rows.csv").write_parquet(data / "rows.parquet")
    (data / "options.csv").write_text("id;label\n1;one\n2;NA\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--engine", choices=("alopex", "polars"))
    parser.add_argument("--case")
    parser.add_argument("--subject-version", default="0.8.16")
    args = parser.parse_args()
    os.environ.update({key: "1" for key in ("POLARS_MAX_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")})
    registry = cases()
    output, data = args.output.resolve(), args.data.resolve()
    if args.engine:
        selected = next(case for case in registry if case["name"] == args.case)
        print(json.dumps(worker(args.engine, selected, data, args.subject_version), ensure_ascii=False, allow_nan=False))
        return
    if args.case:
        parser.error("Parent executes the complete fixed matrix")
    if output.exists() and ((output / "environment.json").exists() or any(output.glob("case-*"))):
        raise RuntimeError("Use a new run directory; do not overwrite evidence")
    output.mkdir(parents=True, exist_ok=True)
    fixtures(data)
    sources = [Path(__file__)]
    save(output / "environment.json", {"schema": "alopex.polars-common-audit/v1",
        "python": platform.python_version(), "platform": platform.platform(),
        "versions": {name: importlib.metadata.version(name) for name in ("alopex", "polars", "polars-runtime-32")},
        "required_subject_version": args.subject_version,
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], text=True).strip(),
        "source_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        "fixture_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in data.iterdir()},
        "scope": "functional audit, not performance or full Polars coverage"})
    save(output / "matrix.json", [{key: value for key, value in case.items() if key != "operation"} for case in registry])
    rows = []
    for case in registry:
        observations = {}
        for engine in ("polars", "alopex"):
            name = f"case-{case['name']}-{engine}"
            command = [sys.executable, "-B", str(Path(__file__).resolve()), "--output", str(output),
                       "--data", str(data), "--engine", engine, "--case", case["name"],
                       "--subject-version", args.subject_version]
            status = run_process(command, output, name, {"case": case["name"], "engine": engine})
            try:
                value = json.loads((output / f"{name}.stdout.txt").read_text()) if status["exit_code"] == 0 else {
                    "execution": "process-error", "exit_code": status["exit_code"]}
            except ValueError:
                value = {"execution": "process-error", "reason": "invalid_worker_json"}
            observations[engine] = value
        result = compare(observations["polars"], observations["alopex"], case["expected_error"])
        row = {"case": case["name"], "family": case["family"], "expected_error": case["expected_error"],
               **result, "reference": observations["polars"], "subject": observations["alopex"]}
        save(output / f"case-{case['name']}.json", row)
        rows.append(row)
    summary = {"case_count": len(rows), "status_counts": dict(Counter(row["status"] for row in rows)),
               "subject_execution_counts": dict(Counter(row["subject"]["execution"] for row in rows)),
               "value_matches": sum(row.get("values") == "match" for row in rows),
               "schema_unverified": sum(row.get("schema") == "unverified" for row in rows),
               "families": {family: dict(Counter(row["status"] for row in rows if row["family"] == family))
                            for family in sorted({row["family"] for row in rows})},
               "results": [{key: value for key, value in row.items() if key not in ("reference", "subject")} for row in rows]}
    save(output / "summary.json", summary)
    print(json.dumps({key: value for key, value in summary.items() if key not in ("results", "families")}), flush=True)
    sys.exit(2 if summary["status_counts"].get("invalid-reference") else (1 if summary["status_counts"].get("failed") else 0))


if __name__ == "__main__":
    main()
