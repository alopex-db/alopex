#!/usr/bin/env python3
"""Prepare isolated manifests and execute exactly one #570 performance cell."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import tempfile
import time

CASES = ("ordinary_update", "scalar_update", "membership_delete", "correlated_update", "two_membership_delete")
ENGINES = ("baseline", "fixed", "sqlite")
SIZES = (600, 1025)
COMMON_IDENTITY = ("phase", "cpu", "warmup", "runs", "noise_fraction", "harness_sha256", "runner_sha256")
ENGINE_IDENTITY = ("source_sha256", "source_revision", "manifest_sha256", "lock_sha256", "binary_sha256", "parser_sha256")
SUITES = {
    "prepared": ("prepared_execute", ENGINES, CASES, "dml_statement_perf"),
    "forced-spill": ("async_sql_execute", ("baseline", "fixed"), ("two_membership_no_write",), "dml_spill_perf"),
}


def validate_identity(identity, suite="prepared"):
    phase, engines, cases, _ = SUITES[suite]
    if identity.get("kind") != "identity":
        raise ValueError("first record must be identity")
    if (identity.get("engine") not in engines or identity.get("case") not in cases
            or type(identity.get("rows")) is not int or identity["rows"] not in SIZES):
        raise ValueError("unknown engine/case/size")
    for key, value in {"phase": phase, "warmup": 1, "runs": 7, "noise_fraction": 0.05}.items():
        if identity.get(key) != value:
            raise ValueError(f"invalid measurement setting {key}")
    if type(identity.get("cpu")) is not int or identity["cpu"] < 0:
        raise ValueError("cpu must be a nonnegative integer")
    if type(identity.get("started_unix_ns")) is not int or identity["started_unix_ns"] <= 0:
        raise ValueError("missing start timestamp")
    for key in (*COMMON_IDENTITY, *ENGINE_IDENTITY):
        if key.endswith("sha256"):
            value = identity.get(key)
            if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                raise ValueError(f"invalid digest {key}")
    revision = identity.get("source_revision")
    if not isinstance(revision, str) or len(revision) != 40 or any(c not in "0123456789abcdef" for c in revision):
        raise ValueError("invalid source revision")
    if suite == "forced-spill":
        if identity.get("memory_limit_bytes") != 4096 or identity.get("spill_policy") != "SpillToDisk":
            raise ValueError("invalid spill configuration")
        if not isinstance(identity.get("spill_parent"), str) or not identity["spill_parent"]:
            raise ValueError("missing spill filesystem identity")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_digest(root):
    entries = []
    for crate in ("alopex-core", "alopex-sql"):
        for path in sorted((root / "crates" / crate / "src").rglob("*.rs")):
            entries.append((str(path.relative_to(root)), digest(path)))
    return hashlib.sha256(json.dumps(entries).encode()).hexdigest()


def prepare(args):
    suite = getattr(args, "suite", "prepared")
    _, engines, _, binary = SUITES[suite]
    root = Path(args.fixed).resolve()
    baseline = Path(args.baseline).resolve()
    destination = Path(args.output).resolve()
    destination.mkdir(parents=True, exist_ok=False)
    source = root / f"crates/alopex-sql/benches/support/{binary}.rs"
    for engine in engines:
        directory = destination / engine
        directory.mkdir()
        dependency_root = baseline if engine == "baseline" else root
        if engine == "sqlite":
            feature = "engine-sqlite"
            dependencies = 'rusqlite = { version = "=0.31.0", features = ["bundled"] }\n'
        else:
            feature = "engine-alopex"
            dependencies = "".join(
                f'{crate} = {{ path = {json.dumps(str(dependency_root / "crates" / crate))}'
                + (', features = ["tokio"]' if suite == "forced-spill" else '') + ' }\n'
                for crate in ("alopex-core", "alopex-sql")
            )
            if suite == "forced-spill":
                dependencies += 'tokio = { version = "1", features = ["rt", "sync"] }\nfutures = "0.3"\ntempfile = "3.10"\n'
        manifest = f'''[package]
name = "v0816-570-perf-{'spill-' if suite == 'forced-spill' else ''}{engine}"
version = "0.0.0"
edition = "2024"
[features]
default = ["{feature}"]
engine-alopex = []
engine-sqlite = []
[dependencies]
{dependencies}serde_json = {{ version = "1", features = ["preserve_order", "arbitrary_precision"] }}
[[bin]]
name = "{binary}"
path = {json.dumps(str(source))}
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
'''
        (directory / "Cargo.toml").write_text(manifest)
        (directory / "Cargo.lock").write_bytes((root / "Cargo.lock").read_bytes())
    print(json.dumps({"status": "prepared_not_built", "manifests": len(engines), "harness_sha256": digest(source)}))


def validate(rows, engine, case, size, suite="prepared"):
    phase, _, _, _ = SUITES[suite]
    samples = [row for row in rows if row.get("kind") == "sample"]
    if len(samples) != 8:
        raise ValueError("expected exactly one warmup and seven measurements")
    for iteration, row in enumerate(samples):
        for key, value in dict(engine=engine, case=case, rows=size, phase=phase,
                               iteration=iteration, warmup=iteration == 0,
                               affected_rows=0 if suite == "forced-spill" else size,
                               result_check=True, status="pass").items():
            if row.get(key) != value:
                raise ValueError(f"invalid sample {iteration}: {key}")
        if type(row.get("elapsed_ns")) is not int or row["elapsed_ns"] <= 0:
            raise ValueError("elapsed_ns must be a positive integer")
        if suite == "forced-spill":
            if row.get("memory_limit_bytes") != 4096 or row.get("spill_cleanup") is not True:
                raise ValueError("spill policy or cleanup mismatch")
            for key in ("spill_bytes", "spill_files"):
                if type(row.get(key)) is not int or row[key] < 0:
                    raise ValueError(f"invalid {key}")
            if engine == "fixed" and (row["spill_files"] != 1 or row["spill_bytes"] <= 4096):
                raise ValueError("fixed engine did not exercise shared disk cache")
    return statistics.median(row["elapsed_ns"] for row in samples[1:])


def run(args):
    suite = getattr(args, "suite", "prepared")
    phase, engines, cases, _ = SUITES[suite]
    if args.engine not in engines or args.case not in cases:
        raise ValueError("engine/case does not belong to selected suite")
    raw = Path(args.raw).resolve()
    manifest = Path(args.manifest).resolve()
    root = Path(args.source_root).resolve()
    identity = {
        "kind": "identity", "engine": args.engine, "case": args.case, "rows": args.rows,
        "phase": phase, "cpu": args.cpu, "warmup": 1, "runs": 7,
        "noise_fraction": 0.05, "started_unix_ns": time.time_ns(),
        "source_sha256": source_digest(root), "source_revision": subprocess.check_output(
            ["rtk", "proxy", "git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip(),
        "manifest_sha256": digest(manifest), "lock_sha256": digest(manifest.with_name("Cargo.lock")),
        "binary_sha256": digest(args.binary), "parser_sha256": digest(args.parser),
        "harness_sha256": digest(args.harness), "runner_sha256": digest(__file__),
    }
    if suite == "forced-spill":
        if not args.spill_parent or not Path(args.spill_parent).is_dir():
            raise ValueError("forced-spill requires an existing --spill-parent on the selected filesystem")
        identity.update(memory_limit_bytes=4096, spill_policy="SpillToDisk",
                        spill_parent=str(Path(args.spill_parent).resolve()))
    validate_identity(identity, suite)
    raw.parent.mkdir(parents=True, exist_ok=True)
    with raw.open("x") as stream:
        stream.write(json.dumps(identity) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    command = ["rtk", "proxy", "taskset", "-c", str(args.cpu), str(Path(args.binary).resolve()),
               args.engine, args.case, str(args.rows), str(raw)]
    spill_directory = None
    if suite == "forced-spill":
        spill_directory = tempfile.TemporaryDirectory(prefix="v0816-570-spill-", dir=args.spill_parent)
        command.append(spill_directory.name)
    # GNU time metrics describe the whole process, not only the timed statement.
    timing = str(raw) + ".process-time"
    command = ["rtk", "proxy", "/usr/bin/time", "-f", "%M %U %S %I %O", "-o", timing] + command
    status, detail, exit_code = "fail", "", None
    with Path(str(raw) + ".log").open("x") as log:
        try:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                exit_code = process.wait(timeout=180)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            if exit_code == 0:
                rows = [json.loads(line) for line in raw.read_text().splitlines()]
                validate(rows, args.engine, args.case, args.rows, suite)
                status = "pass"
            else:
                detail = "process failed"
        except (subprocess.TimeoutExpired, ValueError) as error:
            detail = str(error)
        finally:
            if spill_directory is not None:
                spill_directory.cleanup()  # Includes owned files left by a terminated child.
    metrics = {}
    if suite == "forced-spill":
        try:
            rss, user, system, inputs, outputs = Path(timing).read_text().strip().splitlines()[-1].split()
            metrics = dict(maxrss_kib=int(rss), user_seconds=float(user), system_seconds=float(system),
                           inblock=int(inputs), outblock=int(outputs))
        except (OSError, ValueError, IndexError) as error:
            status, detail = "fail", f"missing process resource metrics: {error}"
    with raw.open("a") as stream:
        stream.write(json.dumps({"kind": "completion", "status": status,
                                 "exit_code": exit_code, "detail": detail,
                                 "process_metrics": metrics}) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    if status != "pass":
        raise SystemExit(1)


def render(args):
    suite = getattr(args, "suite", "prepared")
    phase, engines, cases, _ = SUITES[suite]
    cells = {}
    common = None
    engine_identities = {}
    identity_errors = []
    for path in sorted(Path(args.raw_directory).glob("*.jsonl")):
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        identity = rows[0] if rows else {}
        try:
            validate_identity(identity, suite)
        except ValueError as error:
            identity_errors.append({"raw": path.name, "error": str(error)})
            continue
        key = identity["engine"], identity["case"], identity["rows"]
        settings = tuple(identity[field] for field in COMMON_IDENTITY)
        if suite == "forced-spill":
            settings += (identity["memory_limit_bytes"], identity["spill_policy"], identity["spill_parent"])
        artifact = tuple(identity[field] for field in ENGINE_IDENTITY)
        if common is None:
            common = settings
        elif common != settings:
            identity_errors.append({"raw": path.name, "error": "mixed measurement settings"})
        previous = engine_identities.setdefault(identity["engine"], artifact)
        if previous != artifact:
            identity_errors.append({"raw": path.name, "error": "mixed same-engine artifact identity"})
        if key in cells:
            raise ValueError(f"duplicate cell {key}; retain retries in separate run directories")
        try:
            if rows[-1].get("kind") != "completion" or rows[-1].get("status") != "pass":
                raise ValueError("missing successful completion")
            median = validate(rows, *key, suite)
            cells[key] = {"median_ns": median, "status": "pass", "raw_sha256": digest(path)}
            if suite == "forced-spill":
                metrics = rows[-1].get("process_metrics", {})
                for field in ("maxrss_kib", "user_seconds", "system_seconds", "inblock", "outblock"):
                    if (type(metrics.get(field)) not in (int, float)
                            or not math.isfinite(metrics[field]) or metrics[field] < 0):
                        raise ValueError(f"missing or invalid process metric {field}")
                samples = [row for row in rows if row.get("kind") == "sample" and not row["warmup"]]
                cells[key].update(process_metrics=metrics,
                                  spill_bytes=[row["spill_bytes"] for row in samples],
                                  spill_files=[row["spill_files"] for row in samples])
        except ValueError as error:
            cells[key] = {"status": "fail", "detail": str(error), "raw_sha256": digest(path)}
    comparisons = []
    for case in cases:
        for size in SIZES:
            selected = {engine: cells.get((engine, case, size), {"status": "missing"}) for engine in engines}
            row = {"case": case, "rows": size, "engines": selected, "status": "fail"}
            if not identity_errors and all(cell["status"] == "pass" for cell in selected.values()):
                ratios = {engine: selected["fixed"]["median_ns"] / selected[engine]["median_ns"]
                          for engine in engines if engine != "fixed"}
                row.update(ratios=ratios, status="pass" if all(value <= 1.05 for value in ratios.values()) else "fail")
            comparisons.append(row)
    with Path(args.output).open("x") as stream:
        json.dump({"phase": phase, "noise_fraction": 0.05,
                   "reference_contract": "sqlite_4096B_capability_config_gap" if suite == "forced-spill" else "prepared_sql",
                   "identity_errors": identity_errors, "comparisons": comparisons}, stream, indent=2)
        stream.write("\n")
    if any(row["status"] != "pass" for row in comparisons):
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare", help="write manifests/locks only; no build")
    prep.add_argument("--suite", choices=SUITES, default="prepared")
    for name in ("fixed", "baseline", "output"):
        prep.add_argument(f"--{name}", required=True)
    prep.set_defaults(function=prepare)
    execute = commands.add_parser("run", help="execute exactly one pre-built cell")
    execute.add_argument("--suite", choices=SUITES, default="prepared")
    execute.add_argument("--spill-parent")
    execute.add_argument("--engine", choices=ENGINES, required=True)
    execute.add_argument("--case", choices=(*CASES, "two_membership_no_write"), required=True)
    execute.add_argument("--rows", type=int, choices=SIZES, required=True)
    execute.add_argument("--cpu", type=int, default=1)
    for name in ("binary", "manifest", "source-root", "parser", "harness", "raw"):
        execute.add_argument(f"--{name}", required=True)
    execute.set_defaults(function=run)
    report = commands.add_parser("render", help="missing/failed cells never pass")
    report.add_argument("--suite", choices=SUITES, default="prepared")
    report.add_argument("--raw-directory", required=True)
    report.add_argument("--output", required=True)
    report.set_defaults(function=render)
    args = parser.parse_args()
    args.function(args)


if __name__ == "__main__":
    main()
