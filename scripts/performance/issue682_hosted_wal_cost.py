#!/usr/bin/env python3
"""Hosted #682 release-profile four-cell Python KV/SQL WAL overflow cost runner.

UC-V0817-682-WAL-COST-HOSTED-01:
- Builds exact pinned `before` (d995c550) and `candidate` (09e95da3) release
  `alopex-py` (abi3-py38) extensions offline on the same GitHub-hosted
  ubuntu-24.04 runner using the pinned Nim 2.2.10 / Nimble 0.22.3 parser
  (contract 0.28.0) and a merged locked Cargo vendor tree under HostStorageGuard
  (40 GB stop / 50 GB ceiling).
- Runs the exact 4-cell Python KV/SQL overflow cost comparison (KV/SQL x
  16B/2048B, 1 warmup + 5 measured rounds = 48 writers + 96 independent
  readers verifying all values, SQL secondary index, and gc_failures == 0)
  with stage budgets and per-round checkpoint early-stop.
- Exposure boundary: stdout emits only allowlisted fixed-code status/progress
  JSON; public artifact export contains only independently audited canonical
  public JSON (`issue682-wal-cost-public.json`). Zero `private-evidence.cms`,
  zero certificates, zero Docker, and zero host/path diagnostics.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import stat
import statistics
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import issue681_hosted_acquire_source as acquire_source
import issue681_hosted_build_guard as capacity
import issue681_hosted_cargo_fetch as cargo_fetch
import issue681_hosted_nim_archive as nim_archive
import issue681_hosted_parser_seed as parser_seed
import issue681_hosted_parser_stage as parser_stage
import issue681_hosted_parser_tools as parser_tools
import issue681_hosted_rust_preflight as rust_preflight
import issue681_hosted_storage as scoped_storage
import issue681_public_assets_produce as producer

CASE = "WAL_COST_682_PUBLIC"
ERROR = "WAL_COST_682"
SCHEMA = "alopex.issue682-wal-cost-public/v1"
BOUNDARY_TAG = "hosted_release_public_allowlist_only"
REPOSITORY = "https://github.com/alopex-db/alopex.git"
REPOSITORY_SLUG = "alopex-db/alopex"
HOSTED_RUNNER_KIND = "github_hosted_ubuntu2404"
TARGET = "x86_64-unknown-linux-gnu"
PARSER_CONTRACT_VERSION = "0.28.0"
RUST_VERSION = "1.90.0"
PARSER_SCRATCH = Path(
    "/tmp/alopex-nim-parser-v0.8.17-contract-0.28.0-" + TARGET
)
BASH = Path("/usr/bin/bash")
PYTHON = Path("/usr/bin/python3")
HEX40 = re.compile(r"^[0-9a-f]{40}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")

PINS = {
    "before": {
        "commit": "d995c550152e734ce821eb3de93180b533aad2f7",
        "tree": "09a42bade3400f91836a59ba3104a3dd907704b3",
        "lock": "1679161e7f92fc40f8813cbbf1e9330dfa8796beefeb2317f815f31bf761b501",
    },
    "candidate": {
        "commit": "09e95da3a2629693931eb3df70beccbb18d58368",
        "tree": "92de089b0330a034a404ed9af49e0c49bd76e240",
        "lock": "c1a1abdd274a633f7ff1f8e47eed70fdc05a032c745ba1645bf000df711b6103",
    },
}

OPTIONS = {
    "payload_bytes": 65536,
    "spool_bytes": 131072,
    "directory_bytes": 524288,
    "replay_bytes": 131072,
    "key_bytes": 1024,
    "value_bytes": 8192,
    "fragment_bytes": 256,
    "wal_segment_bytes": 4124,
    "wal_segments": 1,
    "inline_bytes": 1024,
    "activation_bytes": 8192,
    "container_fence_bytes": 32768,
}

ARMS = ("before", "candidate")
SHAPES = (("kv", 16), ("kv", 2048), ("sql", 16), ("sql", 2048))
ROUNDS = 6  # round 0 = warmup (excluded from medians), rounds 1..5 = measured
METRICS = (
    "prepare_ns",
    "open_ns",
    "schema_ns",
    "index_ns",
    "prior_ns",
    "begin_ns",
    "stage_ns",
    "commit_ns",
    "peak_rss_bytes",
    "wal_bytes",
    "spool_bytes",
    "fixture_bytes",
    "allocated_bytes",
)
MEDIAN_FIELDS = frozenset(METRICS) | {"commit_mad_ns", "reopen_ns"}
INVESTIGATE_RATIO_THRESHOLD = 1.20
MAX_FIXTURE_BYTES = 1_048_576
MAX_WRITER_RSS_BYTES = 1_073_741_824
CHILD_TIMEOUT_SECONDS = 30
SOFT_BUDGET_SECONDS = 65 * 60
CLEANUP_MARGIN_SECONDS = 5 * 60
ACQUIRE_BUDGET_SECONDS = 20 * 60
BUILD_ARM_TIMEOUT_SECONDS = 20 * 60
MAX_PUBLIC_BYTES = 262_144

REPORT_KEYS = frozenset({
    "schema",
    "status",
    "boundary",
    "profile",
    "platform",
    "rust_version",
    "parser_contract",
    "options",
    "pins",
    "rounds",
    "warmup_excluded_from_medians",
    "writer_processes",
    "reader_processes",
    "medians",
    "commit_after_before_ratios",
    "investigate",
    "investigate_threshold",
    "scope",
})

PIN_KEYS = frozenset({
    "source_commit",
    "source_tree",
    "cargo_lock_sha256",
    "parser_sha256",
    "parser_record_sha256",
    "native_sha256",
    "python_tree_sha256",
    "profile",
    "ordinary_extension",
})

FORBIDDEN_PUBLIC_TOKENS = (
    "/home/",
    "/Users/",
    "/tmp/",
    "/var/",
    "/runner",
    "RUNNER_",
    "GITHUB_WORKSPACE",
    "private-evidence",
    ".cms",
    "ghs_",
    "ghp_",
)

DIAG_CODES = frozenset({
    "PREFLIGHT_ENV",
    "PREFLIGHT_SOURCE",
    "PREFLIGHT_STORAGE",
    "PREFLIGHT_RUST",
    "ACQUIRE_SOURCES",
    "ACQUIRE_NIM",
    "ACQUIRE_NIMBLE",
    "ACQUIRE_SEED",
    "ACQUIRE_PARSER_BUILD",
    "ACQUIRE_PARSER_STAGE",
    "ACQUIRE_CARGO_VENDOR",
    "BUILD_CARGO",
    "BUILD_ELF",
    "BUILD_STAGE",
    "BUILD_SMOKE",
    "MEASURE_CHILD",
    "MEASURE_VERIFY",
    "MEASURE_SUMMARY",
    "AUDIT_PUBLIC",
    "TIME_BUDGET",
    "STORAGE_CAPACITY",
    "STORAGE",
    "PUBLIC_CAPACITY",
    "TOOLCHAIN",
    "PUBLIC_PARSER_INPUT",
    "PUBLIC_VENDOR",
    "PUBLIC_ACQUIRE",
    ERROR,
})


def require(condition: bool, code: str = ERROR) -> None:
    if not condition:
        raise ValueError(code)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _unique_object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        require(key not in result, "AUDIT_PUBLIC")
        result[key] = value
    return result


def digest_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1_048_576), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _write_private(path: Path, data: bytes, *, mode: int = 0o600) -> None:
    fd = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        mode,
    )
    with os.fdopen(fd, "wb") as stream:
        os.fchmod(stream.fileno(), mode)
        stream.write(data)
        stream.flush()


def _check_elf_amd64(path: Path) -> None:
    require(path.is_file() and not path.is_symlink(), "BUILD_ELF")
    size = path.stat().st_size
    require(64 <= size <= 512 * 1024 * 1024, "BUILD_ELF")
    with path.open("rb") as stream:
        header = stream.read(20)
    require(
        len(header) == 20
        and header[:4] == b"\x7fELF"
        and header[4:6] == b"\x02\x01"
        and header[18:20] == b"\x3e\x00",
        "BUILD_ELF",
    )


def _stop_group(process: subprocess.Popen) -> None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
    else:
        process.wait()


def check_checkpoint_budget(
    elapsed_s: float,
    projected_remaining_s: float,
    *,
    soft_budget_s: float = SOFT_BUDGET_SECONDS,
    cleanup_margin_s: float = CLEANUP_MARGIN_SECONDS,
) -> None:
    """Fail closed with TIME_BUDGET if remaining work exceeds soft budget."""
    require(
        elapsed_s >= 0.0
        and projected_remaining_s >= 0.0
        and soft_budget_s > cleanup_margin_s >= 0.0,
        "TIME_BUDGET",
    )
    if elapsed_s + projected_remaining_s + cleanup_margin_s > soft_budget_s:
        raise ValueError("TIME_BUDGET")


def _measure_ns(action):
    start = time.perf_counter_ns()
    result = action()
    return result, time.perf_counter_ns() - start


def sizes(directory: Path) -> dict[str, int]:
    paths = [directory, *directory.rglob("*")]
    require(not any(p.is_symlink() for p in paths), "MEASURE_VERIFY")
    result = {
        "wal_bytes": 0,
        "spool_bytes": 0,
        "fixture_bytes": 0,
        "allocated_bytes": 0,
    }
    for path in paths:
        info = path.stat()
        result["allocated_bytes"] += info.st_blocks * 512
        if path.is_file():
            result["fixture_bytes"] += info.st_size
            if path.name == "lsm.wal":
                result["wal_bytes"] += info.st_size
            if "overflow" in path.relative_to(directory).parts:
                result["spool_bytes"] += info.st_size
    require(result["fixture_bytes"] <= MAX_FIXTURE_BYTES, "MEASURE_VERIFY")
    return result


def _pin_first_cpu() -> None:
    if hasattr(os, "sched_getaffinity") and hasattr(os, "sched_setaffinity"):
        allowed = os.sched_getaffinity(0)
        if allowed:
            os.sched_setaffinity(0, {min(allowed)})


def run_child(
    phase: str,
    directory: Path,
    surface: str,
    size: int,
    expected_native: str,
) -> None:
    """Isolated write/verify child matching public_api_cost.py semantics."""
    _pin_first_cpu()
    from alopex import Database, TxnMode, WalOverflowOptions, _alopex

    require(digest_file(Path(_alopex.__file__)) == expected_native, "MEASURE_VERIFY")
    require(not hasattr(_alopex, "_arm_wal_test_fault"), "MEASURE_VERIFY")

    db_path = directory / "db.alopex"
    options = WalOverflowOptions(**OPTIONS)
    record: dict[str, object] = {
        "phase": phase,
        "surface": surface,
        "value_bytes": size,
        "native_sha256": expected_native,
        "status": "passed",
    }

    if phase == "write":
        start = time.perf_counter_ns()
        values = (b"x" * size, b"y" * size)
        sql = (
            "INSERT INTO items VALUES (2, 'a', '"
            + "x" * size
            + "'), (3, 'b', '"
            + "y" * size
            + "')"
        )
        record["prepare_ns"] = time.perf_counter_ns() - start
        db, record["open_ns"] = _measure_ns(
            lambda: Database.open(str(db_path), wal_overflow=options)
        )
        record["schema_ns"] = 0
        record["index_ns"] = 0
        if surface == "sql":
            _, record["schema_ns"] = _measure_ns(
                lambda: db.execute_sql(
                    "CREATE TABLE items (id INT PRIMARY KEY, code TEXT, value TEXT)"
                )
            )
            _, record["index_ns"] = _measure_ns(
                lambda: db.execute_sql("CREATE INDEX items_code ON items(code)")
            )
        start = time.perf_counter_ns()
        tx = db.begin(TxnMode.READ_WRITE)
        if surface == "kv":
            tx.put(b"old", b"ok")
        else:
            tx.execute_sql("INSERT INTO items VALUES (1, 'old', 'ok')")
        tx.commit()
        record["prior_ns"] = time.perf_counter_ns() - start

        tx, record["begin_ns"] = _measure_ns(lambda: db.begin(TxnMode.READ_WRITE))
        start = time.perf_counter_ns()
        if surface == "kv":
            tx.put(b"a", values[0])
            tx.put(b"b", values[1])
        else:
            tx.execute_sql(sql)
        record["stage_ns"] = time.perf_counter_ns() - start
        _, record["commit_ns"] = _measure_ns(tx.commit)

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        record["peak_rss_bytes"] = peak if sys.platform == "darwin" else peak * 1024
        require(
            int(record["peak_rss_bytes"]) <= MAX_WRITER_RSS_BYTES,
            "MEASURE_VERIFY",
        )
        record.update(sizes(directory))
        record["overflow_stats"] = db.wal_overflow_stats()
        sys.stdout.write(json.dumps(record, sort_keys=True) + "\n")
        sys.stdout.flush()
        os._exit(0)  # Exit without destructors/checkpoint to preserve live WAL.

    db, record["open_ns"] = _measure_ns(
        lambda: Database.open(str(db_path), wal_overflow=options)
    )
    try:
        if surface == "kv":
            tx = db.begin(TxnMode.READ_ONLY)
            require(
                tx.get(b"old") == b"ok"
                and tx.get(b"a") == b"x" * size
                and tx.get(b"b") == b"y" * size,
                "MEASURE_VERIFY",
            )
            tx.rollback()
        else:
            expected = [
                {"id": 1, "code": "old", "value": "ok"},
                {"id": 2, "code": "a", "value": "x" * size},
                {"id": 3, "code": "b", "value": "y" * size},
            ]
            require(
                db.execute_sql(
                    "SELECT id, code, value FROM items ORDER BY id"
                )
                == expected,
                "MEASURE_VERIFY",
            )
            require(
                db.execute_sql(
                    "SELECT id, code, value FROM items WHERE code = 'a'"
                )
                == expected[1:2],
                "MEASURE_VERIFY",
            )
            require(
                db.execute_sql(
                    "SELECT id, code, value FROM items WHERE code = 'absent'"
                )
                == [],
                "MEASURE_VERIFY",
            )
        stats = db.wal_overflow_stats()
        require(isinstance(stats, dict) and stats.get("gc_failures") == 0, "MEASURE_VERIFY")
    finally:
        db.close()
    record.update(sizes(directory))
    record["verified"] = True
    sys.stdout.write(json.dumps(record, sort_keys=True) + "\n")
    sys.stdout.flush()


def summarize(rows: list[dict[str, object]]) -> dict[str, object]:
    """Summarize exact 48-writer / 96-reader matrix excluding warmup round 0."""
    expected = {
        (engine, surface, size, round_id)
        for engine in ARMS
        for surface, size in SHAPES
        for round_id in range(ROUNDS)
    }
    identities = [
        (r["engine"], r["surface"], r["value_bytes"], r["round"]) for r in rows
    ]
    require(
        len(identities) == len(expected) and set(identities) == expected,
        "MEASURE_SUMMARY",
    )
    require(
        all(
            r.get("status") == "passed"
            and r.get("reopen_cycles") == 2
            and isinstance(r.get("reopen_ns"), list)
            and len(r["reopen_ns"]) == 2
            for r in rows
        ),
        "MEASURE_SUMMARY",
    )
    medians: dict[str, dict[str, float | int]] = {}
    for engine in ARMS:
        for surface, size in SHAPES:
            cell = [
                r
                for r in rows
                if r["engine"] == engine
                and r["surface"] == surface
                and r["value_bytes"] == size
                and int(r["round"]) > 0
            ]
            require(len(cell) == ROUNDS - 1, "MEASURE_SUMMARY")
            key = f"{engine}-{surface}-{size}"
            entry: dict[str, float | int] = {
                metric: statistics.median(r[metric] for r in cell)
                for metric in METRICS
            }
            center = entry["commit_ns"]
            entry["commit_mad_ns"] = statistics.median(
                abs(r["commit_ns"] - center) for r in cell
            )
            entry["reopen_ns"] = statistics.median(
                ns for r in cell for ns in r["reopen_ns"]
            )
            medians[key] = entry
    ratios: dict[str, float] = {}
    for surface, size in SHAPES:
        before_commit = medians[f"before-{surface}-{size}"]["commit_ns"]
        after_commit = medians[f"candidate-{surface}-{size}"]["commit_ns"]
        require(before_commit > 0 and after_commit > 0, "MEASURE_SUMMARY")
        ratios[f"{surface}-{size}"] = after_commit / before_commit
    investigate = sorted(
        key for key, value in ratios.items() if value > INVESTIGATE_RATIO_THRESHOLD
    )
    return {
        "medians": medians,
        "commit_after_before_ratios": ratios,
        "investigate": investigate,
        "investigate_threshold": INVESTIGATE_RATIO_THRESHOLD,
        "rounds": ROUNDS,
        "warmup_excluded_from_medians": True,
        "writer_processes": len(ARMS) * len(SHAPES) * ROUNDS,
        "reader_processes": len(ARMS) * len(SHAPES) * ROUNDS * 2,
        "scope": (
            "hosted release-profile four-cell Python KV/SQL WAL overflow "
            "comparison; process-exit replay is not device power-loss proof"
        ),
    }


def _is_number(value: object) -> bool:
    return type(value) in (int, float) and not isinstance(value, bool)


def audit_public_report(raw: bytes) -> tuple[dict[str, object], str]:
    """Independently verify canonical public JSON allowlist and recalculations."""
    require(
        isinstance(raw, bytes) and 0 < len(raw) <= MAX_PUBLIC_BYTES,
        "AUDIT_PUBLIC",
    )
    text = raw.decode("utf-8")
    for forbidden in FORBIDDEN_PUBLIC_TOKENS:
        require(forbidden not in text, "AUDIT_PUBLIC")
    parsed = json.loads(text, object_pairs_hook=_unique_object_pairs)
    require(type(parsed) is dict and set(parsed) == REPORT_KEYS, "AUDIT_PUBLIC")
    require(_canonical(parsed) == raw, "AUDIT_PUBLIC")
    require(parsed["schema"] == SCHEMA, "AUDIT_PUBLIC")
    require(parsed["status"] == "measured", "AUDIT_PUBLIC")
    require(parsed["boundary"] == BOUNDARY_TAG, "AUDIT_PUBLIC")
    require(parsed["profile"] == "release", "AUDIT_PUBLIC")
    require(parsed["platform"] == TARGET, "AUDIT_PUBLIC")
    require(parsed["rust_version"] == RUST_VERSION, "AUDIT_PUBLIC")
    require(parsed["parser_contract"] == PARSER_CONTRACT_VERSION, "AUDIT_PUBLIC")
    require(parsed["options"] == OPTIONS, "AUDIT_PUBLIC")
    require(parsed["rounds"] == ROUNDS, "AUDIT_PUBLIC")
    require(parsed["warmup_excluded_from_medians"] is True, "AUDIT_PUBLIC")
    require(parsed["writer_processes"] == 48, "AUDIT_PUBLIC")
    require(parsed["reader_processes"] == 96, "AUDIT_PUBLIC")
    require(
        parsed["investigate_threshold"] == INVESTIGATE_RATIO_THRESHOLD,
        "AUDIT_PUBLIC",
    )
    require(isinstance(parsed["scope"], str) and bool(parsed["scope"]), "AUDIT_PUBLIC")

    pins = parsed["pins"]
    require(type(pins) is dict and set(pins) == set(ARMS), "AUDIT_PUBLIC")
    for arm in ARMS:
        entry = pins[arm]
        require(type(entry) is dict and set(entry) == PIN_KEYS, "AUDIT_PUBLIC")
        require(entry["source_commit"] == PINS[arm]["commit"], "AUDIT_PUBLIC")
        require(entry["source_tree"] == PINS[arm]["tree"], "AUDIT_PUBLIC")
        require(entry["cargo_lock_sha256"] == PINS[arm]["lock"], "AUDIT_PUBLIC")
        require(entry["profile"] == "release", "AUDIT_PUBLIC")
        require(entry["ordinary_extension"] is True, "AUDIT_PUBLIC")
        for key in (
            "parser_sha256",
            "parser_record_sha256",
            "native_sha256",
            "python_tree_sha256",
        ):
            require(
                isinstance(entry[key], str) and HEX64.fullmatch(entry[key]) is not None,
                "AUDIT_PUBLIC",
            )
    require(
        pins["before"]["parser_sha256"] == pins["candidate"]["parser_sha256"]
        and pins["before"]["parser_record_sha256"]
        == pins["candidate"]["parser_record_sha256"],
        "AUDIT_PUBLIC",
    )

    medians = parsed["medians"]
    expected_cells = {
        f"{arm}-{surface}-{size}" for arm in ARMS for surface, size in SHAPES
    }
    require(type(medians) is dict and set(medians) == expected_cells, "AUDIT_PUBLIC")
    for cell_key, metrics in medians.items():
        require(
            type(metrics) is dict and set(metrics) == MEDIAN_FIELDS,
            "AUDIT_PUBLIC",
        )
        for metric_key, value in metrics.items():
            require(_is_number(value) and value >= 0, "AUDIT_PUBLIC")
        require(metrics["fixture_bytes"] <= MAX_FIXTURE_BYTES, "AUDIT_PUBLIC")
        require(metrics["peak_rss_bytes"] <= MAX_WRITER_RSS_BYTES, "AUDIT_PUBLIC")

    ratios = parsed["commit_after_before_ratios"]
    expected_ratio_keys = {f"{surface}-{size}" for surface, size in SHAPES}
    require(type(ratios) is dict and set(ratios) == expected_ratio_keys, "AUDIT_PUBLIC")
    recomputed_investigate = []
    for surface, size in SHAPES:
        key = f"{surface}-{size}"
        before_val = medians[f"before-{surface}-{size}"]["commit_ns"]
        after_val = medians[f"candidate-{surface}-{size}"]["commit_ns"]
        require(before_val > 0 and after_val > 0, "AUDIT_PUBLIC")
        expected_ratio = after_val / before_val
        actual_ratio = ratios[key]
        require(_is_number(actual_ratio) and actual_ratio > 0, "AUDIT_PUBLIC")
        require(
            abs(actual_ratio - expected_ratio)
            <= 1e-12 * max(1.0, abs(expected_ratio)),
            "AUDIT_PUBLIC",
        )
        if actual_ratio > INVESTIGATE_RATIO_THRESHOLD:
            recomputed_investigate.append(key)
    require(
        parsed["investigate"] == sorted(recomputed_investigate),
        "AUDIT_PUBLIC",
    )
    return parsed, hashlib.sha256(raw).hexdigest()


def _git_output(args: list[str], cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(
        [
            str(acquire_source.GIT),
            "-c",
            "credential.helper=",
            "-c",
            "http.followRedirects=false",
            "-c",
            "core.hooksPath=/dev/null",
            *args,
        ],
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    require(result.returncode == 0, "ACQUIRE_SOURCES")
    return result.stdout.strip()


def _verify_checkout(source: Path, pin: dict[str, str], env: dict[str, str]) -> None:
    require(_git_output(["rev-parse", "HEAD"], source, env) == pin["commit"], "ACQUIRE_SOURCES")
    require(
        _git_output(["rev-parse", "HEAD^{tree}"], source, env) == pin["tree"],
        "ACQUIRE_SOURCES",
    )
    require(
        _git_output(["status", "--porcelain", "--untracked-files=all"], source, env)
        == "",
        "ACQUIRE_SOURCES",
    )
    require(digest_file(source / "Cargo.lock") == pin["lock"], "ACQUIRE_SOURCES")


def _acquire_pair_sources(
    parent: Path, guard: capacity.HostStorageGuard
) -> tuple[dict[str, Path], dict[str, str]]:
    acquire_source._trusted_git()
    git_home = parent / "git-home"
    git_home.mkdir(mode=0o700)
    git_env = acquire_source._git_env(git_home)
    sources: dict[str, Path] = {}
    log_path = parent / "git-acquire.private.log"
    fd = os.open(
        log_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(fd, "wb") as log:
        for arm in ARMS:
            pin = PINS[arm]
            checkout = parent / arm
            checkout.mkdir(mode=0o700)
            acquire_source._git(["init", "-q"], checkout, guard, log, git_env)
            acquire_source._git(
                ["remote", "add", "origin", REPOSITORY], checkout, guard, log, git_env
            )
            acquire_source._git(
                ["fetch", "-q", "--no-tags", "--depth=1", "origin", pin["commit"]],
                checkout,
                guard,
                log,
                git_env,
            )
            acquire_source._git(
                ["checkout", "-q", "--detach", "FETCH_HEAD"],
                checkout,
                guard,
                log,
                git_env,
            )
            _verify_checkout(checkout, pin, git_env)
            sources[arm] = checkout
    return sources, git_env


def _build_parser_for_candidate(
    candidate_source: Path,
    nim: Path,
    nimble: Path,
    seed: Path,
    build_parent: Path,
    stage_parent: Path,
    guard: capacity.HostStorageGuard,
    git_env: dict[str, str],
) -> dict[str, object]:
    require(not PARSER_SCRATCH.exists(), "ACQUIRE_PARSER_BUILD")
    archive_dir = build_parent / "archive"
    archive_dir.mkdir(mode=0o700)
    env = {
        "PATH": f"{nim.parent}:{nimble.parent}:/usr/bin:/bin",
        "HOME": str(build_parent),
        "XDG_CONFIG_HOME": str(build_parent),
        "XDG_CACHE_HOME": str(build_parent),
        "TMPDIR": str(build_parent),
        "LANG": "C",
        "LC_ALL": "C",
        "TZ": "UTC",
        "PYTHON": str(PYTHON),
        "ALOPEX_NIM_BIN": str(nim),
        "ALOPEX_NIMBLE_BIN": str(nimble),
        "ALOPEX_NIMBLE_SEED_DIR": str(seed),
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ASKPASS": "/bin/false",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
    }
    argv = [
        str(BASH),
        "scripts/build-nim-parser.sh",
        "--backend",
        "native",
        "--target",
        TARGET,
        "--archive-dir",
        str(archive_dir),
    ]
    log_path = build_parent / "parser-build.private.log"
    fd = os.open(
        log_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    deadline = time.monotonic() + 20 * 60
    with os.fdopen(fd, "wb") as log:
        guard(force=True)
        process = subprocess.Popen(
            argv,
            cwd=candidate_source,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                require(time.monotonic() < deadline, "TIME_BUDGET")
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
                guard()
            require(process.returncode == 0, "ACQUIRE_PARSER_BUILD")
        except BaseException:
            _stop_group(process)
            raise
    require(
        not PARSER_SCRATCH.exists() and not Path(str(PARSER_SCRATCH) + ".lock").exists(),
        "ACQUIRE_PARSER_BUILD",
    )
    _verify_checkout(candidate_source, PINS["candidate"], git_env)
    staged = parser_stage.stage_parser(archive_dir, stage_parent, guard)
    require(
        producer.verify_parser(
            staged["parser_record"], staged["parser_root"], staged["parser_library"]
        )
        == staged["parser_record_sha256"],
        "ACQUIRE_PARSER_STAGE",
    )
    return staged


def _stage_cargo_vendor(
    sources: dict[str, Path],
    cargo_home: Path,
    scratch_parent: Path,
    guard: capacity.HostStorageGuard,
    timeout: int,
) -> Path:
    home = Path.home()
    search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
    cargo_bin = rust_preflight._binary("cargo", search)
    partial = Path(tempfile.mkdtemp(prefix=".vendor.", dir=cargo_home))
    os.chmod(partial, 0o700)
    fetch_home = Path(tempfile.mkdtemp(prefix="cargo-fetch-", dir=scratch_parent))
    os.chmod(fetch_home, 0o700)
    try:
        env = {
            "HOME": str(home),
            "PATH": search,
            "LANG": "C.UTF-8",
            "CARGO_HOME": str(fetch_home),
            "RUSTUP_TOOLCHAIN": RUST_VERSION,
            "RUSTUP_AUTO_INSTALL": "0",
        }
        argv = [
            str(cargo_bin),
            "vendor",
            "--locked",
            "--manifest-path",
            str(sources["candidate"] / "Cargo.toml"),
            "--sync",
            str(sources["before"] / "Cargo.toml"),
            str(partial),
        ]
        log_path = scratch_parent / "cargo-vendor.private.log"
        fd = os.open(
            log_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        deadline = time.monotonic() + timeout
        with os.fdopen(fd, "wb") as log:
            guard(force=True)
            process = subprocess.Popen(
                argv,
                cwd=sources["candidate"],
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                while process.poll() is None:
                    require(time.monotonic() < deadline, "TIME_BUDGET")
                    try:
                        process.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        pass
                    guard()
                require(process.returncode == 0, "ACQUIRE_CARGO_VENDOR")
            except BaseException:
                _stop_group(process)
                raise
        shutil.rmtree(fetch_home)
        fetch_home = None
        cargo_fetch._normalize_and_digest(partial, guard, deadline=deadline)
        vendor = cargo_home / "vendor"
        nim_archive._promote_noreplace(partial, vendor)
        partial = None
        config = cargo_home / "config.toml"
        content = (
            '[source.crates-io]\nreplace-with = "vendored-sources"\n\n'
            "[source.vendored-sources]\ndirectory = "
            + json.dumps(str(vendor))
            + "\n"
        ).encode("utf-8")
        _write_private(config, content)
        guard(force=True)
        return cargo_home
    finally:
        if fetch_home is not None and fetch_home.exists():
            shutil.rmtree(fetch_home, ignore_errors=True)
        if partial is not None and partial.exists():
            shutil.rmtree(partial, ignore_errors=True)


def _hash_python_tree(pkg_root: Path) -> str:
    hasher = hashlib.sha256()
    files = sorted(
        p for p in pkg_root.rglob("*") if p.is_file() and p.name != "_alopex.abi3.so"
    )
    require(bool(files), "BUILD_STAGE")
    for path in files:
        rel = path.relative_to(pkg_root).as_posix()
        hasher.update(f"{rel}\0{digest_file(path)}\n".encode("utf-8"))
    return hasher.hexdigest()


def _build_and_stage_arm(
    arm: str,
    arm_source: Path,
    shared_target: Path,
    cargo_home: Path,
    parser: dict[str, object],
    stage_parent: Path,
    work_dir: Path,
    guard: capacity.HostStorageGuard,
    timeout_s: int,
) -> tuple[Path, dict[str, object]]:
    home = Path.home()
    search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
    cargo_bin = rust_preflight._binary("cargo", search)
    parser_library = Path(parser["parser_library"])
    parser_dir = parser_library.parent

    # Remove previous arm's cdylib output while retaining shared dependency artifacts.
    prior_lib = shared_target / "release" / "lib_alopex.so"
    if prior_lib.exists():
        prior_lib.unlink()

    env = {
        "HOME": str(home),
        "PATH": search,
        "LANG": "C.UTF-8",
        "TMPDIR": str(work_dir),
        "CARGO_HOME": str(cargo_home),
        "CARGO_NET_OFFLINE": "true",
        "CARGO_TARGET_DIR": str(shared_target),
        "CARGO_INCREMENTAL": "0",
        "CARGO_BUILD_JOBS": "2",
        "CARGO_PROFILE_RELEASE_DEBUG": "0",
        "CARGO_PROFILE_RELEASE_STRIP": "debuginfo",
        "RUSTUP_TOOLCHAIN": RUST_VERSION,
        "RUSTUP_AUTO_INSTALL": "0",
        "PYO3_BUILD_EXTENSION_MODULE": "1",
        "PYO3_PYTHON": sys.executable,
        "NIM_SQL_PARSER_LIB_DIR": str(parser_dir),
        "ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD": "1",
        "RUSTFLAGS": (
            f"--remap-path-prefix={arm_source}=/alopex-src "
            f"--remap-path-prefix={cargo_home}=/alopex-cargo "
            f"--remap-path-prefix={shared_target}=/alopex-target"
        ),
        "CFLAGS": (
            f"-ffile-prefix-map={arm_source}=/alopex-src "
            f"-ffile-prefix-map={cargo_home}=/alopex-cargo"
        ),
    }
    argv = [
        str(cargo_bin),
        "build",
        "--locked",
        "--offline",
        "--release",
        "-p",
        "alopex-py",
        "--lib",
    ]
    log_path = work_dir / f"build-{arm}.private.log"
    fd = os.open(
        log_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    deadline = time.monotonic() + timeout_s
    with os.fdopen(fd, "wb") as log:
        guard(force=True)
        process = subprocess.Popen(
            argv,
            cwd=arm_source,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            while process.poll() is None:
                require(time.monotonic() < deadline, "TIME_BUDGET")
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
                guard()
            require(process.returncode == 0, "BUILD_CARGO")
        except BaseException:
            _stop_group(process)
            raise

    built_so = shared_target / "release" / "lib_alopex.so"
    _check_elf_amd64(built_so)

    pkg_root = stage_parent / arm
    pkg_dir = pkg_root / "alopex"
    src_pkg = arm_source / "crates/alopex-py/python/alopex"
    shutil.copytree(src_pkg, pkg_dir, symlinks=False)
    os.chmod(pkg_root, 0o700)
    os.chmod(pkg_dir, 0o700)

    ext_path = pkg_dir / "_alopex.abi3.so"
    shutil.copyfile(built_so, ext_path)
    os.chmod(ext_path, 0o700)

    native_dir = pkg_dir / "native"
    native_dir.mkdir(mode=0o700, exist_ok=True)
    for asset_name in (parser_library.name, "CONTRACT_VERSION", "SHA256SUMS"):
        src_asset = parser_dir / asset_name
        require(src_asset.is_file() and not src_asset.is_symlink(), "BUILD_STAGE")
        dst_asset = native_dir / asset_name
        shutil.copyfile(src_asset, dst_asset)
        os.chmod(dst_asset, 0o700 if asset_name.endswith(".so") else 0o600)

    native_sha256 = digest_file(ext_path)
    python_tree_sha256 = _hash_python_tree(pkg_dir)

    # Smoke-verify ordinary extension import & overflow API in a clean child.
    smoke_dir = Path(tempfile.mkdtemp(prefix=f"smoke-{arm}-", dir=work_dir))
    os.chmod(smoke_dir, 0o700)
    try:
        smoke_env = {
            "HOME": str(work_dir),
            "PATH": "/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "PYTHONPATH": str(pkg_root),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONHASHSEED": "0",
            "RAYON_NUM_THREADS": "2",
            "OMP_NUM_THREADS": "2",
            "OPENBLAS_NUM_THREADS": "1",
        }
        smoke_code = (
            "import hashlib, json, pathlib, sys; "
            "from alopex import Database, TxnMode, WalOverflowOptions, _alopex; "
            "h = hashlib.sha256(pathlib.Path(_alopex.__file__).read_bytes()).hexdigest(); "
            "assert h == sys.argv[1]; "
            "assert not hasattr(_alopex, '_arm_wal_test_fault'); "
            "opts = WalOverflowOptions(**json.loads(sys.argv[3])); "
            "db = Database.open(sys.argv[2], wal_overflow=opts); "
            "tx = db.begin(TxnMode.READ_WRITE); tx.put(b'k', b'v'); tx.commit(); "
            "stats = db.wal_overflow_stats(); db.close(); "
            "assert isinstance(stats, dict) and stats.get('gc_failures') == 0; "
            "print('ok')"
        )
        smoke = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                smoke_code,
                native_sha256,
                str(smoke_dir / "smoke.alopex"),
                json.dumps(OPTIONS),
            ],
            env=smoke_env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=CHILD_TIMEOUT_SECONDS,
            check=False,
        )
        require(smoke.returncode == 0 and smoke.stdout.strip() == "ok", "BUILD_SMOKE")
    finally:
        shutil.rmtree(smoke_dir, ignore_errors=True)

    pin_entry = {
        "source_commit": PINS[arm]["commit"],
        "source_tree": PINS[arm]["tree"],
        "cargo_lock_sha256": PINS[arm]["lock"],
        "parser_sha256": parser["parser_sha256"],
        "parser_record_sha256": parser["parser_record_sha256"],
        "native_sha256": native_sha256,
        "python_tree_sha256": python_tree_sha256,
        "profile": "release",
        "ordinary_extension": True,
    }
    return pkg_root, pin_entry


def _run_measurement_matrix(
    packages: dict[str, Path],
    hashes: dict[str, str],
    work_dir: Path,
    guard: capacity.HostStorageGuard,
    job_started: float,
    on_round=None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    script_path = str(Path(__file__).resolve())
    round_durations: list[float] = []

    for round_id in range(ROUNDS):
        round_started = time.monotonic()
        order = ("before", "candidate") if round_id % 2 == 0 else ("candidate", "before")
        for engine in order:
            for surface, size in SHAPES:
                fixture = Path(
                    tempfile.mkdtemp(
                        prefix=f"r{round_id}-{engine}-{surface}-{size}-",
                        dir=work_dir,
                    )
                )
                os.chmod(fixture, 0o700)
                record: dict[str, object] = {
                    "engine": engine,
                    "round": round_id,
                    "surface": surface,
                    "value_bytes": size,
                    "status": "failed",
                    "reopen_cycles": 0,
                    "reopen_ns": [],
                }
                env = {
                    "HOME": str(work_dir),
                    "PATH": "/usr/bin:/bin",
                    "LANG": "C.UTF-8",
                    "PYTHONPATH": str(packages[engine]),
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "PYTHONHASHSEED": "0",
                    "RAYON_NUM_THREADS": "2",
                    "OMP_NUM_THREADS": "2",
                    "OPENBLAS_NUM_THREADS": "1",
                }
                try:
                    for phase in ("write", "verify", "verify"):
                        run = subprocess.run(
                            [
                                sys.executable,
                                "-B",
                                script_path,
                                "--child",
                                phase,
                                "--fixture",
                                str(fixture),
                                "--surface",
                                surface,
                                "--size",
                                str(size),
                                "--native-sha",
                                hashes[engine],
                            ],
                            env=env,
                            stdin=subprocess.DEVNULL,
                            capture_output=True,
                            text=True,
                            timeout=CHILD_TIMEOUT_SECONDS,
                            check=False,
                        )
                        require(run.returncode == 0, "MEASURE_CHILD")
                        data = json.loads(run.stdout)
                        require(
                            data.get("native_sha256") == hashes[engine]
                            and data.get("phase") == phase
                            and data.get("surface") == surface
                            and data.get("value_bytes") == size
                            and data.get("status") == "passed",
                            "MEASURE_VERIFY",
                        )
                        if phase == "write":
                            record.update(data)
                            record["status"] = "failed"
                            record["reopen_ns"] = []
                            record["reopen_cycles"] = 0
                        else:
                            require(data.get("verified") is True, "MEASURE_VERIFY")
                            record["reopen_ns"].append(data["open_ns"])
                            record["reopen_cycles"] = int(record["reopen_cycles"]) + 1
                    record["status"] = "passed"
                finally:
                    shutil.rmtree(fixture, ignore_errors=True)
                guard()
                rows.append(record)

        round_elapsed = time.monotonic() - round_started
        round_durations.append(round_elapsed)
        job_elapsed = time.monotonic() - job_started
        remaining_rounds = ROUNDS - 1 - round_id
        if on_round is not None:
            on_round(round_id, ROUNDS, round_elapsed, job_elapsed)
        if remaining_rounds > 0:
            projected = max(round_durations) * remaining_rounds
            check_checkpoint_budget(job_elapsed, projected)

    return rows


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True), flush=True)


def run_hosted_job() -> int:
    env = os.environ
    job_started = time.monotonic()
    stages = {
        "preflight": "pending",
        "acquire": "pending",
        "build": "pending",
        "measure": "pending",
        "audit": "pending",
        "export": "pending",
    }
    current_stage = "preflight"
    current_substage = "env"
    task_parent = home_parent = None
    try:
        # Stage 1: preflight
        current_substage = "env"
        require(
            env.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and env.get("GITHUB_REPOSITORY") == REPOSITORY_SLUG
            and env.get("ISSUE_NUMBER") == "682"
            and env.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and HEX40.fullmatch(env.get("EXPECTED_CONTROLLER_SHA", "")) is not None
            and env.get("EXPECTED_CONTROLLER_SHA") == env.get("GITHUB_SHA")
            and HEX40.fullmatch(env.get("EXPECTED_WAL_COST_SOURCE_TREE", "")) is not None,
            "PREFLIGHT_ENV",
        )
        workspace = Path(env["GITHUB_WORKSPACE"]).resolve(strict=True)
        runner_temp = Path(env["RUNNER_TEMP"]).resolve(strict=True)
        home = Path.home().resolve(strict=True)
        task_parent = Path(tempfile.mkdtemp(prefix="issue682-task-", dir=runner_temp))
        os.chmod(task_parent, 0o700)
        home_parent = Path(tempfile.mkdtemp(prefix=".alopex-682-home-", dir=home))
        os.chmod(home_parent, 0o700)

        current_substage = "manifest"
        manifest = scoped_storage.make_manifest(
            workspace, runner_temp, task_parent=task_parent, home_parent=home_parent
        )
        current_substage = "storage_guard"
        guard = capacity.HostStorageGuard(workspace, manifest=manifest)
        guard(force=True)
        current_substage = "rust_install"
        rust_preflight.install_pinned_minimal(
            guard, task_parent / "rustup-install.private.log"
        )
        stages["preflight"] = "passed"
        _emit({"case": CASE, "stage": "preflight", "status": "passed"})

        # Stage 2: acquire
        current_stage = "acquire"
        acquire_started = time.monotonic()
        dirs = {}
        for name in (
            "sources",
            "nim",
            "seed",
            "parser-build",
            "parser-stage",
            "target",
            "packages",
            "work",
        ):
            path = task_parent / name
            path.mkdir(mode=0o700)
            dirs[name] = path
        cargo_home = home_parent / "cargo"
        cargo_home.mkdir(mode=0o700)

        current_substage = "sources"
        sources, git_env = _acquire_pair_sources(dirs["sources"], guard)
        _emit({"case": CASE, "stage": "acquire", "substage": "sources", "status": "passed"})

        current_substage = "nim_fetch"
        nim_archive.fetch_archive(dirs["nim"], guard)
        current_substage = "nim_extract"
        extracted = nim_archive.extract_archive(dirs["nim"], guard)
        current_substage = "parser_tools"
        try:
            tools = parser_tools.verify_tools(extracted, guard)
        except ValueError:
            current_substage = "nimble_install"
            parser_tools.install_pinned_nimble(extracted, guard)
            current_substage = "parser_tools"
            tools = parser_tools.verify_tools(extracted, guard)
        current_substage = "parser_seed"
        seed = parser_seed.acquire_seed(dirs["seed"], guard, git_env)
        current_substage = "parser_build"
        parser = _build_parser_for_candidate(
            sources["candidate"],
            tools["nim"],
            tools["nimble"],
            seed,
            dirs["parser-build"],
            dirs["parser-stage"],
            guard,
            git_env,
        )
        _emit({"case": CASE, "stage": "acquire", "substage": "parser", "status": "passed"})

        current_substage = "cargo_vendor"
        remaining_acquire = max(
            60, int(ACQUIRE_BUDGET_SECONDS - (time.monotonic() - acquire_started))
        )
        _stage_cargo_vendor(sources, cargo_home, dirs["work"], guard, remaining_acquire)
        _emit({"case": CASE, "stage": "acquire", "substage": "cargo_vendor", "status": "passed"})
        # Reclaim Nim/seed/parser-build intermediates before Cargo builds.
        for reclaim in ("nim", "seed", "parser-build"):
            shutil.rmtree(dirs[reclaim], ignore_errors=True)
        guard(force=True)
        current_substage = "checkpoint"
        check_checkpoint_budget(
            time.monotonic() - job_started, 10 * 60
        )
        stages["acquire"] = "passed"
        _emit({
            "case": CASE,
            "stage": "acquire",
            "status": "passed",
            "elapsed_s": int(time.monotonic() - acquire_started),
        })

        # Stage 3: build (serialized before -> candidate using shared target)
        current_stage = "build"
        build_started = time.monotonic()
        packages: dict[str, Path] = {}
        pins: dict[str, dict[str, object]] = {}
        for arm in ARMS:
            current_substage = f"build_{arm}"
            arm_started = time.monotonic()
            check_checkpoint_budget(time.monotonic() - job_started, 5 * 60)
            pkg_root, pin_entry = _build_and_stage_arm(
                arm,
                sources[arm],
                dirs["target"],
                cargo_home,
                parser,
                dirs["packages"],
                dirs["work"],
                guard,
                BUILD_ARM_TIMEOUT_SECONDS,
            )
            packages[arm] = pkg_root
            pins[arm] = pin_entry
            _emit({
                "case": CASE,
                "stage": "build",
                "substage": arm,
                "status": "passed",
                "elapsed_s": int(time.monotonic() - arm_started),
            })

        # Reclaim target/sources/cargo_home before running measurements.
        for reclaim in ("target", "sources"):
            shutil.rmtree(dirs[reclaim], ignore_errors=True)
        shutil.rmtree(cargo_home, ignore_errors=True)
        guard(force=True)
        current_substage = "checkpoint"
        check_checkpoint_budget(time.monotonic() - job_started, 3 * 60)
        stages["build"] = "passed"
        _emit({
            "case": CASE,
            "stage": "build",
            "status": "passed",
            "elapsed_s": int(time.monotonic() - build_started),
        })

        # Stage 4: measure (48 writers + 96 readers with per-round checkpoint)
        current_stage = "measure"
        current_substage = "matrix"
        measure_started = time.monotonic()
        hashes = {arm: str(pins[arm]["native_sha256"]) for arm in ARMS}
        rows = _run_measurement_matrix(
            packages,
            hashes,
            dirs["work"],
            guard,
            job_started,
            on_round=lambda idx, total, r_s, j_s: _emit({
                "case": CASE,
                "stage": "measure",
                "substage": "round",
                "round": idx,
                "total_rounds": total,
                "status": "passed",
                "elapsed_s": round(r_s, 3),
                "job_elapsed_s": int(j_s),
            }),
        )
        current_substage = "summarize"
        summary = summarize(rows)
        stages["measure"] = "passed"
        _emit({
            "case": CASE,
            "stage": "measure",
            "status": "passed",
            "elapsed_s": int(time.monotonic() - measure_started),
        })

        # Stage 5: independent public audit
        current_stage = "audit"
        current_substage = "audit_public"
        report = {
            "schema": SCHEMA,
            "status": "measured",
            "boundary": BOUNDARY_TAG,
            "profile": "release",
            "platform": TARGET,
            "rust_version": RUST_VERSION,
            "parser_contract": PARSER_CONTRACT_VERSION,
            "options": OPTIONS,
            "pins": pins,
            **summary,
        }
        canonical_bytes = _canonical(report)
        _, public_sha256 = audit_public_report(canonical_bytes)
        stages["audit"] = "passed"
        _emit({"case": CASE, "stage": "audit", "status": "passed"})

        # Stage 6: export allowlisted public JSON only
        current_stage = "export"
        current_substage = "write_export"
        export_dir = runner_temp / "issue682-public-export"
        if export_dir.exists():
            shutil.rmtree(export_dir)
        export_dir.mkdir(mode=0o700)
        os.chmod(export_dir, 0o700)
        export_file = export_dir / "issue682-wal-cost-public.json"
        _write_private(export_file, canonical_bytes)
        stages["export"] = "passed"
        _emit({
            "case": CASE,
            "outcome": "passed",
            "boundary": BOUNDARY_TAG,
            "public_sha256": public_sha256,
            "commit_after_before_ratios": report["commit_after_before_ratios"],
            "investigate": report["investigate"],
            "stages": stages,
        })
        return 0
    except ValueError as exc:
        code = str(exc) if str(exc) in DIAG_CODES else ERROR
        stages[current_stage] = "failed"
        _emit({
            "case": CASE,
            "outcome": "failed",
            "error_code": code,
            "failed_stage": current_stage,
            "failed_substage": current_substage,
            "stages": stages,
        })
        return 1
    except BaseException as exc:
        stages[current_stage] = "failed"
        _emit({
            "case": CASE,
            "outcome": "failed",
            "error_code": ERROR,
            "error_type": type(exc).__name__,
            "failed_stage": current_stage,
            "failed_substage": current_substage,
            "stages": stages,
        })
        return 1
    finally:
        for directory in (task_parent, home_parent):
            if directory is not None and directory.exists():
                shutil.rmtree(directory, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", choices=("write", "verify"))
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--surface", choices=("kv", "sql"))
    parser.add_argument("--size", type=int, choices=(16, 2048))
    parser.add_argument("--native-sha")
    args = parser.parse_args(argv)
    if args.child:
        require(
            args.fixture is not None
            and args.surface is not None
            and args.size is not None
            and args.native_sha is not None
            and HEX64.fullmatch(args.native_sha) is not None,
            "MEASURE_VERIFY",
        )
        run_child(args.child, args.fixture, args.surface, args.size, args.native_sha)
        return 0
    return run_hosted_job()


if __name__ == "__main__":
    raise SystemExit(main())
