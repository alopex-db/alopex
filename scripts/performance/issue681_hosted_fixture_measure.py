#!/usr/bin/env python3
"""Precondition-vs-SUT separated hosted #681 HNSW paired measurement runner.

UC-V0817-681-FIXTURE-SUT-BOUNDARY-01:
- Precondition (`prepare`): builds each scale's (1k, 10k, 100k) durable on-disk
  HNSW database fixture ONCE using the pinned `before` binary, records fixture
  build timing and immutable directory SHA-256 separately under `fixtures`.
- SUT (`measure`): for every trial (5 interleaved trials x 2 arms x requested
  finishes), copies the immutable fixture to a fresh owner-only directory,
  launches a fresh child process with an empty 256 MiB HNSW cache, and measures
  ONLY the 7 #681 SUT phases under Linux PhaseRss (clear_refs=5), Admission
  (4-CPU pin + 99.0% idle settle window), and HostStorageGuard (40 GB stop).
- Exposure boundary: stdout emits only allowlisted status/progress JSON; public
  artifact upload contains only canonical, independently audited public JSON.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import re
import selectors
import shutil
import signal
import stat
import statistics
import subprocess
import sys
import tempfile
import threading
import time

import issue681_hosted_acquire_all as acquire
import issue681_hosted_acquire_source as acquire_source
import issue681_hosted_build_guard as capacity
import issue681_hosted_identity as identity_gate
import issue681_hosted_qualify as qualify
import issue681_hosted_rust_preflight as rust
import issue681_hosted_storage as scoped_storage
import issue681_hosted_vendor_tree as vendor_tree
import issue681_public as case_mod
import issue681_public_admission as public_admission
import issue681_public_assets as public_assets
import issue681_public_assets_produce as producer
from phase_rss import PhaseRss

ERROR = "PUBLIC_JOB"
CASE = "HNSW_PUBLIC_JOB"
SCHEMA = "alopex.issue681-fixture-sut/v1"
CASE_SCHEMA = "alopex.issue681-fixture-case/v1"
AUDIT_SCHEMA = "alopex.issue681-fixture-audit/v1"
BOUNDARY_TAG = "precondition_fixture_separated_from_sut"
RSS_SCOPE = "Linux VmHWM reset at phase handshakes"
BENCH_REL = Path("scripts/performance/issue681_fixture_bench/src/main.rs")
BINARY_NAME = "issue681-fixture-bench"
RUST_VERSION = "1.90.0"
HOSTED_RUNNER_KIND = "github_hosted_ubuntu2404"

SCALES = (1_000, 10_000, 100_000)
ARMS = ("before", "candidate")
SUT_PHASES = (
    "cold_transaction_1",
    "cold_transaction_2",
    "warm",
    "writer_begin",
    "first_insert",
    "second_insert",
)
PREPARE_TIMEOUTS = {1_000: 120, 10_000: 300, 100_000: 2_400}
MEASURE_TIMEOUTS = {1_000: 120, 10_000: 180, 100_000: 300}
RSS_LIMIT = 4 * 1024 ** 3
SOFT_BUDGET_SECONDS = 120 * 60
CLEANUP_MARGIN_SECONDS = 10 * 60
ACQUIRE_BUDGET_SECONDS = 25 * 60
BUILD_LIMIT_SECONDS = 3_600
MAX_PUBLIC_BYTES = 500_000
MAX_ADMISSION_LEDGER_BYTES = 40_000
POST_FINISH_SECONDS = 30

PIN_FIELDS = {
    "source_commit",
    "source_tree",
    "binary_sha256",
    "bench_sha256",
    "parser_sha256",
    "parser_record_sha256",
    "cargo_lock_sha256",
    "profile",
    "platform",
    "parser_contract",
    "rust_version",
    "version",
}
FIXTURE_FIELDS = {
    "rows",
    "dimensions",
    "builder_arm",
    "builder_commit",
    "seed_elapsed_ns",
    "index_build_elapsed_ns",
    "fixture_sha256",
}
CASE_FIELDS = {
    "schema",
    "status",
    "source_commit",
    "binary_sha256",
    "bench_sha256",
    "fixture_sha256",
    "rows",
    "dimensions",
    "trial",
    "finish",
    "phases",
    "rss_scope",
    "result_validated",
}
PHASE_FIELDS = {
    "phase",
    "elapsed_ns",
    "kernel_peak_rss_bytes",
    "cache_hits",
    "cache_misses",
    "graph_bytes",
}
REPORT_FIELDS = {
    "schema",
    "status",
    "boundary",
    "pins",
    "finish",
    "fixtures",
    "cases",
    "summary",
    "performance_acceptance",
}
RECEIPT_FIELDS = {
    "schema",
    "source_commit",
    "binary_sha256",
    "bench_sha256",
    "fixture_sha256",
    "runner_sha256",
    "rows",
    "dimensions",
    "trial",
    "finish",
    "child_exit",
    "events",
    "phases",
    "public_sha256",
}
SNAPSHOT_FIELDS = {"rss_bytes", "high_water_bytes"}
OBSERVATION_FIELDS = {
    "phase",
    "before_reset",
    "after_reset",
    "after",
    "kernel_reported_peak_rss_bytes",
    "kernel_high_water_decreased",
    "aggregation",
    "scope",
    "counter",
}

FIXTURE_DIAG_CODES = frozenset({
    "BUILD_ADMIT",
    "BUILD_RUSTC",
    "BUILD_SOURCE_PRE",
    "BUILD_PARSER",
    "BUILD_VENDOR_PRE",
    "BUILD_CARGO_EXIT",
    "BUILD_CARGO_TIMEOUT",
    "BUILD_CARGO_CAPACITY",
    "BUILD_VENDOR_POST",
    "BUILD_SOURCE_POST",
    "BUILD_BINARY_MISSING",
    "BUILD_ELF_CHECK",
    "BUILD_PREFIX_LEAK",
    "BUILD_SMOKE_PREPARE",
    "BUILD_SMOKE_COMMIT",
    "BUILD_SMOKE_ROLLBACK",
    "MEASURE_ADMIT",
    "MEASURE_ADMISSION_GATE",
    "MEASURE_TIME_BUDGET",
    "MEASURE_FIXTURE_PREPARE",
    "MEASURE_FIXTURE_TIMEOUT",
    "MEASURE_FIXTURE_IMMUTABILITY",
    "MEASURE_STAGE_ADMIT",
    "MEASURE_CASE_COPY",
    "MEASURE_CASE_ADMIT",
    "MEASURE_CASE_RUN",
    "MEASURE_CASE_COMPLETE",
    "MEASURE_CASE_AUDIT",
    "MEASURE_CASE_PUBLIC_INPUT",
    "MEASURE_CASE_PUBLIC_CAPACITY",
    "MEASURE_CASE_PUBLIC_PROTOCOL",
    "MEASURE_CASE_PUBLIC_COUNTER",
    "MEASURE_CASE_PUBLIC_TIMEOUT",
    "MEASURE_CASE_PUBLIC_CHILD",
    "MEASURE_LEDGER_AUDIT",
    "MEASURE_EXPORT_AUDIT",
})

ALLOWED_DETAIL_CODES = acquire.ALLOWED_DETAIL_CODES | FIXTURE_DIAG_CODES


def require(value, code=ERROR):
    if not value:
        raise ValueError(code)


def _canonical(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def _uint(value, maximum) -> bool:
    return type(value) is int and 0 <= value <= maximum


def _private_dir(path: Path) -> None:
    require(isinstance(path, Path) and path.is_absolute() and path.is_dir() and
            not path.is_symlink() and path.resolve(strict=True) == path)
    info = path.stat()
    require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700)


def _empty_private(path: Path) -> None:
    _private_dir(path)
    require(not any(path.iterdir()))


def _disjoint(paths) -> bool:
    return all(left != right and left not in right.parents and
               right not in left.parents
               for idx, left in enumerate(paths)
               for right in paths[idx + 1:])


def _write_private(path: Path, data: bytes, *, mode: int = 0o600) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), mode)
    with os.fdopen(fd, "wb") as stream:
        os.fchmod(stream.fileno(), mode)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _read_private(path: Path, maximum: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                info.st_uid == os.geteuid() and
                0 < info.st_size <= maximum)
        raw = stream.read(maximum + 1)
        require(len(raw) == info.st_size)
        return raw


def _stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
    else:
        process.wait()


def directory_digest(root: Path) -> str:
    """Compute a deterministic SHA-256 over an immutable fixture directory."""
    require(isinstance(root, Path) and root.is_absolute() and root.is_dir() and
            not root.is_symlink())
    hasher = hashlib.sha256()
    entries = []
    for path in sorted(root.rglob("*"), key=lambda p: p.relative_to(root).as_posix()):
        info = path.lstat()
        require(not stat.S_ISLNK(info.st_mode) and info.st_uid == os.geteuid())
        rel = path.relative_to(root).as_posix()
        if stat.S_ISDIR(info.st_mode):
            entries.append(("D", rel, 0, ""))
        else:
            require(stat.S_ISREG(info.st_mode))
            fh = hashlib.sha256()
            with path.open("rb") as stream:
                while True:
                    chunk = stream.read(1_048_576)
                    if not chunk:
                        break
                    fh.update(chunk)
            entries.append(("F", rel, info.st_size, fh.hexdigest()))
    require(bool(entries))
    for kind, rel, size, digest in entries:
        hasher.update(f"{kind}\0{rel}\0{size}\0{digest}\n".encode("utf-8"))
    return hasher.hexdigest()


def _runtime_env(parser_dir: Path) -> dict:
    env = dict(os.environ, ALOPEX_MANUAL_681="1",
               RAYON_NUM_THREADS="2", OMP_NUM_THREADS="2")
    existing = env.get("LD_LIBRARY_PATH", "").strip()
    env["LD_LIBRARY_PATH"] = (
        f"{parser_dir}:{existing}" if existing else str(parser_dir)
    )
    return env


def _read_json_event(selector, process, deadline: float):
    buf = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        require(remaining > 0)
        require(selector.select(remaining))
        chunk = os.read(process.stdout.fileno(), 4096)
        require(chunk)
        buf.extend(chunk)
        require(len(buf) <= case_mod.MAX_EVENT_BYTES)
        if buf.endswith(b"\n"):
            value = json.loads(buf)
            require(type(value) is dict and set(value) == {"phase", "data"} and
                    type(value["data"]) is dict)
            return value


def _smoke_measure(binary: Path, db_dir: Path, parser_dir: Path, finish: str,
                   *, timeout: int = 30) -> None:
    process = subprocess.Popen(
        [str(binary), "measure", str(db_dir), "2", "4", finish],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, env=_runtime_env(parser_dir),
        start_new_session=True)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout
    phases = (*SUT_PHASES, finish)
    plan = tuple((phase, kind) for phase in phases
                 for kind in ("start", "end")) + (("complete", None),)
    try:
        for phase, kind in plan:
            ev = _read_json_event(selector, process, deadline)
            require(ev["phase"] == phase)
            data = ev["data"]
            if phase == "complete":
                require(data.get("finish") == finish and
                        data.get("result_validated") is True)
            else:
                require(data.get("event") == kind)
            process.stdin.write(b"continue\n")
            process.stdin.flush()
        remaining = deadline - time.monotonic()
        require(remaining > 0 and process.wait(timeout=remaining) == 0)
        require(process.stdout.read(1) == b"")
    finally:
        selector.close()
        _stop_process(process)
        process.stdin.close()
        process.stdout.close()


def smoke_fixture_binary(binary: Path, parser_dir: Path, work_dir: Path,
                         *, diag=None) -> None:
    """Verify prepare once + commit/rollback measure protocols on a 2x4 fixture."""
    smoke_root = Path(tempfile.mkdtemp(prefix="smoke-", dir=work_dir))
    os.chmod(smoke_root, 0o700)
    try:
        if isinstance(diag, dict):
            diag["code"] = "BUILD_SMOKE_PREPARE"
        fixture_dir = smoke_root / "fixture"
        prep = subprocess.run(
            [str(binary), "prepare", str(fixture_dir), "2", "4"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, env=_runtime_env(parser_dir),
            timeout=60, check=False)
        require(prep.returncode == 0 and 0 < len(prep.stdout) <= 4096)
        payload = json.loads(prep.stdout.decode("utf-8"))
        require(type(payload) is dict and payload.get("event") == "prepared" and
                payload.get("rows") == 2 and payload.get("dimensions") == 4 and
                _uint(payload.get("seed_elapsed_ns"), 3_600_000_000_000) and
                _uint(payload.get("index_build_elapsed_ns"), 3_600_000_000_000))
        for mode in ("commit", "rollback"):
            if isinstance(diag, dict):
                diag["code"] = f"BUILD_SMOKE_{mode.upper()}"
            copy_dir = smoke_root / f"trial-{mode}"
            shutil.copytree(fixture_dir, copy_dir)
            _smoke_measure(binary, copy_dir, parser_dir, mode)
            shutil.rmtree(copy_dir)
    finally:
        shutil.rmtree(smoke_root, ignore_errors=True)


def _check_elf_amd64(path: Path) -> None:
    header = _read_private(path, 268_435_456)[:20]
    require(len(header) == 20 and header[:4] == b"\x7fELF" and
            header[4:6] == b"\x02\x01" and header[18:20] == b"\x3e\x00")


def _stage_bench_crate(bench_crate: Path, arm_source: Path,
                       bench_src: Path) -> None:
    (bench_crate / "src").mkdir(mode=0o700, parents=True)
    os.chmod(bench_crate, 0o700)
    os.chmod(bench_crate / "src", 0o700)
    shutil.copyfile(bench_src, bench_crate / "src" / "main.rs")
    shutil.copyfile(arm_source / "Cargo.lock", bench_crate / "Cargo.lock")
    cargo_toml = (
        '[package]\n'
        'name = "issue681-fixture-bench"\n'
        'version = "0.1.0"\n'
        'edition = "2021"\n'
        'publish = false\n\n'
        '[workspace]\n\n'
        '[dependencies]\n'
        f'alopex-core = {{ path = {json.dumps(str(arm_source / "crates/alopex-core"))} }}\n'
        f'alopex-embedded = {{ path = {json.dumps(str(arm_source / "crates/alopex-embedded"))} }}\n'
        f'alopex-sql = {{ path = {json.dumps(str(arm_source / "crates/alopex-sql"))} }}\n'
        'serde_json = "1"\n'
    )
    (bench_crate / "Cargo.toml").write_text(cargo_toml, encoding="utf-8")


def _build_arm_binary(arm_name: str, arm_source: Path, arm_pin: dict,
                      bench_src: Path, bench_sha256: str,
                      target_dir: Path, work_dir: Path, staging_dir: Path,
                      parser: dict, cargo_home: Path, vendor: dict,
                      git_binary: Path, git_env: dict, host_guard,
                      private_roots: tuple, *, diag=None) -> tuple[dict, Path]:
    def mark(code):
        if isinstance(diag, dict):
            diag["code"] = code

    mark("BUILD_RUSTC")
    compiler_env = dict(os.environ, RUSTUP_TOOLCHAIN=RUST_VERSION,
                        RUSTUP_AUTO_INSTALL="0")
    compiler = producer._run(["rustc", "-Vv"], env=compiler_env)
    require("release: " + RUST_VERSION in compiler.splitlines() and
            "host: " + producer.HOST in compiler.splitlines())

    mark("BUILD_SOURCE_PRE")
    producer.verify_source(
        arm_source, arm_pin["commit"], arm_pin["tree"],
        arm_pin["example"], arm_pin["lock"],
        git_binary=git_binary, git_env=git_env)

    mark("BUILD_PARSER")
    parser_record = parser["parser_record"]
    parser_root = parser["parser_root"]
    parser_library = parser["parser_library"]
    require(producer.verify_parser(parser_record, parser_root, parser_library) ==
            parser["parser_record_sha256"] and
            producer.digest(parser_library) == parser["parser_sha256"])

    mark("BUILD_VENDOR_PRE")
    producer.verify_cargo_home(cargo_home)
    vendor_tree.verify_tree(
        cargo_home / "vendor", host_guard, vendor["vendor_mode_sha256"],
        expected_tree_sha256=vendor["vendor_tree_sha256"])

    bench_crate = work_dir / "bench-crate"
    _stage_bench_crate(bench_crate, arm_source, bench_src)

    remap_pairs = (
        (Path.home(), "home"),
        (cargo_home, "cargo"),
        (arm_source, "control"),
        (bench_crate, "bench"),
        (work_dir, "work"),
        (target_dir, "target"),
        (parser_library.parent, "parser"),
    )
    rustflags = " ".join(
        f"--remap-path-prefix={path}=/alopex-build/{label}"
        for path, label in remap_pairs)
    cflags = " ".join(
        f"-ffile-prefix-map={path}=/alopex-build/{label}"
        for path, label in remap_pairs)
    existing_cflags = os.environ.get("CFLAGS", "").strip()
    if existing_cflags:
        cflags = f"{existing_cflags} {cflags}"

    build_env = dict(
        os.environ,
        CARGO_HOME=str(cargo_home),
        CARGO_INCREMENTAL="0",
        CARGO_NET_OFFLINE="true",
        CARGO_PROFILE_RELEASE_DEBUG="0",
        CARGO_PROFILE_RELEASE_STRIP="symbols",
        CARGO_BUILD_JOBS="2",
        CARGO_TARGET_DIR=str(target_dir),
        RUSTUP_TOOLCHAIN=RUST_VERSION,
        NIM_SQL_PARSER_LIB_DIR=str(parser_library.parent),
        ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD="1",
        RUSTFLAGS=rustflags,
        CFLAGS=cflags,
    )
    build_env.pop("CARGO_ENCODED_RUSTFLAGS", None)

    mark("BUILD_CARGO_CAPACITY")
    host_guard()
    build_log = work_dir / "cargo-build.private.log"
    with build_log.open("xb") as output:
        os.chmod(build_log, 0o600)
        mark("BUILD_CARGO_EXIT")
        process = subprocess.Popen(
            ["cargo", "build", "--offline", "--release",
             "--manifest-path", str(bench_crate / "Cargo.toml")],
            cwd=bench_crate, env=build_env, stdin=subprocess.DEVNULL,
            stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + BUILD_LIMIT_SECONDS
        try:
            while process.poll() is None:
                if time.monotonic() >= deadline:
                    mark("BUILD_CARGO_TIMEOUT")
                    require(False)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    pass
                mark("BUILD_CARGO_CAPACITY")
                host_guard()
            mark("BUILD_CARGO_EXIT")
            require(process.returncode == 0)
            mark("BUILD_CARGO_CAPACITY")
            host_guard()
        except BaseException:
            _stop_process(process)
            raise

    mark("BUILD_VENDOR_POST")
    vendor_tree.verify_tree(
        cargo_home / "vendor", host_guard, vendor["vendor_mode_sha256"],
        expected_tree_sha256=vendor["vendor_tree_sha256"])

    mark("BUILD_SOURCE_POST")
    producer.verify_source(
        arm_source, arm_pin["commit"], arm_pin["tree"],
        arm_pin["example"], arm_pin["lock"],
        git_binary=git_binary, git_env=git_env)

    mark("BUILD_BINARY_MISSING")
    built_binary = target_dir / "release" / BINARY_NAME
    require(built_binary.is_file() and not built_binary.is_symlink() and
            built_binary.stat().st_uid == os.geteuid())
    raw_binary = public_assets._read_file(
        built_binary, 268_435_456, allow_hardlinks=True)
    staged_arm = staging_dir / arm_name
    staged_arm.mkdir(mode=0o700)
    staged_bin = staged_arm / BINARY_NAME
    _write_private(staged_bin, raw_binary, mode=0o700)
    binary_sha256 = hashlib.sha256(raw_binary).hexdigest()

    mark("BUILD_ELF_CHECK")
    _check_elf_amd64(staged_bin)

    mark("BUILD_PREFIX_LEAK")
    forbidden = (
        *public_assets.FIXED_PRIVATE_PREFIXES,
        *(value.encode("utf-8") for value in private_roots),
        str(arm_source).encode("utf-8"),
        str(bench_crate).encode("utf-8"),
        str(work_dir).encode("utf-8"),
        str(target_dir).encode("utf-8"),
        str(cargo_home).encode("utf-8"),
        str(Path.home()).encode("utf-8"),
    )
    require(not any(prefix in raw_binary for prefix in forbidden))

    smoke_fixture_binary(staged_bin, parser_library.parent, work_dir, diag=diag)

    pin = {
        "source_commit": arm_pin["commit"],
        "source_tree": arm_pin["tree"],
        "binary_sha256": binary_sha256,
        "bench_sha256": bench_sha256,
        "parser_sha256": parser["parser_sha256"],
        "parser_record_sha256": parser["parser_record_sha256"],
        "cargo_lock_sha256": arm_pin["lock"],
        "profile": "release",
        "platform": "linux/amd64",
        "parser_contract": "0.28.0",
        "rust_version": RUST_VERSION,
        "version": "0.8.17",
    }
    validate_pin(pin)
    return pin, staged_bin


def build_fixture_pair(acquired: dict, workspace: Path, build_parent: Path,
                       *, on_substage=None) -> dict:
    """Build unmodified before/candidate arms with the pinned fixture bench."""
    owned = None
    substage = "admit"
    substage_started = time.monotonic()
    diag = {}

    def emit(name, status, detail=None):
        if on_substage is not None:
            elapsed_s = max(0, int(time.monotonic() - substage_started))
            on_substage(name, status, detail, elapsed_s)

    try:
        diag["code"] = "BUILD_ADMIT"
        require(sys.platform == "linux" and platform.machine() == "x86_64")
        _empty_private(build_parent)
        bench_src = (workspace / BENCH_REL).resolve(strict=True)
        require(bench_src.is_file() and not bench_src.is_symlink() and
                bench_src.is_relative_to(workspace))
        bench_sha256 = hashlib.sha256(bench_src.read_bytes()).hexdigest()
        guard = acquired["guard"]
        guard()
        owned = Path(tempfile.mkdtemp(prefix="issue681-fixbuild-", dir=build_parent))
        os.chmod(owned, 0o700)
        for name in ("target", "before-work", "candidate-work", "staging"):
            (owned / name).mkdir(mode=0o700)
        private_roots = (
            str(acquired["task"]),
            str(acquired["cargo_home"]),
            str(owned),
        )
        pins = {}
        binaries = {}
        for arm in ARMS:
            substage = arm
            substage_started = time.monotonic()
            diag.clear()
            pin, binary = _build_arm_binary(
                arm,
                acquired["sources"][arm],
                acquire_source.PINS[arm],
                bench_src,
                bench_sha256,
                owned / "target",
                owned / f"{arm}-work",
                owned / "staging",
                acquired["parser"],
                acquired["cargo_home"],
                acquired["vendor"],
                acquired["git_binary"],
                acquired["git_env"],
                guard,
                private_roots,
                diag=diag,
            )
            pins[arm] = pin
            binaries[arm] = binary
            emit(arm, "passed")
        substage = "pair"
        substage_started = time.monotonic()
        validate_pins(pins)
        guard()
        emit("pair", "passed")
        return {
            "pins": pins,
            "binaries": binaries,
            "build_task": owned,
            "parser_dir": acquired["parser"]["parser_library"].parent,
            "guard": guard,
        }
    except BaseException:
        detail = diag.get("code")
        if not (isinstance(detail, str) and detail in FIXTURE_DIAG_CODES):
            detail = "PUBLIC_BUILD"
        emit(substage, "failed", detail)
        if owned is not None:
            shutil.rmtree(owned, ignore_errors=True)
        raise ValueError("PUBLIC_BUILD") from None


def validate_pin(pin: dict) -> None:
    require(type(pin) is dict and set(pin) == PIN_FIELDS)
    require(
        all(type(pin[k]) is str and re.fullmatch(r"[0-9a-f]{40}", pin[k])
            for k in ("source_commit", "source_tree")) and
        all(type(pin[k]) is str and re.fullmatch(r"[0-9a-f]{64}", pin[k])
            for k in ("binary_sha256", "bench_sha256", "parser_sha256",
                      "parser_record_sha256", "cargo_lock_sha256")) and
        pin["profile"] == "release" and
        pin["platform"] == "linux/amd64" and
        pin["parser_contract"] == "0.28.0" and
        pin["rust_version"] == RUST_VERSION and
        pin["version"] == "0.8.17"
    )


def validate_pins(pins: dict) -> None:
    require(type(pins) is dict and set(pins) == set(ARMS))
    for arm in ARMS:
        validate_pin(pins[arm])
        expected = acquire_source.PINS[arm]
        require(pins[arm]["source_commit"] == expected["commit"] and
                pins[arm]["source_tree"] == expected["tree"] and
                pins[arm]["cargo_lock_sha256"] == expected["lock"])
    require(pins["before"]["source_commit"] != pins["candidate"]["source_commit"] and
            pins["before"]["binary_sha256"] != pins["candidate"]["binary_sha256"])
    for key in ("bench_sha256", "parser_sha256", "parser_record_sha256",
                "profile", "platform", "parser_contract", "rust_version",
                "version"):
        require(pins["before"][key] == pins["candidate"][key])


def validate_fixture_record(item: dict, expected_rows: int, before_commit: str) -> None:
    require(
        type(item) is dict and set(item) == FIXTURE_FIELDS and
        item["rows"] == expected_rows and item["dimensions"] == 384 and
        item["builder_arm"] == "before" and
        item["builder_commit"] == before_commit and
        _uint(item["seed_elapsed_ns"], 3_600_000_000_000) and
        item["seed_elapsed_ns"] > 0 and
        _uint(item["index_build_elapsed_ns"], 3_600_000_000_000) and
        item["index_build_elapsed_ns"] > 0 and
        type(item["fixture_sha256"]) is str and
        re.fullmatch(r"[0-9a-f]{64}", item["fixture_sha256"]) is not None
    )


def _validate_phase_row(row: dict, expected: str) -> None:
    require(type(row) is dict and set(row) == PHASE_FIELDS and
            row["phase"] == expected and
            _uint(row["kernel_peak_rss_bytes"], 8_000_000_000))
    elapsed = row["elapsed_ns"]
    require((expected == "warm" and elapsed is None) or
            (expected != "warm" and
             _uint(elapsed, 3_600_000_000_000) and elapsed > 0))
    has_stats = expected in ("cold_transaction_1", "cold_transaction_2", "warm",
                             "first_insert", "second_insert")
    require((has_stats and _uint(row["cache_hits"], 1_000_000_000) and
             _uint(row["cache_misses"], 1_000_000_000)) or
            (not has_stats and row["cache_hits"] is None and
             row["cache_misses"] is None))
    require((expected == "warm" and _uint(row["graph_bytes"], 8_000_000_000) and
             row["graph_bytes"] > 0) or
            (expected != "warm" and row["graph_bytes"] is None))


def _validate_case_report(report: dict, arm: str, rows: int, trial: int,
                          pins: dict, finish: str, fixture_sha256: str) -> None:
    require(
        type(report) is dict and set(report) == CASE_FIELDS and
        report["schema"] == CASE_SCHEMA and report["status"] == "measured" and
        report["source_commit"] == pins[arm]["source_commit"] and
        report["binary_sha256"] == pins[arm]["binary_sha256"] and
        report["bench_sha256"] == pins[arm]["bench_sha256"] and
        report["fixture_sha256"] == fixture_sha256 and
        report["rows"] == rows and report["dimensions"] == 384 and
        report["trial"] == trial and report["finish"] == finish and
        report["result_validated"] is True and
        report["rss_scope"] == RSS_SCOPE and
        type(report["phases"]) is list and len(report["phases"]) == 7
    )
    for row, phase in zip(report["phases"], (*SUT_PHASES, finish)):
        _validate_phase_row(row, phase)


def case_order():
    for rows in SCALES:
        for trial in range(5):
            for arm in (ARMS if trial % 2 == 0 else reversed(ARMS)):
                yield rows, trial, arm


def prepare_fixture(binary: Path, fixture_dir: Path, parser_dir: Path,
                    rows: int, *, timeout: int, host_guard, diag=None) -> dict:
    """Build ONE immutable durable on-disk fixture for a scale."""
    if isinstance(diag, dict):
        diag["code"] = "MEASURE_FIXTURE_PREPARE"
    require(not fixture_dir.exists())
    host_guard()
    process = subprocess.Popen(
        [str(binary), "prepare", str(fixture_dir), str(rows), "384"],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, env=_runtime_env(parser_dir),
        start_new_session=True)
    deadline = time.monotonic() + timeout
    try:
        while process.poll() is None:
            if time.monotonic() >= deadline:
                if isinstance(diag, dict):
                    diag["code"] = "MEASURE_FIXTURE_TIMEOUT"
                require(False)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            host_guard()
        require(process.returncode == 0)
        raw = process.stdout.read(4097)
        require(0 < len(raw) <= 4096)
        payload = json.loads(raw.decode("utf-8"))
        require(type(payload) is dict and payload.get("event") == "prepared" and
                payload.get("rows") == rows and payload.get("dimensions") == 384 and
                _uint(payload.get("seed_elapsed_ns"), 3_600_000_000_000) and
                payload["seed_elapsed_ns"] > 0 and
                _uint(payload.get("index_build_elapsed_ns"), 3_600_000_000_000) and
                payload["index_build_elapsed_ns"] > 0)
        host_guard()
        digest = directory_digest(fixture_dir)
        return {
            "rows": rows,
            "dimensions": 384,
            "builder_arm": "before",
            "builder_commit": acquire_source.PINS["before"]["commit"],
            "seed_elapsed_ns": payload["seed_elapsed_ns"],
            "index_build_elapsed_ns": payload["index_build_elapsed_ns"],
            "fixture_sha256": digest,
        }
    finally:
        _stop_process(process)
        if process.stdout is not None:
            process.stdout.close()


def run_sut_case(*, binary: Path, db_dir: Path, parser_dir: Path,
                 public_dir: Path, private_dir: Path,
                 source_commit: str, binary_sha256: str, bench_sha256: str,
                 fixture_sha256: str, rows: int, trial: int, finish: str,
                 timeout: int, rss_limit: int, inventory_roots=(),
                 counter_factory=PhaseRss, host_guard=None) -> dict:
    """Run one bounded 7-phase SUT case against a fresh copy of the fixture."""
    _empty_private(public_dir)
    _empty_private(private_dir)
    require(_disjoint((public_dir, private_dir, db_dir)))
    records = []
    events = queue.Queue(maxsize=32)
    stop_reader = threading.Event()
    process = None
    observer = None
    reader = None
    audit_stream = None
    code = "PUBLIC_PROTOCOL"
    started = time.monotonic()
    last_inventory = started
    try:
        def checked_inventory():
            if host_guard is not None:
                try:
                    host_guard()
                except BaseException:
                    raise case_mod.PublicFailure("PUBLIC_CAPACITY") from None
            case_mod._inventory(public_dir, inventory_roots)

        case_mod.require(
            sys.platform == "linux" and rows in SCALES and
            _uint(trial, 4) and finish in ("commit", "rollback") and
            _uint(timeout, 1800) and timeout > 0 and
            _uint(rss_limit, 8_000_000_000) and rss_limit > 0,
            "PUBLIC_INPUT")
        case_mod._identity(binary, source_commit, binary_sha256)
        checked_inventory()
        fd = os.open(private_dir / "audit.jsonl", os.O_WRONLY | os.O_CREAT |
                     os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        audit_stream = os.fdopen(fd, "wb")
        phases = (*SUT_PHASES, finish)
        expected = tuple((phase, kind) for phase in phases
                         for kind in ("start", "end")) + (("complete", None),)
        process = subprocess.Popen(
            [str(binary), "measure", str(db_dir), str(rows), "384", finish],
            env=_runtime_env(parser_dir),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, start_new_session=True)
        try:
            observer = counter_factory(process.pid)
        except Exception:
            raise case_mod.PublicFailure("PUBLIC_COUNTER") from None

        def read_child():
            def deliver(value):
                while not stop_reader.is_set():
                    try:
                        events.put(value, timeout=0.1)
                        return
                    except queue.Full:
                        pass
            try:
                for line in iter(lambda: process.stdout.readline(
                        case_mod.MAX_EVENT_BYTES + 1), ""):
                    if stop_reader.is_set():
                        break
                    deliver(line)
            finally:
                deliver(None)

        reader = threading.Thread(target=read_child, daemon=True)
        reader.start()
        for sequence, (phase, kind) in enumerate(expected):
            while True:
                now = time.monotonic()
                if now - started > timeout:
                    raise case_mod.PublicFailure("PUBLIC_TIMEOUT")
                if now - last_inventory >= 30:
                    checked_inventory()
                    last_inventory = now
                try:
                    line = events.get(timeout=0.1)
                    break
                except queue.Empty:
                    if (process.poll() is not None and
                            not reader.is_alive() and events.empty()):
                        raise case_mod.PublicFailure("PUBLIC_CHILD")
            case_mod.require(line is not None, "PUBLIC_CHILD")
            data = case_mod._event(line, phase, kind)
            observed = None
            if phase == "complete":
                case_mod.require(observer.active is None and
                                 data.get("finish") == finish)
            elif kind == "start":
                try:
                    observer.begin(phase)
                except Exception:
                    raise case_mod.PublicFailure("PUBLIC_COUNTER") from None
            else:
                try:
                    observed = observer.finish(phase)
                except Exception:
                    raise case_mod.PublicFailure("PUBLIC_COUNTER") from None
                row = case_mod._record(phase, data, observed)
                case_mod.require(row["kernel_peak_rss_bytes"] < rss_limit,
                                 "PUBLIC_COUNTER")
                records.append(row)
            audit_stream.write(_canonical({
                "sequence": sequence,
                "event": json.loads(line),
                "observation": observed,
            }))
            audit_stream.flush()
            process.stdin.write("continue\n")
            process.stdin.flush()
        case_mod.require(process.wait(timeout=POST_FINISH_SECONDS) == 0 and
                         events.get(timeout=1) is None, "PUBLIC_CHILD")
        reader.join(timeout=2)
        case_mod.require(len(records) == len(phases) and
                         not reader.is_alive() and observer.active is None)
        checked_inventory()
        report = {
            "schema": CASE_SCHEMA,
            "status": "measured",
            "source_commit": source_commit,
            "binary_sha256": binary_sha256,
            "bench_sha256": bench_sha256,
            "fixture_sha256": fixture_sha256,
            "rows": rows,
            "dimensions": 384,
            "trial": trial,
            "finish": finish,
            "phases": records,
            "result_validated": True,
            "rss_scope": RSS_SCOPE,
        }
        report_raw = _canonical(report)
        audit_stream.write(_canonical({"receipt": {
            "schema": AUDIT_SCHEMA,
            "source_commit": source_commit,
            "binary_sha256": binary_sha256,
            "bench_sha256": bench_sha256,
            "fixture_sha256": fixture_sha256,
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "rows": rows,
            "dimensions": 384,
            "trial": trial,
            "finish": finish,
            "child_exit": 0,
            "events": len(expected),
            "phases": len(records),
            "public_sha256": hashlib.sha256(report_raw).hexdigest(),
        }}))
        audit_stream.flush()
        os.fsync(audit_stream.fileno())
        _write_private(public_dir / "public.json", report_raw)
        return report
    except case_mod.PublicFailure as error:
        code = error.code
    except Exception:
        code = "PUBLIC_CHILD"
    finally:
        if audit_stream is not None:
            audit_stream.close()
        if process is not None:
            _stop_process(process)
            process.stdin.close()
            process.stdout.close()
        if observer is not None:
            observer.close()
        if reader is not None:
            stop_reader.set()
            reader.join(timeout=2)
    failed = {
        "schema": CASE_SCHEMA,
        "status": "failed",
        "error_code": code,
        "completed_phases": len(records),
    }
    _write_private(public_dir / "public.json", _canonical(failed))
    return failed


def _audit_observation(value: dict, phase: str) -> int:
    require(
        type(value) is dict and set(value) == OBSERVATION_FIELDS and
        value["phase"] == phase and
        value["aggregation"] == "maximum of reset and end kernel high-water readings" and
        value["scope"] == "child process between phase handshakes, including end-event serialization" and
        value["counter"] == "Linux VmHWM after clear_refs=5; kernel RSS accounting accuracy applies"
    )
    for name in ("before_reset", "after_reset", "after"):
        snap = value[name]
        require(type(snap) is dict and set(snap) == SNAPSHOT_FIELDS and
                all(_uint(snap[k], 8_000_000_000) for k in SNAPSHOT_FIELDS) and
                snap["high_water_bytes"] >= snap["rss_bytes"])
    require(value["kernel_reported_peak_rss_bytes"] == max(
        value["after_reset"]["high_water_bytes"],
        value["after"]["high_water_bytes"]))
    return value["kernel_reported_peak_rss_bytes"]


def audit_sut_case(public_dir: Path, private_dir: Path, binary: Path,
                   pin: dict, rows: int, trial: int, finish: str,
                   fixture_sha256: str) -> dict:
    """Independently rejoin private audit.jsonl to public.json for one SUT case."""
    validate_pin(pin)
    require(rows in SCALES and trial in range(5) and
            finish in ("commit", "rollback") and
            binary.is_file() and not binary.is_symlink() and
            hashlib.sha256(binary.read_bytes()).hexdigest() == pin["binary_sha256"])
    _private_dir(public_dir)
    _private_dir(private_dir)
    require(set(public_dir.iterdir()) == {public_dir / "public.json"})
    require(set(private_dir.iterdir()) == {private_dir / "audit.jsonl"})
    public_raw = _read_private(public_dir / "public.json", 16_384)
    audit_raw = _read_private(private_dir / "audit.jsonl", 200_000)
    report = json.loads(public_raw, object_pairs_hook=_unique)
    _validate_case_report(report, "arm", rows, trial, {"arm": pin},
                          finish, fixture_sha256)
    require(public_raw == _canonical(report))
    lines = audit_raw.splitlines(keepends=True)
    require(len(lines) == 16 and all(line.endswith(b"\n") for line in lines))
    records = [json.loads(line, object_pairs_hook=_unique) for line in lines]
    phases = (*SUT_PHASES, finish)
    expected = tuple((phase, kind) for phase in phases
                     for kind in ("start", "end")) + (("complete", None),)
    projected = []
    for seq, (rec, (phase, kind)) in enumerate(zip(records[:15], expected)):
        require(type(rec) is dict and
                set(rec) == {"sequence", "event", "observation"} and
                rec["sequence"] == seq)
        ev = rec["event"]
        require(type(ev) is dict and set(ev) == {"phase", "data"} and
                ev["phase"] == phase and type(ev["data"]) is dict)
        data = ev["data"]
        if phase == "complete":
            require(rec["observation"] is None and
                    data.get("finish") == finish and
                    data.get("result_validated") is True)
        elif kind == "start":
            require(data.get("event") == "start" and rec["observation"] is None)
        else:
            peak = _audit_observation(rec["observation"], phase)
            stats = data.get("stats")
            row = {
                "phase": phase,
                "elapsed_ns": data.get("elapsed_ns"),
                "kernel_peak_rss_bytes": peak,
                "cache_hits": None if stats is None else stats["hits"],
                "cache_misses": None if stats is None else stats["misses"],
                "graph_bytes": data.get("graph_bytes"),
            }
            _validate_phase_row(row, phase)
            projected.append(row)
    require(projected == report["phases"])
    receipt = records[15].get("receipt")
    require(
        type(records[15]) is dict and set(records[15]) == {"receipt"} and
        type(receipt) is dict and set(receipt) == RECEIPT_FIELDS and
        receipt["schema"] == AUDIT_SCHEMA and
        receipt["source_commit"] == pin["source_commit"] and
        receipt["binary_sha256"] == pin["binary_sha256"] and
        receipt["bench_sha256"] == pin["bench_sha256"] and
        receipt["fixture_sha256"] == fixture_sha256 and
        receipt["runner_sha256"] == hashlib.sha256(Path(__file__).read_bytes()).hexdigest() and
        receipt["rows"] == rows and receipt["dimensions"] == 384 and
        receipt["trial"] == trial and receipt["finish"] == finish and
        receipt["child_exit"] == 0 and receipt["events"] == 15 and
        receipt["phases"] == 7 and
        receipt["public_sha256"] == hashlib.sha256(public_raw).hexdigest()
    )
    return report


def verify_fixture_admission_ledger(directory: Path, finishes: tuple,
                                    additional_bytes: int) -> bool:
    """Verify exact admission.jsonl records across all scales and finishes."""
    _private_dir(directory)
    require(set(directory.iterdir()) == {directory / "admission.jsonl"} and
            type(additional_bytes) is int and
            0 < additional_bytes < case_mod.STOP_BYTES)
    raw = _read_private(directory / "admission.jsonl", MAX_ADMISSION_LEDGER_BYTES)
    lines = raw.splitlines(keepends=True)
    expected = [("preflight", None, None, None)]
    for rows in SCALES:
        expected.append(("stage", rows, None, None))
        for _finish in finishes:
            for trial in range(5):
                for arm in (ARMS if trial % 2 == 0 else reversed(ARMS)):
                    expected.append(("case", rows, trial, arm))
                    expected.append(("case_complete", rows, trial, arm))
    require(len(lines) == len(expected) and
            all(line.endswith(b"\n") for line in lines))
    for line, (kind, rows, trial, arm) in zip(lines, expected):
        value = json.loads(line, object_pairs_hook=_unique)
        require(
            type(value) is dict and
            set(value) == public_admission.LEDGER_FIELDS and
            value["schema"] == public_admission.LEDGER_SCHEMA and
            value["kind"] == kind and value["rows"] == rows and
            value["trial"] == trial and value["arm"] == arm and
            value["status"] == "passed" and
            ((kind == "case_complete" and
              value["idle_window_seconds"] == 0 and
              value["min_idle_basis_points"] is None) or
             (kind != "case_complete" and
              value["idle_window_seconds"] == public_admission.WINDOW_SECONDS and
              type(value["min_idle_basis_points"]) is int and
              public_admission.MIN_IDLE_BASIS_POINTS <=
              value["min_idle_basis_points"] <= 10_000)) and
            type(value["allocated_bytes"]) is int and
            0 <= value["allocated_bytes"] and
            value["allocated_bytes"] + additional_bytes <= case_mod.STOP_BYTES and
            line == _canonical(value)
        )
    return True


def _summarize_sut(cases: list, finish: str) -> list:
    by_key = {
        (row["report"]["rows"], row["report"]["trial"], row["arm"]): row["report"]
        for row in cases
    }
    summaries = []
    for rows in SCALES:
        for phase_idx, phase in enumerate((*SUT_PHASES, finish)):
            for metric in ("elapsed_ns", "kernel_peak_rss_bytes"):
                if metric == "elapsed_ns" and phase == "warm":
                    continue
                dists = {}
                medians = {}
                mads = {}
                for arm in ARMS:
                    vals = [by_key[(rows, trial, arm)]["phases"][phase_idx][metric]
                            for trial in range(5)]
                    med = statistics.median(vals)
                    dists[arm] = vals
                    medians[arm] = med
                    mads[arm] = statistics.median(abs(v - med) for v in vals)
                diff = medians["candidate"] - medians["before"]
                bound = 3 * max(mads.values())
                improvement_ratio = (
                    round((medians["before"] - medians["candidate"]) / medians["before"], 6)
                    if medians["before"] > 0 else 0.0
                )
                summaries.append({
                    "rows": rows,
                    "phase": phase,
                    "metric": metric,
                    "before": dists["before"],
                    "candidate": dists["candidate"],
                    "before_median": medians["before"],
                    "candidate_median": medians["candidate"],
                    "before_mad": mads["before"],
                    "candidate_mad": mads["candidate"],
                    "candidate_minus_before": diff,
                    "three_mad_bound": bound,
                    "increase_beyond_bound": diff > bound,
                    "improvement_ratio": improvement_ratio,
                })
    return summaries


def assemble_finish_report(reports: list, fixtures: list, pins: dict,
                           finish: str) -> dict:
    validate_pins(pins)
    require(finish in ("commit", "rollback"))
    require(type(fixtures) is list and len(fixtures) == len(SCALES))
    fixture_by_rows = {}
    for item, rows in zip(fixtures, SCALES):
        validate_fixture_record(item, rows, pins["before"]["source_commit"])
        fixture_by_rows[rows] = item["fixture_sha256"]
    require(type(reports) is list and len(reports) == 30)
    cases = []
    for rep, (rows, trial, arm) in zip(reports, case_order()):
        _validate_case_report(rep, arm, rows, trial, pins, finish,
                              fixture_by_rows[rows])
        cases.append({"arm": arm, "report": rep})
    result = {
        "schema": SCHEMA,
        "status": "measured",
        "boundary": BOUNDARY_TAG,
        "pins": pins,
        "finish": finish,
        "fixtures": fixtures,
        "cases": cases,
        "summary": _summarize_sut(cases, finish),
        "performance_acceptance": False,
    }
    require(len(_canonical(result)) <= MAX_PUBLIC_BYTES)
    return result


def validate_public_report(raw: bytes, *, expected_sha256: str,
                           expected_pins: dict, expected_fixtures: list,
                           finish: str) -> bytes:
    require(type(raw) is bytes and 0 < len(raw) <= MAX_PUBLIC_BYTES and
            hashlib.sha256(raw).hexdigest() == expected_sha256)
    value = json.loads(raw, object_pairs_hook=_unique)
    require(type(value) is dict and set(value) == REPORT_FIELDS and
            value["schema"] == SCHEMA and value["status"] == "measured" and
            value["boundary"] == BOUNDARY_TAG and
            value["pins"] == expected_pins and
            value["fixtures"] == expected_fixtures and
            value["finish"] == finish and
            value["performance_acceptance"] is False and
            type(value["cases"]) is list and len(value["cases"]) == 30)
    reports = []
    for row, (_, _, arm) in zip(value["cases"], case_order()):
        require(type(row) is dict and set(row) == {"arm", "report"} and
                row["arm"] == arm)
        reports.append(row["report"])
    expected = assemble_finish_report(reports, expected_fixtures,
                                      expected_pins, finish)
    require(value == expected and raw == _canonical(expected))
    return raw


def measure_fixture_finishes(acquired: dict, built: dict, workspace: Path,
                             measure_parent: Path, profile: dict,
                             finishes: tuple, *,
                             soft_budget_seconds: int = SOFT_BUDGET_SECONDS,
                             job_started: float | None = None,
                             on_substage=None, on_fixture=None, on_case=None,
                             case_runner=None,
                             fixture_preparer=None) -> dict:
    """Build each scale fixture ONCE, then run all SUT trials per finish."""
    if case_runner is None:
        case_runner = run_sut_case
    if fixture_preparer is None:
        fixture_preparer = prepare_fixture
    owned = None
    substage = "admit"
    substage_started = time.monotonic()
    started = time.monotonic() if job_started is None else job_started
    diag = {}

    def emit(name, status, detail=None):
        if on_substage is not None:
            elapsed_s = max(0, int(time.monotonic() - substage_started))
            on_substage(name, status, detail, elapsed_s)

    def check_budget():
        if time.monotonic() - started >= soft_budget_seconds:
            diag["code"] = "MEASURE_TIME_BUDGET"
            require(False)

    try:
        diag["code"] = "MEASURE_ADMIT"
        require(tuple(finishes) in (("commit",), ("rollback",),
                                    ("commit", "rollback")))
        _empty_private(measure_parent)
        guard = acquired["guard"]
        guard()
        owned = Path(tempfile.mkdtemp(prefix="issue681-fixmeasure-",
                                      dir=measure_parent))
        os.chmod(owned, 0o700)
        for name in ("fixtures", "work", "export", "admission"):
            (owned / name).mkdir(mode=0o700)
        fixtures_root = owned / "fixtures"
        work_root = owned / "work"
        export_root = owned / "export"
        admitted_dir = owned / "admission"
        roots = (workspace, acquired["task"], acquired["cargo_home"],
                 built["build_task"], owned)

        substage = "admission_gate"
        substage_started = time.monotonic()
        diag["code"] = "MEASURE_ADMISSION_GATE"
        total_cases = 30 * len(finishes)
        position = 0
        fixture_records = []
        reports_by_finish = {f: [] for f in finishes}
        locations_by_finish = {f: [] for f in finishes}

        with public_admission.Admission(profile, roots=roots,
                                        log_dir=admitted_dir) as gate:
            emit("admission_gate", "passed")
            guard()
            substage = "cases"
            substage_started = time.monotonic()
            for rows in SCALES:
                check_budget()
                fixture_dir = fixtures_root / f"fixture-{rows}"
                prep_started = time.monotonic()
                fixture_rec = fixture_preparer(
                    built["binaries"]["before"], fixture_dir,
                    built["parser_dir"], rows,
                    timeout=PREPARE_TIMEOUTS[rows], host_guard=guard, diag=diag)
                fixture_records.append(fixture_rec)
                if on_fixture is not None:
                    on_fixture(rows, max(0, int(time.monotonic() - prep_started)),
                               max(0, int(time.monotonic() - started)))
                guard()
                diag["code"] = "MEASURE_STAGE_ADMIT"
                require(gate.admit_stage(rows) is True)

                for finish in finishes:
                    for trial in range(5):
                        for arm in (ARMS if trial % 2 == 0 else reversed(ARMS)):
                            check_budget()
                            case_started = time.monotonic()
                            case_root = work_root / f"{finish}-{rows}-t{trial}-{arm}"
                            case_root.mkdir(mode=0o700)
                            pub = case_root / "public"
                            priv = case_root / "private"
                            pub.mkdir(mode=0o700)
                            priv.mkdir(mode=0o700)
                            trial_db = case_root / "db"
                            try:
                                diag["code"] = "MEASURE_CASE_COPY"
                                shutil.copytree(fixture_dir, trial_db)
                                guard()
                                diag["code"] = "MEASURE_CASE_ADMIT"
                                require(gate.admit_case(rows, trial, arm) is True)
                                diag["code"] = "MEASURE_CASE_RUN"
                                rep = case_runner(
                                    binary=built["binaries"][arm],
                                    db_dir=trial_db,
                                    parser_dir=built["parser_dir"],
                                    public_dir=pub,
                                    private_dir=priv,
                                    source_commit=built["pins"][arm]["source_commit"],
                                    binary_sha256=built["pins"][arm]["binary_sha256"],
                                    bench_sha256=built["pins"][arm]["bench_sha256"],
                                    fixture_sha256=fixture_rec["fixture_sha256"],
                                    rows=rows,
                                    trial=trial,
                                    finish=finish,
                                    timeout=MEASURE_TIMEOUTS[rows],
                                    rss_limit=RSS_LIMIT,
                                    inventory_roots=roots,
                                    host_guard=guard,
                                )
                                if not (type(rep) is dict and
                                        rep.get("status") == "measured"):
                                    err = rep.get("error_code") if type(rep) is dict else None
                                    if isinstance(err, str) and err in case_mod.ERRORS:
                                        diag["code"] = f"MEASURE_CASE_{err}"
                                    require(False)
                                diag["code"] = "MEASURE_CASE_COMPLETE"
                                require(gate.complete_case(rows, trial, arm) is True)
                            finally:
                                if trial_db.exists():
                                    shutil.rmtree(trial_db, ignore_errors=True)
                            diag["code"] = "MEASURE_CASE_AUDIT"
                            audited = audit_sut_case(
                                pub, priv, built["binaries"][arm],
                                built["pins"][arm], rows, trial, finish,
                                fixture_rec["fixture_sha256"])
                            require(audited == rep)
                            reports_by_finish[finish].append(rep)
                            locations_by_finish[finish].append({
                                "arm": arm, "rows": rows, "trial": trial,
                                "public": pub, "private": priv,
                            })
                            if on_case is not None:
                                on_case(position, total_cases, finish, rows,
                                        trial, arm,
                                        max(0, int(time.monotonic() - case_started)),
                                        max(0, int(time.monotonic() - started)))
                            position += 1
                            guard()

                diag["code"] = "MEASURE_FIXTURE_IMMUTABILITY"
                require(directory_digest(fixture_dir) ==
                        fixture_rec["fixture_sha256"])
                shutil.rmtree(fixture_dir)
                guard()
            emit("cases", "passed")

            substage = "verify_export"
            substage_started = time.monotonic()
            diag["code"] = "MEASURE_LEDGER_AUDIT"
            require(verify_fixture_admission_ledger(
                admitted_dir, finishes, profile["additional_bytes"]) is True)

            diag["code"] = "MEASURE_EXPORT_AUDIT"
            exports = {}
            for finish in finishes:
                report = assemble_finish_report(
                    reports_by_finish[finish], fixture_records,
                    built["pins"], finish)
                for pos, ((rows, trial, arm), loc) in enumerate(
                        zip(case_order(), locations_by_finish[finish])):
                    fsha = next(f["fixture_sha256"] for f in fixture_records
                                if f["rows"] == rows)
                    reconstructed = audit_sut_case(
                        loc["public"], loc["private"],
                        built["binaries"][arm], built["pins"][arm],
                        rows, trial, finish, fsha)
                    require(reconstructed == report["cases"][pos]["report"])
                raw = _canonical(report)
                digest = hashlib.sha256(raw).hexdigest()
                validate_public_report(
                    raw, expected_sha256=digest,
                    expected_pins=built["pins"],
                    expected_fixtures=fixture_records, finish=finish)
                out_path = export_root / f"issue681-public-{finish}.json"
                _write_private(out_path, raw)
                exports[finish] = {
                    "path": out_path,
                    "bytes": raw,
                    "sha256": digest,
                }
            emit("verify_export", "passed")
            guard()
        return {
            "exports": exports,
            "fixtures": fixture_records,
            "pins": copy.deepcopy(built["pins"]),
            "measure_task": owned,
            "guard": guard,
        }
    except BaseException:
        detail = diag.get("code")
        if not (isinstance(detail, str) and detail in FIXTURE_DIAG_CODES):
            detail = "PUBLIC_STAGE"
        emit(substage, "failed", detail)
        if owned is not None:
            shutil.rmtree(owned, ignore_errors=True)
        raise ValueError("PUBLIC_STAGE") from None


def execute_hosted_fixture_measure(env: dict, workspace: Path,
                                   task_parent: Path, home_parent: Path,
                                   profile: dict, finishes: tuple, *,
                                   soft_budget_seconds: int = SOFT_BUDGET_SECONDS,
                                   acquire_budget_seconds: int = ACQUIRE_BUDGET_SECONDS,
                                   on_stage=None, on_substage=None,
                                   on_fixture=None, on_case=None) -> dict:
    """Execute one admitted GitHub-hosted #681 precondition-vs-SUT run."""
    owned = []
    stage = "init"
    started = time.monotonic()

    def notify(name):
        if on_stage is not None:
            on_stage(name)

    def check_budget():
        require(time.monotonic() - started < soft_budget_seconds)

    try:
        require(type(env) is dict and isinstance(workspace, Path) and
                workspace == qualify.REPO and
                _disjoint((workspace, task_parent, home_parent)) and
                Path.home() in home_parent.parents and
                tuple(finishes) in (("commit",), ("rollback",),
                                    ("commit", "rollback")))
        _empty_private(task_parent)
        _empty_private(home_parent)
        manifest = scoped_storage.make_manifest(
            workspace, Path(env.get("RUNNER_TEMP", str(task_parent))),
            task_parent=task_parent, home_parent=home_parent)
        task = Path(tempfile.mkdtemp(prefix="issue681-job-", dir=task_parent))
        os.chmod(task, 0o700)
        owned.append(task)
        home = Path(tempfile.mkdtemp(prefix="issue681-home-", dir=home_parent))
        os.chmod(home, 0o700)
        owned.append(home)
        for name in ("acquire", "build", "measure"):
            (task / name).mkdir(mode=0o700)

        stage = "toolchain_install"
        check_budget()
        install_guard = capacity.HostStorageGuard(workspace, manifest=manifest)
        rust.install_pinned_minimal(
            install_guard, task / "rustup-install.private.log")
        notify("toolchain_install")

        stage = "acquire"
        check_budget()
        acquire_kwargs = {
            "manifest": manifest,
            "vendor_mode": "cargo_vendor_locked",
        }
        if on_substage is not None:
            acquire_kwargs["on_substage"] = on_substage
            acquire_kwargs["stage_budget_seconds"] = acquire_budget_seconds
        acquired = acquire.acquire_all(
            workspace, task / "acquire", home, **acquire_kwargs)
        notify("acquire")

        stage = "build"
        check_budget()
        built = build_fixture_pair(
            acquired, workspace, task / "build", on_substage=on_substage)
        notify("build")

        stage = "measure"
        check_budget()
        measured = measure_fixture_finishes(
            acquired, built, workspace, task / "measure", profile, finishes,
            soft_budget_seconds=soft_budget_seconds, job_started=started,
            on_substage=on_substage, on_fixture=on_fixture, on_case=on_case)
        notify("measure")

        stage = "audit"
        for finish in finishes:
            item = measured["exports"][finish]
            raw = _read_private(item["path"], MAX_PUBLIC_BYTES)
            require(raw == item["bytes"])
            validate_public_report(
                raw, expected_sha256=item["sha256"],
                expected_pins=measured["pins"],
                expected_fixtures=measured["fixtures"], finish=finish)
        acquired["guard"]()
        notify("audit")
        return {
            "case": CASE,
            "outcome": "passed",
            "finishes": list(finishes),
            "exports": measured["exports"],
            "pins": measured["pins"],
            "fixtures": measured["fixtures"],
        }
    except BaseException:
        for path in reversed(owned):
            shutil.rmtree(path, ignore_errors=True)
        raise ValueError(f"{ERROR}:{stage}") from None


def run_hosted_fixture_cli(env=None) -> int:
    """CLI entry point for alopex-performance.yml hnsw_release_measure."""
    env = dict(os.environ if env is None else env)
    stages = {
        "admission": "skipped_prereq",
        "toolchain_install": "skipped_prereq",
        "acquire": "skipped_prereq",
        "build": "skipped_prereq",
        "measure": "skipped_prereq",
        "audit": "skipped_prereq",
        "export": "skipped_prereq",
    }
    finish_arg = env.get("HNSW_FINISH", "both")
    if finish_arg == "both":
        finishes = ("commit", "rollback")
    elif finish_arg in ("commit", "rollback"):
        finishes = (finish_arg,)
    else:
        print(json.dumps({"case": CASE, "outcome": "failed",
                          "error_code": "INVALID_FINISH", "stages": stages},
                         sort_keys=True), flush=True)
        return 1

    admission_outcome, admission_status = identity_gate.run(
        env, expected_job="hnsw_release_measure")
    print(json.dumps(admission_outcome, sort_keys=True), flush=True)
    if admission_status != 0 or admission_outcome.get("outcome") != "passed":
        stages["admission"] = "failed"
        print(json.dumps({"case": CASE, "outcome": "failed",
                          "error_code": "ADMISSION", "stages": stages},
                         sort_keys=True), flush=True)
        return 1
    stages["admission"] = "passed"

    runner_temp = Path(env["RUNNER_TEMP"]).resolve(strict=True)
    workspace = Path(env["GITHUB_WORKSPACE"]).resolve(strict=True)
    home_root = Path.home().resolve(strict=True)
    task_parent = Path(tempfile.mkdtemp(prefix="alopex-681-task-", dir=runner_temp))
    os.chmod(task_parent, 0o700)
    home_parent = Path(tempfile.mkdtemp(prefix=".alopex-681-home-", dir=home_root))
    os.chmod(home_parent, 0o700)

    substages = {"acquire": {}, "build": {}, "measure": {}}
    active_stage = {"name": "acquire"}
    failure_detail = {"substage": None, "detail_code": None}
    completed_cases = {"count": 0}

    def on_stage(name):
        stages[name] = "passed"
        if name == "toolchain_install":
            active_stage["name"] = "acquire"
        elif name == "acquire":
            active_stage["name"] = "build"
        elif name == "build":
            active_stage["name"] = "measure"
        print(json.dumps({"case": CASE, "stage": name, "status": "passed"},
                         sort_keys=True), flush=True)

    def on_substage(name, status, detail_code, elapsed_s):
        st = active_stage["name"]
        substages[st][name] = status
        payload = {
            "case": CASE,
            "stage": st,
            "substage": name,
            "status": status,
            "elapsed_s": elapsed_s,
        }
        if status == "failed":
            safe = (detail_code if detail_code in ALLOWED_DETAIL_CODES
                    else "PUBLIC_STAGE")
            payload["detail_code"] = safe
            failure_detail["substage"] = name
            failure_detail["detail_code"] = safe
        print(json.dumps(payload, sort_keys=True), flush=True)

    def on_fixture(rows, elapsed_s, job_elapsed_s):
        print(json.dumps({
            "case": CASE,
            "stage": "measure",
            "substage": "fixture_prepare",
            "rows": rows,
            "status": "passed",
            "elapsed_s": elapsed_s,
            "job_elapsed_s": job_elapsed_s,
        }, sort_keys=True), flush=True)

    def on_case(position, total_cases, finish, rows, trial, arm,
                elapsed_s, job_elapsed_s):
        completed_cases["count"] += 1
        print(json.dumps({
            "case": CASE,
            "stage": "measure",
            "substage": "case",
            "finish": finish,
            "position": position,
            "total_cases": total_cases,
            "rows": rows,
            "trial": trial,
            "arm": arm,
            "status": "passed",
            "elapsed_s": elapsed_s,
            "job_elapsed_s": job_elapsed_s,
        }, sort_keys=True), flush=True)

    profile = {
        "schema": public_admission.SCHEMA,
        "runner_kind": env.get("RUNNER_KIND", HOSTED_RUNNER_KIND),
        "cpu_count": 4,
        "additional_bytes": qualify.RESERVE_BYTES,
    }
    try:
        result = execute_hosted_fixture_measure(
            env, workspace, task_parent, home_parent, profile, finishes,
            on_stage=on_stage, on_substage=on_substage,
            on_fixture=on_fixture, on_case=on_case)
        export_dir = runner_temp / "issue681-public-export"
        if export_dir.exists():
            shutil.rmtree(export_dir)
        export_dir.mkdir(mode=0o700)
        os.chmod(export_dir, 0o700)
        public_digests = {}
        for finish in finishes:
            item = result["exports"][finish]
            export_file = export_dir / f"issue681-public-{finish}.json"
            _write_private(export_file, item["bytes"])
            public_digests[finish] = item["sha256"]
        stages["export"] = "passed"
        print(json.dumps({
            "case": CASE,
            "outcome": "passed",
            "boundary": BOUNDARY_TAG,
            "finishes": list(finishes),
            "public_sha256": public_digests,
            "stages": stages,
        }, sort_keys=True), flush=True)
        return 0
    except ValueError as exc:
        token = str(exc)
        failed_stage = (token.split(":", 1)[1]
                        if token.startswith(ERROR + ":") else "unknown")
        if failed_stage in stages:
            stages[failed_stage] = "failed"
        summary = {
            "case": CASE,
            "outcome": "failed",
            "error_code": ERROR,
            "failed_stage": failed_stage,
            "stages": stages,
        }
        for key in ("acquire", "build", "measure"):
            if substages[key]:
                summary[f"{key}_substages"] = substages[key]
        if completed_cases["count"] > 0 or failed_stage == "measure":
            summary["completed_cases"] = completed_cases["count"]
        if failure_detail["substage"] is not None:
            summary["failed_substage"] = failure_detail["substage"]
        if failure_detail["detail_code"] is not None:
            summary["detail_code"] = failure_detail["detail_code"]
        print(json.dumps(summary, sort_keys=True), flush=True)
        return 1
    except BaseException:
        print(json.dumps({
            "case": CASE,
            "outcome": "failed",
            "error_code": ERROR,
            "failed_stage": "export",
            "stages": stages,
        }, sort_keys=True), flush=True)
        return 1
    finally:
        for directory in (task_parent, home_parent):
            shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(run_hosted_fixture_cli())
