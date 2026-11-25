"""Manual hosted-Windows public WAL acceptance, with bounded serial builds.

This is development acceptance, not a power-loss test or release-wheel proof.
The workflow selects each phase explicitly and always retains receipts before
cleaning its disposable runner. No native build runs when this module is imported.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import traceback
import xml.etree.ElementTree as ET
import zipfile

import storage

REPO = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
STAGES = ("spool_write", "spool_sync", "spool_publish", "before_reference",
          "reference_append_sync", "reference_force_sync")
PHASES = ("prepare", "toolchain", "parser", "normal", "instrumented", "legacy", "verify", "cleanup")
PILOT_PHASES = ("prepare", "toolchain", "parser", "pilot", "cleanup")
PHASE_CODES = {phase: phase.upper() for phase in (*PHASES, "pilot")}
PUBLIC_SCHEMA = "alopex.windows-overflow-public/v1"
PILOT_SCHEMA = "alopex.windows-overflow-pilot/v1"
EVIDENCE_CERT = REPO / ".github/ops/native-evidence-cert.pem"
EVIDENCE_CERT_SHA256 = "e8a39adb8f76185a87f805f6d083bdbe2bd7ea5d3bc05607f1a7a2e0deb8b775"
MAX_PRIVATE_BYTES = 268_435_456
OLD_WHEEL_NAME = "alopex-0.8.16-cp38-abi3-win_amd64.whl"
OLD_WHEEL_SHA256 = "194401d71e5c41ab8e976330f6007529961aeeb1773aa9162a321725f079a0be"
PILOT_MAX_SECONDS = 1_200
PILOT_JOB_EARLY_STOP_SECONDS = 1_500
TOOLCHAIN_MAX_SECONDS = 720
FULL_EARLY_STOP_SECONDS = 6_600
FULL_CLEANUP_RESERVE_SECONDS = 600
EXPECTED_STAGE_SECONDS = {"toolchain": 600, "parser": 240, "normal": 960, "instrumented": 1_920,
                          "legacy": 1_200, "verify": 240}


class EvidenceError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def require_runner(platform, env):
    require(platform == "win32" and env.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and env.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and env.get("ALOPEX_ACCEPTANCE_SUITE") in ("windows-overflow", "windows-overflow-pilot"),
            "requires the explicitly selected manual suite on hosted Windows")
    require(not sys.flags.optimize, "Python assertions must be enabled")


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def native_git_bash(git_executable):
    # PowerShell's PATH can resolve plain `bash` to System32's WSL launcher.
    # Anchor this native parser build to the Git for Windows installation.
    require(git_executable is not None, "Git for Windows executable is required")
    git = Path(git_executable).resolve()
    require(git.name.lower() == "git.exe", "expected native Git for Windows")
    for bash in (git.parent / "bash.exe", git.parent.parent / "bin/bash.exe"):
        if bash.is_file():
            return bash
    raise RuntimeError("Git for Windows bash.exe not found; do not fall back to WSL")


def retry_readonly(function, path, error_info):
    # Git pack files in the job-owned Nimble cache can carry FILE_ATTRIBUTE_READONLY.
    # Retry only that known case; keep sharing/ACL/other IO failures visible.
    error = error_info[1]
    entry = Path(path)
    if (not isinstance(error, PermissionError) or function not in (os.unlink, os.remove, os.rmdir)
            or entry.is_symlink() or entry.stat().st_mode & stat.S_IWRITE):
        raise error
    entry.chmod(entry.stat().st_mode | stat.S_IWRITE)
    function(path)


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def validate_pilot_admission(path, source):
    if not path:
        raise EvidenceError("PILOT")
    try:
        pilot = json.loads(Path(path).read_text(encoding="utf-8"))
        expected_keys = {"schema", "pilot_run_id", "pilot_run_attempt", "source_sha",
                         "source_tree", "workflow_sha256", "driver_sha256",
                         "artifact_sha256", "admitted"}
        if (set(pilot) != expected_keys or
                pilot["schema"] != "alopex.windows-overflow-pilot-admission/v1" or
                pilot["admitted"] is not True or pilot["source_sha"] != source["sha"] or
                pilot["source_tree"] != source["tree"] or
                pilot["workflow_sha256"] != source["workflow_sha256"] or
                pilot["driver_sha256"] != source["driver_sha256"]):
            raise EvidenceError("PILOT")
    except (OSError, ValueError, KeyError, TypeError):
        raise EvidenceError("PILOT") from None
    return pilot


def public_result(output, source_sha, suite="windows-overflow"):
    """Construct the public receipt from fixed fields, never from raw diagnostics."""
    pilot_mode = suite == "windows-overflow-pilot"
    result = {"schema": PILOT_SCHEMA if pilot_mode else PUBLIC_SCHEMA,
              "case": "WINDOWS_OVERFLOW_PILOT" if pilot_mode else "WINDOWS_OVERFLOW_PUBLIC",
              "outcome": "failed", "error_code": "EVIDENCE", "source_sha": source_sha}
    if suite not in ("windows-overflow", "windows-overflow-pilot") or not re.fullmatch(
            r"[0-9a-f]{40}", source_sha):
        return dict(result, source_sha=None)
    for phase in PILOT_PHASES if pilot_mode else PHASES:
        try:
            receipt = json.loads((output / f"phase-{phase}.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return dict(result, error_code="MISSING_" + PHASE_CODES[phase])
        if receipt != {"phase": phase, "status": "passed"}:
            code = receipt.get("error_code") if isinstance(receipt, dict) else None
            allowed = {"OLD_WHEEL", "STORAGE", "SOURCE", "CASE", "TIME_BUDGET",
                       "PILOT", "STOP_FAILURE"}
            return dict(result, error_code=code if isinstance(code, str) and code in allowed
                        else PHASE_CODES[phase])
    try:
        source = json.loads((output / "source.json").read_text(encoding="utf-8"))
        cleanup = json.loads((output / "cleanup.json").read_text(encoding="utf-8"))
        if source["sha"] != source_sha:
            return dict(result, error_code="SOURCE")
        if cleanup["target_removed"] is not True or cleanup["work_removed"] is not True:
            return dict(result, error_code="CLEANUP")
        if pilot_mode:
            pilot = json.loads((output / "pilot.json").read_text(encoding="utf-8"))
            if (pilot["source"] != source or pilot["budget_admitted"] is not True
                    or pilot["probe_counts"] != {"ordinary": 1, "rust": 1,
                                                  "python": 1, "old_wheel": 1}):
                return dict(result, error_code="PILOT")
            return dict(result, outcome="passed", error_code=None,
                        source_tree=source["tree"], workflow_sha256=source["workflow_sha256"],
                        driver_sha256=source["driver_sha256"], run_id=source["run_id"],
                        run_attempt=source["attempt"], budget_admitted=True)
        private = json.loads((output / "result.json").read_text(encoding="utf-8"))
        old_wheel = json.loads((output / "legacy-wheel-identity.json").read_text(encoding="utf-8"))
        if private["source"] != source:
            return dict(result, error_code="SOURCE")
        if (old_wheel != {"filename": OLD_WHEEL_NAME, "sha256": OLD_WHEEL_SHA256}
                or private["legacy_wheel_sha256"] != OLD_WHEEL_SHA256):
            return dict(result, error_code="OLD_WHEEL")
        if (private["status"] != "passed"
                or tuple(private[key] for key in ("rust_boundary_cases", "python_boundary_cases",
                                                  "ordinary_python_cases", "legacy_container_processes",
                                                  "legacy_live_wal_processes")) != (24, 24, 15, 4, 6)):
            return dict(result, error_code="CASE")
        if cleanup["target_removed"] is not True or cleanup["work_removed"] is not True:
            return dict(result, error_code="CLEANUP")
    except (OSError, ValueError, KeyError, TypeError):
        return result
    return dict(result, outcome="passed", error_code=None, old_wheel_sha256=OLD_WHEEL_SHA256)


def private_files(output):
    files = []
    total = 0
    for path in output.rglob("*"):
        if path.name in ("public.json", "private-evidence.cms"):
            continue
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise EvidenceError("PRIVATE_FILE")
        if path.is_file():
            total += path.stat().st_size
            if total > MAX_PRIVATE_BYTES:
                raise EvidenceError("PRIVATE_SIZE")
            files.append(path)
    return files


def native_openssl():
    bash = native_git_bash(shutil.which("git"))
    for openssl in (bash.parent / "openssl.exe",
                    bash.parent.parent / "usr/bin/openssl.exe",
                    bash.parent.parent / "mingw64/bin/openssl.exe"):
        if openssl.is_file():
            return openssl
    raise EvidenceError("SEAL_TOOL")


def seal_private(output, openssl=None):
    if digest(EVIDENCE_CERT) != EVIDENCE_CERT_SHA256:
        raise EvidenceError("CERT")
    files = private_files(output)
    if not files:
        raise EvidenceError("EVIDENCE")
    with tempfile.TemporaryDirectory(prefix="alopex-windows-evidence-",
                                     dir=os.environ.get("RUNNER_TEMP")) as temporary:
        archive = Path(temporary) / "private.zip"
        encrypted = Path(temporary) / "private.cms"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            for path in files:
                bundle.write(path, path.relative_to(output))
        result = subprocess.run([str(openssl or native_openssl()), "cms", "-encrypt",
                                 "-binary", "-aes256", "-outform", "DER", "-in",
                                 str(archive), "-out", str(encrypted), str(EVIDENCE_CERT)],
                                stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=60, check=False)
        if result.returncode or not encrypted.is_file() or not encrypted.stat().st_size:
            raise EvidenceError("SEAL")
        encrypted.replace(output / "private-evidence.cms")
    for child in output.iterdir():
        if child.name in ("private-evidence.cms", "public.json"):
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()
    return digest(output / "private-evidence.cms")


def publish(output, source_sha, openssl=None, suite="windows-overflow"):
    if output.is_symlink():
        raise EvidenceError("PRIVATE_FILE")
    output.mkdir(parents=True, exist_ok=True)
    if (output / "public.json").is_symlink() or (output / "private-evidence.cms").is_symlink():
        raise EvidenceError("PRIVATE_FILE")
    public = public_result(output, source_sha, suite)
    try:
        public["ciphertext_sha256"] = seal_private(output, openssl)
    except EvidenceError as error:
        public.update(outcome="failed", error_code=error.code)
    except (OSError, ValueError, subprocess.TimeoutExpired, subprocess.SubprocessError):
        public.update(outcome="failed", error_code="SEAL")
    write(output / "public.json", public)
    print(("WINDOWS_OVERFLOW_PILOT_" if suite == "windows-overflow-pilot"
           else "WINDOWS_OVERFLOW_PUBLIC_") + ("PASSED" if public["outcome"] == "passed"
                                              else public["error_code"]), flush=True)
    return public["outcome"] == "passed"


def validate_python_report(report, native_sha):
    expected = {f"{surface}-{mode}-{stage}" for surface in ("kv", "sql")
                for mode in ("error", "exit") for stage in STAGES}
    cases = report["cases"]
    require(report["status"] == "passed" and report["native_sha256"] == native_sha,
            "instrumented extension identity/result mismatch")
    require(len(cases) == len(expected) and {c["case"] for c in cases} == expected,
            "missing or duplicate Python boundary cases")
    require(all(c["status"] == "passed" and c["reopen_cycles"] == 2
                and 0 < c["fixture_bytes"] <= 1_048_576 for c in cases),
            "boundary result, reopen count or fixture bound mismatch")


def validate_rust_log(contents):
    expected = {f"{surface}::{mode}_{stage}" for surface in ("kv", "sql")
                for mode in ("error", "exit") for stage in STAGES}
    actual = re.findall(r"^test (\S+) \.\.\. ok\s*$", contents, re.MULTILINE)
    require(len(actual) == 24 and set(actual) == expected
            and "24 passed; 0 failed; 0 ignored" in contents,
            "Rust boundary suite did not execute exactly the 24 parents")


def validate_pytest(path):
    cases = list(ET.parse(path).getroot().iter("testcase"))
    require(len(cases) == 15 and all(not list(c) for c in cases),
            "expected 15 successful ordinary Python overflow cases, no skips")


def pilot_projected_seconds(timings):
    """Conservative full-workload forecast; unmeasured legacy work keeps its reserve."""
    required = ("toolchain", "parser", "runtime-venv", "pytest-install", "normal-build", "normal-package",
                "ordinary-case", "rust-build", "rust-case", "instrumented-build",
                "instrumented-package", "python-case",
                "legacy-venv", "legacy-download", "legacy-install", "legacy-import")
    if set(timings) != set(required) or any(not isinstance(timings[key], (int, float))
                                            or timings[key] <= 0 for key in required):
        raise EvidenceError("PILOT")
    case_cost = (15 * timings["ordinary-case"] + 24 * timings["rust-case"]
                 + 24 * timings["python-case"])
    setup_cost = sum(timings[key] for key in required
                     if key not in ("ordinary-case", "rust-case", "python-case"))
    return 2 * (setup_cost + case_cost) + 25 * 60 + 5 * 60 + 5 * 60


def projected_full_time(job_started, now, phase, phase_started):
    stages = tuple(EXPECTED_STAGE_SECONDS)
    if phase not in ("prepare", *stages) or not (0 <= job_started <= phase_started <= now):
        raise EvidenceError("TIME_BUDGET")
    remaining = sum(EXPECTED_STAGE_SECONDS[name] for name in stages)
    if phase != "prepare":
        index = stages.index(phase)
        stage_elapsed = now - phase_started
        remaining = (max(EXPECTED_STAGE_SECONDS[phase] - stage_elapsed, stage_elapsed)
                     + sum(EXPECTED_STAGE_SECONDS[name] for name in stages[index + 1:]))
    return now - job_started + remaining + FULL_CLEANUP_RESERVE_SECONDS


class Acceptance:
    def __init__(self, phase):
        require_runner(sys.platform, os.environ)
        self.suite = os.environ["ALOPEX_ACCEPTANCE_SUITE"]
        self.phase = phase
        self.phase_started = time.time()
        self.work = Path(os.environ["RUNNER_TEMP"]).resolve() / "alopex-windows-overflow-public"
        self.output = REPO / "artifacts/windows-overflow-public"
        self.target = REPO / "target"
        self.parser = REPO / "crates/alopex-sql/nim-sql-parser"
        self.python = self.work / "runtime/Scripts/python.exe"
        self.env = dict(os.environ, CARGO_TARGET_DIR=str(self.target), CARGO_INCREMENTAL="0",
                        CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
                        CARGO_BUILD_JOBS="2", RUSTFLAGS="-D warnings", CARGO_TERM_COLOR="never",
                        ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD="1",
                        NIM_SQL_PARSER_LIB_DIR=str(self.parser), PYTHONUTF8="1",
                        PYTHONDONTWRITEBYTECODE="1", PIP_NO_CACHE_DIR="1",
                        TMP=str(self.work / "tmp"), TEMP=str(self.work / "tmp"),
                        TMPDIR=str(self.work / "tmp"), PYO3_PYTHON=str(self.python))
        self.env.pop("PYTHONPATH", None)
        self.env.pop("CARGO_ENCODED_RUSTFLAGS", None)
        self.env["PATH"] = str(self.parser) + os.pathsep + self.env["PATH"]

    def run(self, name, command, *, env=None, additional=500_000_000,
            read_output=True):
        log = self.output / f"{name}.log"
        plan = json.loads((self.output / "plan.json").read_text())
        self.check_time_budget(plan)
        current = storage.check(self.output / "storage.jsonl", [self.work])["allocated_bytes"]
        ceiling = plan["initial_allocated_bytes"] + plan["estimated_peak_additional_bytes"]
        additional = min(additional, ceiling - current)
        require(additional > 0, "suite growth allowance exhausted before starting command")
        # A single watchdog owns the command tree and stops on measurement errors,
        # the declared per-command growth, or the 40 GB host stop threshold.
        wrapper = [sys.executable, str(HERE / "storage.py"), "--log",
                   str(self.output / "storage.jsonl"), "--root", str(self.work),
                   "--additional-bytes", str(additional), "--", *map(str, command)]
        write(self.output / f"{name}-command.json", {
            "command": list(map(str, command)), "additional_bytes": additional,
            "output": log.name, "cleanup": "workflow always cleanup phase",
            "rustflags": (env or self.env)["RUSTFLAGS"],
            "pythonpath": (env or self.env).get("PYTHONPATH")})
        print(f"Starting {name}", flush=True)
        with log.open("w", encoding="utf-8") as stream:
            process = subprocess.Popen(wrapper, cwd=REPO, env=env or self.env,
                                       stdin=subprocess.DEVNULL, stdout=stream,
                                       stderr=subprocess.STDOUT)
            while True:
                try:
                    timeout = 15
                    if self.suite == "windows-overflow-pilot" and self.phase == "pilot":
                        timeout = min(timeout, max(0.1, PILOT_MAX_SECONDS -
                                                   (time.time() - self.phase_started)))
                    if self.phase == "toolchain":
                        timeout = min(timeout, max(0.1, TOOLCHAIN_MAX_SECONDS -
                                                   (time.time() - self.phase_started)))
                    returncode = process.wait(timeout=timeout)
                    break
                except subprocess.TimeoutExpired:
                    try:
                        if log.stat().st_size > MAX_PRIVATE_BYTES:
                            raise EvidenceError("EVIDENCE")
                        self.check_time_budget(plan)
                    except EvidenceError:
                        self.stop_process_tree(process)
                        raise
        self.check_time_budget(plan)
        if log.stat().st_size > MAX_PRIVATE_BYTES:
            raise EvidenceError("EVIDENCE")
        contents = log.read_text(encoding="utf-8", errors="replace") if read_output or returncode else ""
        if returncode and ("Alopex host storage" in contents
                                  or "declared additional storage" in contents):
            raise EvidenceError("STORAGE")
        require(returncode == 0, f"{name} failed: {returncode}; see {log.name}")
        return contents if read_output else None

    def check_time_budget(self, plan=None):
        if self.suite == "windows-overflow-pilot":
            if plan is None:
                plan = json.loads((self.output / "plan.json").read_text())
            if time.time() - plan["job_started_unix"] >= PILOT_JOB_EARLY_STOP_SECONDS:
                raise EvidenceError("TIME_BUDGET")
            if self.phase == "toolchain" and time.time() - self.phase_started >= TOOLCHAIN_MAX_SECONDS:
                raise EvidenceError("TIME_BUDGET")
            if self.phase == "pilot" and time.time() - self.phase_started >= PILOT_MAX_SECONDS:
                raise EvidenceError("TIME_BUDGET")
            return
        if self.phase == "cleanup":
            return
        if plan is None:
            plan = json.loads((self.output / "plan.json").read_text())
        projected = projected_full_time(plan["job_started_unix"], time.time(),
                                        self.phase, self.phase_started)
        if projected >= FULL_EARLY_STOP_SECONDS:
            raise EvidenceError("TIME_BUDGET")

    @staticmethod
    def stop_process_tree(process):
        if process.poll() is not None:
            return
        result = subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                                stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=30, check=False)
        if result.returncode:
            raise EvidenceError("STOP_FAILURE")
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            raise EvidenceError("STOP_FAILURE") from None

    def prepare(self):
        self.output.mkdir(parents=True, exist_ok=False)
        try:
            job_started = int(os.environ["ALOPEX_JOB_STARTED_UNIX"])
        except (KeyError, ValueError):
            raise EvidenceError("TIME_BUDGET") from None
        if not 0 <= time.time() - job_started < 600:
            raise EvidenceError("TIME_BUDGET")
        initial = storage.check(self.output / "storage.jsonl", [self.work])
        require(initial["allocated_bytes"] + 28_000_000_000 < storage.STOP_BYTES,
                "28 GB suite growth allowance does not fit below the host stop")
        self.work.mkdir(exist_ok=False)
        (self.work / "tmp").mkdir()
        source = {"sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO,
                                                  text=True, stderr=subprocess.STDOUT).strip(),
                  "tree": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"],
                                                   cwd=REPO, text=True,
                                                   stderr=subprocess.STDOUT).strip(),
                  "run_id": os.environ["GITHUB_RUN_ID"],
                  "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
                  "runner_os": sys.platform,
                  "driver_sha256": digest(__file__),
                  "workflow_sha256": digest(REPO / ".github/workflows/compatibility.yml")}
        if self.suite == "windows-overflow":
            pilot = validate_pilot_admission(
                os.environ.get("ALOPEX_PILOT_ADMISSION_PATH", ""), source)
            write(self.output / "pilot-admission.json", pilot)
        write(self.output / "source.json", source)
        write(self.output / "plan.json", {
            "unanswered": "Windows public API error/exit recovery and real 0.8.16 refusal",
            "phases": PILOT_PHASES if self.suite == "windows-overflow-pilot" else PHASES,
            "target": str(self.target), "work": str(self.work),
            "estimated_peak_additional_bytes": 28_000_000_000,
            "job_started_unix": job_started,
            "initial_allocated_bytes": initial["allocated_bytes"],
            "stop_bytes": storage.STOP_BYTES, "cleanup": "always, after compact receipts",
            "scope": "24 Rust + 24 Python boundaries, 15 ordinary cases, old container/live WAL",
            "excluded": "load/performance, hardware power loss, release-wheel qualification"})
        self.run("contract-tests", [sys.executable, "-m", "unittest", "discover",
                                    "-s", "scripts/compatibility", "-p", "test_windows_*.py",
                                    "-q"], additional=100_000_000, read_output=False)

    def build_parser(self):
        started = time.monotonic()
        bash = native_git_bash(shutil.which("git"))
        self.run("parser-build", [str(bash), "--noprofile", "--norc",
                                 "scripts/build-nim-parser.sh", "--backend", "host"],
                 additional=2_000_000_000)
        write(self.output / "parser-identity.json", {
            "sha256": digest(self.parser / "alopex_sql_parser.dll"),
            "contract": (self.parser / "CONTRACT_VERSION").read_text().strip()})
        write(self.output / "parser-timing.json", {"seconds": max(0.001, time.monotonic() - started)})

    def toolchain(self):
        started = time.monotonic()
        bash = native_git_bash(shutil.which("git"))
        (self.work / "nim-bootstrap").mkdir(exist_ok=False)
        self.run("nim-toolchain", [sys.executable, HERE / "windows_nim_toolchain.py",
                                   "--work-dir", self.work / "nim-bootstrap",
                                   "--bash", bash], additional=2_000_000_000,
                 read_output=False)
        write(self.output / "toolchain-timing.json", {"seconds": max(0.001, time.monotonic() - started)})

    def package(self, name):
        root = self.work / name
        shutil.copytree(REPO / "crates/alopex-py/python/alopex", root / "alopex",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        suffix = subprocess.check_output([str(self.python), "-c",
                                          "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"],
                                         env=self.env, text=True,
                                         stderr=subprocess.STDOUT).strip()
        native = root / "alopex" / ("_alopex" + suffix)
        shutil.copy2(self.target / "debug/_alopex.dll", native)
        assets = root / "alopex/native"
        assets.mkdir(exist_ok=True)
        for file in ("alopex_sql_parser.dll", "CONTRACT_VERSION", "SHA256SUMS"):
            shutil.copy2(self.parser / file, assets / file)
        write(self.output / f"{name}-identity.json", {"native_sha256": digest(native),
                                                    "parser_sha256": digest(assets / "alopex_sql_parser.dll")})
        return dict(self.env, PYTHONPATH=str(root))

    def normal(self):
        self.run("runtime-venv", [sys.executable, "-m", "venv", str(self.work / "runtime")])
        self.run("pytest-install", [self.python, "-m", "pip", "install", "--no-cache-dir", "pytest==8.4.2"])
        self.run("normal-build", ["cargo", "build", "--locked", "-p", "alopex-py"],
                 additional=12_000_000_000)
        env = self.package("normal")
        self.run("normal-hook-absent", [self.python, "-c",
            "from alopex import _alopex, WalOverflowOptions; "
            "assert not hasattr(_alopex, '_arm_wal_test_fault'); "
            "assert WalOverflowOptions.writes_supported(); print('normal hook absent; writer supported')"], env=env)
        self.run("ordinary-python", [self.python, "-m", "pytest", "-q",
            "crates/alopex-py/tests/test_wal_overflow.py", "--basetemp", str(self.work / "pytest"),
            "--junitxml", str(self.output / "ordinary-python.xml")], env=env)
        validate_pytest(self.output / "ordinary-python.xml")

    def pilot(self):
        timings = {phase: json.loads((self.output / f"{phase}-timing.json").read_text())["seconds"]
                   for phase in ("toolchain", "parser")}
        started = time.monotonic()

        def timed(name, command, *, env=None, additional=500_000_000):
            before = time.monotonic()
            contents = self.run("pilot-" + name, command, env=env, additional=additional)
            timings[name] = max(time.monotonic() - before, 0.001)
            if time.monotonic() - started >= PILOT_MAX_SECONDS:
                raise EvidenceError("TIME_BUDGET")
            return contents

        timed("runtime-venv", [sys.executable, "-m", "venv", self.work / "runtime"])
        timed("pytest-install", [self.python, "-m", "pip", "install", "--no-cache-dir",
                                 "pytest==8.4.2"])
        timed("normal-build", ["cargo", "build", "--locked", "-p", "alopex-py"],
              additional=12_000_000_000)
        before = time.monotonic()
        normal_env = self.package("pilot-normal")
        timings["normal-package"] = max(time.monotonic() - before, 0.001)
        timed("ordinary-case", [self.python, "-m", "pytest", "-q",
            "crates/alopex-py/tests/test_wal_overflow.py::test_sql_transaction_larger_than_ring_reopens_atomically",
            "--basetemp", self.work / "pilot-pytest", "--junitxml", self.output / "pilot-ordinary.xml"],
            env=normal_env)
        ordinary = list(ET.parse(self.output / "pilot-ordinary.xml").getroot().iter("testcase"))
        if len(ordinary) != 1 or list(ordinary[0]):
            raise EvidenceError("CASE")
        instrumented_env = dict(self.env, RUSTFLAGS="-D warnings --cfg alopex_wal_test_hooks")
        rust = ["cargo", "test", "--locked", "-p", "alopex-embedded", "--test",
                "wal_publication"]
        timed("rust-build", [*rust, "--no-run"], env=instrumented_env,
              additional=10_000_000_000)
        rust_log = timed("rust-case", [*rust, "kv::error_spool_write", "--", "--exact",
                                       "--test-threads=1"], env=instrumented_env)
        if (re.findall(r"^test (\S+) \.\.\. ok\s*$", rust_log, re.MULTILINE)
                != ["kv::error_spool_write"] or "1 passed; 0 failed" not in rust_log):
            raise EvidenceError("CASE")
        timed("instrumented-build", ["cargo", "build", "--locked", "-p", "alopex-py"],
              env=instrumented_env, additional=8_000_000_000)
        before = time.monotonic()
        python_env = self.package("pilot-instrumented")
        timings["instrumented-package"] = max(time.monotonic() - before, 0.001)
        timed("python-case", [self.python, HERE / "publication_python.py", "--output",
                              self.output / "pilot-python", "--pilot-case"], env=python_env)
        python_report = json.loads((self.output / "pilot-python/report.json").read_text())
        python_identity = json.loads((self.output / "pilot-instrumented-identity.json").read_text())
        if (python_report["status"] != "passed" or len(python_report["cases"]) != 1
                or python_report["cases"][0]["case"] != "kv-error-spool_write"
                or python_report["cases"][0]["status"] != "passed"
                or python_report["native_sha256"] != python_identity["native_sha256"]):
            raise EvidenceError("CASE")
        old = self.work / "pilot-legacy/Scripts/python.exe"
        timed("legacy-venv", [sys.executable, "-m", "venv", self.work / "pilot-legacy"])
        wheel_dir = self.work / "pilot-old-wheel"
        timed("legacy-download", [old, "-m", "pip", "download", "--no-deps",
              "--only-binary=:all:", "--dest", wheel_dir, "alopex==0.8.16"],
              additional=2_000_000_000)
        wheels = list(wheel_dir.iterdir())
        if len(wheels) != 1 or wheels[0].name != OLD_WHEEL_NAME or digest(wheels[0]) != OLD_WHEEL_SHA256:
            raise EvidenceError("OLD_WHEEL")
        timed("legacy-install", [old, "-m", "pip", "install", "--no-cache-dir",
              "--only-binary=:all:", wheels[0]])
        timed("legacy-import", [old, "-c",
              "import alopex, importlib.metadata as m, pathlib, sys; "
              "p=pathlib.Path(alopex.__file__).resolve(); "
              "sys.exit(0 if m.version('alopex') == '0.8.16' and "
              "p.is_relative_to(pathlib.Path(sys.prefix).resolve()) else 1)"])
        projected = pilot_projected_seconds(timings)
        if projected + FULL_CLEANUP_RESERVE_SECONDS >= FULL_EARLY_STOP_SECONDS:
            raise EvidenceError("TIME_BUDGET")
        samples = [json.loads(line) for line in (self.output / "storage.jsonl").read_text().splitlines()]
        if not samples or any(not isinstance(sample.get("allocated_bytes"), int)
                              for sample in samples):
            raise EvidenceError("STORAGE")
        peak_allocated = max(sample["allocated_bytes"] for sample in samples)
        if peak_allocated >= storage.STOP_BYTES:
            raise EvidenceError("STORAGE")
        write(self.output / "pilot.json", {
            "source": json.loads((self.output / "source.json").read_text()),
            "probe_counts": {"ordinary": 1, "rust": 1, "python": 1, "old_wheel": 1},
            "timings_seconds": timings, "projected_full_seconds": projected,
            "peak_allocated_bytes": peak_allocated,
            "pilot_elapsed_seconds": time.monotonic() - started,
            "budget_admitted": True})

    def instrumented(self):
        env = dict(self.env, RUSTFLAGS="-D warnings --cfg alopex_wal_test_hooks")
        contents = self.run("rust-boundaries", ["cargo", "test", "--locked", "-p", "alopex-embedded",
            "--test", "wal_publication", "--", "--skip", "child", "--test-threads=1"],
            env=env, additional=10_000_000_000)
        validate_rust_log(contents)
        self.run("instrumented-build", ["cargo", "build", "--locked", "-p", "alopex-py"],
                 env=env, additional=8_000_000_000)
        package_env = self.package("instrumented")
        self.run("python-boundaries", [self.python, HERE / "publication_python.py", "--output",
                 self.output / "python-boundaries"], env=package_env)
        report = json.loads((self.output / "python-boundaries/report.json").read_text())
        identity = json.loads((self.output / "instrumented-identity.json").read_text())
        validate_python_report(report, identity["native_sha256"])

    def legacy(self):
        old = self.work / "legacy/Scripts/python.exe"
        self.run("legacy-venv", [sys.executable, "-m", "venv", self.work / "legacy"])
        wheel_dir = self.work / "old-wheel"
        self.run("legacy-download", [old, "-m", "pip", "download", "--no-deps",
            "--only-binary=:all:", "--dest", wheel_dir, "alopex==0.8.16"],
            additional=2_000_000_000)
        wheels = list(wheel_dir.iterdir())
        if len(wheels) != 1 or wheels[0].name != OLD_WHEEL_NAME or digest(wheels[0]) != OLD_WHEEL_SHA256:
            raise EvidenceError("OLD_WHEEL")
        write(self.output / "legacy-wheel-identity.json", {
            "filename": wheels[0].name, "sha256": OLD_WHEEL_SHA256})
        self.run("legacy-install", [old, "-m", "pip", "install", "--no-cache-dir",
            "--only-binary=:all:", "--report", self.output / "legacy-install.json",
            wheels[0]], additional=2_000_000_000)
        self.run("legacy-import-identity", [old, "-c",
            "import alopex, importlib.metadata as m, pathlib, sys; "
            "p=pathlib.Path(alopex.__file__).resolve(); "
            "root=pathlib.Path(sys.prefix).resolve(); "
            "sys.exit(0 if m.version('alopex') == '0.8.16' and p.is_relative_to(root) else 1)"])
        normal = json.loads((self.output / "normal-identity.json").read_text())
        current_env = dict(self.env, PYTHONPATH=str(self.work / "normal"))
        for phase in ("legacy-write", "upgrade", "legacy-reject", "reopen"):
            legacy = phase.startswith("legacy-")
            self.run("container-" + phase, [old if legacy else self.python, HERE / "wal_overflow.py",
                phase, "--db", self.work / "legacy.alopex", "--evidence", self.output / "container",
                "--candidate-native-sha256", normal["native_sha256"]], env=self.env if legacy else current_env)
        for name in ("wal-v0816", "wal-current"):
            self.run(name + "-build", ["cargo", "build", "--locked", "--manifest-path",
                HERE / name / "Cargo.toml"], additional=5_000_000_000)
        self.run("live-wal", [sys.executable, HERE / "live_wal.py", "--legacy",
            self.target / "debug/alopex-wal-v0816.exe", "--candidate",
            self.target / "debug/alopex-wal-current.exe", "--output", self.output / "live-wal"])
        # The receipt retains per-file hashes and sizes; do not upload raw DB data.
        shutil.rmtree(self.output / "live-wal/store")

    def verify(self):
        for phase in ("prepare", "toolchain", "parser", "normal", "instrumented", "legacy"):
            require(json.loads((self.output / f"phase-{phase}.json").read_text())["status"] == "passed",
                    f"missing successful phase: {phase}")
        source = json.loads((self.output / "source.json").read_text())
        if source["sha"] != subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO,
                                                    text=True,
                                                    stderr=subprocess.STDOUT).strip():
            raise EvidenceError("SOURCE")
        for phase in ("legacy-write", "upgrade", "legacy-reject", "reopen"):
            require(json.loads((self.output / f"container/{phase}.json").read_text())["result"] == "pass",
                    "incomplete container acceptance")
        live = json.loads((self.output / "live-wal/receipt.json").read_text())
        if live["result"] != "pass" or len(live["phases"]) != 6:
            raise EvidenceError("CASE")
        old_wheel = json.loads((self.output / "legacy-wheel-identity.json").read_text())
        require(old_wheel == {"filename": OLD_WHEEL_NAME, "sha256": OLD_WHEEL_SHA256},
                "old wheel identity mismatch")
        write(self.output / "result.json", {"status": "passed", "source": source,
            "rust_boundary_cases": 24, "python_boundary_cases": 24, "ordinary_python_cases": 15,
            "legacy_container_processes": 4, "legacy_live_wal_processes": 6,
            "legacy_wheel_sha256": OLD_WHEEL_SHA256,
            "scope": "hosted Windows development acceptance; no hardware power-loss or wheel qualification"})

    def cleanup(self):
        self.output.mkdir(parents=True, exist_ok=True)
        # Only the guarded, disposable hosted job may remove this shared target.
        for path in (self.target, self.work, self.output / "live-wal/store",
                     Path(os.environ["RUNNER_TEMP"]) / "alopex-nim-parser-toolchain"):
            if path.exists():
                shutil.rmtree(path, onerror=retry_readonly)
        # Remove generated parser binaries, preserving tracked contract/source files.
        tracked = set(subprocess.check_output(["git", "ls-files", "--", str(self.parser)],
                                              cwd=REPO, text=True,
                                              stderr=subprocess.STDOUT).splitlines())
        for path in self.parser.iterdir():
            if path.suffix in (".dll", ".a", ".lib", ".pdb") and str(path.relative_to(REPO)).replace("\\", "/") not in tracked:
                path.unlink()
        final = storage.check(self.output / "storage.jsonl")
        write(self.output / "cleanup.json", {"allocated_bytes": final["allocated_bytes"],
                                             "target_removed": not self.target.exists(),
                                             "work_removed": not self.work.exists()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=(*PHASES, "pilot", "publish"))
    phase = parser.parse_args().phase
    output = REPO / "artifacts/windows-overflow-public"
    if phase == "publish":
        try:
            return 0 if publish(output, os.environ.get("GITHUB_SHA", ""),
                                suite=os.environ.get("ALOPEX_ACCEPTANCE_SUITE", "")) else 1
        except (OSError, ValueError, EvidenceError):
            print("WINDOWS_OVERFLOW_PUBLIC_EVIDENCE", file=sys.stderr)
            return 1
    result = {"phase": phase, "status": "failed"}
    try:
        owner = Acceptance(phase)
        if (phase in ("pilot",) and owner.suite != "windows-overflow-pilot"
                or phase in ("normal", "instrumented", "legacy", "verify")
                and owner.suite != "windows-overflow"):
            raise EvidenceError("SUITE")
        getattr(owner, "build_parser" if phase == "parser" else phase)()
        if phase != "cleanup":
            owner.check_time_budget()
        result["status"] = "passed"
        return 0
    except Exception as error:
        if isinstance(error, EvidenceError):
            result["error_code"] = error.code
        try:
            output.mkdir(parents=True, exist_ok=True)
            (output / f"phase-{phase}.private.log").write_text(
                traceback.format_exc(), encoding="utf-8")
        except OSError:
            pass
        print("WINDOWS_OVERFLOW_" + result.get("error_code", PHASE_CODES[phase]),
              file=sys.stderr)
        return 1
    finally:
        try:
            if output.exists():
                write(output / f"phase-{phase}.json", result)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
