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
import xml.etree.ElementTree as ET

import storage

REPO = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
STAGES = ("spool_write", "spool_sync", "spool_publish", "before_reference",
          "reference_append_sync", "reference_force_sync")
PHASES = ("prepare", "parser", "normal", "instrumented", "legacy", "verify", "cleanup")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def require_runner(platform, env):
    require(platform == "win32" and env.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and env.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and env.get("ALOPEX_ACCEPTANCE_SUITE") == "windows-overflow",
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


class Acceptance:
    def __init__(self):
        require_runner(sys.platform, os.environ)
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

    def run(self, name, command, *, env=None, additional=500_000_000):
        log = self.output / f"{name}.log"
        plan = json.loads((self.output / "plan.json").read_text())
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
            result = subprocess.run(wrapper, cwd=REPO, env=env or self.env,
                                    stdout=stream, stderr=subprocess.STDOUT)
        contents = log.read_text(encoding="utf-8", errors="replace")
        # Native Windows console encodings must not turn a failed command into a
        # secondary UnicodeEncodeError, obscuring its saved diagnostic log.
        tail = "\n".join(contents.splitlines()[-12:])
        print(tail.encode(sys.stdout.encoding or "utf-8", "backslashreplace")
              .decode(sys.stdout.encoding or "utf-8"), flush=True)
        require(result.returncode == 0, f"{name} failed: {result.returncode}; see {log.name}")
        return contents

    def prepare(self):
        self.output.mkdir(parents=True, exist_ok=False)
        initial = storage.check(self.output / "storage.jsonl", [self.work])
        require(initial["allocated_bytes"] + 28_000_000_000 < storage.STOP_BYTES,
                "28 GB suite growth allowance does not fit below the host stop")
        self.work.mkdir(exist_ok=False)
        (self.work / "tmp").mkdir()
        source = {"sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO,
                                                  text=True).strip(),
                  "tree": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"],
                                                   cwd=REPO, text=True).strip(),
                  "run_id": os.environ["GITHUB_RUN_ID"],
                  "attempt": os.environ["GITHUB_RUN_ATTEMPT"],
                  "runner_os": sys.platform,
                  "driver_sha256": digest(__file__)}
        write(self.output / "source.json", source)
        write(self.output / "plan.json", {
            "unanswered": "Windows public API error/exit recovery and real 0.8.16 refusal",
            "phases": PHASES, "target": str(self.target), "work": str(self.work),
            "estimated_peak_additional_bytes": 28_000_000_000,
            "initial_allocated_bytes": initial["allocated_bytes"],
            "stop_bytes": storage.STOP_BYTES, "cleanup": "always, after compact receipts",
            "scope": "24 Rust + 24 Python boundaries, 15 ordinary cases, old container/live WAL",
            "excluded": "load/performance, hardware power loss, release-wheel qualification"})

    def build_parser(self):
        bash = native_git_bash(shutil.which("git"))
        self.run("parser-build", [str(bash), "--noprofile", "--norc",
                                 "scripts/build-nim-parser.sh", "--backend", "host"],
                 additional=2_000_000_000)
        write(self.output / "parser-identity.json", {
            "sha256": digest(self.parser / "alopex_sql_parser.dll"),
            "contract": (self.parser / "CONTRACT_VERSION").read_text().strip()})

    def package(self, name):
        root = self.work / name
        shutil.copytree(REPO / "crates/alopex-py/python/alopex", root / "alopex",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        suffix = subprocess.check_output([str(self.python), "-c",
                                          "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"],
                                         env=self.env, text=True).strip()
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
        self.run("legacy-install", [old, "-m", "pip", "install", "--no-cache-dir", "--only-binary=:all:",
            "--report", self.output / "legacy-install.json", "alopex==0.8.16"], additional=2_000_000_000)
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
        for phase in ("prepare", "parser", "normal", "instrumented", "legacy"):
            require(json.loads((self.output / f"phase-{phase}.json").read_text())["status"] == "passed",
                    f"missing successful phase: {phase}")
        source = json.loads((self.output / "source.json").read_text())
        require(source["sha"] == subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO,
                                                         text=True).strip(), "checkout changed")
        for phase in ("legacy-write", "upgrade", "legacy-reject", "reopen"):
            require(json.loads((self.output / f"container/{phase}.json").read_text())["result"] == "pass",
                    "incomplete container acceptance")
        live = json.loads((self.output / "live-wal/receipt.json").read_text())
        require(live["result"] == "pass" and len(live["phases"]) == 6, "incomplete live WAL acceptance")
        write(self.output / "result.json", {"status": "passed", "source": source,
            "rust_boundary_cases": 24, "python_boundary_cases": 24, "ordinary_python_cases": 15,
            "legacy_container_processes": 4, "legacy_live_wal_processes": 6,
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
                                              cwd=REPO, text=True).splitlines())
        for path in self.parser.iterdir():
            if path.suffix in (".dll", ".a", ".lib", ".pdb") and str(path.relative_to(REPO)).replace("\\", "/") not in tracked:
                path.unlink()
        final = storage.check(self.output / "storage.jsonl")
        write(self.output / "cleanup.json", {"allocated_bytes": final["allocated_bytes"],
                                             "target_removed": not self.target.exists(),
                                             "work_removed": not self.work.exists()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=PHASES)
    phase = parser.parse_args().phase
    owner = Acceptance()
    result = {"phase": phase, "status": "failed"}
    try:
        getattr(owner, "build_parser" if phase == "parser" else phase)()
        result["status"] = "passed"
    finally:
        if owner.output.exists():
            write(owner.output / f"phase-{phase}.json", result)


if __name__ == "__main__":
    main()
