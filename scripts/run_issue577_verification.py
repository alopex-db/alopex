import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import threading

from verification_results import cargo_result

worktree = Path(__file__).resolve().parents[1]
root = next(parent for parent in worktree.parents if (parent / "alopex/Cargo.toml").is_file())
evidence = worktree / "target/verification/issue577-focused"
evidence.mkdir(parents=True, exist_ok=True)
if (evidence / "resource-preflight-required.json").exists():
    sys.exit("Build/test paused: host resource preflight is required.")
label, selector = sys.argv[1:]
assert label.replace("-", "").isalnum(), "invalid verification label"
if os.environ.get("CODEX_RESOURCE_CHILD") != "1":
    unit = "codex-issue597-" + label + "-" + str(os.getpid())
    launch = ["systemd-run", "--user", "--wait", "--pipe", "--unit=" + unit,
              "--property=MemoryMax=4G", "--property=MemoryHigh=3G",
              "--property=MemorySwapMax=0", "--property=CPUQuota=100%",
              "--property=TasksMax=128", "--property=RuntimeMaxSec=600",
              "--property=KillMode=control-group", "--property=TimeoutStopSec=5",
              "--property=Type=notify", "--property=NotifyAccess=all",
              "--property=WatchdogSec=4", "--property=WatchdogSignal=SIGTERM",
              "--property=TimeoutAbortSec=2",
              "--working-directory=" + str(root), "/usr/bin/env",
              "CODEX_RESOURCE_CHILD=1", "PATH=" + os.environ["PATH"],
              sys.executable, "-B", str(Path(__file__).resolve()), label, selector]
    from host_resources import HostMonitor
    from verification_watchdog import ResourcePolicy, run_guarded
    result = run_guarded(launch, unit, evidence / label, HostMonitor(), ResourcePolicy())
    if not result["cleanup_complete"]:
        sys.exit(125)
    if result["exit"] == 0 and not (evidence / (label + ".json")).is_file():
        sys.exit(125)
    sys.exit(result["exit"] if result["exit"] is not None else 125)

group = Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1]
assert group and "/codex-issue597-" in group, "verification requires its own resource unit"
cgroup = Path("/sys/fs/cgroup") / group.lstrip("/")
resource_limits = {name: (cgroup / name).read_text().strip() for name in
                   ("memory.max", "memory.high", "memory.swap.max", "cpu.max", "pids.max")}
assert resource_limits == {"memory.max": "4294967296", "memory.high": "3221225472",
                          "memory.swap.max": "0", "cpu.max": "100000 100000",
                          "pids.max": "128"}, "resource limits were not enforced"


from verification_watchdog import keep_watchdog_alive
subprocess.run(["systemd-notify", "--ready"], check=True, timeout=2)
threading.Thread(target=keep_watchdog_alive,
                 args=(evidence / (label + "-events.jsonl"),), daemon=True).start()
env = os.environ.copy()
env.update(CARGO_TARGET_DIR=str(root / "alopex/target"), CARGO_INCREMENTAL="0",
           CARGO_BUILD_JOBS="1", ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD="1",
           NIM_SQL_PARSER_LIB_DIR=str(worktree / "target/verification/issue577-parser-027"))
command = ["rtk", "proxy", "cargo", "test", "--offline", "--locked", "-p", "alopex-sql",
           "--test", "read_parquet_allocation", "--message-format=json-render-diagnostics",
           selector, "--", "--nocapture", "--test-threads=1"]
if label.endswith("indexed"):
    command = ["rtk", "proxy", "cargo", "test", "--offline", "--locked", "-p", "alopex-sql",
               "--lib", "--message-format=json-render-diagnostics", selector,
               "--", "--exact", "--nocapture", "--test-threads=1"]
if label.endswith("lint"):
    command = ["rtk", "proxy", "cargo", "clippy", "--offline", "--locked", "-p", "alopex-sql",
               "--lib", "--test", "read_parquet_allocation", "--test", "read_parquet", "--no-deps",
               "--message-format=json", "--", "-D", "warnings"]
if label == "red":
    command.append("--exact")
def capacity():
    return int(subprocess.check_output(["du", "-sb", str(root)], text=True).split()[0])
before = capacity()
assert before + (200_000_000 if label.endswith("lint") else 1_200_000_000) < 39_500_000_000
existing = set((root / "alopex/target/debug/deps").iterdir())
log = evidence / (label + ".log")
head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=worktree, text=True).strip()
source_files = ["Cargo.toml", "Cargo.lock", "crates/alopex-sql/Cargo.toml",
                "crates/alopex-sql/src/executor/bulk/parquet.rs",
                "crates/alopex-sql/src/executor/bulk/parquet/pages.rs",
                "crates/alopex-sql/src/executor/bulk/parquet/preflight.rs",
                "crates/alopex-sql/src/executor/bulk/parquet/pages/bounded.rs",
                "crates/alopex-sql/tests/read_parquet_allocation.rs"]
source_files.append("scripts/verification_results.py")
source_files.append("crates/alopex-sql/tests/read_parquet.rs")
source_files.extend(["crates/alopex-sql/src/executor/query/mod.rs",
                     "crates/alopex-sql/src/executor/query/project.rs"])
source_digests = {name: hashlib.sha256((worktree / name).read_bytes()).hexdigest()
                  for name in source_files}
with log.open("w") as output:
    process = subprocess.Popen(command, cwd=worktree, env=env, stdout=output,
                               stderr=subprocess.STDOUT, start_new_session=True)
    while process.poll() is None:
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            if capacity() > 39_500_000_000:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
                raise RuntimeError("owned build exceeded its operating budget")
text = log.read_text()
executables = set()
check_outputs = set()
for line in text.splitlines():
    if line.startswith("{"):
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("reason") == "compiler-artifact" and entry.get("executable"):
            path = Path(entry["executable"])
            prefix = "alopex_sql-" if label.endswith("indexed") else "read_parquet_allocation-"
            if path.name.startswith(prefix):
                executables.add(path)
        if label.endswith("lint") and entry.get("reason") == "compiler-artifact":
            for filename in entry.get("filenames", []):
                path = Path(filename)
                if path not in existing and path.suffix == ".rmeta" and path.name.startswith(("libalopex_sql-", "libread_parquet_allocation-", "libread_parquet-")):
                    check_outputs.add(path)
removed = 0
for executable in executables:
    assert executable.parent == root / "alopex/target/debug/deps"
    for path in (executable, executable.with_name(executable.name + ".d")):
        if path.exists():
            removed += path.stat().st_size
            path.unlink()
for path in check_outputs:
    assert path.parent == root / "alopex/target/debug/deps"
    removed += path.stat().st_size
    path.unlink()
receipt = {"owner": "parent Codex", "head": head, "command": command,
           "exit": process.returncode, "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
           "before_bytes": before, "after_bytes": capacity(), "removed_owned_bytes": removed}
receipt["result"] = cargo_result(text, process.returncode,
                               "lint" if label.endswith("lint") else "test")
assert source_digests == {name: hashlib.sha256((worktree / name).read_bytes()).hexdigest()
                         for name in source_files}
receipt["source_sha256"] = source_digests
receipt["resource_limits"] = resource_limits
receipt["verification_unit_memory_peak_bytes"] = int((cgroup / "memory.peak").read_text())
receipt["resource_scope"] = "verification unit, including setup, compilation and execution; not product allocation usage"
receipt["cpu_stat"] = (cgroup / "cpu.stat").read_text()
receipt["memory_events"] = (cgroup / "memory.events").read_text()
receipt["runtime_limit_seconds"] = 600
receipt["runner_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
(evidence / (label + ".json")).write_text(json.dumps(receipt, indent=2))
for line in text.splitlines():
    if "maximum_request=" in line or "test result:" in line:
        print(line[:500])
print(json.dumps(receipt))
if receipt["result"]["outcome"] == "invalid":
    sys.exit(125)
sys.exit(process.returncode if process.returncode >= 0 else 128 - process.returncode)
