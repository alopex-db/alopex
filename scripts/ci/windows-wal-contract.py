"""Run the existing release build and tiny Windows WAL contracts serially.

Hosted Windows only; not a self-hosted/WSL inventory substitute. Additional
storage is bounded to 20 GB per command, 40 GB host stop, using one target.
Build and contract phases share one target but have separate workflow deadlines.
Artifacts contain source identity and raw results; the workflow removes target
after retaining those results. No durability/power-loss claim for the writer.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from windows_wal_console import print_log_tail

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--phase", required=True, choices=("build", "contracts"))
args = parser.parse_args()

repo = Path(__file__).resolve().parents[2]
output = repo / "artifacts/windows-wal"
output.mkdir(parents=True, exist_ok=True)
if sys.platform != "win32" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
    raise SystemExit("requires an inventoried, isolated GitHub-hosted Windows runner")
os.environ["CARGO_TARGET_DIR"] = str(repo / "target")
identity = {"sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            "tree": subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], text=True).strip(),
            "run_id": os.environ.get("GITHUB_RUN_ID"),
            "attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
            "platform": sys.platform,
            "writer_supported": True,
            "lowlevel_wal_writer_supported": True,
            "lsm_file_publication_supported": True}
(output / "source.json").write_text(json.dumps(identity, indent=2), encoding="utf-8")
commands = [
    ("release-build", ["cargo", "build", "--release", "--locked", "--all-features"]),
    ("readonly-recovery", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                           "--lib", "lsm::wal::overflow::recovery_portability_tests::"]),
    ("lowlevel-writer", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                          "--lib", "lsm::wal::overflow::tests::"]),
    ("windows-publication", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                        "--lib", "lsm::wal::overflow::windows_tests::"]),
    ("lsm-publication", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                         "--lib", "lsm::publication_tests::"]),
    ("container-fence", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                        "--lib", "lsm::overflow::fence_tests::"]),
    ("staging-recovery", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                         "--lib", "storage::publication::staging::tests::"]),
    ("activation-staging", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                         "--lib", "lsm::wal::overflow::activation_staging_tests::"]),
    ("overflow-garbage", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                         "--lib", "lsm::wal::overflow::garbage::tests::"]),
    ("overflow-lifecycle", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                         "--lib", "lsm::overflow::lifecycle_tests::"]),
    ("inline-lifecycle", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                          "--lib", "lsm::overflow::inline_lifecycle_tests::"]),
    ("publication-boundaries", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                                "--lib", "lsm::overflow::publication_acceptance::", "--",
                                "--skip", "lsm::overflow::publication_acceptance::child"]),
    ("public-overflow", ["cargo", "test", "--release", "--locked", "-p", "alopex-embedded",
                         "--test", "wal_overflow"]),
    ("public-journal", ["cargo", "test", "--release", "--locked", "-p", "alopex-embedded",
                        "--test", "wal_overflow_journal"]),
    ("directory-publication", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                        "--lib", "storage::publication::directories::tests::"]),
    ("memory-snapshots", ["cargo", "test", "--release", "--locked", "-p", "alopex-core",
                           "--test", "memory_snapshot_overwrite"]),
    ("hnsw-transaction-snapshots", ["cargo", "test", "--release", "--locked", "-p", "alopex-embedded",
                                     "--test", "hnsw_transaction_cache"]),
]
for name, command in commands:
    if (name == "release-build") != (args.phase == "build"):
        continue
    print(f"Starting {name}", flush=True)
    with (output / (name + ".log")).open("w", encoding="utf-8") as log:
        result = subprocess.run([sys.executable, str(repo / "scripts/compatibility/storage.py"),
                                 "--log", str(output / "storage.jsonl"),
                                 "--additional-bytes", "20000000000", "--", *command],
                                cwd=repo, stdout=log, stderr=subprocess.STDOUT)
    contents = (output / (name + ".log")).read_text(encoding="utf-8")
    print_log_tail(contents)
    if result.returncode:
        raise SystemExit(result.returncode)
    required = {"readonly-recovery": "3 passed; 0 failed", "lowlevel-writer": "33 passed; 0 failed",
                "windows-publication": "5 passed; 0 failed", "lsm-publication": "12 passed; 0 failed",
                "container-fence": "10 passed; 0 failed", "directory-publication": "8 passed; 0 failed",
                "overflow-garbage": "17 passed; 0 failed", "overflow-lifecycle": "4 passed; 0 failed",
                "public-overflow": "9 passed; 0 failed; 0 ignored",
                "public-journal": "2 passed; 0 failed; 0 ignored",
                "inline-lifecycle": "2 passed; 0 failed",
                "publication-boundaries": "12 passed; 0 failed; 0 ignored",
                "staging-recovery": "13 passed; 0 failed", "activation-staging": "4 passed; 0 failed",
                "memory-snapshots": "20 passed; 0 failed; 0 ignored",
                "hnsw-transaction-snapshots": "5 passed; 0 failed; 0 ignored"}
    if name in required and required[name] not in contents:
        raise SystemExit(f"{name}: expected native tests did not run")
