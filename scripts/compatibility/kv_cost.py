#!/usr/bin/env python3
"""Manual bounded KV cost comparison; no throughput, load, or CI gate."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "candidate-legacy", "candidate-overflow"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--baseline-sha", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "linux" or sys.flags.optimize:
        parser.error("requires Linux and assertions")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    cpu = min(os.sched_getaffinity(0))
    os.sched_setaffinity(0, {cpu})
    binaries = {name: getattr(args, name.replace("-", "_")).resolve()
                for name in ("baseline", "candidate-legacy", "candidate-overflow")}
    manifest = dict(binaries={k: hashlib.sha256(v.read_bytes()).hexdigest() for k, v in binaries.items()},
                    baseline_sha=args.baseline_sha, candidate_sha=args.candidate_sha, cpu=cpu,
                    rounds=5, warmup_rounds=1, cache="warm OS cache; fresh DB per case; no cache eviction",
                    shape="prior1 plus pending2, one commit, values16/2048 bytes, 4096-byte ring",
                    durability="EveryWrite, one writer, automatic checkpoint disabled",
                    policy="report median ratios; >1.20 is an investigation flag, never automatic acceptance",
                    scope="tiny KV cases only; excludes SQL, throughput and release qualification")
    (output / "plan.json").write_text(json.dumps(manifest, indent=2))
    rows = []
    for round_id in range(6):
        names = list(binaries)
        names = names[round_id % 3:] + names[:round_id % 3]
        for size in (16, 2048):
            for name in names:
                case = f"{round_id}-{size}-{name}"
                fixture = output / case
                fixture.mkdir()
                record = dict(case=case, engine=name, round=round_id, warmup=round_id == 0)
                try:
                    binary = binaries[name]
                    for phase in ("write", "verify"):
                        result = subprocess.run([str(binary), phase, str(fixture / "db"), str(size)],
                                                capture_output=True, text=True, timeout=30)
                        (output / (case + "." + phase + ".log")).write_text(result.stdout + result.stderr)
                        assert result.returncode == 0, (case, phase, result.stderr)
                        data = json.loads(result.stdout)
                        if phase == "write":
                            assert data["source_sha"] == (args.baseline_sha if name == "baseline" else args.candidate_sha)
                            assert data["overflow"] == (name == "candidate-overflow")
                            record.update(data)
                        else:
                            assert data["verified"]
                            record["reopen_ns"] = data["open_ns"]
                    record["verification"] = "passed"
                except Exception as error:
                    record.update(verification="failed", failure=repr(error))
                    raise
                finally:
                    with (output / "rows.jsonl").open("a") as stream:
                        stream.write(json.dumps(record) + "\n")
                    shutil.rmtree(fixture)
                rows.append(record)
    medians = {}
    for size in (16, 2048):
        for name in binaries:
            cell = [r for r in rows if not r["warmup"] and r["engine"] == name and r["value_bytes"] == size]
            assert len(cell) == 5
            medians[f"{size}-{name}"] = dict(status=cell[0]["status"], **{
                metric: statistics.median(r[metric] for r in cell)
                for metric in ("prepare_ns", "stage_ns", "commit_ns", "peak_rss_kib", "wal_bytes", "spool_bytes", "reopen_ns")})
    baseline = medians["16-baseline"]["commit_ns"]
    ratios = {name: medians["16-" + name]["commit_ns"] / baseline
              for name in ("candidate-legacy", "candidate-overflow")}
    report = dict(status="measured", plan=manifest, medians=medians, small_commit_ratios=ratios,
                  investigate=[name for name, ratio in ratios.items() if ratio > 1.20],
                  large_case="legacy capacity refusal is a capability gap; failure latency is not compared to success")
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
