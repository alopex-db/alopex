#!/usr/bin/env python3
"""Persist release verification results and render the public Markdown report."""

from __future__ import annotations

import argparse
import json
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "alopex-release-verification/v2"
DIAGNOSTIC = re.compile(r"SKIP|ERROR|FAIL|FAILED|失敗", re.IGNORECASE)
SKIP_CASE = re.compile(r"^\s*SKIP\s+\S", re.IGNORECASE)
SKIP_DETAIL = re.compile(r"^\s*###\s+.*\(SKIP\)\s*$", re.IGNORECASE)
SKIP_COUNT = re.compile(r"\bSKIP=(\d+)\b", re.IGNORECASE)


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != SCHEMA:
        raise SystemExit(
            f"unsupported release verification schema: {payload.get('schema')!r}"
        )
    return payload


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    temporary.replace(path)


def save(path: Path, payload: dict[str, Any]) -> None:
    atomic_write(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def init(args: argparse.Namespace) -> None:
    save(
        args.results,
        {
            "schema": SCHEMA,
            "version": args.version,
            "started_at": now(),
            "completed_at": None,
            "outcome": "incomplete",
            "failure_stage": None,
            "environment": {
                "package_source": "crates.io / PyPI",
                "rust": args.rust,
                "nim": args.nim,
                "python": "3.11",
            },
            "identity": {
                "commit": args.commit,
                "tag": args.tag,
                "run_id": args.run_id,
                "run_attempt": args.run_attempt,
                "run_url": args.run_url,
                "responsibility": args.responsibility,
            },
            "steps": [],
        },
    )


def record(args: argparse.Namespace) -> None:
    payload = load(args.results)
    lines = args.log.read_text(encoding="utf-8", errors="replace").splitlines()
    diagnostics = [line for line in lines if DIAGNOSTIC.search(line)]
    status = args.status
    if status == "success" and any(
        SKIP_CASE.search(line)
        or SKIP_DETAIL.search(line)
        or any(int(value) for value in SKIP_COUNT.findall(line))
        for line in diagnostics
    ):
        status = "incomplete"
    tail = lines[-60:]
    excerpt = diagnostics + [line for line in tail if line not in diagnostics]
    payload["steps"].append(
        {
            "name": args.name,
            "status": status,
            "description": args.description,
            "log_excerpt": excerpt,
            "diagnostics": diagnostics,
        }
    )
    payload["outcome"] = "incomplete"
    payload["failure_stage"] = None
    payload["completed_at"] = None
    save(args.results, payload)


def finalize(args: argparse.Namespace) -> None:
    payload = load(args.results)
    steps = payload["steps"]
    failed = next((step for step in steps if step["status"] == "failure"), None)
    incomplete = next((step for step in steps if step["status"] == "incomplete"), None)
    if failed:
        payload["outcome"] = "failure"
        payload["failure_stage"] = failed["name"]
    elif incomplete or not steps:
        payload["outcome"] = "incomplete"
        payload["failure_stage"] = (
            incomplete["name"] if incomplete else "workflow orchestration"
        )
    else:
        payload["outcome"] = "success"
        payload["failure_stage"] = None
    payload["completed_at"] = now()
    save(args.results, payload)


def render(args: argparse.Namespace) -> None:
    payload = load(args.results)
    version = payload["version"]
    status = payload["outcome"]
    summaries = {
        "success": "✅ 全ステップ成功",
        "failure": "❌ 失敗あり",
        "incomplete": "⚠️ 未完了",
    }
    lines = [
        f"# リリース確認レポート: v{version}",
        "",
        f"> 総合結果: **{summaries[status]}**",
        "",
    ]
    if status == "success":
        lines.extend(
            [
                f"v{version} は、PyPIから完全一致wheelを取得し、隔離先への導入と",
                "最小importが成功している。既知機能・実行経路・性能の正しさは、",
                "対象commitのDevelopment CI / Extended Verificationが所有する。",
            ]
        )
    elif status == "failure":
        lines.append(f"v{version} の確認中に失敗したステップがある。詳細は下記を参照。")
    else:
        lines.append(
            f"v{version} の確認は完了していない。完了済みの証跡と中断段階を下記に残す。"
        )
    lines.extend(["", "## ステップ", ""])
    for index, step in enumerate(payload["steps"], start=1):
        mark = {"success": "✅", "failure": "❌", "incomplete": "⚠️"}[
            step["status"]
        ]
        lines.extend(
            [
                f"### {index}. {step['name']} {mark}",
                "",
                step["description"],
                "",
            ]
        )
        if step["log_excerpt"]:
            lines.extend(["```", *step["log_excerpt"], "```", ""])
    environment = payload["environment"]
    identity = payload.get("identity", {})
    lines.extend(
        [
            "---",
            "",
            "## 検証環境",
            "",
            "| 項目 | 値 |",
            "|---|---|",
            f"| 対象バージョン | v{version} |",
            f"| 開始日時 (UTC) | {payload['started_at']} |",
            f"| 終了日時 (UTC) | {payload['completed_at'] or 'incomplete'} |",
            f"| パッケージ取得元 | {environment['package_source']} |",
            "| ソースビルド | なし(公開パッケージのみ使用) |",
            f"| Rust | `{environment['rust']}` |",
            f"| Nim(ビルド専用イメージ) | `{environment['nim']}` |",
            f"| Python | `{environment['python']}` |",
            "",
            "## 証跡",
            "",
            "| 項目 | 値 |",
            "|---|---|",
            f"| Commit | `{identity.get('commit', 'unknown')}` |",
            f"| Tag | `{identity.get('tag', 'unknown')}` |",
            f"| Run | `{identity.get('run_id', 'unknown')}` / attempt "
            f"`{identity.get('run_attempt', 'unknown')}` |",
            f"| 責務層 | {identity.get('responsibility', 'unknown')} |",
            f"| 失敗段階 | {payload.get('failure_stage') or 'なし'} |",
            f"| 実行 | {identity.get('run_url', 'unknown')} |",
            "",
        ]
    )
    output = args.output_dir / f"v{version}.md"
    atomic_write(output, "\n".join(lines))
    print(output)


def validate_report_payload(payload: dict[str, Any]) -> None:
    outcomes = {"success", "failure", "incomplete"}
    if payload.get("outcome") not in outcomes:
        raise SystemExit("release verification report has an invalid outcome")
    if not payload.get("started_at") or not payload.get("completed_at"):
        raise SystemExit("release verification report timing is incomplete")
    identity = payload.get("identity")
    if (
        not isinstance(identity, dict)
        or not isinstance(identity.get("run_id"), str)
        or not identity["run_id"]
        or not isinstance(identity.get("run_attempt"), int)
        or identity["run_attempt"] < 1
    ):
        raise SystemExit("release verification report run identity is incomplete")
    for field in ("commit", "tag", "run_url", "responsibility"):
        if not isinstance(identity.get(field), str) or not identity[field]:
            raise SystemExit(f"release verification report identity lacks {field}")
    steps = payload.get("steps")
    if not isinstance(steps, list):
        raise SystemExit("release verification report steps are missing")
    if any(
        not isinstance(step, dict)
        or not isinstance(step.get("name"), str)
        or not step["name"]
        or step.get("status") not in outcomes
        for step in steps
    ):
        raise SystemExit("release verification report contains an invalid step")
    statuses = {step["status"] for step in steps}
    expected = (
        "failure"
        if "failure" in statuses
        else "incomplete"
        if "incomplete" in statuses or not steps
        else "success"
    )
    if payload["outcome"] != expected:
        raise SystemExit("release verification report outcome does not match its steps")
    expected_stage = next(
        (
            step["name"]
            for step in steps
            if step["status"] == expected and expected != "success"
        ),
        "workflow orchestration" if not steps else None,
    )
    if payload.get("failure_stage") != expected_stage:
        raise SystemExit("release verification report failure stage does not match its outcome")


def validate_report(args: argparse.Namespace) -> None:
    validate_report_payload(load(args.results))
    print("release verification report is complete")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    initialize = commands.add_parser("init")
    initialize.add_argument("--results", type=Path, required=True)
    initialize.add_argument("--version", required=True)
    initialize.add_argument("--rust", required=True)
    initialize.add_argument("--nim", required=True)
    initialize.add_argument("--commit", default="unknown")
    initialize.add_argument("--tag", default="unknown")
    initialize.add_argument("--run-id", default="unknown")
    initialize.add_argument("--run-attempt", type=int, default=1)
    initialize.add_argument("--run-url", default="unknown")
    initialize.add_argument("--responsibility", default="unknown")
    initialize.set_defaults(func=init)

    append = commands.add_parser("record")
    append.add_argument("--results", type=Path, required=True)
    append.add_argument("--name", required=True)
    append.add_argument(
        "--status", choices=("success", "failure", "incomplete"), required=True
    )
    append.add_argument("--description", required=True)
    append.add_argument("--log", type=Path, required=True)
    append.set_defaults(func=record)

    complete = commands.add_parser("finalize")
    complete.add_argument("--results", type=Path, required=True)
    complete.set_defaults(func=finalize)

    markdown = commands.add_parser("render")
    markdown.add_argument("--results", type=Path, required=True)
    markdown.add_argument("--output-dir", type=Path, required=True)
    markdown.set_defaults(func=render)

    report = commands.add_parser("validate-report")
    report.add_argument("--results", type=Path, required=True)
    report.set_defaults(func=validate_report)
    return root


if __name__ == "__main__":
    arguments = parser().parse_args()
    arguments.func(arguments)
