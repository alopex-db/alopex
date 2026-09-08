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

SCHEMA = "alopex-release-verification/v1"
DIAGNOSTIC = re.compile(r"SKIP|ERROR|FAIL|FAILED|失敗", re.IGNORECASE)


def load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != SCHEMA:
        raise SystemExit(f"unsupported release verification schema: {payload.get('schema')!r}")
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
            "overall_status": "ok",
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "environment": {
                "package_source": "crates.io / PyPI",
                "rust": args.rust,
                "nim": args.nim,
                "python": "3.11",
            },
            "identity": {
                "commit": args.commit,
                "tag": args.tag,
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
    tail = lines[-60:]
    excerpt = diagnostics + [line for line in tail if line not in diagnostics]
    payload["steps"].append(
        {
            "name": args.name,
            "status": args.status,
            "description": args.description,
            "log_excerpt": excerpt,
            "diagnostics": diagnostics,
        }
    )
    if args.status == "fail":
        payload["overall_status"] = "fail"
    save(args.results, payload)


def render(args: argparse.Namespace) -> None:
    payload = load(args.results)
    version = payload["version"]
    status = payload["overall_status"]
    lines = [
        f"# リリース確認レポート: v{version}",
        "",
        f"> 総合結果: **{'✅ 全ステップ成功' if status == 'ok' else '❌ 失敗あり'}**",
        "",
    ]
    if status == "ok":
        lines.extend(
            [
                f"v{version} は、PyPIから完全一致wheelを取得し、隔離先への導入と",
                "最小importが成功している。既知機能・実行経路・性能の正しさは、",
                "対象commitのDevelopment CI / Extended Verificationが所有する。",
            ]
        )
    else:
        lines.append(f"v{version} の確認中に失敗したステップがある。詳細は下記を参照。")
    lines.extend(["", "## ステップ", ""])
    for index, step in enumerate(payload["steps"], start=1):
        mark = "✅" if step["status"] == "ok" else "❌"
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
            f"| 生成日時 (UTC) | {payload['generated_at']} |",
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
            f"| 責務層 | {identity.get('responsibility', 'unknown')} |",
            f"| 実行 | {identity.get('run_url', 'unknown')} |",
            "",
        ]
    )
    output = args.output_dir / f"v{version}.md"
    atomic_write(output, "\n".join(lines))
    print(output)


def validate_report_payload(payload: dict[str, Any]) -> None:
    if payload.get("overall_status") not in {"ok", "fail"}:
        raise SystemExit("release verification report has an invalid overall status")
    steps = payload.get("steps")
    if not isinstance(steps, list):
        raise SystemExit("release verification report steps are missing")
    statuses = {step.get("status") for step in steps if isinstance(step, dict)}
    if any(not isinstance(step, dict) for step in steps) or not statuses <= {"ok", "fail"}:
        raise SystemExit("release verification report contains an invalid step")
    has_failure = any(step.get("status") == "fail" for step in steps)
    if has_failure != (payload["overall_status"] == "fail"):
        raise SystemExit("release verification report status does not match its steps")


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
    initialize.add_argument("--run-url", default="unknown")
    initialize.add_argument("--responsibility", default="unknown")
    initialize.set_defaults(func=init)

    append = commands.add_parser("record")
    append.add_argument("--results", type=Path, required=True)
    append.add_argument("--name", required=True)
    append.add_argument("--status", choices=("ok", "fail"), required=True)
    append.add_argument("--description", required=True)
    append.add_argument("--log", type=Path, required=True)
    append.set_defaults(func=record)

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
