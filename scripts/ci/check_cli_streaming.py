#!/usr/bin/env python3
"""Exercise real CLI output and its byte limit; fail on incomplete results."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
from pathlib import Path

ROWS = 15_000


def validate_output(output: str, output_format: str, rows: int = ROWS) -> None:
    if output_format == "jsonl":
        values = [json.loads(line) for line in output.splitlines()]
        expected = [{"id": i} for i in range(1, rows + 1)]
        if values != expected or any(type(row["id"]) is not int for row in values):
            raise ValueError(f"JSONL rows differ from expected IDs 1..{rows}")
    else:
        ids = []
        for line in output.splitlines():
            match = re.fullmatch(r"│\s*(\d+)\s*│", line)
            if match:
                ids.append(int(match[1]))
            elif not (
                re.fullmatch(r"│\s*id\s*│", line)
                or re.fullmatch(r"[┌┬┐└┴┘├┼┤─╌═╞╪╡]+", line)
            ):
                raise ValueError(f"Unexpected table output: {line[:120]}")
        if ids != list(range(1, rows + 1)):
            raise ValueError(f"Table rows differ from expected IDs 1..{rows}")


def run_sql(
    cli: Path,
    directory: Path,
    sql: str,
    output_format: str,
    *,
    expect_overflow: bool = False,
) -> str:
    sql_file = directory / "query.sql"
    sql_file.write_text(sql, encoding="utf-8")
    result = subprocess.run(
        [
            str(cli),
            "--data-dir",
            str(directory / "db"),
            "--batch",
            "--quiet",
            "--output",
            output_format,
            "sql",
            "--file",
            str(sql_file),
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if expect_overflow:
        if (
            result.returncode <= 0
            or result.stdout
            or "Buffer limit exceeded (~10MB)" not in result.stderr
        ):
            raise RuntimeError(
                f"Expected table byte-limit error: {result.returncode}; "
                f"{result.stderr[-2000:]}"
            )
    elif result.returncode != 0:
        raise RuntimeError(f"CLI failed ({result.returncode}): {result.stderr[-2000:]}")
    return result.stdout


def check(cli: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="alopex-cli-streaming-") as temporary:
        directory = Path(temporary)
        run_sql(
            cli, directory, "CREATE TABLE streamed (id INTEGER PRIMARY KEY);", "jsonl"
        )
        # Bound each parser AST; 15,000 individual statements in one input
        # exceed the wire decoder's 1 MiB limit before output is exercised.
        for start in range(1, ROWS + 1, 500):
            values = ",".join(f"({i})" for i in range(start, start + 500))
            run_sql(cli, directory, f"INSERT INTO streamed VALUES {values};", "jsonl")
        for output_format in ("table", "jsonl"):
            output = run_sql(
                cli, directory, "SELECT id FROM streamed ORDER BY id;", output_format
            )
            validate_output(output, output_format)
            print(
                f"PASS: {output_format}: all {ROWS} ordered IDs, no duplicates or omissions"
            )
        # Table output is byte bounded (10 MiB), not capped at 10,000 rows.
        run_sql(
            cli,
            directory,
            f"SELECT id, '{'x' * 1024}' AS payload FROM streamed;",
            "table",
            expect_overflow=True,
        )
        print("PASS: table over 10 MiB fails before emitting partial stdout")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cli", type=Path)
    args = parser.parse_args()
    check(args.cli.resolve())


if __name__ == "__main__":
    main()
