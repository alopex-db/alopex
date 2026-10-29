#!/usr/bin/env python3
"""Reject parser wire-schema changes that retain the old contract version."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys


CONTRACT_PATH = "crates/alopex-sql/nim-sql-parser/PARSER_CONTRACT_VERSION"
CONSUMER_PATH = "crates/alopex-sql/build_support.rs"
MANIFEST_TOOL_PATH = "scripts/release/parser_asset_manifest.py"
WIRE_FILES = {
    "crates/alopex-sql/nim-sql-parser/src/alopex_sql_parser.nim",
    "crates/alopex-sql/nim-sql-parser/src/ast.nim",
    "crates/alopex-sql/nim-sql-parser/src/promql_ast.nim",
    "crates/alopex-sql/src/nim_bridge.rs",
}
WIRE_PREFIX = "crates/alopex-sql/src/ast/"
AST_MODULE = WIRE_PREFIX + "mod.rs"
VERSION_PATTERN = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")


def git(*args: str) -> bytes:
    return subprocess.run(
        ["git", *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).stdout


def source_at(revision: str, path: str) -> str:
    return git("show", f"{revision}:{path}").decode("utf-8")


def version_at(revision: str) -> tuple[int, int, int]:
    value = source_at(revision, CONTRACT_PATH).strip()
    if not VERSION_PATTERN.fullmatch(value):
        raise ValueError(f"invalid parser contract version at {revision}: {value!r}")
    return tuple(int(part) for part in value.split("."))


def declared_version(source: str, name: str) -> tuple[int, int, int]:
    match = re.search(
        rf'^\s*{re.escape(name)}\s*(?::\s*[^=]+)?=\s*"([0-9]+\.[0-9]+\.[0-9]+)"',
        source,
        re.MULTILINE,
    )
    if match is None:
        raise ValueError(f"missing parser contract declaration: {name}")
    return tuple(int(part) for part in match.group(1).split("."))


def wire_related(path: str) -> bool:
    return path in WIRE_FILES or path.startswith(WIRE_PREFIX)


def without_write_classifier_body(source: str) -> str:
    """Mask only the existing non-serialized method, failing closed on ambiguity.

    This is deliberately not a Rust parser or a general impl/test exemption.
    Only brace-balanced identifier/punctuation expressions are accepted; strings,
    comments, attributes and unfamiliar syntax retain the original source.
    Every byte outside this one method body still participates in comparison.
    """
    signature = "    pub fn requires_write(&self) -> bool {"
    if source.count(signature) != 1:
        return source
    start = source.index(signature) + len(signature)
    depth = 1
    for end in range(start, len(source)):
        token = source[end]
        if token == "{":
            depth += 1
        elif token == "}":
            depth -= 1
            if depth == 0:
                body = source[start:end]
                if re.fullmatch(r"[A-Za-z0-9_\s:.,{}()&*!=>?;+\-]*", body):
                    return source[:start] + " <runtime-write-classifier> " + source[end:]
                return source
    return source


def runtime_only_ast_change(before: str, after: str) -> bool:
    return without_write_classifier_body(before) == without_write_classifier_body(after)


def check(base: str, head: str) -> list[str]:
    changed = git("diff", "--no-renames", "--name-only", "-z", base, head).decode(
        "utf-8"
    ).split("\0")
    wire_changes = sorted(path for path in changed if wire_related(path))
    if AST_MODULE in wire_changes and runtime_only_ast_change(
        source_at(base, AST_MODULE), source_at(head, AST_MODULE)
    ):
        wire_changes.remove(AST_MODULE)
    old_version = version_at(base)
    new_version = version_at(head)
    consumer_version = declared_version(
        source_at(head, CONSUMER_PATH), "pub(crate) const REQUIRED_CONTRACT_VERSION"
    )
    manifest_version = declared_version(
        source_at(head, MANIFEST_TOOL_PATH), "REQUIRED_CONTRACT_VERSION"
    )
    if consumer_version != new_version or manifest_version != new_version:
        raise ValueError(
            "parser contract declarations disagree: "
            f"source={new_version}, Rust={consumer_version}, "
            f"manifest tool={manifest_version}"
        )
    if wire_changes and new_version <= old_version:
        raise ValueError(
            "parser wire schema changed without a contract version increase "
            f"({old_version} -> {new_version}): " + ", ".join(wire_changes)
        )
    return wire_changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", help="base commit SHA")
    parser.add_argument("head", help="head commit SHA")
    args = parser.parse_args()
    try:
        changed = check(args.base, args.head)
    except (subprocess.CalledProcessError, UnicodeError, ValueError) as error:
        print(f"parser wire contract check failed: {error}", file=sys.stderr)
        return 1
    print(f"parser wire contract check passed: {len(changed)} wire file(s) changed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
