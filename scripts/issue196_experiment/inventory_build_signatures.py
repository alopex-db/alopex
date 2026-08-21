#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import shlex
from typing import Any, Iterable

from responsibility_graph import load_manifest


SCHEMA = "alopex-issue-196-build-signature-inventory-v1"
CARGO_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_-])cargo\s+(fmt|clippy|check|test|build|bench|doc|run|metadata|clean)\b"
)


@dataclass(frozen=True)
class CargoCommand:
    source: str
    line: int
    command: str


@dataclass(frozen=True)
class Signature:
    source: str
    line: int
    command: str
    verb: str
    packages: tuple[str, ...]
    workspace: bool
    profile: str
    features: tuple[str, ...]
    selectors: tuple[str, ...]
    locked: bool
    offline: bool
    exact_key: str
    near_key: str


def _cargo_from_text(source: Path, line: int, text: str) -> CargoCommand | None:
    stripped = text.strip()
    if not stripped or stripped.startswith(("#", "echo ", "printf ")):
        return None
    match = CARGO_PATTERN.search(stripped)
    if match is None:
        return None
    command = stripped[match.start() :]
    command = re.split(r"\s+(?:&&|\|\||;\s*(?:then)?\s*$)", command, maxsplit=1)[0]
    command = " ".join(command.replace("\\\n", " ").split())
    return CargoCommand(str(source), line, command)


def _yaml_run_blocks(source: Path, text: str) -> Iterable[CargoCommand]:
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        match = re.match(r"^(\s*)-?\s*run:\s*(.*?)\s*$", lines[index])
        if match is None:
            index += 1
            continue
        indent = len(match.group(1))
        value = match.group(2)
        start_line = index + 1
        if value in {"|", "|-", ">", ">-"}:
            block: list[tuple[int, str]] = []
            index += 1
            while index < len(lines):
                candidate = lines[index]
                if candidate.strip() and len(candidate) - len(candidate.lstrip()) <= indent:
                    break
                block.append((index + 1, candidate.strip()))
                index += 1
            if value.startswith(">"):
                cargo = _cargo_from_text(source, start_line, " ".join(part for _, part in block))
                if cargo is not None:
                    yield cargo
            else:
                logical = "\n".join(part for _, part in block).replace("\\\n", " ")
                for offset, command_line in enumerate(logical.splitlines(), start=start_line):
                    cargo = _cargo_from_text(source, offset, command_line)
                    if cargo is not None:
                        yield cargo
            continue
        cargo = _cargo_from_text(source, start_line, value)
        if cargo is not None:
            yield cargo
        index += 1


def _shell_logical_lines(text: str) -> list[tuple[int, str]]:
    logical: list[tuple[int, str]] = []
    buffer: list[str] = []
    start_line = 1
    for line_number, physical in enumerate(text.splitlines(), start=1):
        if not buffer:
            start_line = line_number
        stripped = physical.strip()
        continued = stripped.endswith("\\")
        buffer.append(stripped[:-1].rstrip() if continued else stripped)
        if not continued:
            logical.append((start_line, " ".join(part for part in buffer if part)))
            buffer = []
    if buffer:
        logical.append((start_line, " ".join(part for part in buffer if part)))
    return logical


def _shell_commands(source: Path, text: str) -> Iterable[CargoCommand]:
    logical = _shell_logical_lines(text)
    wrappers: dict[str, str] = {}
    wrapper_template_lines: set[int] = set()
    active_function: str | None = None
    for line, command_line in logical:
        definition = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\(\)\s*\{$", command_line)
        if definition is not None:
            active_function = definition.group(1)
            continue
        if active_function is not None and command_line == "}":
            active_function = None
            continue
        cargo = _cargo_from_text(source, line, command_line)
        if active_function is not None and cargo is not None and "$@" in cargo.command:
            wrappers[active_function] = re.sub(r"\s+[\"']?\$@[\"']?\s*$", "", cargo.command)
            wrapper_template_lines.add(line)

    for line, command_line in logical:
        if line in wrapper_template_lines:
            continue
        cargo = _cargo_from_text(source, line, command_line)
        if cargo is not None:
            yield cargo
            continue
        invocation = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s+(.+)$", command_line)
        if invocation is not None and invocation.group(1) in wrappers:
            expanded = f"{wrappers[invocation.group(1)]} {invocation.group(2)}"
            cargo = _cargo_from_text(source, line, expanded)
            if cargo is not None:
                yield cargo


def extract_cargo_commands(source: Path, text: str) -> list[CargoCommand]:
    if source.suffix in {".yml", ".yaml"}:
        return list(_yaml_run_blocks(source, text))
    return list(_shell_commands(source, text))


def _option_values(tokens: list[str], short: str, long: str) -> tuple[str, ...]:
    values: list[str] = []
    for index, token in enumerate(tokens):
        if token in {short, long} and index + 1 < len(tokens):
            values.append(tokens[index + 1])
        elif token.startswith(f"{long}="):
            values.append(token.split("=", 1)[1])
    return tuple(sorted(set(values)))


def normalize_signature(source: Path, line: int, command: str) -> Signature:
    tokens = shlex.split(command)
    if len(tokens) < 2 or tokens[0] != "cargo":
        raise ValueError(f"not a Cargo command: {command}")
    verb = tokens[1]
    packages = _option_values(tokens, "-p", "--package")
    features = tuple(
        sorted(
            {
                feature
                for value in _option_values(tokens, "-F", "--features")
                for feature in value.replace(",", " ").split()
            }
        )
    )
    selectors: list[str] = []
    for option in ("--test", "--bin", "--example", "--bench"):
        selectors.extend(f"{option}={value}" for value in _option_values(tokens, option, option))
    for flag in ("--tests", "--lib", "--doc", "--all-targets"):
        if flag in tokens:
            selectors.append(flag)
    if "--release" in tokens:
        profile = "release"
    else:
        profiles = _option_values(tokens, "--profile", "--profile")
        profile = profiles[0] if profiles else ("test" if verb in {"test", "bench"} else "dev")
    core = {
        "verb": verb,
        "packages": packages,
        "workspace": "--workspace" in tokens,
        "profile": profile,
        "features": features,
        "all_features": "--all-features" in tokens,
        "selectors": tuple(sorted(selectors)),
        "locked": "--locked" in tokens,
        "offline": "--offline" in tokens,
    }
    near = {"verb": verb, "profile": profile}
    return Signature(
        source=str(source),
        line=line,
        command=command,
        verb=verb,
        packages=packages,
        workspace=core["workspace"],
        profile=profile,
        features=features + (("*",) if core["all_features"] else ()),
        selectors=tuple(sorted(selectors)),
        locked=core["locked"],
        offline=core["offline"],
        exact_key=json.dumps(core, sort_keys=True),
        near_key=json.dumps(near, sort_keys=True),
    )


def classify_duplicate_groups(signatures: Iterable[Signature]) -> dict[str, list[dict[str, Any]]]:
    items = list(signatures)
    groups: dict[str, list[Signature]] = {}
    near_groups: dict[str, list[Signature]] = {}
    for signature in items:
        groups.setdefault(signature.exact_key, []).append(signature)
        near_groups.setdefault(signature.near_key, []).append(signature)

    def describe(key: str, values: list[Signature]) -> dict[str, Any]:
        return {
            "key": json.loads(key),
            "count": len(values),
            "commands": [
                {"source": value.source, "line": value.line, "command": value.command}
                for value in values
            ],
        }

    exact = [describe(key, values) for key, values in groups.items() if len(values) > 1]
    near = [
        describe(key, values)
        for key, values in near_groups.items()
        if len(values) > 1 and len({value.exact_key for value in values}) > 1
    ]
    return {
        "exact": sorted(exact, key=lambda group: (-group["count"], json.dumps(group["key"]))),
        "near": sorted(near, key=lambda group: (-group["count"], json.dumps(group["key"]))),
    }


def _matching_owners(signature: Signature, rules: list[dict[str, Any]]) -> list[str]:
    matches: list[str] = []
    default_owner: str | None = None
    for rule in rules:
        if rule.get("default"):
            default_owner = rule["owner"]
            continue
        if "verbs" in rule and signature.verb not in rule["verbs"]:
            continue
        if "command_regex" in rule and re.search(rule["command_regex"], signature.command) is None:
            continue
        if "source_regex" in rule and re.search(rule["source_regex"], signature.source) is None:
            continue
        matches.append(rule["owner"])
    if matches:
        return matches
    return [default_owner] if default_owner is not None else []


def build_inventory(root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    signatures: list[Signature] = []
    missing_sources: list[str] = []
    for source_name in manifest["inventory"]["sources"]:
        path = root / source_name
        if not path.is_file():
            missing_sources.append(source_name)
            continue
        for command in extract_cargo_commands(Path(source_name), path.read_text(encoding="utf-8")):
            signatures.append(normalize_signature(Path(command.source), command.line, command.command))

    rules = manifest["inventory"]["ownership_rules"]
    build_boundaries = manifest["inventory"]["source_build_boundaries"]
    records: list[dict[str, Any]] = []
    unowned: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    for signature in signatures:
        matches = _matching_owners(signature, rules)
        record = asdict(signature) | {
            "packages": list(signature.packages),
            "features": list(signature.features),
            "selectors": list(signature.selectors),
            "ownership_matches": matches,
            "owner": matches[0] if len(matches) == 1 else None,
            "build_boundary": build_boundaries[signature.source],
        }
        records.append(record)
        if not matches:
            unowned.append(record)
        elif len(matches) > 1:
            ambiguous.append(record)

    return {
        "schema": SCHEMA,
        "responsibility_graph_schema": manifest["schema"],
        "sources": manifest["inventory"]["sources"],
        "missing_sources": missing_sources,
        "owners": manifest["owners"],
        "signatures": records,
        "duplicate_groups": classify_duplicate_groups(signatures),
        "operational_side_effects": [
            record for record in records if record["verb"] == "clean"
        ],
        "unowned": unowned,
        "ambiguous": ambiguous,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Inventory issue #196 Cargo build signatures.")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    inventory = build_inventory(args.root, load_manifest(args.manifest))
    if inventory["missing_sources"] or inventory["unowned"] or inventory["ambiguous"]:
        raise SystemExit("build-signature inventory is incomplete")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    counts = {
        "signatures": len(inventory["signatures"]),
        "exact_duplicate_groups": len(inventory["duplicate_groups"]["exact"]),
        "near_duplicate_groups": len(inventory["duplicate_groups"]["near"]),
    }
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
