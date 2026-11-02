#!/usr/bin/env python3
"""Explicit, offline installation of a digest-pinned optional Intel parser.

This verifies asset identity, not native execution or publication qualification.
Obtain pins from the reviewed release record, never from the downloaded file itself.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import re
import shutil
import sys
import tempfile

if __package__:
    from . import parser_asset_manifest as assets
else:
    import parser_asset_manifest as assets

SCHEMA = "alopex-parser-supplement-v1"
FILENAME = "PARSER_SUPPLEMENT.json"
TARGET = "x86_64-apple-darwin"


def pinned_json(path: Path, pin: str, description: str) -> tuple[dict, bytes]:
    if re.fullmatch(r"[0-9a-f]{64}", pin) is None:
        raise assets.ParserAssetError(f"{description} requires a lowercase SHA-256 pin")
    content = assets.read_regular_file(
        path, description, max_bytes=assets.MAX_CANONICAL_JSON_BYTES
    )
    if assets.sha256_bytes(content) != pin:
        raise assets.ParserAssetError(f"{description} SHA-256 mismatch")
    value = assets.parse_json_bytes(content, description)
    if not isinstance(value, dict) or content != assets.canonical_json_bytes(value):
        raise assets.ParserAssetError(f"{description} must be a canonical JSON object")
    return value, content


def validate(supplement: dict, primary: dict, primary_bytes: bytes) -> dict:
    assets.require_exact_keys(
        supplement, {"schema", "primary_manifest_sha256", "source", "record"},
        "parser supplement",
    )
    if supplement["schema"] != SCHEMA:
        raise assets.ParserAssetError("wrong supplement schema")
    if supplement["primary_manifest_sha256"] != assets.sha256_bytes(primary_bytes):
        raise assets.ParserAssetError("supplement primary manifest SHA-256 mismatch")
    assets.validate_manifest_shape(primary)
    if any(asset["target"] == TARGET for asset in primary["assets"]):
        raise assets.ParserAssetError("supplement cannot replace a primary asset")
    source = supplement["source"]
    if not isinstance(source, dict):
        raise assets.ParserAssetError("supplement source must be an object")
    assets.require_exact_keys(source, {"tag", "tag_sha"}, "supplement source")
    if source["tag"] != f"v{primary['alopex_version']}":
        raise assets.ParserAssetError("supplement requires the matching stable tag")
    if not isinstance(source["tag_sha"], str) or re.fullmatch(r"[0-9a-f]{40}", source["tag_sha"]) is None:
        raise assets.ParserAssetError("supplement requires a peeled source SHA")
    record = supplement["record"]
    if not isinstance(record, dict):
        raise assets.ParserAssetError("supplement record must be an object")
    assets.validate_record_shape(record)
    if record["target"] != TARGET:
        raise assets.ParserAssetError("supplement target must be Intel Mac")
    common = assets.record_common_identity(record)
    if common != {key: primary[key] for key in common}:
        raise assets.ParserAssetError("supplement inputs differ from primary manifest")
    return record


def create(args: argparse.Namespace) -> None:
    primary, primary_bytes = pinned_json(Path(args.primary), args.primary_sha256, "primary manifest")
    envelope, _ = pinned_json(Path(args.envelope), args.envelope_sha256, "release envelope")
    manifest_identity = envelope.get("manifest")
    if (envelope.get("schema") != assets.RELEASE_ENVELOPE_SCHEMA
            or not isinstance(manifest_identity, dict)
            or manifest_identity.get("sha256") != args.primary_sha256
            or manifest_identity.get("size") != len(primary_bytes)
            or envelope.get("alopex_version") != primary["alopex_version"]
            or envelope.get("contract_version") != primary["contract_version"]):
        raise assets.ParserAssetError("release envelope does not bind the primary manifest")
    record = assets.load_canonical_json(Path(args.record), "Intel target record")
    supplement = {"schema": SCHEMA, "primary_manifest_sha256": args.primary_sha256,
                  "source": envelope.get("source"), "record": record}
    validate(supplement, primary, primary_bytes)
    assets.verify_record_archive(record, Path(args.archive))
    # Creation never replaces an earlier qualification result.
    with Path(args.output).open("xb") as output:
        output.write(assets.canonical_json_bytes(supplement))


def install(args: argparse.Namespace) -> None:
    primary, primary_bytes = pinned_json(Path(args.primary), args.primary_sha256, "primary manifest")
    supplement, content = pinned_json(Path(args.supplement), args.supplement_sha256, "parser supplement")
    record = validate(supplement, primary, primary_bytes)
    # Snapshot the bounded archive once. Both verification and installation use
    # these bytes, preventing a changed input file between validation and copy.
    archive = assets.read_regular_file(Path(args.archive), "Intel archive", max_bytes=assets.MAX_ARCHIVE_BYTES)
    destination = Path(args.destination)
    if destination.exists() or destination.is_symlink():
        raise assets.ParserAssetError("installation destination already exists")
    with tempfile.TemporaryDirectory(prefix="alopex-parser-install-", dir=destination.parent) as staging:
        staging = Path(staging)
        snapshot = staging / "archive.tar.gz"
        snapshot.write_bytes(archive)
        assets.verify_record_archive(record, snapshot)
        members = assets.read_normalized_archive(archive)
        ready = staging / "ready"
        ready.mkdir()
        for field in ("library", "static_library", "build_identity"):
            member = record[field]["path"]
            (ready / Path(member).name).write_bytes(members[member])
        (ready / "CONTRACT_VERSION").write_text(record["contract_version"] + "\n", encoding="utf-8")
        checksums = "".join(f"{record[field]['sha256']}  {Path(record[field]['path']).name}\n"
                            for field in ("library", "static_library"))
        (ready / "SHA256SUMS").write_text(checksums, encoding="utf-8")
        (ready / FILENAME).write_bytes(content)
        # mkdir is exclusive; concurrent installers cannot overwrite each other.
        # A failed copy removes only the directory created by this invocation.
        destination.mkdir()
        try:
            for path in sorted(ready.iterdir(), key=lambda path: path.name == FILENAME):
                shutil.copyfile(path, destination / path.name)
        except BaseException:
            shutil.rmtree(destination)
            raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command, function in (("create", create), ("install", install)):
        sub = commands.add_parser(command)
        for option in ("primary", "primary-sha256", "archive"):
            sub.add_argument(f"--{option}", required=True)
        for option in (("envelope", "envelope-sha256", "record", "output") if command == "create"
                       else ("supplement", "supplement-sha256", "destination")):
            sub.add_argument(f"--{option}", required=True)
        sub.set_defaults(function=function)
    args = parser.parse_args(argv)
    try:
        args.function(args)
    except (assets.ParserAssetError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
