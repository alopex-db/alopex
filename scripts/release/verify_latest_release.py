#!/usr/bin/env python3
"""Read-only check of Core latest identity after independent Python publication.

Asset bytes are qualified by the promotion gates. This check records the live
release/asset identities and verifies availability, without changing metadata,
moving tags, or downloading/rebuilding artifacts.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess


CLI_ASSETS = {
    "alopex-linux-x86_64",
    "alopex-windows-x86_64.exe",
    "alopex-macos-aarch64",
    "alopex-macos-x86_64",
}
INTEL_CLI = "alopex-macos-x86_64"
ASSET_IDENTITY_FIELDS = ("id", "size", "state", "digest", "browser_download_url")


def required_cli_assets(version: str) -> set[str]:
    if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
        raise ValueError("version must be X.Y.Z")
    return CLI_ASSETS - {INTEL_CLI} if tuple(map(int, version.split("."))) >= (0, 8, 17) else set(CLI_ASSETS)


def optional_intel_asset(name: str, version: str) -> bool:
    """The additive inventory owned by #686, not frozen primary assets.

    Keep incomplete optional uploads observable without making them a primary
    delivery gate. The independent Intel publisher verifies the complete bundle.
    """
    if INTEL_CLI in required_cli_assets(version):
        return False
    if name in {INTEL_CLI, f"parser-supplement-v{version}-intel.json", f"intel-release-v{version}.json",
                "parser-smoke.log", "cli-smoke.log", "wheel-smoke.log", "crate-smoke.log"}:
        return True
    return re.fullmatch(
        rf"alopex-parser-v{re.escape(version)}-contract-[0-9]+\.[0-9]+\.[0-9]+-x86_64-apple-darwin\.(?:json|tar\.gz)", name
    ) is not None or re.fullmatch(
        rf"alopex-{re.escape(version)}-[A-Za-z0-9_.]+-[A-Za-z0-9_.]+-macosx_[0-9_]+_x86_64\.whl", name
    ) is not None


def intel_cli_observation(core_assets: dict, latest_assets: dict, required: bool) -> dict:
    core, observed = core_assets.get(INTEL_CLI), latest_assets.get(INTEL_CLI)
    if core is None and observed is None:
        return {"required": required, "status": "unavailable"}
    if core is None or observed is None or any(
        item.get("state") != "uploaded" or item.get("size", 0) <= 0 for item in (core, observed)
    ):
        return {"required": required, "status": "pending"}
    if any(core.get(field) != observed.get(field) for field in ASSET_IDENTITY_FIELDS):
        return {"required": required, "status": "inconsistent"}
    return {"required": required, "status": "available"}


def get(endpoint: str):
    result = subprocess.run(
        ["gh", "api", "--method", "GET", endpoint],
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)


def observe(repository: str) -> dict:
    """Observe latest separately; callers fetch the two exact tags as well."""
    return get(f"repos/{repository}/releases/latest")


def release(repository: str, tag: str) -> dict:
    value = get(f"repos/{repository}/releases/tags/{tag}")
    # The release's inline asset list is not used as an unbounded inventory.
    assets = []
    page = 1
    while True:
        batch = get(
            f"repos/{repository}/releases/{value['id']}/assets?per_page=100&page={page}"
        )
        assets.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    value["assets"] = assets
    return value


def asset_inventory(value: dict) -> list[dict]:
    fields = ("id", "name", "size", "state", "digest", "browser_download_url")
    return [{key: item.get(key) for key in fields} for item in value["assets"]]


def validate(version: str, core: dict, python: dict, latest: dict) -> dict:
    required = required_cli_assets(version)
    for value, tag in [(core, f"v{version}"), (python, f"alopex-py-v{version}")]:
        if value.get("tag_name") != tag or value.get("draft") is not False or value.get("prerelease") is not False:
            raise ValueError(f"expected published stable release {tag}")
        assets = value["assets"]
        names = [item["name"] for item in assets]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate asset names in {tag}")
        if any((item.get("state") != "uploaded" or item.get("size", 0) <= 0)
               and not optional_intel_asset(item["name"], version) for item in assets):
            raise ValueError(f"unfinished or empty assets in {tag}")
    if core["id"] == python["id"]:
        raise ValueError("Core and Python releases must remain independent")
    if (latest.get("id"), latest.get("tag_name")) != (core["id"], core["tag_name"]):
        raise ValueError("repository latest is not the requested Core stable release")
    if latest.get("draft") is not False or latest.get("prerelease") is not False:
        raise ValueError("repository latest is not a published stable release")
    core_assets = {item["name"]: item for item in core["assets"]}
    latest_assets = {item["name"]: item for item in latest.get("assets", [])}
    if len(latest_assets) != len(latest.get("assets", [])):
        raise ValueError("duplicate asset names in latest")
    for name in sorted(required):
        if name not in core_assets or name not in latest_assets:
            raise ValueError(f"Core latest is missing CLI asset {name}")
        for field in ASSET_IDENTITY_FIELDS:
            if core_assets[name].get(field) != latest_assets[name].get(field):
                raise ValueError(f"Core/latest asset identity mismatch: {name} {field}")
    names = {item["name"] for item in python["assets"] if not optional_intel_asset(item["name"], version)}
    if f"alopex-{version}.tar.gz" not in names or not any(
        name.startswith(f"alopex-{version}-") and name.endswith(".whl") for name in names
    ):
        raise ValueError("Python release is missing its versioned wheel or sdist")
    return intel_cli_observation(core_assets, latest_assets, INTEL_CLI in required)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository):
        parser.error("repository must be owner/name")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", args.version):
        parser.error("version must be X.Y.Z")
    record = {
        "repository": args.repository, "version": args.version,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "status": "failure", "responsibility": "release identity and asset availability",
        "required_cli_assets": sorted(required_cli_assets(args.version)),
    }
    try:
        core = release(args.repository, f"v{args.version}")
        python = release(args.repository, f"alopex-py-v{args.version}")
        latest = observe(args.repository)
        for name, value in [("core", core), ("python", python), ("latest", latest)]:
            record[name] = {key: value.get(key) for key in ("id", "tag_name", "html_url", "published_at")}
            record[name]["assets"] = asset_inventory(value)
        record["intel_cli"] = validate(args.version, core, python, latest)
        record["status"] = "success"
    except (ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        record["error"] = str(error)
        raise SystemExit(str(error)) from error
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(f"Core latest verified: v{args.version}; independent Python assets retained")


if __name__ == "__main__":
    main()
