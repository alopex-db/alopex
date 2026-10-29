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


def validate(version: str, core: dict, python: dict, latest: dict) -> None:
    for value, tag in [(core, f"v{version}"), (python, f"alopex-py-v{version}")]:
        if value.get("tag_name") != tag or value.get("draft") is not False or value.get("prerelease") is not False:
            raise ValueError(f"expected published stable release {tag}")
        assets = value["assets"]
        names = [item["name"] for item in assets]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate asset names in {tag}")
        if any(item.get("state") != "uploaded" or item.get("size", 0) <= 0 for item in assets):
            raise ValueError(f"unfinished or empty assets in {tag}")
    if core["id"] == python["id"]:
        raise ValueError("Core and Python releases must remain independent")
    if (latest.get("id"), latest.get("tag_name")) != (core["id"], core["tag_name"]):
        raise ValueError("repository latest is not the requested Core stable release")
    if latest.get("draft") is not False or latest.get("prerelease") is not False:
        raise ValueError("repository latest is not a published stable release")
    core_assets = {item["name"]: item for item in core["assets"]}
    latest_assets = {item["name"]: item for item in latest.get("assets", [])}
    for name in sorted(CLI_ASSETS):
        if name not in core_assets or name not in latest_assets:
            raise ValueError(f"Core latest is missing CLI asset {name}")
        for field in ("id", "size", "state", "digest", "browser_download_url"):
            if core_assets[name].get(field) != latest_assets[name].get(field):
                raise ValueError(f"Core/latest asset identity mismatch: {name} {field}")
    names = {item["name"] for item in python["assets"]}
    if f"alopex-{version}.tar.gz" not in names or not any(
        name.startswith(f"alopex-{version}-") and name.endswith(".whl") for name in names
    ):
        raise ValueError("Python release is missing its versioned wheel or sdist")


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
    }
    try:
        core = release(args.repository, f"v{args.version}")
        python = release(args.repository, f"alopex-py-v{args.version}")
        latest = observe(args.repository)
        for name, value in [("core", core), ("python", python), ("latest", latest)]:
            record[name] = {key: value.get(key) for key in ("id", "tag_name", "html_url", "published_at")}
            record[name]["assets"] = asset_inventory(value)
        validate(args.version, core, python, latest)
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
