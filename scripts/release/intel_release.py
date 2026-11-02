#!/usr/bin/env python3
"""Bind optional Intel delivery to immutable Core assets; never overwrite assets."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
from urllib.request import urlopen

if __package__:
    from . import candidate_manifest as candidate_format
    from . import parser_asset_manifest as parser
    from . import parser_supplement as supplement
else:
    import candidate_manifest as candidate_format
    import parser_asset_manifest as parser
    import parser_supplement as supplement

REPO = "alopex-db/alopex"
TARGET = "x86_64-apple-darwin"
SCHEMA = "alopex-intel-release-v1"
LOGS = ("parser-smoke.log", "cli-smoke.log", "wheel-smoke.log", "crate-smoke.log")


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def api(endpoint):
    return json.loads(run("gh", "api", f"repos/{REPO}/{endpoint}"))


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def write(path, value):
    path.write_bytes(parser.canonical_json_bytes(value))


def identity(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"not a regular artifact: {path}")
    return {"name": path.name, "size": path.stat().st_size, "sha256": digest(path)}


def tag_source(tag):
    if re.fullmatch(r"(?:alopex-py-)?v[0-9]+\.[0-9]+\.[0-9]+(?:-rc\.[1-9][0-9]*)?", tag) is None:
        raise ValueError("invalid release tag")
    ref = api(f"git/ref/tags/{tag}")["object"]
    if ref["type"] != "tag":
        raise ValueError("release tag must be annotated")
    obj = api(f"git/tags/{ref['sha']}")["object"]
    if obj["type"] != "commit" or re.fullmatch(r"[0-9a-f]{40}", obj["sha"]) is None:
        raise ValueError("release tag must peel directly to a commit")
    return obj["sha"]


def release(tag):
    result = api(f"releases/tags/{tag}")
    if result["tag_name"] != tag or result["draft"] or result["prerelease"]:
        raise ValueError("stable public release is not available yet; retry later")
    # The release response may truncate its asset collection; fetch every page.
    result["assets"] = []
    page = 1
    while True:
        batch = api(f"releases/{result['id']}/assets?per_page=100&page={page}")
        result["assets"].extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return result


def download(tag, name, directory, assets, limit):
    records = [a for a in assets if a["name"] == name]
    if len(records) != 1 or records[0]["state"] != "uploaded" or not 0 < records[0]["size"] <= limit:
        raise ValueError(f"missing, duplicate, or oversized release asset: {name}")
    path = directory / name
    run("gh", "release", "download", tag, "--repo", REPO, "--pattern", name, "--dir", str(directory))
    if path.is_symlink() or path.stat().st_size != records[0]["size"]:
        raise ValueError(f"download size mismatch: {name}")
    return path


def prepare(args):
    if re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", args.tag) is None:
        raise ValueError("Intel delivery requires a stable Core tag")
    version = args.tag[1:]
    source = tag_source(args.tag)
    if source != args.source_sha or run("git", "rev-parse", "HEAD") != source:
        raise ValueError("Intel checkout/workflow must be the immutable Core source")
    if version != parser.REQUIRED_ALOPEX_VERSION:
        raise ValueError("source version differs from Core tag")
    args.context.mkdir()  # Fresh snapshot, never replace prior evidence.
    core = release(args.tag)
    candidate_path = download(args.tag, f"candidate-manifest-v{version}.json", args.context, core["assets"], 2 * 1024 * 1024)
    candidate = json.loads(candidate_path.read_bytes())
    if (set(candidate) != {"schema", "version", "rc_tag", "commit_sha", "verification", "artifacts"}
            or not candidate_format._verification_from_manifest(candidate.get("verification"))
            or candidate.get("schema") != "alopex-release-candidate-v1"
            or candidate.get("commit_sha") != source or candidate.get("version") != version
            or re.fullmatch(rf"v{re.escape(version)}-rc\.[1-9][0-9]*", candidate.get("rc_tag", "")) is None
            or tag_source(candidate["rc_tag"]) != source):
        raise ValueError("Core candidate identity differs from the stable source")
    entries = {item["name"]: item for item in candidate["artifacts"]}
    if len(entries) != len(candidate["artifacts"]):
        raise ValueError("frozen candidate has duplicate artifacts")
    paths = []
    for name in (f"parser-vendor-manifest-v{version}.json", f"parser-assets-v{version}.json"):
        path = download(args.tag, name, args.context, core["assets"], parser.MAX_CANONICAL_JSON_BYTES)
        if identity(path) != entries.get(name):
            raise ValueError("Core parser bytes differ from frozen candidate manifest")
        paths.append(path)
    primary, envelope_path = paths
    manifest = parser.load_canonical_json(primary, "primary manifest")
    parser.validate_manifest_shape(manifest)
    if any(a["target"] == TARGET for a in manifest["assets"]):
        raise ValueError("Intel is already in the frozen primary manifest")
    envelope = parser.load_canonical_json(envelope_path, "RC parser envelope")
    if (envelope.get("source") != {"tag": candidate["rc_tag"], "tag_sha": source}
            or envelope.get("manifest", {}).get("sha256") != digest(primary)):
        raise ValueError("RC envelope does not bind the same parser and source")
    # Preserve the RC bytes. A separate local envelope binds the same inputs to
    # the stable tag; only the supplement is published as new metadata.
    stable_envelope = dict(envelope, source={"tag": args.tag, "tag_sha": source})
    write(args.context / "stable-parser-envelope.json", stable_envelope)
    write(args.context / "context.json", {"tag": args.tag, "source_sha": source, "version": version,
          "primary": primary.name, "primary_sha256": digest(primary),
          "candidate_sha256": digest(candidate_path)})


def context(path):
    value = json.loads((path / "context.json").read_bytes())
    if digest(path / value["primary"]) != value["primary_sha256"]:
        raise ValueError("primary context bytes changed")
    return value


def stage(args):
    ctx = context(args.context)
    records = sorted(args.parser_dir.glob(f"*{TARGET}.json"))
    if len(records) != 1:
        raise ValueError("expected exactly one Intel target record")
    record = parser.load_canonical_json(records[0], "Intel target record")
    archive = args.parser_dir / record["archive"]["filename"]
    args.bundle.mkdir()
    extra = args.bundle / f"parser-supplement-v{ctx['version']}-intel.json"
    envelope = args.context / "stable-parser-envelope.json"
    supplement.create(argparse.Namespace(primary=args.context / ctx["primary"], primary_sha256=ctx["primary_sha256"],
        envelope=envelope, envelope_sha256=digest(envelope), record=records[0], archive=archive, output=extra))
    supplement.install(argparse.Namespace(primary=args.context / ctx["primary"], primary_sha256=ctx["primary_sha256"],
        supplement=extra, supplement_sha256=digest(extra), archive=archive, destination=args.native))
    shutil.copyfile(records[0], args.bundle / records[0].name)
    shutil.copyfile(archive, args.bundle / archive.name)
    # Wheel receives the same dynamic library, with wheel-specific sidecars.
    package_native = Path("crates/alopex-py/python/alopex/native")
    package_native.mkdir(parents=True, exist_ok=True)
    for path in package_native.iterdir():
        if path.is_file():
            path.unlink()
    name = Path(record["library"]["path"]).name
    shutil.copyfile(args.native / name, package_native / name)
    (package_native / "CONTRACT_VERSION").write_text(record["contract_version"] + "\n")
    (package_native / "SHA256SUMS").write_text(f"{record['library']['sha256']}  {name}\n")
    print(digest(extra))


def wheel_names(names, version):
    pattern = rf"alopex-{re.escape(version)}-[A-Za-z0-9_.]+-[A-Za-z0-9_.]+-macosx_[0-9_]+_x86_64\.whl"
    result = [name for name in names if re.fullmatch(pattern, name)]
    if len(result) != 1:
        raise ValueError("expected exactly one Intel-only wheel")
    return result


def verify_bundle(bundle, ctx):
    manifest_path = bundle / f"intel-release-v{ctx['version']}.json"
    value = json.loads(manifest_path.read_bytes())
    if (value.get("schema") != SCHEMA or value.get("source") != {"tag": ctx["tag"], "sha": ctx["source_sha"]}
            or value.get("primary_manifest_sha256") != ctx["primary_sha256"] or value.get("target") != TARGET
            or value.get("candidate_sha256") != ctx["candidate_sha256"]):
        raise ValueError("Intel bundle source or frozen primary identity mismatch")
    actual = [identity(p) for p in sorted(bundle.iterdir()) if p != manifest_path]
    if actual != value.get("artifacts"):
        raise ValueError("Intel bundle artifact bytes or inventory changed")
    names = {a["name"] for a in actual}
    wheel = wheel_names(names, ctx["version"])[0]
    record_names = [n for n in names if n.startswith("alopex-parser-") and n.endswith(f"{TARGET}.json")]
    if len(record_names) != 1:
        raise ValueError("Intel record missing or duplicate")
    record = parser.load_canonical_json(bundle / record_names[0], "Intel record")
    parser.verify_record_archive(record, bundle / record["archive"]["filename"])
    extra = parser.load_canonical_json(bundle / f"parser-supplement-v{ctx['version']}-intel.json", "supplement")
    if (extra.get("record") != record or extra.get("primary_manifest_sha256") != ctx["primary_sha256"]
            or extra.get("source") != {"tag": ctx["tag"], "tag_sha": ctx["source_sha"]}):
        raise ValueError("supplement differs from the qualified target record or source")
    expected = {"alopex-macos-x86_64", wheel, record_names[0], record["archive"]["filename"],
                f"parser-supplement-v{ctx['version']}-intel.json", *LOGS}
    if names != expected or any((bundle / log).stat().st_size == 0 for log in LOGS):
        raise ValueError("Intel native qualification inventory is incomplete")
    if type(value.get("qualification_run_id")) is not int or value["qualification_run_id"] <= 0:
        raise ValueError("native qualification run identity missing")
    return value, wheel


def seal(args):
    if platform.system() != "Darwin" or platform.machine().lower() not in ("x86_64", "amd64"):
        raise ValueError("Intel qualification must execute on native Intel macOS")
    ctx = context(args.context)
    if (args.bundle / f"intel-release-v{ctx['version']}.json").exists():
        raise ValueError("qualification is already sealed; reuse its artifact")
    value = {"schema": SCHEMA, "source": {"tag": ctx["tag"], "sha": ctx["source_sha"]}, "target": TARGET,
             "primary_manifest_sha256": ctx["primary_sha256"], "candidate_sha256": ctx["candidate_sha256"],
             "qualification_run_id": args.run_id, "artifacts": [identity(p) for p in sorted(args.bundle.iterdir())]}
    write(args.bundle / f"intel-release-v{ctx['version']}.json", value)
    verify_bundle(args.bundle, ctx)


def qualification_run(run_id, source_sha):
    record = api(f"actions/runs/{run_id}")
    if (record["head_sha"] != source_sha or record["path"] != ".github/workflows/alopex-py-release.yml"
            or record["repository"]["full_name"] != REPO or record["event"] != "workflow_dispatch"):
        raise ValueError("qualification run is not bound to the Intel source/workflow")
    jobs = api(f"actions/runs/{run_id}/jobs?per_page=100")["jobs"]
    if not any(j["name"] == "Qualify optional Intel artifacts" and j["conclusion"] == "success" for j in jobs):
        raise ValueError("Intel qualification job has not succeeded")


def missing_github(tag, expected):
    published = release(tag)
    by_name = {}
    for asset in published["assets"]:
        if asset["name"] in by_name:
            raise ValueError("duplicate published asset name")
        by_name[asset["name"]] = asset
    missing = []
    for item in expected:
        found = by_name.get(item["name"])
        if found is None:
            missing.append(item["name"])
            continue
        if found["size"] != item["size"] or found["state"] != "uploaded":
            raise ValueError(f"existing GitHub asset differs: {item['name']}")
        remote_digest = found.get("digest")
        if remote_digest is None:
            with tempfile.TemporaryDirectory(prefix="alopex-intel-compare-") as tmp:
                path = download(tag, item["name"], Path(tmp), published["assets"], item["size"])
                remote_digest = "sha256:" + digest(path)
        if remote_digest != "sha256:" + item["sha256"]:
            raise ValueError(f"existing GitHub asset differs: {item['name']}")
    return missing


def missing_pypi(version, item):
    with urlopen(f"https://pypi.org/pypi/alopex/{version}/json", timeout=30) as response:
        content = response.read(4 * 1024 * 1024 + 1)
    if len(content) > 4 * 1024 * 1024:
        raise ValueError("PyPI metadata exceeds bound")
    payload = json.loads(content)
    if payload.get("info", {}).get("version") != version:
        raise ValueError("primary PyPI version is not available")
    found = [p for p in payload.get("urls", []) if p["filename"] == item["name"]]
    if len(found) > 1 or (found and (found[0]["digests"]["sha256"] != item["sha256"] or found[0]["size"] != item["size"])):
        raise ValueError("existing PyPI wheel differs from qualified bytes")
    return not found


def publication(args):
    ctx = context(args.context)
    value, wheel = verify_bundle(args.bundle, ctx)
    if value["qualification_run_id"] != args.run_id:
        raise ValueError("downloaded artifact is not from the requested qualification run")
    qualification_run(value["qualification_run_id"], ctx["source_sha"])
    if tag_source(ctx["tag"]) != ctx["source_sha"] or tag_source(f"alopex-py-v{ctx['version']}") != ctx["source_sha"]:
        raise ValueError("Core/Python tags changed or differ from qualified source")
    extra = parser.load_canonical_json(args.bundle / f"parser-supplement-v{ctx['version']}-intel.json", "supplement")
    primary_bytes = (args.context / ctx["primary"]).read_bytes()
    supplement.validate(extra, json.loads(primary_bytes), primary_bytes)
    expected = value["artifacts"] + [identity(args.bundle / f"intel-release-v{ctx['version']}.json")]
    wheel_item = next(item for item in expected if item["name"] == wheel)
    # Validate both GitHub surfaces and PyPI before performing any publication.
    core_missing = missing_github(ctx["tag"], expected)
    python_tag = f"alopex-py-v{ctx['version']}"
    python_missing = missing_github(python_tag, [wheel_item])
    pypi_missing = missing_pypi(ctx["version"], wheel_item)
    if args.command == "plan":
        args.upload.mkdir()
        if pypi_missing:
            shutil.copyfile(args.bundle / wheel, args.upload / wheel)
        print("publish=" + str(pypi_missing).lower())
        return
    if pypi_missing:
        raise ValueError("qualified Intel wheel is not yet verified on PyPI")
    for tag, names in ((ctx["tag"], core_missing), (python_tag, python_missing)):
        for name in names:
            # No --clobber: a concurrent upload fails safely; retry compares bytes.
            run("gh", "release", "upload", tag, str(args.bundle / name), "--repo", REPO)
    if missing_github(ctx["tag"], expected) or missing_github(python_tag, [wheel_item]):
        raise ValueError("Intel GitHub publication remains incomplete")


def main():
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    for command, function in (("prepare", prepare), ("stage", stage), ("seal", seal), ("plan", publication), ("publish", publication)):
        sub = commands.add_parser(command)
        sub.add_argument("--context", type=Path, required=True)
        if command == "prepare":
            sub.add_argument("--tag", required=True)
            sub.add_argument("--source-sha", required=True)
        else:
            sub.add_argument("--bundle", type=Path, required=True)
        if command == "stage":
            sub.add_argument("--parser-dir", type=Path, required=True)
            sub.add_argument("--native", type=Path, required=True)
        if command in ("seal", "plan", "publish"):
            sub.add_argument("--run-id", type=int, required=True)
        if command == "plan":
            sub.add_argument("--upload", type=Path, required=True)
        sub.set_defaults(function=function)
    args = root.parse_args()
    args.function(args)


if __name__ == "__main__":
    main()
