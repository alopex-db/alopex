"""Owned #804-policy staging for one source-pinned #712 pair.

The server and worker run within the same isolated container. The returned
deleted Run is verified independently by the controller's supervisor.
"""
from __future__ import annotations

from pathlib import Path
import hashlib
import importlib
import json
import os
import shutil
import stat

from . import supervisor


def validate_stage_binding(config, output, proof, receipt):
    """Reject stale or malformed stage inputs before any runtime side effect."""
    if type(config) is not dict or type(proof) is not dict or type(receipt) is not dict:
        raise ValueError("PAIR_STAGE")
    source = config.get("source")
    assets = config.get("assets_digest")
    job_id = config.get("same_job_id")
    try:
        supervisor.validate_config(config, source, assets, job_id)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("PAIR_STAGE") from error
    profile = proof.get("profile")
    product = proof.get("product")
    accepted = proof.get("accepted_public")
    security = profile.get("security") if type(profile) is dict else None
    if (type(profile) is not dict or type(product) is not dict or
            type(accepted) is not dict or type(security) is not dict or
            proof.get("job_id") != job_id or receipt.get("job_id") != job_id or
            receipt.get("qualification_status") != "passed" or
            proof.get("temp_snapshot_verified") is not True or
            proof.get("image_digest") != receipt.get("image_digest") or
            proof.get("image_digest") != accepted.get("image_digest") or
            profile.get("run_id") != "hosted-804-" + job_id or
            profile.get("assets_digest") != assets or
            product.get("assets_digest") != assets or
            receipt.get("assets_digest") != assets or
            product.get("source_commit") != source["commit"] or
            product.get("source_tree") != source["tree"] or
            receipt.get("source_commit") != source["commit"] or
            receipt.get("source_tree") != source["tree"] or
            security.get("lsm_kind") != "apparmor" or
            security.get("lsm_profile") != "docker-default" or
            security.get("seccomp_sha256") !=
            receipt.get("security_profile_digest") or
            profile.get("storage") != {"stop_bytes": 40_000_000_000,
                                       "reserve_bytes": 1_000_000_000,
                                       "poll_seconds": 10} or
            proof.get("runs") != {
                name: {"phase": "deleted", "collection_verified": True,
                       "cleanup_errors": []}
                for name in ("primary", "repeat", "probe")}):
        raise ValueError("PAIR_STAGE")
    try:
        path = Path(output)
        info = path.lstat()
    except (OSError, TypeError) as error:
        raise ValueError("PAIR_STAGE") from error
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or
            stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("PAIR_STAGE")


def stage_pair(config, output, proof, receipt):
    """Refuse an unbound call; use make_stage_pair with reviewed inputs."""
    validate_stage_binding(config, output, proof, receipt)
    raise ValueError("PAIR_STAGE")


def _pinned_file(source, expected, destination, root):
    """Copy one named source file only after its bytes match the reviewed map."""
    source, destination, root = Path(source), Path(destination), Path(root)
    try:
        root_info = root.lstat()
        source_info = source.lstat()
        members = [part for part in (source, *source.parents)
                   if part == root or part.is_relative_to(root)]
        member_info = [part.lstat() for part in members]
    except OSError as error:
        raise ValueError("MISSING_PIN") from error
    if (not root.is_absolute() or root.is_symlink() or not root.is_dir() or
            root_info.st_uid != os.geteuid() or root_info.st_mode & 0o022 or
            not source.is_relative_to(root) or
            any(stat.S_ISLNK(info.st_mode) or info.st_uid != os.geteuid() or
                info.st_mode & 0o022 for info in member_info) or
            not supervisor._hex(expected, 64) or source.is_symlink() or
            not stat.S_ISREG(source_info.st_mode) or
            source_info.st_uid != os.geteuid() or source_info.st_mode & 0o022 or
            source_info.st_size > 2_000_000 or
            hashlib.sha256(source.read_bytes()).hexdigest() != expected or
            destination.exists()):
        raise ValueError("MISSING_PIN")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    destination.chmod(0o600)
    if hashlib.sha256(destination.read_bytes()).hexdigest() != expected:
        raise ValueError("MISSING_PIN")


def _extra_bundle(manifest, config, proof, profile, bundle, budget, *, repo,
                  runner_temp):
    """Stage only allowlisted Python/harness files under /input/four-case."""
    required = {"__init__", "adapter", "log_parser", "operation", "oracle",
                "runner", "worker", "supervisor", "public_attribution", "public_timing",
                "container_pair_entrypoint"}
    package = manifest.get("package_files")
    docs, product = manifest.get("docs_files"), manifest.get("product_files")
    if (type(package) is not dict or set(package) !=
            {f"four_case_diagnostic/{name}.py" for name in required} or
            type(docs) is not dict or not docs or
            type(product) is not dict or not product):
        raise ValueError("MISSING_PIN")
    repo, runner_temp = Path(repo), Path(runner_temp)
    package_root = repo / "scripts/ops"
    docs_root = runner_temp / "alopex-712-docs"
    try:
        repo_info = repo.lstat()
        docs_info = docs_root.lstat()
    except OSError as error:
        raise ValueError("MISSING_PIN") from error
    if (not repo.is_absolute() or repo.resolve(strict=True) != repo or
            not stat.S_ISDIR(repo_info.st_mode) or
            repo_info.st_uid != os.geteuid() or repo_info.st_mode & 0o022 or
            not runner_temp.is_absolute() or runner_temp.is_symlink() or
            not stat.S_ISDIR(docs_info.st_mode) or
            docs_info.st_uid != os.geteuid() or
            stat.S_IMODE(docs_info.st_mode) != 0o700):
        raise ValueError("MISSING_PIN")
    paths = {}
    for name, expected in package.items():
        paths[name] = (package_root / name, expected, package_root)
    for group, inventory in (("docs", docs), ("product", product)):
        for name, expected in inventory.items():
            if (type(name) is not str or not name or name.startswith("/") or
                    ".." in Path(name).parts or "\\" in name or
                    not name.endswith(".py")):
                raise ValueError("MISSING_PIN")
            root = docs_root if group == "docs" else repo
            paths[f"{group}/scripts/performance/{name}"] = (
                root / "scripts/performance" / name, expected, root)
    if len(paths) != len(package) + len(docs) + len(product):
        raise ValueError("MISSING_PIN")
    bundle.mkdir(mode=0o700)
    for name, (source, expected, root) in paths.items():
        _pinned_file(source, expected, bundle / name, root)
        budget.check()
    files = {name: expected for name, (_, expected, _) in paths.items()}
    data = {"schema": "alopex.sql-four-case-stage/v1", "config": config,
            "assets_digest": config["assets_digest"],
            "image_digest": proof["image_digest"],
            "profile_sha256": proof["profile_sha256"],
            "platform": profile["platform"], "files": files,
            "docs_files": docs, "product_files": product}
    path = bundle / "stage.json"
    with path.open("x") as stream:
        json.dump(data, stream, sort_keys=True, allow_nan=False)
        stream.write("\n")
    path.chmod(0o600)
    if sum(p.stat().st_size for p in bundle.rglob("*") if p.is_file()) > 100_000_000:
        raise ValueError("CAPACITY")
    return files


def _retain_error(primary, operation, secondary, common):
    if primary is None:
        return secondary
    common.record_cleanup_error(primary, operation, secondary)
    return primary


def _record_failure(run, error, common):
    try:
        common.atomic_json(run.root / "failure.json", {"error_code":
            error.args[0] if type(error) is ValueError and error.args else
            getattr(error, "code", "INTERNAL")})
    except BaseException as secondary:
        _retain_error(error, "persist_pair_failure", secondary, common)


def _remove_staging(run, primary, common):
    for name in ("extra", "extra-verified"):
        path = run.root / name
        try:
            if path.exists():
                shutil.rmtree(path)
        except BaseException as secondary:
            primary = _retain_error(primary, "remove_" + name, secondary, common)
    return primary


def run_in_container(config, pair_output, proof, receipt, *, manifest,
                     repo, runner_temp, operations):
    """Own one #804-policy container through stage, run, collection and deletion.

    All executable and harness inputs come from exact reviewed SHA maps. The
    caller must supply the independently authenticated proof and receipt.
    No final asset/image identity is built into this module.
    """
    validate_stage_binding(config, pair_output, proof, receipt)
    required_ops = {"read_json", "load_profile", "verify_assets", "source_identity",
                    "docker_factory", "verified_image", "run_factory",
                    "verify_temp_snapshot"}
    if type(manifest) is not dict or type(operations) is not dict or \
            set(operations) != required_ops:
        raise ValueError("MISSING_PIN")
    profile_path = Path(runner_temp) / "alopex-804-qualification/profile.json"
    profile_raw = supervisor._private_file(profile_path, 1_000_000)
    if hashlib.sha256(profile_raw).hexdigest() != proof["profile_sha256"]:
        raise ValueError("POLICY")
    profile = operations["load_profile"](profile_path)
    if (profile["run_id"] != proof["profile"]["run_id"] or
            profile["assets"] != str(Path(runner_temp) /
                                     "alopex-804-candidate/assets/assets.json") or
            hashlib.sha256(Path(profile["assets"]).read_bytes()).hexdigest() !=
            config["assets_digest"] or
            profile["security"]["seccomp_sha256"] !=
            receipt["security_profile_digest"] or
            profile["storage"]["stop_bytes"] != 40_000_000_000):
        raise ValueError("POLICY")
    ops_assets = operations["verify_assets"](profile["assets"], profile["platform"])
    if ops_assets["source_commit"] != config["source"]["commit"]:
        raise ValueError("MISSING_PIN")
    base_docker = operations["docker_factory"]

    class PairDocker(base_docker):
        def arguments(self, run_id, image, input_volume, work_volume, token):
            args = super().arguments(run_id, image, input_volume, work_volume, token)
            supervisor.require(args[-1] == image, "POLICY")
            return [*args[:-1], "--env", "PYTHONPATH=/input/four-case",
                    "--entrypoint", "python3", image, "-B", "-m",
                    "four_case_diagnostic.container_pair_entrypoint"]

        def validate_container(self, name):
            live = super().validate_container(name)
            supervisor.require(live["Config"]["Entrypoint"] == ["python3"] and
                               live["Config"]["Cmd"] == ["-B", "-m",
                                "four_case_diagnostic.container_pair_entrypoint"] and
                               "PYTHONPATH=/input/four-case" in live["Config"]["Env"],
                               "POLICY")
            return live

    docker = PairDocker(profile)
    image = operations["verified_image"](profile, docker)
    if image != "sha256:" + proof["image_digest"]:
        raise ValueError("IDENTITY")
    envctl = importlib.import_module("envctl")
    capacity = importlib.import_module("capacity")
    common = importlib.import_module("common")
    backend_module = importlib.import_module(base_docker.__module__)
    trusted_dir = Path(backend_module.__file__).resolve().parent
    if (not trusted_dir.is_dir() or
            any(Path(module.__file__).resolve().parent != trusted_dir
                for module in (envctl, capacity, common))):
        raise ValueError("OPERATOR_IDENTITY")
    selected = dict(profile, run_id=f"f712-{config['same_job_id']}-{config['case']}-{config['trial']}",
                    private_output=str(Path(pair_output) / "owned-runs"))
    Path(selected["private_output"]).mkdir(mode=0o700)
    budget = capacity.Budget(profile["storage"])
    budget.before("accept")
    run = operations["run_factory"](selected["private_output"], selected["run_id"])
    run.create({"image_digest": proof["image_digest"],
                "assets_digest": config["assets_digest"],
                "source": config["source"], "job_id": config["same_job_id"]})
    primary = None
    try:
        bundle = run.root / "extra"
        files = _extra_bundle(manifest, config, proof, profile, bundle, budget,
                              repo=repo, runner_temp=runner_temp)
        inputs = docker.create_volume(run, "input")
        work = docker.create_volume(run, "work")
        envctl.stage(selected, docker, budget, run, image, inputs, work, [])
        helper = "alopex-stage-" + run.run_id
        helper_item = next(item for item in run.record["resources"]
                           if item["kind"] == "container" and item["name"] == helper)
        supervisor.require(helper_item["id"], "CLEANUP")
        budget.run([*docker.prefix, "cp", str(bundle), helper + ":/input/four-case"],
                   timeout=profile["limits"]["case_seconds"], log=run.root / "extra-stage.log")
        budget.run([*docker.prefix, "start", "--attach", helper],
                   timeout=profile["limits"]["case_seconds"], log=run.root / "extra-stage.log",
                   cancel=lambda: docker.stop(helper))
        live = json.loads(docker.call("container", "inspect", helper))[0]
        supervisor.require(live["State"]["ExitCode"] == 0 and
                           not live["State"]["OOMKilled"], "POLICY")
        verified = run.root / "extra-verified"
        budget.run([*docker.prefix, "cp", helper + ":/input/four-case", str(verified)],
                   timeout=profile["limits"]["case_seconds"], log=run.root / "extra-verify.log")
        supervisor.require({p.relative_to(verified).as_posix() for p in verified.rglob("*")
                            if p.is_file()} == set(files) | {"stage.json"} and
                           all(not p.is_symlink() for p in verified.rglob("*")) and
                           all(hashlib.sha256((verified / name).read_bytes()).hexdigest() ==
                               value for name, value in files.items()) and
                           (verified / "stage.json").read_bytes() ==
                           (bundle / "stage.json").read_bytes(), "MISSING_PIN")
        shutil.rmtree(verified)
        shutil.rmtree(bundle)
        name = "alopex-test-" + run.run_id
        run.intent("container", name)
        identity = docker.call(*docker.arguments(run.run_id, image, inputs, work,
                                                run.record["ownership_token"]))
        run.resource("container", name, identity)
        docker.validate_container(name)
        run.transition("running")
        budget.run([*docker.prefix, "start", "--attach", name],
                   timeout=min(profile["limits"]["run_seconds"], supervisor.PAIR_SECONDS),
                   log=run.root / "container.log",
                   cancel=lambda: docker.stop(name))
        state = json.loads(docker.call("container", "inspect", name))[0]["State"]
        common.atomic_json(run.root / "exit.json", state)
        supervisor.require(state["ExitCode"] == 0 and not state["OOMKilled"], "PAIR_FAILED")
    except BaseException as error:
        primary = error
        _record_failure(run, error, common)
    finally:
        try:
            envctl.collect(selected, docker, budget, run.run_id)
            run.load()
            run.clean(docker.inspect, docker.remove)
            budget.check()
        except BaseException as error:
            primary = _retain_error(primary, "pair_collection_cleanup", error, common)
        primary = _remove_staging(run, primary, common)
    if primary is not None:
        raise primary
    return run


def make_stage_pair(manifest, repo, runner_temp, operations):
    """Bind reviewed paths and #804 operations to the controller callback."""
    return lambda config, output, proof, receipt: run_in_container(
        config, output, proof, receipt, manifest=manifest,
        repo=repo, runner_temp=runner_temp, operations=operations)
