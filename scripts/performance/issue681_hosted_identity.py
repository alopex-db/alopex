"""Admit one selected #681 hosted job; emit fixed public outcomes only.

UC-681-SELECTED-HOSTED-ADMISSION. This module does not acquire build inputs or
create an artifact. Network responses and credentials stay in memory.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

import issue681_hosted_qualify as qualify
import issue681_hosted_rust_preflight as rust_preflight

CASE = "HNSW_HOSTED_ADMISSION"
ISSUER = "https://token.actions.githubusercontent.com"
JWKS = ISSUER + "/.well-known/jwks"
AUDIENCE = "alopex-db/alopex:issue681:selected-hosted-admission"
REPOSITORY = "alopex-db/alopex"
WORKFLOW = ".github/workflows/alopex-performance.yml"
API = "https://api.github.com/repos/" + REPOSITORY
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
OIDC_HOST_RE = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.actions\.githubusercontent\.com\Z"
)


def _is_allowed_oidc_host(hostname):
    return type(hostname) is str and OIDC_HOST_RE.fullmatch(hostname) is not None


def require(condition):
    if not condition:
        raise ValueError("ADMISSION")


def _decimal(value):
    require(type(value) is str and value.isascii() and value.isdecimal() and
            0 < int(value) < 10**20)
    return int(value)


def _json(raw, limit):
    require(type(raw) is bytes and 0 < len(raw) <= limit)
    return json.loads(raw)


def _b64(value):
    require(type(value) is str and re.fullmatch(r"[A-Za-z0-9_-]+", value))
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _der_length(length):
    require(0 <= length < 65536)
    if length < 128:
        return bytes([length])
    data = length.to_bytes((length.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(data)]) + data


def _der(tag, content):
    return bytes([tag]) + _der_length(len(content)) + content


def _rsa_public_key(n, e):
    require(128 <= len(n) <= 1024 and 1 <= len(e) <= 8)
    modulus, exponent = int.from_bytes(n, "big"), int.from_bytes(e, "big")
    require(modulus.bit_length() >= 2048 and exponent == 65537)
    def integer(raw):
        raw = raw.lstrip(b"\0")
        require(bool(raw))
        return _der(2, (b"\0" if raw[0] & 128 else b"") + raw)
    rsa = _der(0x30, integer(n) + integer(e))
    algorithm = bytes.fromhex("300d06092a864886f70d0101010500")
    return _der(0x30, algorithm + _der(3, b"\0" + rsa))


def verify_jwt(token, jwks, now, *, temp_root=None):
    require(type(token) is str and len(token) <= 16384)
    parts = token.split(".")
    require(len(parts) == 3 and all(parts))
    header = _json(_b64(parts[0]), 2048)
    claims = _json(_b64(parts[1]), 8192)
    signature = _b64(parts[2])
    require(type(header) is dict and header.get("alg") == "RS256" and
            type(header.get("kid")) is str and len(header["kid"]) <= 256 and
            header.get("typ") in (None, "JWT") and
            "jku" not in header and "x5u" not in header)
    require(type(jwks) is dict and type(jwks.get("keys")) is list and
            len(jwks["keys"]) <= 32)
    keys = [key for key in jwks["keys"] if type(key) is dict and
            key.get("kid") == header["kid"] and key.get("kty") == "RSA" and
            key.get("use", "sig") == "sig" and key.get("alg", "RS256") == "RS256"]
    require(len(keys) == 1)
    public_key = _rsa_public_key(_b64(keys[0]["n"]), _b64(keys[0]["e"]))
    require(len(signature) == (int.from_bytes(_b64(keys[0]["n"]), "big").bit_length() + 7) // 8)
    if temp_root is not None:
        require(isinstance(temp_root, Path) and temp_root.is_absolute() and
                temp_root.is_dir() and not temp_root.is_symlink() and
                temp_root.resolve(strict=True) == temp_root and
                temp_root.stat().st_uid == os.geteuid())
    openssl = Path("/usr/bin/openssl")
    executable = openssl.stat()
    require(stat.S_ISREG(executable.st_mode) and executable.st_uid == 0 and
            not (executable.st_mode & 0o022))
    # Claims are sent through stdin; only the public key and signature touch
    # owner-only temporary storage, never a reconstructable bearer token.
    with tempfile.TemporaryDirectory(dir=temp_root) as directory:
        root = Path(directory)
        for name, data in (("key.der", public_key), ("signature", signature)):
            target = root / name
            target.write_bytes(data)
            target.chmod(0o600)
        key = root / "key.pem"
        conversion = subprocess.run([str(openssl), "pkey", "-pubin", "-inform", "DER",
                                     "-in", str(root / "key.der"), "-out", str(key)],
                                    stdin=subprocess.DEVNULL, capture_output=True, timeout=5)
        require(conversion.returncode == 0)
        key.chmod(0o600)
        result = subprocess.run([str(openssl), "dgst", "-sha256", "-verify", str(key),
                                 "-signature", str(root / "signature")],
                                input=(parts[0] + "." + parts[1]).encode("ascii"),
                                capture_output=True, timeout=5)
        require(result.returncode == 0)
    require(type(claims) is dict and claims.get("iss") == ISSUER and
            claims.get("aud") == AUDIENCE)
    issued, start, expiry = (claims.get(key) for key in ("iat", "nbf", "exp"))
    require(all(type(value) is int for value in (issued, start, expiry)) and
            now - 300 <= issued <= now + 30 and
            issued - 600 <= start <= now + 30 and
            now < expiry <= issued + 900)
    return claims


def _request(url, *, token=None, limit=65536, accept=None):
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == "https" and parsed.port in (None, 443) and
            parsed.username is None and parsed.password is None and
            parsed.fragment == "" and
            (parsed.hostname in ("token.actions.githubusercontent.com",
                                 "api.github.com") or
             _is_allowed_oidc_host(parsed.hostname)))
    if accept is None:
        if parsed.hostname == "api.github.com":
            accept = "application/vnd.github+json"
        elif parsed.hostname == "token.actions.githubusercontent.com":
            accept = "application/json"
        else:
            accept = "application/json; api-version=2.0"
    headers = {"Accept": accept, "User-Agent": "alopex-681-admission"}
    if token is not None:
        require(type(token) is str and 0 < len(token) <= 4096)
        headers["Authorization"] = "Bearer " + token
    opener = urllib.request.build_opener(_NoRedirect)
    with opener.open(urllib.request.Request(url, headers=headers), timeout=10) as response:
        require(response.status == 200 and response.url == url)
        raw = response.read(limit + 1)
    return _json(raw, limit)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise ValueError("ADMISSION")


def _checkout(env, checkout):
    require(isinstance(checkout, Path) and checkout.is_absolute() and
            checkout.is_dir() and not checkout.is_symlink() and
            checkout.resolve(strict=True) == checkout and
            checkout.stat().st_uid == os.geteuid() and
            not (checkout.stat().st_mode & 0o077))
    binary = Path("/usr/bin/git")
    info = binary.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and
            not (info.st_mode & 0o022))
    def git(*args):
        environment = {"HOME": str(Path.home()), "PATH": "/usr/bin:/bin",
                       "LANG": "C", "GIT_CONFIG_NOSYSTEM": "1",
                       "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_OPTIONAL_LOCKS": "0"}
        result = subprocess.run([str(binary), "-C", str(checkout), *args],
                                stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=10, env=environment)
        require(result.returncode == 0 and len(result.stdout) <= 4096)
        return result.stdout.decode("ascii").strip()
    require(git("rev-parse", "HEAD") == env["EXPECTED_CONTROLLER_SHA"] and
            git("rev-parse", "HEAD^{tree}") == env["EXPECTED_HNSW_SOURCE_TREE"] and
            git("status", "--porcelain", "--untracked-files=all") == "")


def _context(env):
    require(env.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and
            env.get("GITHUB_JOB") == "hnsw_release_admission" and
            env.get("ISSUE_NUMBER") == "681" and
            env.get("GITHUB_REPOSITORY") == REPOSITORY and
            env.get("RUNNER_ENVIRONMENT") == "github-hosted" and
            type(env.get("GITHUB_REF")) is str and
            env["GITHUB_REF"].startswith("refs/heads/") and
            all(HEX40.fullmatch(env.get(key, "")) for key in
                ("EXPECTED_CONTROLLER_SHA", "EXPECTED_HNSW_SOURCE_TREE")) and
            env["EXPECTED_CONTROLLER_SHA"] == env.get("GITHUB_SHA"))
    for key in ("GITHUB_REPOSITORY_ID", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"):
        _decimal(env.get(key))
    baseline = _decimal(env.get("HNSW_ADMISSION_BASELINE"))
    require(baseline < 40_000_000_000)
    _checkout(env, Path(env["GITHUB_WORKSPACE"]))


def _identity(env, fetch=_request, now=None, *, failed_subchecks=None,
              failed_claim_keys=None):
    sub = "identity_oidc_url"
    try:
        request_url = env["ACTIONS_ID_TOKEN_REQUEST_URL"]
        parsed = urllib.parse.urlsplit(request_url)
        require(parsed.scheme == "https" and parsed.port in (None, 443) and
                _is_allowed_oidc_host(parsed.hostname) and
                parsed.fragment == "" and parsed.username is None and
                parsed.password is None)
        sub = "identity_oidc_fetch"
        separator = "&" if parsed.query else "?"
        token_response = fetch(request_url + separator + "audience=" +
                               urllib.parse.quote(AUDIENCE, safe=""),
                               token=env["ACTIONS_ID_TOKEN_REQUEST_TOKEN"], limit=20000)
        require(type(token_response) is dict and type(token_response.get("value")) is str)
        sub = "identity_jwks_fetch"
        jwks = fetch(JWKS, limit=65536)
        sub = "identity_jwt_verify"
        claims = verify_jwt(token_response["value"], jwks,
                            int(time.time()) if now is None else now,
                            temp_root=Path(env["RUNNER_TEMP"]))
        sub = "identity_jwt_claims"
        sha = env["EXPECTED_CONTROLLER_SHA"]
        run_id = _decimal(env["GITHUB_RUN_ID"])
        attempt = _decimal(env["GITHUB_RUN_ATTEMPT"])
        expected_claims = (
            ("repository", claims.get("repository") == REPOSITORY),
            ("repository_id", claims.get("repository_id") == env["GITHUB_REPOSITORY_ID"]),
            ("workflow_ref", claims.get("workflow_ref") ==
             REPOSITORY + "/" + WORKFLOW + "@" + env["GITHUB_REF"]),
            ("workflow_sha", claims.get("workflow_sha") == sha),
            ("sha", claims.get("sha") == sha),
            ("ref", claims.get("ref") == env["GITHUB_REF"]),
            ("event_name", claims.get("event_name") == "workflow_dispatch"),
            ("runner_environment", claims.get("runner_environment") == "github-hosted"),
            ("run_id", claims.get("run_id") == str(run_id)),
            ("run_attempt", claims.get("run_attempt") == str(attempt)),
        )
        mismatched = [key for key, ok in expected_claims if not ok]
        try:
            check_id = _decimal(str(claims.get("check_run_id", "")))
        except BaseException:
            check_id = None
            mismatched.append("check_run_id")
        if mismatched:
            if failed_claim_keys is not None:
                failed_claim_keys.extend(mismatched)
            require(False)
        sub = "identity_rest_run"
        api_token = env["GH_ACTIONS_READ_TOKEN"]
        run = fetch(f"{API}/actions/runs/{run_id}", token=api_token)
        require(run.get("id") == run_id and run.get("run_attempt") == attempt and
                run.get("event") == "workflow_dispatch" and
                run.get("head_sha") == sha and run.get("head_branch") ==
                env["GITHUB_REF"].removeprefix("refs/heads/") and
                run.get("path") == WORKFLOW and
                run.get("repository", {}).get("id") == _decimal(env["GITHUB_REPOSITORY_ID"]))
        sub = "identity_rest_job"
        job = fetch(f"{API}/actions/jobs/{check_id}", token=api_token)
        require(job.get("id") == check_id and job.get("run_id") == run_id and
                job.get("head_sha") == sha and
                job.get("check_run_url") ==
                f"{API}/check-runs/{check_id}" and
                job.get("name") == "hnsw_release_admission" and
                job.get("status") == "in_progress" and
                "ubuntu-24.04" in job.get("labels", []))
    except BaseException:
        if failed_subchecks is not None:
            failed_subchecks.append(sub)
        raise


def _classify_platform_failure(*, allow_vm_unfiltered_seccomp: bool = True):
    """Return fixed allowlisted failing subchecks for platform_security()."""
    failures = []
    try:
        if not (sys.platform == "linux" and os.uname().machine == "x86_64" and
                os.geteuid() != 0):
            failures.append("platform_os_uid")
            return failures
    except BaseException:
        failures.append("platform_os_uid")
        return failures
    try:
        status = {}
        for line in Path("/proc/self/status").read_text().splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                status[key] = value.strip()
        if status.get("CapEff") != "0000000000000000":
            failures.append("platform_capeff_zero")
        allowed_seccomp = ("0", "2") if allow_vm_unfiltered_seccomp else ("2",)
        if status.get("Seccomp") not in allowed_seccomp:
            failures.append("platform_seccomp_mode")
        if status.get("NoNewPrivs") not in ("0", "1"):
            failures.append("platform_no_new_privs")
    except BaseException:
        failures.append("platform_proc_status")
    try:
        mounts = [line.split() for line in
                  Path("/proc/self/mountinfo").read_text().splitlines()
                  if " - cgroup2 " in line and
                  len(line.split()) > 5 and line.split()[4] == "/sys/fs/cgroup"]
        if not (len(mounts) == 1 and mounts[0][3] == "/"):
            failures.append("platform_cgroup2_mount")
    except BaseException:
        failures.append("platform_cgroup2_mount")
    selected = ()
    try:
        selected = tuple(sorted(os.sched_getaffinity(0))[:4])
        if len(selected) != 4:
            failures.append("platform_cpu_affinity_4")
    except BaseException:
        failures.append("platform_cpu_affinity_4")
    if len(selected) == 4:
        try:
            qualify.admission.current_allocation(selected)
        except BaseException:
            failures.append("platform_cgroup_alloc")
    try:
        qualify.admission.child_counter_probe()
    except BaseException:
        failures.append("platform_child_counter")
    try:
        for proc in Path("/proc").iterdir():
            if not proc.name.isdecimal():
                continue
            try:
                if (proc.stat().st_uid == os.geteuid() and
                        (proc / "comm").read_text().strip() in
                        ("cargo", "rustc", "sccache")):
                    failures.append("platform_no_concurrent_rust")
                    break
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
    except BaseException:
        failures.append("platform_no_concurrent_rust")
    return failures or ["platform_other"]


def _classify_toolchain_failure():
    """Return fixed allowlisted failing subchecks for rust_preflight.check_admission()."""
    failures = []
    try:
        home = Path.home()
        if not (home.is_absolute() and home.is_dir() and not home.is_symlink() and
                home.stat().st_uid == os.geteuid()):
            return ["toolchain_home"]
    except BaseException:
        return ["toolchain_home"]
    search = str(home / ".cargo/bin") + ":/usr/local/bin:/usr/bin:/bin"
    environment = {"HOME": str(home), "PATH": search, "LANG": "C",
                   "RUSTUP_AUTO_INSTALL": "0"}
    rustup = rustc = cargo = None
    try:
        rustup = rust_preflight._binary("rustup", search)
    except BaseException:
        failures.append("toolchain_rustup_bin")
    try:
        rustc = rust_preflight._binary("rustc", search)
    except BaseException:
        failures.append("toolchain_rustc_bin")
    try:
        cargo = rust_preflight._binary("cargo", search)
    except BaseException:
        failures.append("toolchain_cargo_bin")
    if rustup is not None:
        try:
            manager = rust_preflight._run(rustup, environment, "-V").strip()
            if not re.fullmatch(
                    r"rustup [0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)? \([^)]+\)",
                    manager):
                failures.append("toolchain_rustup_version")
        except BaseException:
            failures.append("toolchain_rustup_version")
    if rustc is not None:
        try:
            compiler = rust_preflight._run(rustc, environment, "-Vv").splitlines()
            if not (any(re.fullmatch(r"release: [0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)?", line)
                        for line in compiler) and
                    compiler.count("host: " + rust_preflight.HOST) == 1):
                failures.append("toolchain_rustc_version")
        except BaseException:
            failures.append("toolchain_rustc_version")
    if cargo is not None:
        try:
            package = rust_preflight._run(cargo, environment, "-V").strip()
            if not re.fullmatch(
                    r"cargo [0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.]+)? \([^)]+\)",
                    package):
                failures.append("toolchain_cargo_version")
        except BaseException:
            failures.append("toolchain_cargo_version")
    return failures or ["toolchain_other"]


def run(env, *, fetch=_request, now=None, checkout=None):
    checks = {
        "source": "skipped_prereq",
        "identity": "skipped_prereq",
        "platform": "skipped_prereq",
        "toolchain": "skipped_prereq",
        "storage": "skipped_prereq",
    }
    first_error = None
    failed_subchecks = []
    failed_claim_keys = []

    def fail_stage(stage_key, err_code):
        nonlocal first_error
        checks[stage_key] = "failed"
        if first_error is None:
            first_error = err_code

    try:
        _context(env) if checkout is None else _context_with_checkout(env, checkout)
        checks["source"] = "passed"
    except BaseException:
        fail_stage("source", "SOURCE")
        return {
            "case": CASE,
            "outcome": "failed",
            "error_code": first_error,
            "checks": checks,
        }, 1

    try:
        _identity(env, fetch=fetch, now=now,
                  failed_subchecks=failed_subchecks,
                  failed_claim_keys=failed_claim_keys)
        checks["identity"] = "passed"
    except BaseException:
        fail_stage("identity", "IDENTITY")

    try:
        qualify.platform_security(allow_vm_unfiltered_seccomp=True)
        checks["platform"] = "passed"
    except BaseException:
        fail_stage("platform", "PLATFORM")
        failed_subchecks.extend(_classify_platform_failure(allow_vm_unfiltered_seccomp=True))

    try:
        rust_preflight.check_admission()
        checks["toolchain"] = "passed"
    except BaseException:
        fail_stage("toolchain", "TOOLCHAIN")
        failed_subchecks.extend(_classify_toolchain_failure())

    storage_sub = "storage_manifest"
    try:
        manifest = qualify.scoped_storage.make_manifest(
            Path(env["GITHUB_WORKSPACE"]), Path(env["RUNNER_TEMP"]))
        storage_sub = "storage_root_seal"
        require(qualify.scoped_storage.root_seal(manifest) ==
                env["HNSW_ADMISSION_ROOT_SEAL"])
        storage_sub = "storage_policy"
        qualify.storage(env, manifest)
        storage_sub = "storage_allocation"
        current, available = qualify.mounted_allocation(manifest)
        baseline = _decimal(env["HNSW_ADMISSION_BASELINE"])
        storage_sub = "storage_delta"
        require(0 <= current - baseline <= 250_000_000)
        storage_sub = "storage_budget"
        require(current + 10_000_000_000 < 40_000_000_000)
        storage_sub = "storage_free_space"
        require(available[os.stat(env["GITHUB_WORKSPACE"]).st_dev] >
                10_000_000_000)
        checks["storage"] = "passed"
    except BaseException:
        fail_stage("storage", "STORAGE")
        failed_subchecks.append(storage_sub)

    if first_error is None:
        return {"case": CASE, "outcome": "passed", "checks": checks}, 0
    payload = {
        "case": CASE,
        "outcome": "failed",
        "error_code": first_error,
        "checks": checks,
    }
    if failed_subchecks:
        payload["failed_subchecks"] = sorted(set(failed_subchecks))
    if failed_claim_keys:
        payload["failed_claim_keys"] = sorted(set(failed_claim_keys))
    return payload, 1


def _context_with_checkout(env, checkout):
    # The injected path is for synthetic tests only; production uses GITHUB_WORKSPACE.
    copied = dict(env)
    copied["GITHUB_WORKSPACE"] = str(checkout)
    _context(copied)


def main():
    result, status = run(os.environ)
    print(json.dumps(result, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
