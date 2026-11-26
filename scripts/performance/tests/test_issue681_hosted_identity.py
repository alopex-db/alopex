"""Disposable-key synthetic checks for UC-681-SELECTED-HOSTED-ADMISSION."""

import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import issue681_hosted_identity as identity


SHA = "a" * 40
TREE = "b" * 40
RUN = 12345
CHECK = 67890
REPO_ID = "111"
REF = "refs/heads/prep/example"


def b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def tlv(data, position=0):
    tag = data[position]
    length = data[position + 1]
    position += 2
    if length & 128:
        count = length & 127
        length = int.from_bytes(data[position:position + count], "big")
        position += count
    return tag, data[position:position + length], position + length


class HostedIdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.keys = tempfile.TemporaryDirectory()
        cls.private = Path(cls.keys.name) / "key.pem"
        cls.public = Path(cls.keys.name) / "key.der"
        subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt",
                        "rsa_keygen_bits:2048", "-out", str(cls.private)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=True, timeout=10)
        subprocess.run(["openssl", "pkey", "-in", str(cls.private), "-pubout",
                        "-outform", "DER", "-out", str(cls.public)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=True, timeout=5)
        _, spki, _ = tlv(cls.public.read_bytes())
        _, _, offset = tlv(spki)
        _, bitstring, _ = tlv(spki, offset)
        _, rsa, _ = tlv(bitstring[1:])
        _, n, offset = tlv(rsa)
        _, e, _ = tlv(rsa, offset)
        cls.jwks = {"keys": [{"kid": "synthetic", "kty": "RSA", "alg": "RS256",
                              "n": b64(n.lstrip(b"\0")), "e": b64(e.lstrip(b"\0"))}]}

    @classmethod
    def tearDownClass(cls):
        cls.keys.cleanup()

    def setUp(self):
        self.now = int(time.time())
        self.env = {
            "ACTIONS_ID_TOKEN_REQUEST_URL":
                "https://pipelines.actions.githubusercontent.com/oidc?token=opaque",
            "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "request-token",
            "GH_ACTIONS_READ_TOKEN": "api-token",
            "EXPECTED_CONTROLLER_SHA": SHA,
            "EXPECTED_HNSW_SOURCE_TREE": TREE,
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_JOB": "hnsw_release_admission",
            "ISSUE_NUMBER": "681",
            "GITHUB_REPOSITORY": identity.REPOSITORY,
            "GITHUB_REPOSITORY_ID": REPO_ID,
            "GITHUB_RUN_ID": str(RUN),
            "GITHUB_RUN_ATTEMPT": "2",
            "GITHUB_REF": REF,
            "GITHUB_SHA": SHA,
            "RUNNER_ENVIRONMENT": "github-hosted",
            "HNSW_ADMISSION_BASELINE": "1000000000",
            "HNSW_ADMISSION_ROOT_SEAL": "synthetic-seal",
            "RUNNER_TEMP": str(Path(self.keys.name).resolve()),
        }
        self.claims = {
            "iss": identity.ISSUER, "aud": identity.AUDIENCE,
            "iat": self.now, "nbf": self.now, "exp": self.now + 300,
            "repository": identity.REPOSITORY, "repository_id": REPO_ID,
            "workflow_ref": identity.REPOSITORY + "/" + identity.WORKFLOW + "@" + REF,
            "workflow_sha": SHA, "sha": SHA, "ref": REF,
            "event_name": "workflow_dispatch", "runner_environment": "github-hosted",
            "run_id": str(RUN), "run_attempt": "2", "check_run_id": str(CHECK),
        }

    def token(self, claims=None):
        header = {"alg": "RS256", "kid": "synthetic", "typ": "JWT"}
        payload = self.claims if claims is None else claims
        message = (b64(json.dumps(header).encode()) + "." +
                   b64(json.dumps(payload).encode()))
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "message"
            source.write_bytes(message.encode())
            signature = subprocess.run(["openssl", "dgst", "-sha256", "-sign",
                                        str(self.private), str(source)],
                                       capture_output=True, check=True, timeout=5).stdout
        return message + "." + b64(signature)

    def fetch(self, token):
        def response(url, *, token=None, limit=65536):
            if ".actions.githubusercontent.com/" in url and "/oidc" in url:
                return {"value": self.signed}
            if url == identity.JWKS:
                return self.jwks
            if url == f"{identity.API}/actions/runs/{RUN}":
                return {"id": RUN, "run_attempt": 2, "event": "workflow_dispatch",
                        "head_sha": SHA, "head_branch": "prep/example",
                        "path": identity.WORKFLOW, "repository": {"id": int(REPO_ID)}}
            if url.startswith(f"{identity.API}/actions/jobs/"):
                return {"id": CHECK, "run_id": RUN, "head_sha": SHA,
                        "check_run_url": f"{identity.API}/check-runs/{CHECK}",
                        "name": "hnsw_release_admission", "status": "in_progress",
                        "labels": ["ubuntu-24.04"]}
            raise AssertionError("unexpected request")
        return response

    def test_signed_exact_job_and_run(self):
        self.signed = self.token()
        identity._identity(self.env, fetch=self.fetch(self.signed), now=self.now)
        # Sharded GitHub Actions OIDC hostnames and nbf <= iat clock skew pass.
        for host in ("pipelinesghubeus23.actions.githubusercontent.com",
                     "run-actions-1-azure-eastus.actions.githubusercontent.com"):
            with self.subTest(host=host):
                skewed = dict(self.claims, nbf=self.now - 300)
                self.signed = self.token(skewed)
                env = dict(self.env,
                           ACTIONS_ID_TOKEN_REQUEST_URL=f"https://{host}/oidc?token=opaque")
                identity._identity(env, fetch=self.fetch(self.signed), now=self.now)

    def test_untrusted_oidc_hosts_and_bad_nbf_fail(self):
        self.signed = self.token()
        for bad_url in (
            "http://pipelinesghubeus23.actions.githubusercontent.com/oidc",
            "https://evil.example.com/oidc",
            "https://a.b.actions.githubusercontent.com/oidc",
            "https://actions.githubusercontent.com.evil.com/oidc",
            "https://pipelinesghubeus23.actions.githubusercontent.com:8443/oidc",
        ):
            with self.subTest(bad_url=bad_url):
                env = dict(self.env, ACTIONS_ID_TOKEN_REQUEST_URL=bad_url)
                with self.assertRaises(ValueError):
                    identity._identity(env, fetch=self.fetch(self.signed), now=self.now)
        for bad_claims in (
            dict(self.claims, nbf=self.now + 31),
            dict(self.claims, nbf=self.now - 601),
            dict(self.claims, exp=self.now + 901),
        ):
            with self.subTest(bad_claims=bad_claims):
                bad_jwt = self.token(bad_claims)
                with self.assertRaises(ValueError):
                    identity.verify_jwt(bad_jwt, self.jwks, self.now)

    def test_tamper_expiry_and_bound_claims_fail(self):
        self.signed = self.token()
        changed = self.signed[:-1] + ("A" if self.signed[-1] != "A" else "B")
        with self.assertRaises(ValueError):
            identity.verify_jwt(changed, self.jwks, self.now)
        for key, value in (("exp", self.now - 1), ("run_attempt", "3"),
                           ("check_run_id", "67891"), ("repository_id", "222"),
                           ("workflow_sha", "c" * 40),
                           ("runner_environment", "self-hosted")):
            with self.subTest(key=key):
                claims = dict(self.claims, **{key: value})
                self.signed = self.token(claims)
                with self.assertRaises(ValueError):
                    identity._identity(self.env, fetch=self.fetch(self.signed), now=self.now)

    def test_rest_job_mismatch_fails(self):
        self.signed = self.token()
        original = self.fetch(self.signed)
        def changed(url, **kwargs):
            result = original(url, **kwargs)
            if url.endswith(f"/actions/jobs/{CHECK}"):
                result["run_id"] = RUN + 1
            return result
        with self.assertRaises(ValueError):
            identity._identity(self.env, fetch=changed, now=self.now)

    def test_source_drift_fails_before_network(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            (root / "file").write_text("source")
            subprocess.run(["git", "-C", str(root), "add", "file"], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c",
                            "user.email=test@example.invalid", "commit", "-qm", "fixture"], check=True)
            env = dict(self.env, GITHUB_WORKSPACE=str(root))
            result, status = identity.run(env, fetch=lambda *a, **k: self.fail("network"))
            self.assertEqual((status, result["error_code"]), (1, "SOURCE"))

    def test_full_admission_fixed_outcome_and_fail_closed_stages(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            (root / "file").write_text("source")
            subprocess.run(["git", "-C", str(root), "add", "file"], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c",
                            "user.email=test@example.invalid", "commit", "-qm", "fixture"], check=True)
            head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"]).decode().strip()
            tree = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD^{tree}"]).decode().strip()
            env = dict(self.env, GITHUB_WORKSPACE=str(root), GITHUB_SHA=head,
                       EXPECTED_CONTROLLER_SHA=head, EXPECTED_HNSW_SOURCE_TREE=tree)
            device = os.stat(root).st_dev
            with mock.patch.object(identity, "_identity"), \
                    mock.patch.object(identity.qualify, "platform_security") as plat_mock, \
                    mock.patch.object(identity.qualify, "storage"), \
                    mock.patch.object(identity.qualify.scoped_storage,
                                      "make_manifest", return_value=object()), \
                    mock.patch.object(identity.qualify.scoped_storage,
                                      "root_seal", return_value="synthetic-seal"), \
                    mock.patch.object(identity.rust_preflight, "check_admission"), \
                    mock.patch.object(identity.qualify, "mounted_allocation",
                                      return_value=(1_100_000_000, {device: 11_000_000_000})):
                result, status = identity.run(env)
                plat_mock.assert_called_once_with(allow_vm_unfiltered_seccomp=True)
                self.assertEqual(status, 0)
                self.assertEqual(result, {
                    "case": identity.CASE,
                    "outcome": "passed",
                    "checks": {
                        "source": "passed",
                        "identity": "passed",
                        "platform": "passed",
                        "toolchain": "passed",
                        "storage": "passed",
                    },
                })
                stale = dict(env, HNSW_ADMISSION_ROOT_SEAL="stale")
                stale_res, stale_code = identity.run(stale)
                self.assertEqual((stale_code, stale_res["error_code"]), (1, "STORAGE"))
                self.assertIn("storage_root_seal", stale_res["failed_subchecks"])
                with mock.patch.object(identity.qualify, "platform_security",
                                       side_effect=PermissionError("private path")):
                    plat_res, _ = identity.run(env)
                    self.assertEqual(plat_res["error_code"], "PLATFORM")
                    self.assertNotIn("private path", json.dumps(plat_res))
                with mock.patch.object(identity.rust_preflight, "check_admission",
                                       side_effect=RuntimeError("private path")):
                    tc_res, _ = identity.run(env)
                    self.assertEqual(tc_res["error_code"], "TOOLCHAIN")
                    self.assertNotIn("private path", json.dumps(tc_res))
                with mock.patch.object(identity.qualify, "mounted_allocation",
                                       return_value=(1_300_000_000, {device: 11_000_000_000})):
                    delta_res, _ = identity.run(env)
                    self.assertEqual(delta_res["error_code"], "STORAGE")
                    self.assertIn("storage_delta", delta_res["failed_subchecks"])
                # Simultaneous multi-stage failure reports all failing stages in one run without leaking secrets/paths.
                with mock.patch.object(identity, "_identity",
                                       side_effect=ValueError("secret-token-xyz")), \
                        mock.patch.object(identity.qualify, "platform_security",
                                          side_effect=ValueError("private-platform")), \
                        mock.patch.object(identity.rust_preflight, "check_admission",
                                          side_effect=ValueError("private-toolchain")):
                    multi_res, multi_code = identity.run(stale)
                    self.assertEqual(multi_code, 1)
                    self.assertEqual(multi_res["error_code"], "IDENTITY")
                    self.assertEqual(multi_res["checks"], {
                        "source": "passed",
                        "identity": "failed",
                        "platform": "failed",
                        "toolchain": "failed",
                        "storage": "failed",
                    })
                    serialized = json.dumps(multi_res)
                    self.assertNotIn("secret-token-xyz", serialized)
                    self.assertNotIn("private-platform", serialized)
                    self.assertNotIn("private-toolchain", serialized)
                    self.assertNotIn(str(root), serialized)

    def test_request_uses_endpoint_specific_accept_headers(self):
        seen_headers = []

        class FakeResponse:
            def __init__(self, url):
                self.status = 200
                self.url = url

            def read(self, _limit):
                return b'{"ok":true}'

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class FakeOpener:
            def open(self, req, timeout=10):
                seen_headers.append((req.full_url, req.get_header("Accept")))
                return FakeResponse(req.full_url)

        with mock.patch.object(identity.urllib.request, "build_opener",
                               return_value=FakeOpener()):
            identity._request("https://pipelinesghubeus23.actions.githubusercontent.com/oidc?api-version=2.0",
                              token="tok")
            identity._request("https://token.actions.githubusercontent.com/.well-known/jwks")
            identity._request("https://api.github.com/repos/alopex-db/alopex/actions/runs/1",
                              token="tok")
        self.assertEqual(
            [accept for _, accept in seen_headers],
            ["application/json; api-version=2.0",
             "application/json",
             "application/vnd.github+json"],
        )


if __name__ == "__main__":
    unittest.main()
