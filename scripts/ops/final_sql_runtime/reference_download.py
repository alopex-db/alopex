"""Read-only hosted retrieval for independently reviewed #712 reference artifacts.

The caller supplies reviewed descriptors and original byte pins. The staging
workflow and the download redirect hosts must also be reviewed separately.
No token is sent to a signed download URL, and no network response supplies a
pin. This module does not attest product manifests or receipts.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import http.client
import json
import os
from pathlib import Path
import re
import ssl
import time
from urllib.parse import urljoin, urlsplit

from . import reference_ingress as ingress


API_HOST = "api.github.com"
JSON_BOUND = 1_000_000
CHUNK = 1024 * 1024
HOST = re.compile(r"[a-z0-9][a-z0-9.-]*[a-z0-9]")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")


def _require(ok, code):
    if not ok:
        raise ValueError(code)


@contextmanager
def _https_transport(url, headers, timeout):
    parsed = urlsplit(url)
    _require(parsed.scheme == "https" and parsed.hostname and
             parsed.port in (None, 443) and not parsed.username and
             not parsed.password and not parsed.fragment, "URL_BOUNDARY")
    connection = http.client.HTTPSConnection(
        parsed.hostname, port=443, timeout=timeout, context=ssl.create_default_context())
    try:
        connection.request("GET", parsed.path + ("?" + parsed.query if parsed.query else ""),
                           headers=headers)
        yield connection.getresponse()
    finally:
        connection.close()


def _response(transport, url, headers, deadline):
    remaining = deadline - time.monotonic()
    _require(remaining > 0, "DOWNLOAD_DEADLINE")
    return transport(url, headers, min(30, remaining))


def _json(transport, url, token, deadline):
    headers = {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}",
               "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "alopex-reference-ingress"}
    with _response(transport, url, headers, deadline) as response:
        _require(response.status == 200, "GITHUB_API_STATUS")
        length = response.getheader("Content-Length")
        _require(length is None or (length.isdecimal() and int(length) <= JSON_BOUND),
                 "GITHUB_API_BOUND")
        data = response.read(JSON_BOUND + 1)
        _require(len(data) <= JSON_BOUND and time.monotonic() <= deadline,
                 "GITHUB_API_BOUND")
    try:
        value = json.loads(data)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("GITHUB_API_JSON") from exc
    _require(type(value) is dict, "GITHUB_API_JSON")
    return value


def _download(transport, url, token, trusted_hosts, target, expected_size, deadline):
    api_headers = {"Accept": "application/vnd.github+json",
                   "Authorization": f"Bearer {token}",
                   "X-GitHub-Api-Version": "2022-11-28",
                   "User-Agent": "alopex-reference-ingress"}
    with _response(transport, url, api_headers, deadline) as response:
        _require(response.status == 302, "ARTIFACT_REDIRECT")
        location = response.getheader("Location")
    _require(type(location) is str, "ARTIFACT_REDIRECT")
    signed_url = urljoin(url, location)
    parsed = urlsplit(signed_url)
    _require(parsed.scheme == "https" and parsed.hostname in trusted_hosts and
             parsed.port in (None, 443) and not parsed.username and
             not parsed.password and not parsed.fragment, "ARTIFACT_REDIRECT_HOST")
    # The second request intentionally contains no Authorization or API token.
    with _response(transport, signed_url, {"User-Agent": "alopex-reference-ingress"},
                   deadline) as response:
        _require(response.status == 200, "ARTIFACT_DOWNLOAD_STATUS")
        length = response.getheader("Content-Length")
        _require(length is None or (length.isdecimal() and
                 int(length) == expected_size), "ARTIFACT_DOWNLOAD_SIZE")
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            with os.fdopen(fd, "wb") as output:
                count = 0
                while block := response.read(CHUNK):
                    count += len(block)
                    _require(count <= expected_size and time.monotonic() <= deadline,
                             "ARTIFACT_DOWNLOAD_BOUND")
                    output.write(block)
                _require(count == expected_size, "ARTIFACT_DOWNLOAD_SIZE")
                output.flush()
                os.fsync(output.fileno())
        except BaseException:
            target.unlink(missing_ok=True)
            raise


def retrieve_and_admit(*, repository, descriptors, original_pins, destination,
                       trusted_download_hosts, token=None, transport=_https_transport,
                       now=None, timeout_seconds=300):
    """Retrieve three exact Actions wrappers and admit their original ZIPs.

    ``destination`` is an existing caller-owned, empty, mode-0700 directory.
    The caller reviews exact redirect hostnames before calling this function.
    The token defaults to ``GITHUB_TOKEN`` and never appears in an exception.
    """
    _require(type(repository) is str and REPOSITORY.fullmatch(repository) and
             all(part not in (".", "..") for part in repository.split("/")),
             "REPOSITORY")
    _require(type(trusted_download_hosts) in (set, frozenset) and
             trusted_download_hosts and all(type(host) is str and HOST.fullmatch(host)
                                            for host in trusted_download_hosts),
             "TRUSTED_HOSTS")
    _require(type(timeout_seconds) in (int, float) and
             0 < timeout_seconds <= 900, "DOWNLOAD_TIMEOUT")
    token = os.environ.get("GITHUB_TOKEN") if token is None else token
    _require(type(token) is str and token and "\n" not in token and "\r" not in token,
             "GITHUB_TOKEN")
    _require(type(descriptors) is dict and set(descriptors) == ingress.SUBJECTS and
             type(original_pins) is dict and set(original_pins) == ingress.SUBJECTS,
             "REFERENCE_INVENTORY")
    fd = ingress._destination_fd(destination)
    os.close(fd)
    _require(not any(destination.iterdir()), "DESTINATION_NOT_EMPTY")
    if now is None:
        now = datetime.now(timezone.utc)
    _require(isinstance(now, datetime) and now.tzinfo is not None, "CLOCK")
    deadline = time.monotonic() + timeout_seconds
    run_ids = {descriptors[s].get("run_id") for s in ingress.SUBJECTS
               if type(descriptors[s]) is dict}
    _require(len(run_ids) == 1 and len(descriptors) == 3 and
             type(next(iter(run_ids))) is int and next(iter(run_ids)) > 0,
             "STAGING_RUN")
    run_id = next(iter(run_ids))
    api = f"https://{API_HOST}/repos/{repository}"
    wrappers = {}
    metadata = {}
    try:
        run = _json(transport, f"{api}/actions/runs/{run_id}", token, deadline)
        for subject in sorted(ingress.SUBJECTS):
            descriptor = descriptors[subject]
            artifact_id = descriptor.get("artifact_id")
            _require(type(artifact_id) is int and artifact_id > 0,
                     "STAGING_DESCRIPTOR")
            metadata[subject] = _json(
                transport, f"{api}/actions/artifacts/{artifact_id}", token, deadline)
        ids = set()
        names = set()
        for subject in sorted(ingress.SUBJECTS):
            descriptor = descriptors[subject]
            ingress._review_descriptor(subject, descriptor, original_pins[subject],
                                       run, metadata[subject], now.astimezone(timezone.utc))
            _require(descriptor["artifact_id"] not in ids and
                     descriptor["artifact_name"] not in names, "DUPLICATE_ARTIFACT")
            ids.add(descriptor["artifact_id"])
            names.add(descriptor["artifact_name"])
        for subject in sorted(ingress.SUBJECTS):
            descriptor = descriptors[subject]
            path = destination / f"artifact-{descriptor['artifact_id']}.zip"
            _download(transport, f"{api}/actions/artifacts/{descriptor['artifact_id']}/zip",
                      token, trusted_download_hosts, path,
                      descriptor["artifact_size_bytes"], deadline)
            wrappers[subject] = path
        return ingress.admit_wrapped_originals(
            descriptors=descriptors, original_pins=original_pins,
            staging_run=run, artifact_metadata=metadata, wrappers=wrappers,
            destination=destination, now=now)
    except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
        raise ValueError("REFERENCE_TRANSPORT") from None
    finally:
        for path in wrappers.values():
            path.unlink(missing_ok=True)
