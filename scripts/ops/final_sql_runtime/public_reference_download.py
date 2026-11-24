"""Bounded GitHub retrieval of two independently pinned public native ZIPs."""
from __future__ import annotations

import http.client
import os
from pathlib import Path
import shutil
import ssl

from . import reference_download as https
from . import reference_public as public

REPOSITORY = "alopex-db/alopex"


def require(ok, code):
    if not ok:
        raise ValueError(code)


def retrieve(*, pins, destination, private_roots, trusted_download_hosts,
             token, transport=https._https_transport, timeout_seconds=300):
    """Return two admitted manifest paths; never retain or upload ZIP wrappers.

    Pins come from independent review. API metadata is fetched separately, and
    the bearer token is used only for api.github.com, not signed body URLs.
    """
    require(type(pins) is dict and set(pins) == set(public.SUBJECTS),
            "PUBLIC_INVENTORY")
    for subject in public.SUBJECTS:
        public._pin(subject, pins[subject])
    require(type(trusted_download_hosts) in (set, frozenset) and
            trusted_download_hosts and all(type(host) is str and
            https.HOST.fullmatch(host) for host in trusted_download_hosts),
            "PUBLIC_HOSTS")
    require(type(token) is str and token and "\r" not in token and "\n" not in token,
            "PUBLIC_TOKEN")
    require(type(timeout_seconds) in (int, float) and
            0 < timeout_seconds <= 900, "PUBLIC_DEADLINE")
    public._private_directory(destination)
    downloads = destination / "downloads"
    admitted = destination / "admitted"
    api = f"https://{https.API_HOST}/repos/{REPOSITORY}"
    deadline = https.time.monotonic() + timeout_seconds
    runs, artifacts, wrappers = {}, {}, {}
    created_downloads = created_admitted = completed = False
    try:
        downloads.mkdir(mode=0o700)
        created_downloads = True
        admitted.mkdir(mode=0o700)
        created_admitted = True
        for subject in public.SUBJECTS:
            pin = pins[subject]
            runs[subject] = https._json(
                transport, f"{api}/actions/runs/{pin['run_id']}", token, deadline)
            artifacts[subject] = https._json(
                transport, f"{api}/actions/artifacts/{pin['artifact_id']}",
                token, deadline)
        require(pins["before"]["run_id"] != pins["published"]["run_id"] and
                pins["before"]["artifact_id"] != pins["published"]["artifact_id"],
                "PUBLIC_DUPLICATE")
        for subject in public.SUBJECTS:
            public._remote(pins[subject], runs[subject], artifacts[subject])
        for subject in public.SUBJECTS:
            pin = pins[subject]
            path = downloads / f"artifact-{pin['artifact_id']}.zip"
            https._download(
                transport, f"{api}/actions/artifacts/{pin['artifact_id']}/zip",
                token, trusted_download_hosts, path, pin["zip_size_bytes"],
                deadline)
            wrappers[subject] = path
        result = public.admit_public_references(
            pins=pins, runs=runs, artifacts=artifacts, wrappers=wrappers,
            destination=admitted, private_roots=private_roots)
        completed = True
        return result
    except (OSError, http.client.HTTPException, ssl.SSLError):
        raise ValueError("PUBLIC_TRANSPORT") from None
    finally:
        cleanup_failed = False
        if created_downloads:
            try:
                shutil.rmtree(downloads)
            except OSError:
                cleanup_failed = True
        if created_admitted and (not completed or cleanup_failed):
            try:
                shutil.rmtree(admitted)
            except OSError:
                cleanup_failed = True
        if cleanup_failed:
            raise ValueError("PUBLIC_CLEANUP") from None
