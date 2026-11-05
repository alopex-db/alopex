# SQL test containers

Issue: https://github.com/alopex-db/alopex/issues/804

This controller operates an existing Docker context with build-time networking
disabled and runtime networking isolated. It does not provision or reconfigure
the host, VM, daemon or security profiles. Read `configuration.md` before preparing
the image lock or an execution profile.

## Inputs

Copy `examples/run-profile.template.json` into a private directory and fill the
execution parameters. Keep that file mode 0600. The template has null values on
purpose: a profile is rejected until all required values are supplied. Local
machine values are never committed.

Set `execution_mode` to `emulated` when either the VM or the container emulates
the product architecture. Matching Docker daemon and container architectures do
not establish native VM execution. Preserve the VM's execution configuration in
private evidence; emulated runs qualify functional cases only.

Run the controller on the physical execution host, using a local Unix-socket
Docker context. The private `storage.runtime_storage_roots` must identify the
daemon's backing storage or its VM disk directory and be covered by the measured
roots. A Docker context on another physical host is rejected; its budget cannot
be established by inspecting the controller host's files.

`image.lock.json` contains the OCI base index and a `platforms` mapping. Each
platform entry contains:

- `packages`: complete installed Debian package names mapped to exact versions;
- `allowed_hosts`: HTTPS download and redirect hostnames;
- `files`: `{path, url, size, sha256}` records for `deps/*.deb` and
  `wheelhouse/*.whl`, including transitive dependencies.

Use Ubuntu 24.04 packages, Python 3.12/venv/pip, PostgreSQL 16.14 server/client,
sudo, CA certificates, procps, util-linux and iproute2. Include psycopg 3.2.10 and
the selected Alopex wheel's Python dependencies. PostgreSQL package provenance
must come from PGDG. Build verifies the installed package versions and rejects
missing dependencies offline.

Copy the checked-in image lock to a private file and set `image_lock` to that
copy. Resolve its platform entry once before the first preflight:

```sh
python3 scripts/test-env/envctl.py resolve-lock --profile "$RUN_PROFILE"
```

This acquisition step starts a disposable container from the pinned base, reads
its installed package inventory, and resolves the dependency closure using
authenticated Ubuntu/PGDG APT indexes. It does not install packages into the VM.
The resolver pins PostgreSQL server, client and libpq together. It uses a fresh
archive cache so already downloaded packages cannot disappear from the closure.
Package SHA256 values come from the signed indexes, including when APT's URI
listing uses MD5. Exact pure-Python wheels are selected from `requirements.lock`
using PyPI metadata. The resulting lock records every dependency's URL, size and
SHA256; subsequent `fetch` and `build` use that lock without re-resolving it.

The resolver uses the profile's `fetch` capacity allowance and execution timeout.
Include the base image, APT indexes, container writable layer, logs and copied
metadata in that allowance, and the copied metadata in the `collect` allowance.
It retains compact resolution evidence and removes its container after verified
collection. After an interrupted or failed resolution, use `collect --resolution`
then `clean --resolution`; use a new run ID for another resolution. Keep the lock
with the evidence. Re-resolution is an explicit dependency update, not a step to
repeat for every build.

`assets.json` identifies the products independently of the image. Its file list
uses bundle-relative paths and SHA256/size for each server, wheel, shared library,
build receipt and fixed harness archive. The original build receipt is retained.
Supported receipts are `alopex.performance-build/v1` and receipts with
`source_commit`, `platform`, and an `artifacts` path-to-hash map. Shared libraries
must also be identified by the receipt (`runtime_libraries` in performance
receipts). Existing performance receipts may bind `libalopex_sql_parser.so`
through their `parser_sha256` field. That field cannot authorize another library
or override a conflicting explicit library binding. Do not edit old receipts to
make an unmatched artifact pass.

## Operation

From the repository root, with `RUN_PROFILE` pointing to the private profile:

```sh
python3 scripts/test-env/envctl.py doctor --phase preflight --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py fetch --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py build --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py doctor --phase runtime --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py collect --probe --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py clean --probe --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py accept --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py collect --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py export-public --profile "$RUN_PROFILE"
python3 scripts/test-env/envctl.py clean --profile "$RUN_PROFILE"
```

`accept` collects successful runs to compare a fresh repeat for R01. Explicit
`collect` is idempotent. After failure, run `collect` before `clean`; an unverified
collection cannot be deleted. Each new acceptance invocation needs a new run ID.
For a single case use `run --case E02`. A single R01 invocation includes the
required environment and SQL cases. `clean` also handles a recorded repeat run.

Console output contains only fixed status/error codes. Detailed diagnostics and
raw Docker output remain in the private output directory. Share only the explicit
`public.json` export. A nonzero result, missing case, failed copy or skipped
prerequisite is not accepted.

## Bounded developer verification

```sh
python3 -B -m unittest discover -s scripts/test-env/tests -v
```

These tests use temporary tiny fixtures and mocked process/resource failures.
They neither build images nor run load/performance tests. The normal CI workflow
runs this command only. Live acceptance is an explicit separate operation.
