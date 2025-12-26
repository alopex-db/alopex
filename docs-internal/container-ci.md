# Containerized CI for alopex-py

This repo already includes an `act` setup for running GitHub Actions locally in containers.
Use it to keep your host environment clean and idempotent.

## Images

- Runner image (GitHub Actions jobs): `catthehacker/ubuntu:act-latest`
- Optional custom runner image: `alopex-py-ci:latest` (built from `.act/Dockerfile.alopex-py`, includes `patchelf` and Rust toolchain)
- act CLI image: `ghcr.io/catthehacker/act:latest` (used by `scripts/container-ci.sh`)

## Build the custom runner image (optional)

```bash
./scripts/local-ci.sh --build-image
```

## Run workflows with act (host binary)

```bash
./scripts/local-ci.sh alopex-py
./scripts/local-ci.sh alopex-py rust-check
./scripts/local-ci.sh --custom alopex-py test
```

## Run workflows with act inside a container

This avoids installing `act` on the host. It still requires access to the Docker daemon.

```bash
./scripts/container-ci.sh alopex-py
./scripts/container-ci.sh alopex-py test
./scripts/container-ci.sh --custom alopex-py test
```

## Notes

- `act` only runs the `ubuntu-latest` matrix; macOS/Windows jobs are skipped.
- Artifacts are stored in `.act-artifacts/`.
- If Docker access fails, ensure your user can access `/var/run/docker.sock`.
