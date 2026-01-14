.PHONY: act-ci act-ci-coverage act-ci-security \
	act-compat act-compat-x86 act-compat-wasm \
	act-compat-x86-none act-compat-x86-snappy act-compat-x86-zstd act-compat-x86-lz4 \
	act-compat-wasm-none act-compat-wasm-snappy \
	act-cli act-cli-check act-cli-functional act-cli-streaming act-cli-signal act-cli-s3 \
	act-py act-py-rust-check act-py-test-numpy act-py-test-no-numpy act-py-polars-020 \
	act-py-polars-latest act-py-typecheck act-py-benchmarks

ACT ?= act
ACT_ARTIFACT_SERVER ?= 127.0.0.1
ACT_COMMON_FLAGS ?= --reuse=false --artifact-server-addr $(ACT_ARTIFACT_SERVER)
ACT_PY_PLATFORM ?= ubuntu-latest=alopex-act-ubuntu:latest
ACT_PY_FLAGS ?= --env ACT=true --platform $(ACT_PY_PLATFORM) --pull=false $(ACT_COMMON_FLAGS)

act-ci:
	$(ACT) -W .github/workflows/ci.yml -j fmt $(ACT_COMMON_FLAGS)
	$(ACT) -W .github/workflows/ci.yml -j clippy $(ACT_COMMON_FLAGS)
	$(ACT) -W .github/workflows/ci.yml -j test $(ACT_COMMON_FLAGS)
	$(ACT) -W .github/workflows/ci.yml -j build $(ACT_COMMON_FLAGS)

act-ci-security:
	$(ACT) -W .github/workflows/ci.yml -j security-audit $(ACT_COMMON_FLAGS)

act-ci-coverage:
	$(ACT) -W .github/workflows/ci.yml -j coverage $(ACT_COMMON_FLAGS)

act-compat: act-compat-x86 act-compat-wasm

act-compat-x86: act-compat-x86-none act-compat-x86-snappy act-compat-x86-zstd act-compat-x86-lz4

act-compat-x86-none:
	$(ACT) -W .github/workflows/compatibility.yml -j native --matrix target:x86_64-unknown-linux-gnu --matrix compression:none $(ACT_COMMON_FLAGS)

act-compat-x86-snappy:
	$(ACT) -W .github/workflows/compatibility.yml -j native --matrix target:x86_64-unknown-linux-gnu --matrix compression:snappy $(ACT_COMMON_FLAGS)

act-compat-x86-zstd:
	$(ACT) -W .github/workflows/compatibility.yml -j native --matrix target:x86_64-unknown-linux-gnu --matrix compression:zstd $(ACT_COMMON_FLAGS)

act-compat-x86-lz4:
	$(ACT) -W .github/workflows/compatibility.yml -j native --matrix target:x86_64-unknown-linux-gnu --matrix compression:lz4 $(ACT_COMMON_FLAGS)

act-compat-wasm: act-compat-wasm-none act-compat-wasm-snappy

act-compat-wasm-none:
	$(ACT) -W .github/workflows/compatibility.yml -j wasm --matrix compression:none $(ACT_COMMON_FLAGS)

act-compat-wasm-snappy:
	$(ACT) -W .github/workflows/compatibility.yml -j wasm --matrix compression:snappy $(ACT_COMMON_FLAGS)

act-cli: act-cli-check act-cli-functional act-cli-streaming act-cli-signal act-cli-s3

act-cli-check:
	$(ACT) -W .github/workflows/alopex-cli.yml -j cli-check $(ACT_COMMON_FLAGS)

act-cli-functional:
	$(ACT) -W .github/workflows/alopex-cli.yml -j functional-tests $(ACT_COMMON_FLAGS)

act-cli-streaming:
	$(ACT) -W .github/workflows/alopex-cli.yml -j streaming-fallback-test $(ACT_COMMON_FLAGS)

act-cli-signal:
	$(ACT) -W .github/workflows/alopex-cli.yml -j signal-handling-test $(ACT_COMMON_FLAGS)

act-cli-s3:
	$(ACT) -W .github/workflows/alopex-cli.yml -j s3-compatibility-test $(ACT_COMMON_FLAGS)

act-py: act-py-rust-check act-py-test-numpy act-py-test-no-numpy act-py-polars-020 act-py-polars-latest act-py-typecheck act-py-benchmarks

act-py-rust-check:
	$(ACT) -W .github/workflows/alopex-py.yml -j rust-check $(ACT_PY_FLAGS)

act-py-test-numpy:
	$(ACT) -W .github/workflows/alopex-py.yml -j test --matrix os:ubuntu-latest --matrix python:3.11 --matrix feature:numpy $(ACT_PY_FLAGS)

act-py-test-no-numpy:
	$(ACT) -W .github/workflows/alopex-py.yml -j test --matrix os:ubuntu-latest --matrix python:3.11 --matrix feature:no-numpy $(ACT_PY_FLAGS)

act-py-polars-020:
	$(ACT) -W .github/workflows/alopex-py.yml -j polars-test --matrix python:3.11 --matrix polars:0.20.* $(ACT_PY_FLAGS)

act-py-polars-latest:
	$(ACT) -W .github/workflows/alopex-py.yml -j polars-test --matrix python:3.11 --matrix polars:latest $(ACT_PY_FLAGS)

act-py-typecheck:
	$(ACT) -W .github/workflows/alopex-py.yml -j typecheck $(ACT_PY_FLAGS)

act-py-benchmarks:
	$(ACT) -W .github/workflows/alopex-py.yml -j benchmarks $(ACT_PY_FLAGS)
