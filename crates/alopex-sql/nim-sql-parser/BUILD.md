# Nim SQL Parser Build Procedure

The parser shared library is a generated local artifact and is intentionally
ignored by Git. Use the repository-level scripts so the toolchain and output
name stay aligned with Cargo's FFI build script.

## Linux, macOS, and Windows

From the repository root:

```sh
make nim-parser
bash scripts/test-nim-parser.sh
```

`auto` uses a host Nim/Nimble installation when both are available and falls
back to Docker. Exact host builds require Nim 2.2.10, Nimble 0.22.3 at commit
`42ef70c2102a942c46f13eb76872326edd525cec`, and an offline dependency seed in
`ALOPEX_NIMBLE_SEED_DIR` (or `ALOPEX_NIMBLE_DIR`). To select a backend
explicitly:

```sh
bash scripts/build-nim-parser.sh --backend host
bash scripts/build-nim-parser.sh --backend docker
bash scripts/test-nim-parser.sh --backend host
bash scripts/test-nim-parser.sh --backend docker
```

The Docker backend uses the digest-pinned `nimlang/nim:2.2` image and mounts
only `crates/alopex-sql/nim-sql-parser`. Mounting only the parser directory is
intentional: a Git worktree's `.git` file points outside the container and can
break Nimble's dependency resolution.

The build output is one of:

- Linux: `libalopex_sql_parser.so`
- macOS: `libalopex_sql_parser.dylib`
- Windows: `alopex_sql_parser.dll`

The library exports `alopex_parse_sql`, `alopex_parse_promql`,
`alopex_parser_version`, `alopex_parser_init`, and `alopex_free_buffer`.
The current SQL/PromQL MessagePack contract version is `0.4.0`.

For Skulk v0.5 development, consume the public Alopex v0.8.5 parser envelope
and its `parser-vendor-manifest-v0.8.5.json`. Select the exact target entry,
verify the archive and library SHA-256 values, verify `CONTRACT_VERSION=0.4.0`,
then stage the unchanged library and both sidecars under
`crates/skulk/nim-parser/vendor/<target-triple>/`. A manually copied library or
an unverified local build is not a release input. A local development override
is permitted only when the same two-level identity checks pass; it must never
replace the public envelope in CI or publication.

## Rust checks

Cargo uses the target-qualified release library under `vendor/` by default.
The build script also writes `CONTRACT_VERSION` and `SHA256SUMS` beside a local
development output. Cargo validates the pinned vendor manifest, target,
contract, byte size, and SHA-256 before emitting link directives. For a
worktree or an explicit output directory, keep both generated identity
sidecars with the library and use:

```sh
NIM_SQL_PARSER_LIB_DIR="$PWD/crates/alopex-sql/nim-sql-parser" \
LD_LIBRARY_PATH="$PWD/crates/alopex-sql/nim-sql-parser" \
cargo test -p alopex-sql --lib
```

CI calls the same build and test scripts. The Skulk resolver must not fall back
to a source build or an arbitrary host path. Keep Nim ownership in this
repository and treat the public Alopex parser asset as the single
cross-repository contract. WASM is outside this v0.8.5 contract and remains
deferred to Alopex v1.0 or later.
