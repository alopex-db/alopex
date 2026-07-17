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
back to Docker. The host toolchain must be Nim 2.2 or newer. To select a
backend explicitly:

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

## Rust checks

Cargo automatically discovers the generated library in the parser directory.
For a worktree or an explicit output directory, use:

```sh
NIM_SQL_PARSER_LIB_DIR="$PWD/crates/alopex-sql/nim-sql-parser" \
LD_LIBRARY_PATH="$PWD/crates/alopex-sql/nim-sql-parser" \
cargo test -p alopex-sql --lib
```

CI calls the same build and test scripts. Release-wheel jobs that compile Nim
inside a manylinux image remain separate because they target the wheel's
architecture and toolchain image.
