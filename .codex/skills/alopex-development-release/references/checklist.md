# Alopex command checklist

Use these as patterns, substituting the target version and approved worktree. Keep `rtk` on every shell segment.

## State and hygiene

```bash
rtk git -C alopex status --short --branch
rtk git -C alopex worktree list
rtk scripts/check-rust-cache-budget.sh --check
```

Before handoff, run:

```bash
rtk scripts/cleanup-generated-artifacts.sh --force
rtk scripts/check-rust-cache-budget.sh --check
```

## Surface verification

Set `NIM_SQL_PARSER_LIB_DIR`/`NIM_SQL_PARSER_DIR` and Python library paths as required by the checked-out environment. The canonical v0.8 verifier is:

```bash
rtk bash crates/alopex-tools/v08/verify-v08-surfaces.sh
```

Also run the repository's candidate verifier, then the focused test suites for any changed contract. Before and after Cargo/maturin work, perform the hygiene commands above.

## Release safety and tags

```bash
rtk bash scripts/release/safe-tag.sh v<version>
rtk bash scripts/release/safe-tag.sh alopex-py-v<version>
rtk git tag -a v<version> -m "Release v<version>" <intended-commit>
rtk git push origin v<version>
rtk git tag -a alopex-py-v<version> -m "Release alopex-py-v<version>" <intended-commit>
rtk git push origin alopex-py-v<version>
```

Run `safe-tag.sh` from the branch it requires and treat every failure as blocking. The script checks safety; it does not create the tag.

## Post-release evidence

Record all of the following:

- Rust and Python workflow URLs and final conclusions.
- `git show` output for both annotated tags and `git ls-remote` dereferenced SHAs.
- GitHub Release URLs and expected platform assets.
- Each published crate (`alopex-core`, `alopex-sql`, `alopex-dataframe`, `alopex-cluster`, `alopex-embedded`, `alopex-server`, `alopex-cli`) at the target version.
- PyPI target version, all expected wheel platforms, and sdist.
- Clean main/release worktrees and measured workspace size.

If GitHub API rate limits prevent polling, use the public workflow/release pages and crates.io sparse index, then state the evidence source explicitly.
