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

## spec-workflow approval sequence

Call the guide first, then complete one spec in this order:

```text
mcp__spec_workflow__spec_workflow_guide
  → requirements.md → approvals(request) → approvals(status) → approvals(delete)
  → design.md       → approvals(request) → approvals(status) → approvals(delete)
  → tasks.md        → approvals(request) → approvals(status) → approvals(delete)
  → spec_status → task [-] → implement/test → log_implementation → task [x]
```

For every approval request:

- Set `projectPath` to `/home/roomtv/works/alopex-db`.
- Set `category: "spec"`, `categoryName: "<spec-name>"`, and `type: "document"`.
- Pass only the relative `filePath` (never document content).
- Generate a new request; do not reuse an old `approvalId`.
- Treat only dashboard `approved` as approval. If `needs-revision`, revise and create a new request.
- Delete the exact completed request and stop if deletion fails.

The three documents have different boundaries: requirements define observable outcomes, design maps approved outcomes to implementation contracts, and tasks divide the design into small executable work. Implementation details belong in design/tasks and implementation logs, not requirements. `tasks.md` status markers are `- [ ]` pending, `- [-]` in progress, and `- [x]` complete.

Implementation logs are mandatory searchable evidence. Include task ID, summary, files, line statistics, tests, and all relevant structured artifacts; do not submit an empty `artifacts` object. Search prior logs before adding endpoints, functions, classes, components, or integrations.

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
