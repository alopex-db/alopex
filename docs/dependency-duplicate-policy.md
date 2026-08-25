# Cargo dependency duplicate policy

## Invariant and current baseline

> New duplicate crate versions fail CI. Existing duplicate versions may remain only as exact, reasoned exceptions that become errors when stale.

`deny.toml` is the machine-facing source of truth. The initial inventory found 84 duplicate crate families and required 101 exact-version exceptions across the configured tier-1, Windows, WASM, all-feature, and dev-dependency graph. The first reduction converged Arrow/Parquet 52 onto 53. The current configured graph has 69 duplicate crate families and 86 exact-version exceptions. This baseline is debt to remove, not a permitted count to refill after an exception disappears.

The same reduction removed 15 duplicate families and 15 package entries from `Cargo.lock`: Arrow Array, Buffer, Cast, Data, IPC, Schema, Select, Parquet, Brotli, and the six Lexical crates. The lockfile inventory moved from 95 to 80 duplicate families and from 792 to 777 package entries.

This policy does not pin Cargo resolution. It does not alter `Cargo.toml`, `Cargo.lock`, the rustc crate graph, or compilation. Exact versions identify audit exceptions only; the runtime cost is one metadata-based policy check in the existing dependency-audit job.

The blocking command is:

```bash
cargo deny --all-features --locked check bans \
  --hide-inclusion-graph \
  --deny unmatched-skip \
  --deny unnecessary-skip
```

The GitHub Action is pinned to an immutable revision packaging cargo-deny 0.20.2. Wildcard dependency declarations are outside this rule; the bans check currently allows them so duplicate-version control does not silently expand into a different migration.

## Ownership

| Concern | Owner | Behavior |
|---|---|---|
| Duplicate rule and exact exceptions | `deny.toml` | machine-facing, blocking |
| Execution | `ci.yml:security-audit` | runs before RustSec audit for production changes |
| Dependency path diagnosis | `cargo tree --invert <crate>@<version>` | read-only operator evidence |
| Prioritization and remaining acceptance | Issue #196 | human-facing planning and rolling evidence |
| Final CI verdict | `ci-success` | status-only join; performs no dependency analysis |

## Policy semantics

- `multiple-versions = "deny"` rejects any duplicate family not reduced to one unskipped version.
- `multiple-versions-include-dev = true` includes test-only dependencies because they contribute to compile time and local `target` growth.
- Exact-version exceptions skip all but one current version in each existing family. Introducing another version leaves two unskipped versions and fails CI.
- `unmatched-skip` and `unnecessary-skip` are promoted to errors. When an old version disappears, the same dependency change must remove its stale exception.
- `skip-tree` is prohibited because it can conceal unrelated new duplicates below a broad dependency subtree.
- Removing an exception must lower the exception count. Replacing an old exception with a new version is not progress unless the graph has fewer versions or the change documents why convergence is temporarily impossible.

## Reduction order

Prioritize by expected compile and artifact cost, then by how directly this workspace controls the edge:

1. Completed: Arrow/Parquet 52 was converged onto 53, also removing the old Brotli and Lexical branches.
2. Next: Bevy 0.14/0.15 and TUI crates. `vendor/ratatui-testlib` and `alopex-cli` dev dependencies own much of this test-only cluster.
3. Then: `alopex-core` 0.3.4/0.8.7. The older version arrives through the Chirps Raft storage dependency.
4. Ongoing: general cryptography, randomness, platform, and Windows support crates. Converge through direct-owner upgrades where possible; retain only exact transitive exceptions otherwise.

For each candidate:

```bash
cargo tree --locked --workspace --all-features --target all \
  --invert <crate>@<version>
```

Update the nearest workspace-controlled owner, regenerate `Cargo.lock`, remove the obsolete exact exception, then run the blocking command. A dependency update that adds a duplicate must either converge the graph in the same change or fail.

## Lifecycle

| Path | Current responsibility | Target responsibility | Action | Removal condition | Verification method |
|---|---|---|---|---|---|
| `deny.toml` | absent | canonical duplicate policy and exact exception inventory | create | never while Cargo is used | blocking `cargo deny check bans` |
| `ci.yml:security-audit` | RustSec audit and one advisory reachability guard | duplicate policy followed by RustSec audit | extend | replace only if another required owner preserves both checks | workflow contract plus GitHub run |
| exact `bans.skip` entry | acknowledge one existing version | shrink-only temporary debt | delete | version is absent or family converges | denied unmatched/unnecessary skip diagnostics |
| `bans.skip-tree` | absent | illegal broad suppression | keep absent | always | contract test and source review |
| this runbook | absent | operator procedure and ownership map | create | only with an equivalent maintained runbook | documentation contract |

## Failure, rollback, and operations

- A new duplicate, stale exception, malformed config, or unavailable pinned action is a mandatory blocker. It is not advisory and has no `continue-on-error` path.
- RustSec remains an independent check in the same job; passing duplicate policy cannot conceal an advisory failure.
- Rollback means reverting `deny.toml`, its blocking step, contracts, and this runbook as one unit. Do not leave a non-executed exception file or an action without a reviewed policy.
- If the pinned action revision becomes unavailable, update it to another reviewed immutable revision and prove the same local cargo-deny command first. Do not replace it with an unpinned moving tag.
