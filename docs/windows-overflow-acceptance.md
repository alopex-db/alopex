# Windows public overflow: implementation and acceptance

#682, candidate base `5fe5cbc19dd8fa1bbebe3e7add3a708dda3c2a0f`.

**Status: implementation and macOS development checks complete; Windows public
acceptance and candidate integration pending.** This does not close #682.

The public LSM, Database, SQL and Python opt-in now reaches the existing Windows
publication pipeline. Budget validation still precedes filesystem effects.
Missing ancestors use parent-first write-through publication; files are synced
and closed before publication. No durability helper, WAL format, budget, commit
boundary or default non-overflow configuration is weakened. Python capability
reporting follows the native public implementation.

The previous Windows-only Unsupported assertion is replaced by public lifecycle
and API coverage. Existing invalid-budget tests still prove refusal before
storage mutation. Historical internal-only Windows results do not establish the
new public API's acceptance.

## Evidence and pending gates

Implementation build source: `5789b820e10c9dff89e638968c55ec9ac778810b`.
Subsequent changes correct one Python fixture to use explicit commit and clarify
comments/documentation; no Rust executable behavior changed after these builds.

| Boundary | Current evidence |
| --- | --- |
| Public Database overflow/cancellation/budget/nested Unicode ancestors | macOS 9 passed, 0 failed/ignored |
| Row/index/journal aggregate budget | macOS 2 passed, 0 failed/ignored |
| Public LSM persistence error/process exit | macOS 12 passed, 0 failed/ignored; child helper explicitly excluded from direct selection |
| Inline/reference recovery | macOS 2 passed, 0 failed/ignored |
| Public Python KV/SQL/cancellation/budgets/reopen | macOS 15 passed, 0 failed/skipped |
| Normal extension isolation | private fault hook absent in a fresh process |
| Windows ordinary native contracts | 134 selected; execution pending |
| Windows ordinary Python overflow cases | 15 selected; execution pending |
| Windows instrumented Database/SQL boundaries | manual Rust24 and Python24 execution pending |
| Independent published old reader | Windows activation/refusal/bytes-preserved/reopen acceptance pending |

The Windows runner total is the previous 124, minus one obsolete platform-refusal
assertion, plus nine public overflow and two journal cases. Lifecycle, inline and
the existing twelve publication-boundary cases now use the public LSM entrypoint.
Build and contracts run serially with separate 45-minute deadlines, reusing the
already validated phase split from #750. They keep one target, the existing
storage budget and cleanup. Neither deadlines nor tests are weakened to accept
an incomplete run. The candidate must be requalified after composition with
other PRs that add Windows cases.

Data is at most one prior and two pending keys, a 65-byte value, a 64-byte ring
payload, or two SQL rows. No load/performance workload is added to ordinary CI.
The instrumented Rust/Python harnesses are Windows-capable but remain manual;
ordinary builds and `--all-features` do not enable their private controls.

The initial new Python ancestor case omitted explicit commit, incorrectly
assuming that context exit committed. The API deliberately rolls back an active
transaction on exit. The fixture now commits explicitly and retains its spool
and reopen assertions. The initial 14-pass/1-fail result is retained separately
from the corrected 15-pass result; this was not a product defect or a weakened
expectation.

## Development checks

- `cargo test --offline --locked -p alopex-embedded --test wal_overflow --test wal_overflow_journal -- --test-threads=1`
- `cargo test --offline --locked -p alopex-core --lib lsm::overflow::publication_acceptance:: -- --skip child --test-threads=1`
- `cargo test --offline --locked -p alopex-core --lib lsm::overflow::inline_lifecycle_tests:: -- --test-threads=1`
- Owned CPython 3.13 environment and ordinary extension: `pytest -q crates/alopex-py/tests/test_wal_overflow.py`.
- Core library, embedded overflow/journal targets and Python library Clippy with
  `-D warnings`, formatting, diff and parser-contract checks passed. Windows
  console tests: 2 passed.

Target: `LsmOverflowConfig::validate` and its public preflight boundary.
Mutation check: one scoped cargo-mutants replacement of validation with
`Ok(())` was caught by the new existing-file regression. No survivor, timeout or
unviable mutant; restored source verified. Other unchanged validation predicates
were not included in this bounded mutation run. Windows-only execution is still
pending. No new unsafe code was added; Miri is unavailable for the installed
toolchain, and Kani was not run.

Scoped coupling analysis covered LSM (33 modules, critical0/high2/medium12) and
Python types (6 modules, critical0/high0/medium5). Existing LSM/WAL facade
dependencies are retained. The new reachable path is public validation/open to
the existing lifecycle/publication boundary, with Python capability reporting.
No new runtime module or refactor is introduced. Static analysis cannot establish
cfg-dependent behavior, runtime lock/order relationships or filesystem durability.
The direct single-file Python analysis failed with an I/O error; the six-module
types scope succeeded.

Builds were serialized on macOS arm64 with a shared target, incremental/debug
disabled and two jobs. A pinned v0.8.16 contract0.28 parser was used as an explicit
development override, not release-asset qualification. Host storage peaked at
16,438,079,488 bytes and ended at 16,105,586,688 after cleanup. Owned executables,
extension copies, Python environment, parser archive/extraction and pytest
fixtures were removed. Shared Cargo dependencies remain for active release work.
Compact source hashes, logs, mutation/coupling records and cleanup identities are
in `/private/tmp/alopex-682-windows-public`.

Next: verify the exact PR source on native Windows, including both normal and
manual public-API gates, before candidate integration. The broader #682
performance comparison and device-failure/power-loss qualification remain
separate from these controlled process-exit tests. Release-package qualification,
publication, post-release checks and main integration remain tracked by #690.
