# Windows public overflow: implementation and acceptance

#682, candidate base `e4d5f59e67b93d8e3f4a491dc725cb96a47639f8`.

**Status: implementation, macOS development checks and the Windows manual public
acceptance suite passed; ordinary CI and candidate integration remain pending.**
This does not close #682.

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
| Windows ordinary Python overflow cases | 15 passed, 0 failed/skipped in the manual suite's normal extension |
| Windows instrumented Database/SQL boundaries | Rust24 and Python24 passed; Python cases each verified two reopen cycles |
| Independent published old reader | Published 0.8.16 container and live-WAL activation/refusal/bytes-preserved/reopen passed |

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
were not included in this bounded mutation run. Windows manual execution is
recorded below. No new unsafe code was added; Miri is unavailable for the installed
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

Manual execution is available in the existing Compatibility workflow through
`acceptance_suite=windows-overflow`, dispatched on this PR branch. Its dedicated
hosted Windows job checks normal Python15, instrumented Rust24/Python24,
published 0.8.16 container refusal, and six independent live-WAL processes.
The receipts bind the exact checkout, normal/instrumented DLL hashes and old
registry/wheel identities. Ordinary CI does not run the instrumented suite.

## Native Windows manual result

[Run 40960742467, attempt 1](https://github.com/alopex-db/alopex/actions/runs/40960742467)
completed successfully on source `4194599371452e10d7be712006f2fa76fb83ee17`, tree
`e9832f3de5f4ef940a2eebf2713a5462f9a9c936`. It includes candidate `e4d5f59e`.
The normal extension has no private fault hook. The 15 ordinary Python cases,
24 Rust boundary cases and 24 Python boundary cases passed without skips.
Python's maximum fixture was 5,790 bytes, with two successful reopen cycles per
case. The six injection points cover spool write/sync/publication, before
reference, reference append/sync and force-sync, for errors and process exits
through both KV and SQL.

The published PyPI 0.8.16 wheel passed four independent container processes.
The independent crates.io core 0.8.16 and current clients passed six live-WAL
processes: legacy write, current read, upgrade, old-reader refusal and two
current reopens. Both old readers rejected the new format without modifying
files. Live-WAL reopens recovered the committed entry without warnings and
preserved the files. This is controlled process-exit and I/O-injection evidence,
not hardware power-loss or released-wheel qualification.

Artifact `12935754854` contains 89 compact evidence files. Its downloaded ZIP
SHA-256 is `cf94882269f344a734262eaf9f27ef144c9aee7d562e3d076f45123397d3ed48`,
matching GitHub's artifact digest. Independent reconciliation checked exact
case names, XML counts, normal/instrumented native hashes, old-reader identities,
source/driver hashes, file snapshots, and all phase receipts. Windows checkout
uses CRLF; driver/config hashes match the fixed Git blobs after that explicit
checkout conversion. The retained [compact evidence](build-performance/v0.8.17/windows-overflow-public.json)
includes the receipts and source identities. Full local logs remain under
`/private/tmp/alopex-771-windows-manual/attempt2`.

The 110 storage samples peaked at 3,546,337,640 allocated bytes. Cleanup removed
the owned shared target and work directory and ended at 597,685,320 bytes.
No raw database, native DLL or executable was uploaded.

The earlier run `40957708284` failed before product tests: plain `bash` selected
the WSL launcher, and cleanup hit a read-only Nimble Git pack. Those failures
remain recorded. The successful source explicitly selects native Git Bash and
handles read-only owned files. Ordinary CI subsequently found the new manual
job missing from the workflow responsibility inventory; the documentation-only
follow-up registers it as Extended Verification. That follow-up does not change
product, tests, runners or workflow bytes, so it does not require repeating this
manual execution. Its ordinary CI and final candidate composition still need
separate confirmation.

Next: verify ordinary CI and the composed candidate before integration. The broader #682
performance comparison and device-failure/power-loss qualification remain
separate from these controlled process-exit tests. Release-package qualification,
publication, post-release checks and main integration remain tracked by #690.
