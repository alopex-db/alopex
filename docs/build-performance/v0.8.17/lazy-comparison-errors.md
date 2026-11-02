# Lazy comparison-error construction (#712)

## Change and identity

#785 identified error formatting in successful `enabled = true` predicates. Both fallible branches in `alopex-sql/src/executor/evaluator/binary_op.rs::compare` constructed `ExecutorError::Evaluation(TypeMismatch)` eagerly with `ok_or`, including the expected string and formatted operand type names. This change uses `ok_or_else`, so those strings are constructed only when comparison returns None. Successful result selection, NULL precedence, exact integral/Decimal branches, NaN/type-error variant and message stay the same. No public API, format or durability change is made.

- Parent: #785, `6304840c2d4660c299de9ca5b5d83cea8cb9131e`.
- Before build: `f1092017a282b151add28d1e2891ddaa5c517a0f`, with the first three semantic regression tests but unchanged product behavior.
- Implementation: `e87666ae` (two error-construction branches; eight added/eight removed lines).
- After build/harness: `a5d4e1bc7763142fda2b166401d89a29f5f2caeb`, adding the mutation-driven precision property without further product changes.
- The harness/imported helpers and all non-native Python package files are byte-identical between sides. The JSON records both native SHA256 values, all package/harness hashes, raw observations, profiles, commands, verification failures and cleanup.

Two serialized native builds reused one shared Cargo target: `env PYO3_BUILD_EXTENSION_MODULE=1 cargo build --offline --locked --profile test -p alopex-py`, opt-level 1, incremental/debug disabled, jobs 2. Independent package copies prevented the after build from overwriting before. The verified v0.8.16 contract0.28 parser is an explicit development override, not release-asset qualification. No worktree was created.

## Focused correctness and test-gap verification

`cargo test --offline --locked -p alopex-sql --lib executor::evaluator::binary_op::tests` passed six tests before and after the implementation, then seven after strengthening. Three new deterministic tests cover all six comparison operators with boolean/text/blob/float/mixed-numeric values, exact integers beyond f64 precision, infinity/signed zero, NULL on either side including NaN, and exact NaN/incompatible/vector error variants and text. Inputs are individual values and one-element vectors, not data-volume tests.

All six generated cargo-mutants mutations in `compare` were examined in place, serialized, with 60-second test / 180-second build bounds. The initial outcome was **2 caught, 2 missed, 2 unviable**, retained in full rather than reported as success:

- NULL guard and deletion of the BigInt exact-comparison arm were caught.
- Changing the mixed-Decimal guard from OR to AND survived. This exposed a test gap: Decimal/integer comparisons could fall back to f64 and lose a fractional difference near a large integer. A **24-case proptest** now compares both operand directions and all six operators against independent scaled-i128 arithmetic around 2^54. The strengthened seven-test suite passed, and re-running this exact mutant caught it.
- Deleting the Integer/Integer arm is functionally equivalent for this contract: all i32 values convert exactly to f64, so all six comparison results and error/NULL outcomes are unchanged. This is not a claim that the alternative has identical performance. Its exclusion is documented, not silently counted as caught.
- The Default-return mutation cannot compile because SqlValue has no Default; the let-chain OR mutation cannot compile because Rust disallows OR in that construct. Both remain unviable, not passing tests.

Final disposition: three distinct non-equivalent mutants caught, one reasoned equivalent, two unviable, zero timeouts and no unresolved genuine survivor. Initial and strengthened source-bound results are separate. Mutation testing does not directly prove reduced allocations; the lazy control flow and separate before/after profile provide that evidence.

`cargo clippy --offline --locked -p alopex-sql --lib --tests -- -D warnings`, fmt and diff checks passed. SDK-observation/development-parser warnings are retained in the build logs. No coupling run was needed for one private function with unchanged calls/module boundaries. No unsafe or new bounded-arithmetic product code was introduced, so Miri/Kani were not run. The arithmetic strengthening is test-only.

## Fixed before/after manual comparison

The unchanged #785 harness preserves the adopted `SELECT id, value FROM bench WHERE enabled = true` fixture and SQL semantics. 3 / 34,250 input rows return 2 / 17,125 rows. Both the original fully consumed counting lane and the separate full Python-row materialization lane are retained. There is no ORDER BY added to SQL; validation canonicalizes rows outside timing. Original-lane full values are checked before/after the case and counts every iteration; the rows lane checks every timed full typed result. All result and input identities match.

Both sides ran three trials, each with three warmups and 21 samples, in before/after, after/before, before/after order: **24 timed cells, 504 measurements, 72 warmups**, all successful. Four additional larger profiles passed and are not included in timing medians. Separate plan calls/prewarming are preserved and not substituted for consumption time. macOS CPU affinity is not pinned; RAYON/TOKIO/OMP/OPENBLAS request one worker. All data are warm. This is internal current-source comparison, not published v0.8.16, DataFusion or final release-build acceptance.

Times are milliseconds and medians of three trial medians. Before execution, the diagnostic band was fixed at max(5% of before median, three times pooled before MAD). Every sample and paired trial ratio is retained.

| Input rows / lane | Before ms | After ms | Change | Diagnostic band ms |
|---|---:|---:|---:|---:|
| 3-alopex-original | 0.017959 | 0.017750 | -1.16% | 0.002877 |
| 3-alopex-rows | 0.017708 | 0.017500 | -1.17% | 0.000996 |
| 34250-alopex-original | 15.135958 | 12.175208 | -19.56% | 0.756798 |
| 34250-alopex-rows | 14.828625 | 11.815625 | -20.32% | 0.741431 |

At 34,250 input rows, original consumption decreased **19.56%** and Python-row materialization decreased **20.32%**. Their paired time ratios were 0.824/0.804/0.796 and 0.777/0.790/0.797. All four median comparisons decreased. The small rows case in trial 3 increased **0.71%** (0.017708 to 0.017833 ms); that observation is preserved. It is within the predeclared band, which is not proof of noise or universal no-regression. The large improvements must not be multiplied into the original Linux 46.522x or prior DataFusion ratios to claim a new release/reference result.

## Profile and resource observations

Separate original profiles completed 226 before / 284 after iterations; rows profiles completed 245 / 316. Before's original native sample contains a compare branch with 271 inclusive samples, 261 below error formatting; the rows sample contains 214 compare / 198 formatting samples. After's corresponding successful comparison stacks have numeric checks/comparison/drop paths and no observed error-formatting subtree. The source makes error construction conditional on failure, while sampling corroborates removal of the observed hot path. These are nested inclusive counts, not additive exclusive phase percentages.

After's original cProfile still spends 3.990 of 4.000 consumer seconds in remaining-row consumption, with 0.009 in stream creation. Row decode, cursor/lease work, output metadata and Python conversion remain; this change does not resolve the full SQL streaming reference gap. Profiles validate only preflight/final output and include profiler/disposal overhead.

| Side / 34,250-row lane | CPU including close ms | Process peak RSS bytes |
|---|---:|---:|
| before / alopex-original | 15.222000 | 71,942,144 |
| before / alopex-rows | 14.961000 | 72,728,576 |
| after / alopex-original | 12.302000 | 71,942,144 |
| after / alopex-rows | 11.842000 | 72,744,960 |

RSS is process-lifetime macOS getrusage high-water, including setup, oracle and hashes. It is not engine-only memory, and no memory reduction is claimed. CPU/block observations surround consumption and context close before validation/output; the original Alopex total timing ends before context exit. Raw IO and timing-boundary fields remain in the JSON.

## Cleanup and continuation

All 28 worker PIDs were absent and all case work directories removed. Both native packages, parser/archive, four native target exports and the owned SQL unit-test executable were deleted after evidence capture. In-place mutation runs left source restored and no new worktree. Compact mutation logs, sources, reports and shared Cargo dependencies remain. Observed host peak: 19,286,130,688 bytes; after cleanup: 19,090,083,840 bytes. The predeclared additional bound was 3 GB, each manual case retained the 180-second / 2 GiB RSS stop, and all host measurements stayed below the 40 GB stop / 50 GB ceiling.

Target: `alopex/alopex-sql/executor/evaluator/binary_op.rs::compare`.
Mutation check: six initial candidates plus one targeted strengthened recheck; dispositions above.
Survivors: one functionally equivalent i32 fallback; the genuine Decimal precision gap is covered.
Strengthening: four minimal regression tests, including the 24-case exact-reference property.
Verification: 6 before / 6 after / 7 strengthened unit tests, focused Clippy/fmt, 504 manual measurements and four separate profiles.

Next owner/action: Codex will diagnose the adopted plain CSV streaming full-consumption case while continuing candidate CI/integration. Remaining SQL/reference guards, all other accepted #712 scenarios, final candidate/package qualification, release/publication and post-release main merge remain under #690. This report supports a development PR handoff only; it does not close #712 or declare v0.8.17 release-ready.
