# Buffered CSV framing (#712)

## Change and preserved contract

This follows the plain CSV full-consumption diagnosis in #787. `BoundedCsvFramer::next_frame` previously invoked `BufReader::read` with a one-byte destination for every byte, and pushed each byte into the frame. It now inspects the borrowed `fill_buf` slice, tracks the same quote transitions, copies the selected prefix once, and consumes only that prefix. It leaves subsequent records buffered for the next call.

The byte cap and reservation remain unchanged. A frame exactly filling the cap is accepted at EOF. An oversized frame consumes its first excess byte before the unchanged quote-aware tail drain, preserving the next record, error precedence and resource-release behavior. Header skipping and overflow draining retain their existing byte readers; no public API, buffering limit or decode change is introduced.

- Parent: `128f33e056b6e7a8c521efce51d1d3d4071ab4c3` (#787).
- Before native source: `0da19506e02668922fb60e9bf2ba8a55e7101d04` (new tests, original product behavior).
- After native source: `fb705f016fa2e4e911ff43c4e635e66397e9a426`.
- Both native builds: `env PYO3_BUILD_EXTENSION_MODULE=1 cargo build --offline --locked --profile test -p alopex-py`, shared target, test opt1, incremental/debug0, jobs2. Python/parser/package hashes are retained in JSON. Python/package files other than the native extension are byte-identical between sides.
- Parser archive SHA256 `8ac1f708efdfdbf6c85a65aebd34a368de41e798d9441a2785abad011cec038d`; verified parser sidecar/library identities use the development override and are not release-asset qualification.

## Small functional checks

Ten focused tests passed before and after the change, and again after a test-only clippy correction. The first clippy run rejected an allocated PathBuf used only for equality; the test now compares borrowed paths. This does not change product code or the mutation-checked function. The initial failure log and corrected result are both retained. Three new fixed tests cover fragmented quoted CRLF/multiline/escaped-quote records with a header and final record without newline; exact-cap EOF and newline plus quote-aware overflow draining into the next record; and I/O error path, malformed quote termination and budget release.

A new 24-case proptest compares each frame, row count, error, EOF state, unread buffered bytes, underlying cursor, injected I/O error state and live/released byte reservations against the frozen previous byte-at-a-time implementation. Input is at most 48 bytes, read fragments 1–8 bytes, caps 1–24 bytes, batch rows 1–3, with quote recognition enabled/disabled and an optional one-shot read error. Explicit record oracles independently check the unchanged quote tracker. Existing functional cases remain; this PR adds no benchmark or load test to CI. The existing workspace-locked proptest is added as a dataframe dev-dependency without upgrading packages.

Target: `alopex/crates/alopex-dataframe/src/io/streaming_csv.rs::BoundedCsvFramer::next_frame`.
Mutation check: cargo-mutants 27.1.0, in-place serial, changed lines in this function only, focused test filter, 30-second test/180-second build timeout. The initial CLI request combined mutually exclusive in-place/jobs options and ran no mutations; the corrected command omits jobs. Candidate listing exposed unrelated struct-field mutations despite the regex, so the diff filter excludes them before execution.

Sixteen mutants: 10 caught by failing assertions, 4 timed out at 30 seconds, 2 unviable, 0 passed/missed. Timeouts remove forward progress (multiply a zero consumed offset), suppress the overflow branch by deleting !frame_complete or changing the cap equality, or replace the valid unread-buffer bound with >. In the full/partial-cap cases these leave unconsumed bytes and a zero-length next scan; the watchdog records nontermination separately from assertion catches. The Ok(None) whole-body replacement fails unused-import/argument lints under -D warnings; Ok(Some(Default::default())) cannot construct FramedBytes because it has no Default. These compile failures do not count as behavioral detections. All mutation diffs and logs are retained; original source is restored exactly.

Survivors: No successfully completed mutant survived. Four nonterminating mutants and two compile failures remain explicitly distinguished; no equivalence exclusion or untested product fix is claimed.
Strengthening: three small fixed tests plus the 24-case reference-model proptest, added before implementation. No new unsafe code or pointers: Miri not applicable. Kani was considered for indexing, but the private buffered I/O state and quote transitions are covered by bounded model comparisons and mutation checks; no Kani run. The change is contained in one private function with unchanged module/crate calls, so no cargo-coupling execution was needed. Focused clippy (`--lib --tests -- -D warnings`) and formatting/diff checks passed; mutation lint failures are preserved separately from successful original-source checks.

## Fixed before/after measurement

The unchanged #787 manual harness uses the original plain CSV workload: 34,250 rows, 530,326 bytes, SHA256 `eec24557dcbdf71821b2e7afad1a7083dfb3256b71b57adf2e7ec387e1fd20ff`; full result digest `98afeb725a4f51925adec4ad654a07088175c9652d7ec7c29d8a19bf475f5707`. A three-row control is 32 bytes. Both sides consume `collect_batches(chunk_size=8192)` fully with the original timed boundaries. Plan and collect are separately observed, with temporary disposal before their end timestamp. Typed/NULL/column-order/physical-row-order full collect and stream oracles run before and after each worker outside timing. Every timed stream result checks row count; input hashes must remain unchanged.

Three trials run before/after, after/before, before/after. Each cell has 3 warmups and 21 measurements: 12 cells, 252 measured stream samples and 36 warmups. Separate plan and collect phases each add 252 measured samples and 36 warmups. Two large profile workers add 96 plan/collect preparation calls, and are excluded from medians. No reference engine runs in this comparison.

Times in milliseconds, median of the three trial medians. The predeclared diagnostic band is max(5% of before median, 3×pooled before MAD). The bounded small-control follow-up below adds six cells/126 stream measurements/18 warmups, plus 126 plan and 126 collect measurements and 18 warmups each. A result inside the band is not proof that a regression is impossible; all increases and paired trial ratios remain in JSON.

| Rows | Before full stream | After full stream | Change | Diagnostic band ms |
|---|---:|---:|---:|---:|
| 3 | 0.058250 | 0.062125 | +6.65% | 0.003753 |
| 34,250 | 2.703375 | 1.628708 | -39.75% | 0.135169 |

| Case | Plan ms | Collect ms | First batch ms | Remaining ms | Process peak RSS bytes |
|---|---:|---:|---:|---:|---:|
| before 3-alopex | 0.000292 | 0.029500 | 0.057583 | 0.001000 | 40,468,480 |
| before 34250-alopex | 0.000292 | 1.009334 | 0.649333 | 2.064917 | 49,938,432 |
| after 3-alopex | 0.000334 | 0.030584 | 0.061416 | 0.001000 | 40,386,560 |
| after 34250-alopex | 0.000292 | 1.035708 | 0.433167 | 1.200250 | 49,774,592 |

At 34,250 rows the full-stream median falls from 2.703375 to 1.628708ms (-39.75%). All three paired trial ratios are below 1 (0.614/0.601/0.596), and full row/NULL/type/order oracles pass. First-batch latency falls 0.649333→0.433167ms and remaining consumption 2.064917→1.200250ms. The full materialized collect observation, whose implementation is unchanged, is +2.61%; it remains visible and is not attributed to the framing change.

The initial three-row control rises 0.058250→0.062125ms (+6.65%): the 3.875µs difference exceeds the declared 3.753µs band by 0.122µs. Its paired ratios 0.793/1.073/0.958 do not consistently increase. This is preserved as an initial guard excursion, not discarded as noise. A single predeclared follow-up uses the same binaries and 32-byte input, reverses the order to BA/AB/BA, and runs only this control: six cells, 126 additional stream measurements/18 warmups, with the same separate plan/collect observations. It yields 0.057000→0.056584ms (-0.73%), paired ratios 0.887/0.988/1.004; the last pair's +0.36% is retained. The initial increase is not reproduced in this bounded check, but its cause remains unproved. No further repeated measurement was used to obtain a passing result. Final candidate guards remain required.

Separate profiles complete 1,430 before and 2,304 after iterations. In one remaining-batch native branch, the old framer has 755 inclusive samples, including 449 direct next_frame samples and 280 samples under its per-byte BufReader::read path. The new branch has 403 direct next_frame samples, 11 under fill_buf and 6 in slice-copy/memmove; Arrow decoding is now a 647-sample sibling branch. These are profile observations of different iteration counts, not normalized per-operation CPU percentages. Source and stack evidence support removal of the per-byte reader-call path. Arrow decode and quote tracking still have cost; this change does not resolve every CSV/reference gap.


These are warmed-input, one-host native test-opt1 before/after observations. Worker environments request one thread; CPU affinity is not pinned. They do not establish published v0.8.16 parity, equivalence to reference release wheels, or release readiness. RSS includes runtime setup and full-value validation. Independent medians need not sum exactly. Profile samples are inclusive and hierarchical and cannot be added as exclusive percentages.

## Cleanup and continuation

All 20 worker PIDs were absent; per-case work directories were removed immediately. Both native packages, generated CSV inputs, parser/archive, the owned focused dataframe test executable and four native target exports were removed after evidence capture. Shared Cargo dependencies and compact source-bound logs remain. Initial host usage: 19,095,080,960 bytes; observed peak: 19,394,740,224; after cleanup: 19,209,977,856. The task stayed inside its 3 GB additional bound and below the 40 GB stop/50 GB ceiling. Builds were serialized and each manual worker had 180 seconds/2 GiB RSS limits.

Next owner/action: Codex will advance candidate integration once source-bound required CI is green, then verify the adopted CSV and other #712 guards on the fixed candidate composition. Candidate CI, remaining #712 adoption/acceptance, package qualification, release publication, postrelease verification and main merge remain tracked in #690. This report supports a Draft PR handoff only.
