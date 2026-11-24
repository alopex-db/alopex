# Four-case public measurement contract

This is the use-case contract for the four-case diagnostic. A passing local
test does not by itself establish that every acceptance check below is met.

The four-case diagnostic selects its evidence purpose before execution. Its
public result is a numeric measurement that readers can inspect directly.
The diagnostic's `small_overhead` result describes observer overhead under
the fixed paired formula. It does not qualify product performance.

| Use case | Actor and trigger | Inputs | Output and exposure | Failure behavior | Acceptance check |
| --- | --- | --- | --- | --- | --- |
| Public timing | Release operator starts the fixed four-case job | Exact source and subject identities; fixed case, trial, mode and pair order | Every timed call immediately creates one typed public timing row with case, trial, mode, sample, pair ordinal and elapsed seconds | Missing, duplicate, nonfinite or misordered rows prevent a passing public result | The 24 fixed cells contain 504 valid timing rows and match the measured operations |
| Public operation attribution | Release operator starts the fixed four-case job | Measured client callbacks, per-operation resource observer and joined server stages | 504 operation rows with client/server CPU, RSS and I/O; on-mode client phases; 1,764 server-stage rows | Missing, duplicate or nonnumeric attribution prevents a passing public result | Every operation row joins the timing row; all 252 on-mode measured calls have seven route-appropriate stages |
| Public review | Reader opens the completed artifact | Timing, operation and stage rows plus the paired formula | Case-level `D_ms`, `MAD_ms`, `U_ms`, `bound_ms`, three `delta_ms` and `trial_U_ms`, `small_overhead`, and numeric phase/resource attribution | Failure is reported with a fixed code and without a complete-measurement claim | The reader can recompute comparisons and phase/resource summaries from the published rows |
| Private integrity audit | CI verifier checks the run before upload | Detailed client, server and resource records plus public rows | Owner-only audit of source, operation, stage and timing consistency | Mismatch stops upload | Every public value agrees with the corresponding verified private observation at final export |
| Local diagnosis | Developer explicitly uses a local debug path | Same workload and owner-only diagnostic destination | Detailed traces stay local | Retain failure context for investigation | Debug records and arbitrary local text are rejected by the public upload boundary |

The pair runner selects `public` or `debug` before connecting to the server.
The hosted worker explicitly selects `public`; this mode requires the fixed
public timing and operation roots and writes typed rows at every timed call.
Fixed server-stage rows are projected after the private trace join. A developer may
select `debug` only outside GitHub Actions, with two raw paths under an
owner-only local debug root. Debug mode writes detailed raw rows and produces
no public measurement files. Unknown modes, a mixed public/debug destination, or a
debug request in CI fail before the workload starts. The public uploader
accepts neither a debug root nor raw records.

## Public schema and verification

The timing writer emits canonical JSONL with exactly `schema`, `case`,
`trial`, `mode`, `sample`, `pair_ordinal` and `elapsed_seconds`. Case and mode
are fixed enums. Trial, sample and ordinal are bounded integers, and elapsed
seconds is finite and nonnegative. The writer creates distinct, exclusive
files for each case/trial/mode and records a value immediately after its
timed operation. The operation writer accepts only fixed numeric client
callback and resource counters. Both modes contain per-call client/server user
and system CPU seconds, RSS before/after bytes, and read/write/cancelled-write
bytes. On-mode rows also contain `bind_ns`, `transport_ns`, `parse_ns`,
`rows_ns`, `residual_ns`, `total_ns`, `response_chars` and `result_rows`.
Server counters sum the observed process tree; summed RSS is not unique
physical memory. The seven server stages are nanosecond durations per on-mode
measured call. Insert/rollback uses session setup/completion; the other cases
use transaction setup and completion/commit. Stages may not sum to client
round-trip time. No public row accepts a trace ID, PID, machine path, hostname,
raw log or arbitrary exception text.

The successful `public.json` contains complete timing, operation and stage
inventories, declared units and server aggregation semantics, the computed
paired comparison, fixed source and subject digests, and outcome fields. The uploader
rebuilds the expected public object from the typed inventories and
requires exact canonical bytes. It rejects extra fields, files and text.
On failure, `measurement` is null, the error code is fixed, and
`completed_pairs` records the verified pair count when a valid controller
receipt exists. A missing or invalid receipt yields null rather than an
invented count.
The private audit checks public timing, phases, resource counters and server
stages against the same operation's collected observations before publication
and again at final export. The fixed subject digests are checked against the
independently pinned lock. The public writers belong to both the pinned
operator inventory and staged package inventory. `performance_qualification`
remains `false` until the separate product-performance acceptance gate passes.
The hosted public job runs the independent full-cell audit locally and uploads
only `public.json`. Its owner-only raw records and server logs are outside the
publication artifact. The public job has no recipient certificate or private
decryption-key requirement.
The prerequisite #804 qualification for this mode likewise publishes only
fixed status and case outcomes as public JSON; its detailed diagnostics stay
local. Other qualification modes have separate evidence contracts.

## Remaining acceptance work

- Verify the full hosted run and public artifact against the exact source and
  qualification pins before treating these local checks as acceptance evidence.
