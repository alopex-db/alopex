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
| Public review | Reader opens the completed artifact | The 504 timing rows and fixed paired formula | Case-level `D_ms`, `MAD_ms`, `U_ms`, `bound_ms`, three `delta_ms` and `trial_U_ms`, and `small_overhead` | Failure is reported with a fixed code and without a complete-measurement claim | The reader can recompute the comparisons from the published rows |
| Private integrity audit | CI verifier checks the run before upload | Detailed client, server and resource records plus public timing rows | Owner-only audit of source, operation and timing consistency | Mismatch stops upload | Every public row agrees with the corresponding verified private measurement |
| Local diagnosis | Developer explicitly uses a local debug path | Same workload and owner-only diagnostic destination | Detailed traces stay local | Retain failure context for investigation | Debug records and arbitrary local text are rejected by the public upload boundary |

The pair runner selects `public` or `debug` before connecting to the server.
The hosted worker explicitly selects `public`; this mode requires the fixed
public timing root and writes a typed row at every timed call. A developer may
select `debug` only outside GitHub Actions, with two raw paths under an
owner-only local debug root. Debug mode writes detailed raw rows and produces
no public timing files. Unknown modes, a mixed public/debug destination, or a
debug request in CI fail before the workload starts. The public uploader
accepts neither a debug root nor raw records.

## Public schema and verification

The timing writer emits canonical JSONL with exactly `schema`, `case`,
`trial`, `mode`, `sample`, `pair_ordinal` and `elapsed_seconds`. Case and mode
are fixed enums. Trial, sample and ordinal are bounded integers, and elapsed
seconds is finite and nonnegative. The writer creates distinct, exclusive
files for each case/trial/mode and records a value immediately after its
timed operation. Resource observations and private raw output have a separate
writer.

The successful `public.json` contains the complete timing inventory, the
computed paired comparison, fixed source and subject digests, and outcome fields. The uploader
rebuilds the expected public object from the typed timing inventory and
requires exact canonical bytes. It rejects extra fields, files and text.
The private audit checks each public elapsed value and ordinal against the
same operation's raw record before publication and again at the final export
boundary. The fixed subject digests are checked against the independently
pinned lock. `performance_qualification`
remains `false` until the separate product-performance acceptance gate passes.

## Remaining acceptance work

- Verify the full hosted run and public artifact against the exact source and
  qualification pins before treating these local checks as acceptance evidence.
