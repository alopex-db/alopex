# Stress evidence use cases

This contract applies to the shared stress harness and the v0.8.17 Stage 2
stress, sanitizer, fuzz and performance jobs. Choose the evidence purpose before
the workload starts. Evidence mode changes recording, not the workload or its
pass criteria.

| Use case | Actor and trigger | Inputs | Output and exposure | Failure behavior | Acceptance check |
| --- | --- | --- | --- | --- | --- |
| Public measurement | Release operator starts a scheduled or manual Stage 2 job | Exact source, fixed test configuration, seed and pinned baseline | Public, structured case result, configuration needed to reproduce the workload, numeric measurements and fixed check outcomes | Publish a bounded failed result with a fixed code; keep raw process output on the runner only | The public artifact has the expected case/source, measured values and no machine identity, capacity, absolute path, environment variable, command output or free-text exception |
| Local diagnosis | Developer explicitly selects debug mode outside CI | Same workload inputs plus an owner-only local destination | Detailed local artifacts and logs for investigating a failed test; never passed to a publishing step | Preserve detailed local failure context | Debug mode requires explicit selection and cannot run as the Stage 2 publication mode |
| Public review and replay | Reader downloads a Stage 2 artifact | Published source identity, seed, fixed configuration, measurements and baseline identity | Reproducible result and numeric comparison without a private key | Missing or malformed required measurements fail the case evidence check | Reader can tell what ran, whether it passed, and how measured values compare with the pinned baseline |

## Fixed output contract

- `STRESS_EVIDENCE_MODE=public` is the Stage 2 mode and the safe default. The
  harness generates public fields from typed values. It does not gather host
  specifications or environment variables for its public record. Its run and
  check records contain no arbitrary topology, path, command or error text.
  Metrics and performance comparisons retain finite numeric values and fixed
  identifiers. Fuzz jobs record a bounded execution count and duration;
  sanitizer jobs record duration. Raw stacks and crash inputs are not public.
- `STRESS_EVIDENCE_MODE=debug` is explicit local opt-in. It may collect detailed
  diagnostics under an owner-only local root, and CI rejects this mode. The
  Stage 2 uploader accepts only the public output root and checks source,
  schema, file type and bounded size before transfer.
- The legacy performance reference is an exact successful scheduled artifact
  with pinned run/source/ZIP digest. Public results include baseline identity,
  current values and comparison results; no run silently seeds or changes it.
- An unknown mode, missing required public record, unexpected field or file,
  nonfinite measurement, symlink, raw text or capacity check failure stops
  publication. A passing test without its required public measurement is not
  accepted as a complete evidence case.

Operational capacity inventories remain private to the runner. They enforce the
per-host work budget and are never included in measurement artifacts.
