# Final SQL public channel contract

The release operator starts the fixed same-host comparison after the selected
environment passes #804 qualification and four exact subject pins are reviewed.
The inputs are four `select-all` preflights followed by the 48 planned cells,
with 21 measured calls per cell. Each call returns its full SQL result and a
client elapsed time and resource observation.

The public-purpose `worker.public_measure` entry point runs the same pinned
SQL, fixture, three warmups and 21 measured calls, using the archived harness
only for its pinned observer and exact-result validation dependency. The
archived private `measure` entry point is never called by this public job.
The public entry point writes one fixed numeric row from the local elapsed
value and observer return during each measured call. The private raw log is
never the source of the public row. The worker and collection
boundary check the public row against private raw; the independent audit
rechecks it against authenticated private collection bytes and binds the
1,008 public rows by digest.

The output is a public-only JSON object with fixed subject hash identities,
four preflight result checksums, 1,008 ordered numeric operation rows and a
comparison summary reproducible from those rows. It excludes process IDs,
paths, host and runtime identity, raw log and free-form errors. A failure
status uses a fixed error code and a bounded completed-run count, never a
complete-measurement claim. The separate private debug and audit material
stays in caller-owned local storage.

Acceptance checks are exact 4 + 48 schedule coverage, 21 samples per cell,
fixed field allowlists, finite numeric values, full SQL result equality,
unchanged comparison rules, independent private/public equality and 1,008-row
completeness. Missing, duplicate, altered or extra public rows stop export.
The public comparison recomputes trial medians and comparison rules from the
public rows, checks private summary agreement, and exports those public
calculations. Controller failures carry a fixed-code public status with a
bounded completed-run count while retaining the underlying cause privately.
The count advances only after cleanup and the run and matrix deadline checks
succeed; a collected row from a failed cleanup is not counted.
Local tests establish code behavior only; selected-host execution, current
subject pins and #804 qualification remain required for release acceptance.

## Public artifact boundary before workflow integration

| Use case | Actor and trigger | Inputs | Output and exposure | Failure | Acceptance |
| --- | --- | --- | --- | --- | --- |
| Publish the final comparison | The release job finishes the exact 52-run controller and its independent audit | Controller operation, audited collections, fixed subject pins and the controller's public projection | One canonical, owner-only `public.json` containing only the fixed public comparison schema; the workflow artifact includes that file alone | A mismatch, incomplete run or extra export member prevents successful upload | Recompute all 1,008 public rows and summary against authenticated private observations at export time; verify the exact file allowlist and schema again immediately before upload |
| Report a failed run | The release job catches a controller failure | Only the fixed public failure code | A fixed failure line in the job log with an explicit unknown completed-run count; no measurement artifact | Any failed or uncertain cleanup prevents upload | Reject arbitrary exception text and additional fields; never turn a failed run into a completed artifact |
| Diagnose locally | A developer explicitly starts a debug run outside the public workflow | Private operation, collection and exception detail | Owner-only local diagnostic data, never an upload input | Preserve the private cause locally | The public writer and uploader reject debug files and extra fields |

The success writer must receive the controller's private operation and audit
objects in the same process and independently regenerate the public projection.
At export and immediately before upload, a fresh evidence callback must reopen
all private collections and reauthenticate receipts after cleanup; the verifier
reruns the independent audit and matches the public file to it. Cached operation
and audit objects alone cannot detect a private file changed after the first
audit. The verifier also checks exact directory membership, canonical bytes,
fixed fields, row order, finite values and recomputed summary. The workflow
names only this verified file in its upload step. A failed run produces no
artifact, including no stale status file. Neither a CMS bundle nor post-hoc removal of local fields is part of
the public path. A local test of this boundary does not establish selected-host
qualification or measured performance.
