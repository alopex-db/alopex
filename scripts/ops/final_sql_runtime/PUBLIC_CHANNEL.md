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
