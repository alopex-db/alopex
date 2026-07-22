# v0.7.4 to v0.8 candidate upgrade and recovery

Use this procedure only after the v0.8 candidate matrix identifies the exact
artifact versions and platforms to use. It does not publish or install a public
package, and it must not be used to overwrite a production data directory
without a verified backup.

## Before changing a node

1. Record the running v0.7.4 artifact identity and storage location.
2. Create and verify a backup/restore snapshot with the existing operational
   workflow. See the [minimum operations runbook](operations/v0.6-minimum-runbook.md)
   for backup completion and restore-state checks.
3. Retain the backup, its manifest, and the candidate's local readiness report.
   Do not treat a build log, a Markdown status field, or a local approval ID as
   approved scope evidence.
4. For a cluster candidate, verify that the compatible external foundation,
   authenticated transport, durable metadata storage, and read-point evidence
   are available. If any is unavailable, keep the corresponding cluster or
   distributed-read capability marked unavailable.

## Upgrade boundary

- v0.8 cluster management is metadata control. It does not introduce remote
  user-data writes, distributed SQL DDL/DML, range split/merge, or distributed
  transactions.
- Start or resume a cluster-aware upgrade only through the documented
  `alopex server cluster upgrade` operation with an explicit request ID, target,
  and confirmation. Observe its durable operation status; a pending result is
  not a completed upgrade.
- Do not substitute a single-node/in-memory fallback when a configured
  multi-node prerequisite is missing. Preserve the classified unavailable or
  failure result instead.
- Keep the v0.7.4 backup until the upgraded node's documented local workflow,
  metadata state, and recovery status have been verified.

## Recovery and rollback

If upgrade validation fails or is interrupted, stop before accepting new writes
to the affected data. Inspect the durable upgrade/recovery status, then restore
only from the recorded verified snapshot using the normal restore workflow.
The recovery/upgrade operation is resumable only when its source identity,
checkpoint, and compatibility evidence match; otherwise it must return a
classified terminal outcome rather than silently continuing.

Recovery confirmation requires a completed restore status and a verified local
read workflow. It does not establish that a cluster or distributed-read feature
is supported unless its separate prerequisite evidence is present in the
candidate matrix.
