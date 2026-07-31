# v0.9 Phase 3 verifier handoff

The common-gate owner consumes `evidence/v09-phase3-verifier-input.json` with
the same checkout recorded by its `source_sha`. The input binds the changefeed
manifest, inherited ledger, common-gate input, parity fixture and lockfile by
SHA-256.

The verifier must reject missing, unknown or duplicate inventory rows and a
support-status promotion that lacks approved evidence. Its report must emit the
source SHA, every input hash, row counts, fixture hash, artifact digests,
Durable capability verdict and final Phase 3 verdict.

The current expected verdict is **Blocked**: the compiled capability lacks the
verified Durable requirements listed in the input. A target-version v0.9 gate
may become Ready only after it receives compatible capability evidence and
candidate artifact identities. Existing v0.7/v0.8 scripts cannot be used as a
substitute. This handoff neither implements a verifier nor authorizes a tag,
publish, Docker push, release or deployment.
