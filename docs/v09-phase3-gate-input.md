# v0.9 Phase 3 common-gate input

`evidence/v09-phase3-common-gate.json` is the hash-pinned Phase 3 handoff for
the target-version common gate. It records I-24 (tools), I-25 (Chirps/Durable)
and I-26 (docs/CI/release identity) without changing their common-gate owner.

The input verdict is deliberately `blocked`. The compiled capability reports
no verified Durable service, durable storage, range routing, retention or
authenticated dispatcher. `DurableProfileAdapter::compiled()` consequently
returns the canonical prerequisite failure; this is a required safety outcome,
not a partial implementation or a reason to claim feed support.

The common gate must hash-check every listed input from the same checkout and
must use a target-version v0.9 workflow. `v07_gate.sh`, v0.8 verifier scripts
and a successful local source build are insufficient substitutes. This handoff
does not run a release action and never authorizes tags, registry publication,
Docker publication, GitHub Release creation or deployment.

When compatible Durable evidence and the approved/released identity exist,
Tasks 3.28 and the release-wide workflow may update the gate input and produce
a reproducible report. Until then any Phase 3 or v0.9 release verdict must
remain blocked.
