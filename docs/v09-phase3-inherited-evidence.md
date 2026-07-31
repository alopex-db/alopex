# v0.9 Phase 3 inherited evidence ledger

`evidence/v09-phase3-inherited-ledger.json` is the machine-readable, one-row
crosswalk for the Phase 1 roadmap inventory. It covers I-01 through I-26
(including I-04a/I-04b, I-09a/I-09b, I-10a/I-10b and I-23a/I-23b) plus F2,
F3 and F4. No range shorthand is used in the JSON rows.

## Meaning of the evidence

Each row retains the canonical roadmap status, primary owner phase and Phase 1
task anchor from the approved inventory. Test paths are regression pointers,
not a claim that Phase 3 implemented their feature. In particular, SQL and
DataFrame changefeed boundary tests prove pre-execution rejection; they do not
make SQL, CRDT, distributed transactions or DataFrame execution a feed
surface.

The source SHA records the release worktree from which these pointers were
collected. It deliberately is not labelled as a release or approval SHA. I-24
through I-26 retain `pending-3.27` until the common gate records the verified
Durable capability, verifier input and target-version release identity. A
missing common-gate identity remains visible and must fail closed rather than
being substituted with a v0.7 or v0.8 gate.

## Validation contract

The ledger must have exactly 33 unique rows: 30 inventory rows (I-01 through
I-26 including four suffix rows) and F2/F3/F4. Every row carries `id`, roadmap
status, classification, owner phase/task, at least one concrete evidence
pointer, verification viewpoint and gate disposition. `3.27` and `3.28` use
this ledger as input; they may add common-gate evidence but may not move an
owner or promote an unsupported boundary to supported.

The current F3 row is backed by the committed-event, checkpoint/replay,
retention/backpressure, boundary and parity fixtures. Its coverage is separate
from F2 and F4, whose semantics remain owned by their approved phases.
