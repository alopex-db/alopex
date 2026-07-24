# Phase-complete requirements reference

Use this reference while creating a version's initial requirements. It records the
pattern used by the approved v0.8.0 specifications; it is a completeness contract,
not a fixed feature list for every future release.

## Required kickoff matrix

Create a version inventory before writing acceptance criteria. One row is required for
each concrete item, not only for a crate name:

| Field | Required content |
|---|---|
| Roadmap source | Exact roadmap section/version and dependency evidence |
| Product surface | Crate/module, binary, endpoint, API, SQL item, CLI command, or verifier |
| Classification | `new`, `inherited`, `deferred`, or `out-of-scope` with reason |
| Owning phase | Exactly one broad phase; cross-phase policy may be referenced separately |
| Requirement | User story and observable EARS acceptance criteria |
| Compatibility | Local/remote, single-node/cluster, sync/async, supported/rejected behavior |
| Evidence | Unit/integration/e2e/tooling viewpoint and release-gate evidence |

Reject an inventory that contains only coarse rows such as “SQL”, “CLI”, or “Python”.
Expand those rows into the concrete public surfaces that users can invoke or observe.

## v0.8-style phase shape

Use the smallest set of coherent phases that covers the inventory. The approved v0.8
shape is a useful default:

1. **Cluster metadata and operations** — membership, metadata, placement, schema and
   lifecycle/authorization outcomes, plus the inherited cluster CLI and diagnostics.
2. **Distributed-read SQL and CLI** — exact SQL statement/function/PRAGMA matrix,
   routing, local-only and pre-execution rejection, result equivalence, and CLI output.
3. **DataFrame streaming and expressions** — supported sources, bounded stream
   lifecycle, expressions/namespaces, concat, resource limits, and explicit cluster
   boundary.
4. **Python local API surfaces** — sync/async Database and Transaction, SQL/scan
   streams, thread/lifecycle behavior, DataFrame binding, and compatibility/error
   semantics.

For later versions, rename or regroup phases around the new roadmap capabilities, but
keep the same properties: each phase is broad, independently verifiable, and contains
all affected surfaces for its capability. A large feature such as distributed
transactions may be one phase, but its SQL, embedded, server, CLI, Python, failure,
and recovery requirements must be distributed under that phase rather than deferred to
an unspecified “integration” phase.

## Completeness gates before approval

Run this review before requesting requirements approval:

- Compare every roadmap version/phase table, product crate table, DataFrame roadmap,
  Python roadmap, dependency roadmap, and prior approved release spec against the
  inventory.
- Enumerate exact inherited SQL: DDL/DML, SELECT clauses, JOIN/subquery, aggregates,
  scalar/vector functions, PRAGMA/system functions, COPY/Bulk, and HNSW/vector
  statements. Assign each to supported, local-only, or pre-execution rejection for
  the target capability; do not defer the list to design.
- Enumerate every inherited CLI top-level command, subcommand, option/mode, output
  format, TUI/admin path, and lifecycle/cluster operation. Assign the same explicit
  support/rejection status.
- Enumerate server HTTP/gRPC routes, embedded APIs, Python sync/async methods,
  DataFrame operations/namespaces, Nim/FFI surfaces, and `alopex-tools`/candidate
  verifiers. Include already-published surfaces even when the new release does not
  change them.
- Reconcile roadmap status with evidence from released tags, approved specs, source,
  and tests. Never state “inherited complete” while the roadmap or evidence says
  planned without recording the discrepancy and an explicit decision.
- Require a target-version verifier/gate in the requirements. State that an older
  release gate cannot substitute for it, and enumerate the full target-version
  surface matrix it must execute.
- Require every concrete row to map through `requirements → design → task →
  test/evidence`; “one of these documents” is insufficient.
- Compare phase effort, surface count, dependency order, and verification load. Block
  approval when one phase is materially larger or when a later phase is being used to
  hide unfinished earlier scope.

## Boundary of requirements

Keep the matrix, phase ownership, support/rejection status, compatibility outcomes,
policy gates, and evidence expectations in requirements. Keep concrete Rust/Python
types, module layouts, algorithms, wire formats, SQL planner choices, and file-level
implementation steps in design/tasks. Workflow instructions may be kept in the skill,
but must not be presented as product behavior or acceptance criteria.
