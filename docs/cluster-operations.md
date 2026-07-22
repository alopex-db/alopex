# Cluster management operations

v0.8 exposes metadata management through `alopex server cluster`. These
commands manage cluster metadata only: they do not add remote user-data writes,
distributed SQL DDL/DML, range split/merge, or a distributed transaction API.

Every operation is sent to `POST /api/admin/cluster/operations` using the
configured server profile's normal HTTP authentication. The caller supplies a
stable `--request-id`; reusing it for a retry preserves the server-side
idempotency key. `--expected-version` is optional optimistic concurrency input.

## Preconditions and mutation safety

Read operations require `--request-id REQUEST_ID`. A targeted read also
requires `--target JSON`. Every mutation requires all of:

```text
--request-id REQUEST_ID --target JSON --confirm
```

`--target` must be syntactically valid JSON before the CLI sends a request. Its
operation-specific meaning is validated by the server's committed metadata
layer. The CLI never fabricates a target, auto-confirms a mutation, or treats a
missing cluster foundation as success.

Cluster control is usable only when the configured node reports the necessary
cluster capability and an attached metadata consensus adapter. In single-node
mode, or when a prerequisite is unavailable, the response reports that state;
it does not fall back to in-memory multi-node control.

## Command coverage

| CLI operation | HTTP `operation` | Kind | Target / confirmation | Normal response | Unavailable or rejected response |
| --- | --- | --- | --- | --- | --- |
| `metadata show` | `metadata_show` | read | request ID | committed metadata-control status | classified outcome with capability prerequisite |
| `members list` | `members_list` | read | request ID | committed member projection | classified outcome |
| `members replace` | `members_replace` | mutation | target + confirm | committed replacement version | terminal/retryable/pending outcome |
| `ranges list` | `ranges_list` | read | request ID | committed range projection | classified outcome |
| `ranges show` | `ranges_list` | targeted read | target | range-filtered projection | classified outcome |
| `ranges register` | `ranges_register` | mutation | target + confirm | registered, provisioned range version | terminal/retryable/pending outcome |
| `ranges update` | `ranges_update` | mutation | target + confirm | updated range version | terminal/retryable/pending outcome |
| `ranges retire` | `ranges_retire` | mutation | target + confirm | retired range version | terminal/retryable/pending outcome |
| `placement get` | `placement_get` | targeted read | target | placement projection | classified outcome |
| `placement set` | `placement_set` | mutation | target + confirm | placement version | terminal/retryable/pending outcome |
| `placement replace` | `placement_replace` | mutation | target + confirm | replacement version | terminal/retryable/pending outcome |
| `read-policy get` | `read_policy_get` | read | request ID | read-policy projection | classified outcome |
| `read-policy set` | `read_policy_set` | mutation | target + confirm | policy version | terminal/retryable/pending outcome |
| `schema owner get` | `schema_owner_get` | read | request ID | schema-owner projection | classified outcome |
| `schema owner set` | `schema_owner_set` | mutation | target + confirm | owner version | terminal/retryable/pending outcome |
| `schema rollout start` | `schema_rollout_start` | mutation | target + confirm | rollout operation/version | terminal/retryable/pending outcome |
| `schema rollout status` | `schema_rollout_status` | read | request ID | rollout projection | classified outcome |
| `recovery status` | `recovery_status` | read | request ID | recovery operation state | classified outcome |
| `recovery restore` | `recovery_restore` | mutation | target + confirm | durable recovery operation | terminal/retryable/pending outcome |
| `upgrade status` | `upgrade_status` | read | request ID | resumable upgrade state | classified outcome |
| `upgrade start` | `upgrade_start` | mutation | target + confirm | durable upgrade operation | terminal/retryable/pending outcome |

`members` and `ranges` also accept the visible aliases `member` and `range`.

## Output contract

Table output and `--output json` contain the same fields:

| Field | Meaning |
| --- | --- |
| `Operation ID` | The caller-supplied idempotency and correlation ID. |
| `Operation` | Wire operation invoked by the CLI. |
| `Outcome` | `succeeded`, `pending`, `retryable_failure`, `terminal_failure`, or an authorization failure. |
| `Reason` | Stable diagnostic reason; never infer success from a missing value. |
| `State Version` | Committed metadata version, only when one is available. |
| `Control Available`, `Control Mode`, `Control Reason` | Capability and prerequisite state for the node handling the request. |
| `Missing Prerequisites` | Named prerequisites preventing cluster control, if any. |
| `Actor` | Authenticated actor when the server exposes it. |

For example, a machine-readable pending result is emitted before the non-zero
exit status:

```json
[
  {
    "Operation ID": "schema-rollout-42",
    "Operation": "schema_rollout_start",
    "Outcome": "pending",
    "Reason": "metadata_consensus_adapter_not_attached",
    "State Version": "N/A",
    "Control Available": true
  }
]
```

The remaining documented fields are present in the actual object as well. JSON
column names intentionally mirror the table headings, so they cannot disagree.

## Exit-status matrix

The CLI writes its structured response before returning the following status.
An HTTP authorization rejection that prevents a management response uses the
same authorization exit class and is reported on stderr.

| Outcome class | Exit status | Automation meaning |
| --- | ---: | --- |
| `succeeded` / `success` | 0 | Completed with the returned committed version, if any. |
| `pending` | 2 | Accepted but not complete; poll status with the same operation ID. |
| `retryable_failure` | 3 | No success was claimed; retry with the same request ID after the stated condition changes. |
| `terminal_failure` or unknown class | 1 | Rejected or permanently failed; do not infer a commit. |
| `authorization_failure`, `authorization_*`, `permission_*`, `forbidden` | 4 | The authenticated principal lacks the required authority. |

The v0.8 route may return `cluster_capability_unavailable` or
`metadata_consensus_adapter_not_attached` depending on the runtime foundation.
Both are truthful non-success states, not indications that distributed metadata
was committed.
