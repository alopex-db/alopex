# Standard predicates and row values

Alopex v0.8.8 supports portable truth predicates, null-safe equality, and
row-value comparisons on every SQL execution surface. Parser contract `0.16.0`
is compatibility metadata within the same Alopex release; it is not a separate
parser version lane.

## Supported SQL

| Form | Result contract |
| --- | --- |
| `value IS [NOT] TRUE` | total Boolean test; NULL is not TRUE |
| `value IS [NOT] FALSE` | total Boolean test; NULL is not FALSE |
| `value IS [NOT] UNKNOWN` | total Boolean test for SQL NULL |
| `left IS [NOT] DISTINCT FROM right` | null-safe scalar or row equality; never UNKNOWN |
| `(a, b) =, <>, <, <=, >, >= (c, d)` | SQL three-valued row comparison |
| `(a, b) [NOT] IN ((...), (...))` | row equality applied to each candidate |
| `(a, b) [NOT] BETWEEN (...) AND (...)` | inclusive lexicographic bounds |

Truth predicates accept BOOLEAN or NULL. Row operands must have the same field
count, and corresponding fields must be comparable under the existing scalar
numeric-widening and type rules.

## NULL and ordering rules

Row equality compares every field. A definite unequal field makes `=` false
even if another field is NULL; otherwise any unknown field makes the result
UNKNOWN. `<>` is the three-valued negation of `=`.

Ordering compares fields from left to right. A definite unequal pair decides
the result. If the first undecided pair contains NULL, the result is UNKNOWN;
later fields cannot override it. `IS DISTINCT FROM` instead treats two NULLs as
equal and one NULL as distinct, so its result is always TRUE or FALSE.

Examples:

```sql
SELECT (1, NULL) = (2, NULL);                 -- FALSE
SELECT (1, NULL) = (1, NULL);                 -- NULL
SELECT (1, NULL) < (2, 0);                    -- TRUE
SELECT (1, NULL) < (1, 0);                    -- NULL
SELECT (1, NULL) IS DISTINCT FROM (1, NULL);  -- FALSE
```

## Controlled errors and execution boundary

| Invalid input | Stable error |
| --- | --- |
| truth predicate on a non-Boolean scalar | `ALOPEX-T001` |
| incompatible corresponding row fields | `ALOPEX-T001` |
| different row widths | `ALOPEX-T013` |
| standalone projected row value | fail-closed unsupported-feature diagnostic |

Rows are expression-level comparison operands, not persisted `SqlValue`
columns. The planner validates and lowers them to executable predicates before
storage or columnar boundaries, keeping the existing scalar storage format
unchanged. Materialized and streaming plans therefore share the same evaluator
and three-valued behavior.

## Parser and release lifecycle

Contract `0.8.0` adds `Row`, `TruthPredicate`, and `IsDistinctFrom` expression
variants. A producer/consumer mismatch is rejected before MessagePack decode.
The tracked v0.8.4/contract-0.4.0 vendor files remain historical; v0.8.8 must
publish fresh Linux x86_64, macOS x86_64/arm64, and Windows x86_64 parser
assets using current contract `0.16.0`, retarget the manifest and wheels, and
pass the public release verifier.

| Path or artifact | Current responsibility | v0.8.8 responsibility | Action | Replacement condition | Verification |
| --- | --- | --- | --- | --- | --- |
| `PARSER_CONTRACT_VERSION` and Rust consumer pin | select the current wire schema | require contract `0.16.0` | keep in sync | every schema-changing parser commit | contract mismatch and exported-version tests |
| tracked v0.8.4/contract-0.4.0 vendor files | immutable historical fixture | remain historical evidence | keep | never relabel or overwrite in development | manifest and checksum tests |
| four target parser archives and manifest | release-native parser identity | carry fresh contract-0.16.0 bytes | replace in release staging | all target records and native smoke checks pass | release join verifier |
| Python package-local native files | wheel runtime parser | match the target archive exactly | replace during wheel assembly | target digest and contract match | wheel-content verifier and Python demo |

Contract `0.13.0` and every earlier identifier are no longer legal current
producers. A stale producer or a relabelled sidecar fails before payload
decoding; rollback therefore means returning the whole Alopex release
candidate to its previous source and asset set, not mixing parser contracts.

## Portability fixture

[`standard_predicates_reference.json`](../crates/alopex-sql/tests/fixtures/standard_predicates_reference.json)
pins exact expected rows and reference versions. The fixture test executes
Alopex and checks those rows; it does not claim to launch the external engines.
The expectations follow the published comparison semantics for
[PostgreSQL row comparisons](https://www.postgresql.org/docs/current/functions-comparisons.html),
[PostgreSQL truth and distinctness predicates](https://www.postgresql.org/docs/current/functions-comparison.html),
[DuckDB distinctness comparisons](https://duckdb.org/docs/stable/sql/expressions/comparison_operators),
and [DataFusion comparison operators](https://datafusion.apache.org/user-guide/sql/operators.html).
