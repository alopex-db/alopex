# JSON and JSONB SQL contract

Alopex v0.8.10 provides a canonical native `JSON` type; `JSONB` is a dialect
alias for the same storage representation. The older JSON-on-`TEXT` functions
remain compatible and continue to return `TEXT`.

## Native JSON/JSONB

```sql
CREATE TABLE documents (id INTEGER PRIMARY KEY, body JSONB);
INSERT INTO documents VALUES
  (1, JSONB '{"b":1,"a":{"items":[null,"ok"]}}');

SELECT body -> 'a' -> 'items' -> 0,
       body #>> '{a,items,1}'
FROM documents;
```

Native input is strict RFC 8259 JSON. Alopex stores compact UTF-8 JSON with
object keys sorted recursively; the last duplicate object key wins. JSON
numbers retain their source precision, including values wider than IEEE 754 or
signed 64-bit integers. `CAST(TEXT AS JSON)` validates and canonicalizes input,
while `CAST(JSON AS TEXT)` returns its canonical representation.

| Class | Surface |
| --- | --- |
| Operators | `->`, `->>`, `#>`, `#>>` |
| Update | `JSONB_SET`, `JSONB_INSERT` |
| Build | `JSONB_BUILD_OBJECT`, `JSONB_BUILD_ARRAY` |
| Aggregate | `JSONB_AGG`, `JSONB_OBJECT_AGG` |

`->` and `#>` return native JSON, so a selected JSON `null` remains distinct
from SQL `NULL`. `->>` and `#>>` return `TEXT`; a selected JSON `null` or a
missing path returns SQL `NULL`. Single-step operators accept a text key or a
non-negative integer index. Path operators accept PostgreSQL-style `'{a,0}'`
or the existing JSONPath subset such as `'$.a[0]'`.

Native JSON equality compares canonical values. Ordering operators and B-tree
indexes are unsupported because Alopex does not define a JSON sort order.
JSON/JSONB is encoded with an append-only row tag and an append-only catalog
variant, so existing rows and catalogs remain readable. Columnar storage uses
canonical UTF-8 bytes; Arrow, DataFrame, and Parquet surfaces map the value to
UTF-8, gRPC uses the appended `json_value` field, Python returns decoded native
`dict`/`list`/scalar objects, and CLI/HTTP output uses canonical JSON text.

The distributed-read catalog classifies all JSON scalar functions as
local-only. Remote aggregate execution does not include JSONB aggregates.

## JSON-on-TEXT compatibility

| Class | Functions |
| --- | --- |
| Scalar | `JSON`, `JSON_VALID`, `JSON_TYPE`, `JSON_EXTRACT`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_INSERT`, `JSON_REPLACE`, `JSON_SET`, `JSON_REMOVE`, `JSON_ARRAY_LENGTH` |
| Table | `JSON_EACH`, `JSON_TREE` |
| Aggregate | `JSON_GROUP_ARRAY`, `JSON_GROUP_OBJECT` |

The compatibility functions accept JSON input as `TEXT` or native JSON but
retain their existing `TEXT` result types. Paths start with `$` and support
`.label`, `[N]`, `[#-N]`, and `[#]`. Ordinary SQL `TEXT` constructor values
remain JSON strings. JSON input and output are limited to 1 MiB, traversal is
limited to 100,000 rows, and excessive nesting is rejected. Aggregate-local
ordering is outside this compatibility subset; callers that require stable
array order must order the input relation.

## Verification

`crates/alopex-sql/tests/json_native.rs` covers native DDL, casts,
canonicalization, precision, operators, updates, builders, aggregates, NULL
semantics, and row-codec round trips. `json_text_functions.rs` retains SQLite
JSON1 comparison coverage for all compatibility functions. Workspace checks
cover catalog, columnar, Arrow/Parquet, gRPC, CLI/HTTP, and Python mappings.
