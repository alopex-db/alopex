# JSON-on-TEXT SQL contract

This document defines the v0.8.10 JSON compatibility surface. Alopex stores
these values as `TEXT`; native JSON/JSONB types remain outside this contract.

## Supported surface

| Class | Functions |
| --- | --- |
| Scalar | `JSON`, `JSON_VALID`, `JSON_TYPE`, `JSON_EXTRACT`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON_INSERT`, `JSON_REPLACE`, `JSON_SET`, `JSON_REMOVE`, `JSON_ARRAY_LENGTH` |
| Table | `JSON_EACH`, `JSON_TREE` |
| Aggregate | `JSON_GROUP_ARRAY`, `JSON_GROUP_OBJECT` |

`JSON_EACH` and `JSON_TREE` use the existing implicitly-LATERAL table-function
path. Their visible columns are `key`, `value`, `type`, `atom`, `id`, `parent`,
`fullkey`, and `path`; callers may use a relation alias column list when a name
such as `key` is reserved by the SQL grammar.

## Compatibility decisions

- Input accepts strict RFC 8259 JSON. `JSON_VALID` returns `FALSE` for malformed
  input; other JSON functions return a controlled SQL evaluation error.
- Paths start with `$` and support `.label`, `[N]`, `[#-N]`, and `[#]` (append
  for update functions). A missing path returns SQL `NULL` for extraction and
  type inspection.
- Parsed object input uses `serde_json`'s last-member-wins rule for duplicate
  labels. `JSON_OBJECT` and `JSON_GROUP_OBJECT` preserve duplicate output labels
  in call/row order.
- Signed 64-bit integers remain exact. Decimal and larger unsigned values use
  IEEE 754 double precision when they cannot be represented as an Alopex
  integer. Unicode strings are preserved as UTF-8.
- Ordinary SQL `TEXT` arguments to constructors and updates become JSON
  strings. JSON stored as `TEXT` is parsed only by arguments documented as JSON
  input; this boundary avoids an implicit JSONB/native-JSON type claim.
- JSON input and output are limited to 1 MiB, traversal is limited to 100,000
  rows, and the JSON parser's bounded recursion rejects excessive nesting.
- `JSON_GROUP_ARRAY` includes SQL `NULL`. `JSON_GROUP_OBJECT` skips rows whose
  label is `NULL`. Empty groups return `[]` and `{}` respectively.
- Aggregate-local `ORDER BY` is validated but is not part of this JSON subset;
  callers that require a stable array/object order must order the aggregate's
  input relation.

## Verification

`crates/alopex-sql/tests/json_text_functions.rs` covers all 15 functions and
compares representative canonical outputs with bundled SQLite JSON1. The CLI
stdin corpus, embedded SQL integration test, Python v0.8 demo, and public
release verifier exercise the same engine through their public surfaces.
