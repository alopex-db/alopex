# Nested SQL types

Alopex 0.8.10 provides native `ARRAY`/`LIST`, `MAP`, and `STRUCT` values. `LIST<T>`
is a spelling alias for `ARRAY<T>`; catalog output uses `ARRAY<T>`.

```sql
CREATE TABLE events (
  tags ARRAY<TEXT>,
  counters MAP<TEXT, INTEGER>,
  actor STRUCT<name TEXT, groups ARRAY<TEXT>>
);

SELECT ARRAY[10, 20, 30][2], ARRAY[10, 20, 30][2:3];
SELECT u.value, u.ordinality
FROM UNNEST(ARRAY['a', 'b']) WITH ORDINALITY AS u(value, ordinality);
```

## Functions and semantics

The scalar catalog contains `ARRAY_APPEND`, `ARRAY_PREPEND`, `ARRAY_CAT`,
`ARRAY_REMOVE`, `ARRAY_REPLACE`, `ARRAY_LENGTH`, `ARRAY_POSITION`,
`ARRAY_POSITIONS`, `STRING_TO_ARRAY`, and `ARRAY_TO_STRING`. `ARRAY_AGG` is the
aggregate form. `MAP(keys, values)` and `STRUCT_PACK(name, value, ...)` construct
the other nested values.

Array subscripts are one-based. An out-of-range subscript returns `NULL`; a slice
uses inclusive one-based bounds. Arrays preserve `NULL` elements. Scalar array
functions return `NULL` for a `NULL` array input, except constructors, which retain
their supplied `NULL` elements. `UNNEST` preserves element order, and `WITH
ORDINALITY` adds a one-based `BIGINT` position. Existing `LATERAL` correlation is
used when the input refers to a preceding relation.

The planner infers a common homogeneous element type. Numeric elements widen to
the common numeric type; incompatible element types fail during planning. `MAP`
keys and values are homogeneous, while `STRUCT` fields retain their declared
names and individual types.

## Storage and public mappings

Nested row tags are append-only and recursive, so old scalar rows remain readable.
Catalog persistence records recursive element, key/value, and field types. Primary
and B-tree indexes reject nested values because Alopex does not define their sort
order.

Rust exposes `SqlValue::Array`, `SqlValue::Map`, and `SqlValue::Struct`. Python
maps arrays to `list` and maps/structs to `dict`. CLI and gRPC compatibility fields
use canonical JSON text. Parquet input accepts Arrow `List`/`LargeList`, `Map`, and
`Struct`; UTF-8 canonical JSON remains an import fallback. The embedded DataFrame
surface currently exports nested values as UTF-8 canonical JSON because its public
column builder has no recursive schema input. Text-keyed maps use JSON objects;
maps with another key type use ordered `[key, value]` pair arrays.

A single nested collection is limited to 100,000 entries and nesting depth is
limited to 16. Decoders validate lengths before allocation. The v0.8 distributed
read catalog classifies nested scalar functions, `ARRAY_AGG`, and `UNNEST` as
local-only before transport.

## Compatibility notes

The SQL spelling and one-based subscripting follow PostgreSQL/DuckDB conventions.
`ARRAY_AGG` retains `NULL` values, matching PostgreSQL. Arrow and Parquet physical
mapping follows their native nested arrays; the DataFrame UTF-8 fallback differs
from Polars native `List`/`Struct` columns and is intentionally documented rather
than presented as zero-copy interoperability.
