# TRY_CAST and conversion safety

Alopex v0.8.8 supports `TRY_CAST(expression AS type)` on Rust, CLI, Embedded,
and Python SQL surfaces. Ordinary `CAST` keeps failing the statement when a
conversion is impossible; `TRY_CAST` returns SQL NULL for that row. Both forms
use one conversion implementation, so safety rules cannot drift by surface.

## Behavioral boundary

`TRY_CAST` catches only conversion failures. Errors raised while evaluating
the source expression, such as division by zero, remain errors. NULL input
always produces NULL. Failed CAST reports `ALOPEX-E004` with source type,
target type, and a bounded public reason; AST, MessagePack, and planner-internal
names are never included.

| Target | Accepted source and rule | Controlled failure examples |
| --- | --- | --- |
| `INTEGER` / `BIGINT` | NULL, numeric, BOOLEAN, TIMESTAMP epoch micros, base-10 TEXT; floating values truncate toward zero | parse failure, non-finite value, target overflow |
| `FLOAT` / `DOUBLE` | NULL, finite numeric/BOOLEAN/TIMESTAMP, finite numeric TEXT | NaN, Infinity, text parse failure, finite-to-FLOAT overflow |
| `TEXT` | NULL, scalar numeric/BOOLEAN/TIMESTAMP, TEXT, valid UTF-8 BLOB | invalid UTF-8 BLOB, VECTOR |
| `BLOB` | NULL, BLOB, UTF-8 TEXT bytes | numeric, BOOLEAN, TIMESTAMP, VECTOR |
| `BOOLEAN` | NULL, BOOLEAN, finite numeric zero/non-zero, TIMESTAMP, accepted case-insensitive TEXT forms | NaN/Infinity, unrecognized TEXT, BLOB, VECTOR |
| `TIMESTAMP` | NULL, TIMESTAMP, canonical UTC text, integral epoch-microsecond numeric | timezone suffix, fractional/non-finite/out-of-range epoch, other types |
| `VECTOR(n)` | NULL or a finite VECTOR with exactly `n` elements | dimension mismatch, NaN/Infinity element, non-vector source |

Accepted BOOLEAN text is `true/t/yes/y/1` or `false/f/no/n/0` after trimming.
Timestamp text uses `YYYY-MM-DD HH:MM:SS[.fraction]` without a timezone suffix.

## Literal and runtime parity

The planner retains a typed `TryCast` node for literal and column expressions;
the evaluator calls the same conversion function for both. Regression tests
compare valid and invalid literal results with rows read from a TEXT column.
Any future constant-folding pass must call that conversion function and retain
the rule that source-expression errors are not converted to NULL.

## Parser and release lifecycle

Parser contract `0.9.0` adds the dedicated `TryCast` expression variant. The
identifier is compatibility metadata inside the unified Alopex v0.8.8 release,
not an independent parser release lane. A contract-0.8.0 producer is rejected
before MessagePack decode because it cannot represent the semantic distinction.

| Path or artifact | Current responsibility | v0.8.8 action | Replacement condition | Verification |
| --- | --- | --- | --- | --- |
| Nim source, descriptor, Rust consumer | emit and require contract `0.20.0` | keep synchronized | every wire-schema change | parser, MessagePack, exported-version tests |
| historical vendor files | immutable released evidence | preserve; never relabel | never during development | manifest and checksum tests |
| four parser archives and manifest | native producer identity | stage fresh contract-0.20.0 Linux x86_64, macOS x86_64/arm64, Windows x86_64 assets | all target records and native smoke pass | release join verifier |
| Python wheel native copies | target runtime parser | copy the same verified bytes and sidecars | target digest and contract match | wheel-content verifier and Python demo |

Rollback returns the whole release candidate to its previous source and asset
set. Mixing stale and current parser contracts is always rejected.

The NULL-on-conversion-failure behavior follows the published
[DuckDB TRY_CAST contract](https://duckdb.org/docs/current/sql/expressions/cast)
and the dedicated
[DataFusion TryCast expression](https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/struct.TryCast.html).
