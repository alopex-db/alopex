# DATE, TIME, and INTERVAL in v0.8.10

Alopex v0.8.10 adds native `DATE`, `TIME`, and `INTERVAL` values without changing the existing `TIMESTAMP` representation. Existing row tags `0x00`–`0x09` and existing catalog variants retain their meanings.

## Contract summary

| Type | Canonical value | Precision | Time-zone rule |
| --- | --- | --- | --- |
| `DATE` | signed days from `1970-01-01` | one day | time-zone free |
| `TIME` | microseconds after midnight | one microsecond | time-zone free |
| `TIMESTAMP` | microseconds from the Unix epoch | one microsecond | UTC, unchanged |
| `INTERVAL` | independent signed months, days, and microseconds | one microsecond | time-zone free |

`DATE` validates Gregorian calendar dates, including leap years. `TIME` accepts `HH:MM:SS` with at most microsecond precision. `INTERVAL` keeps month and day components separate so month-end arithmetic can clamp `2024-01-31 + INTERVAL '1 month'` to `2024-02-29` without treating a month as a fixed number of seconds.

Time-zone names, UTC offsets, and DST-aware types remain outside v0.8.10. Alopex rejects time-zone suffixes in typed values rather than applying a machine-local zone.

## SQL surface

Alopex accepts typed literals and casts such as:

```sql
DATE '2024-02-29'
TIME '23:59:59.123456'
TIMESTAMP '2024-02-29 12:00:00'
INTERVAL '-1 month 2 days 03:04:05.000006'
CAST('2024-02-29' AS DATE)
```

The v0.8.10 function set is `CURRENT_DATE`, `CURRENT_TIME`, `MAKE_DATE`, `MAKE_TIME`, `MAKE_TIMESTAMP`, `MAKE_INTERVAL`, `DATE`, `TIME`, `DATETIME`, `TO_DATE`, `AGE`, `DATE_ADD`, and `DATE_SUB`. `CURRENT_DATE` and `CURRENT_TIME` use the same statement-stable UTC clock as `CURRENT_TIMESTAMP`.

Comparisons require matching temporal types; `INTERVAL` comparison is lexicographic by months, days, then microseconds because a calendar month has no fixed duration without an anchor date. Arithmetic supports temporal value plus or minus `INTERVAL`, matching temporal subtraction to `INTERVAL`, and `INTERVAL` addition or subtraction. A `DATE` accepts only whole-day sub-day components during direct arithmetic; callers that need a time-of-day result use `TIMESTAMP`.

## Compatibility notes

PostgreSQL-compatible typed literals, casts, and microsecond precision form the portable core. Alopex and PostgreSQL clamp month-end arithmetic, so `DATE '2024-01-31' + INTERVAL '1 month'` yields `2024-02-29`. SQLite's `DATETIME(..., '+1 day')` result matches Alopex, but SQLite rolls that month-end example into March; Alopex intentionally keeps the PostgreSQL-style clamp. `AGE` returns the exact elapsed day or microsecond components rather than PostgreSQL's symbolic year/month decomposition.

## Storage and API mapping

Row encoding appends `DATE=0x0a`, `TIME=0x0b`, and `INTERVAL=0x0c`. Readers continue to reject unknown tags as corrupted data; readers never reinterpret an unknown tag as `TEXT` or `TIMESTAMP`.

Arrow and DataFrame mappings are `Date32`, `Time64(Microsecond)`, and `Interval(MonthDayNano)`. The Arrow interval adapter rejects nanoseconds that cannot be represented exactly as Alopex microseconds. Python returns `datetime.date`, `datetime.time`, and an interval mapping with `months`, `days`, and `microseconds`. gRPC appends fields 10–12 to its `Value` oneof.

## Lifecycle map

| Path or module | Current responsibility | v0.8.10 responsibility | Migration action | Deletion condition | Verification |
| --- | --- | --- | --- | --- | --- |
| Nim parser and Rust AST | `DATE`/`TIME` collapsed into `TIMESTAMP` | emit distinct types and typed casts | replace the collapsing mapping; bump parser contract to `0.18.0` | old mapping disappears when new parser assets are staged | parser and exported-contract tests |
| `SqlValue` and row codec | tags `0x00`–`0x09` | append tags `0x0a`–`0x0c` | keep old tags byte-identical | no old tag is deleted | old fixture plus round-trip tests |
| persistent catalog | existing enum variants | append temporal variants | old serialized catalogs remain readable | no prior variant is deleted | catalog compatibility tests |
| evaluator and scalar catalog | UTC `TIMESTAMP` functions | calendar parsing, arithmetic, and public functions | reuse the statement clock and `chrono` | unsupported-literal rejection is removed | boundary and regression tests |
| columnar, Arrow, and Parquet | numeric and timestamp mappings | exact temporal mappings | reject lossy interval precision | no numeric fallback remains | conversion round trips |
| Python, Rust, HTTP, and gRPC | timestamp integer transport | distinct temporal values | append wire fields and typed adapters | no previous field number changes | public-surface tests |
| release verifier | parser contract and prior type gate | require temporal demo and all public surfaces | stage fresh `0.18.0` parser assets | stale `0.15.0` assets fail closed | release tests and demo |

The implementation keeps existing `TEXT` columns and values unchanged. Alopex does not migrate old text that happens to contain a date; applications opt into native behavior with a new temporal column or an explicit cast.

## Verification checklist

- Parser tests cover DDL types, typed literals, casts, and contract `0.18.0`.
- Calendar tests cover leap years, invalid dates, month-end clamping, negative intervals, and microsecond precision.
- Storage tests cover all new tags, old rows, unknown-tag rejection, and catalog round trips.
- Public-surface tests cover embedded Rust, Python, DataFrame, HTTP, gRPC, and CLI output.
- Release verification runs the temporal demo and rejects stale parser or capability metadata.
