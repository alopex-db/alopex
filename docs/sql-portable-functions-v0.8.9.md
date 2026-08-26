# Portable SQL functions in v0.8.9

Alopex v0.8.9 adds portable functions over the scalar types that already
exist. Timestamp values remain UTC epoch microseconds; this release does not
add `DATE`, `TIME`, or `INTERVAL` storage types.

## Surface summary

- Temporal: `CURRENT_TIMESTAMP`, `NOW`, `EXTRACT`, `DATE_PART`, `DATE_TRUNC`,
  `TO_CHAR`, `TO_TIMESTAMP`, `STRFTIME`, `JULIANDAY`, and `UNIXEPOCH`.
- Integer table function: `GENERATE_SERIES(start, stop [, step])`.
- Bitwise operators: `~`, `<<`, `>>`, `&`, `^`, and `|`.
- Math, string, regex, statistics, regression, value-selecting, bitwise, and
  boolean aggregates are listed in the SQL README.

All SQL execution surfaces use the same parser, planner, and evaluator. The
Nim/Rust parser wire contract for these expressions is `0.15.0`.

## Timestamp contract

`CURRENT_TIMESTAMP` and `NOW()` are fixed for one statement. An optional
precision from 0 through 6 truncates fractional microseconds. Timestamp
functions are UTC-only and do not apply session time zones or DST rules.
Unsupported units, invalid formats, and out-of-range values return stable SQL
errors.

`EXTRACT` and `DATE_PART` accept microsecond, millisecond, second, minute,
hour, day, day-of-week, ISO day-of-week, day-of-year, week, month, quarter,
year, and epoch fields. `DATE_TRUNC` accepts microsecond through year units.
`TO_CHAR` and the two-argument `TO_TIMESTAMP` use the supported PostgreSQL
tokens `YYYY`, `MM`, `DD`, `HH24`, `MI`, `SS`, and `US`. `STRFTIME` uses
Chrono/strftime formatting. One-argument numeric `TO_TIMESTAMP` interprets its
input as Unix seconds.

## Integer and resource contract

Bitwise operators accept `INTEGER` and `BIGINT`; mixed operands return
`BIGINT`. Right shift is signed. A negative shift, a shift at least as wide as
the result, or a left shift that loses significant bits returns integer
overflow. Precedence from tightest to loosest is unary `~`, arithmetic,
shifts, `&`, `^`, `|`, string concatenation, comparison, `NOT`, `AND`, `OR`.

Integer `GENERATE_SERIES` includes both boundaries, accepts positive or
negative non-zero steps, and returns no rows when the step moves away from the
stop value. It rejects arithmetic overflow and output beyond 100,000 rows.
Arguments are implicitly lateral, so the function composes with CTEs, joins,
and windows. The timestamp/interval overload remains dependent on native
`INTERVAL` support in issue #159; it is not silently treated as complete.
