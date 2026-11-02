# Columnar exports in v0.8.17

`COPY table TO` selects the table's storage format when retrieving rows.
Columnar tables use the existing fallible columnar scan; row tables continue
using row-key storage. This applies to CSV and Parquet files, Rust
`copy_to_csv_writer`, Python `copy_to_csv`, and CSV STDOUT. CSV values follow the
[NULL and quoting contract](csv-null-compatibility.md).

A nonempty columnar table must export all of its rows, including rows from
separate COPY imports and NULL values. Empty tables legitimately export zero
rows. Check the reported count and compare all values after importing into a
new database and reopening it. File output retains temporary-file replacement:
a read, encoding, or rename failure is an error, keeps the previous destination,
and removes the temporary output. Application-owned streams cannot roll back
bytes already written; callers must discard stream output after an error.

The regression cases use two one-row COPY imports, or an empty table. They
cover disk reopen, full scalar values (TEXT/empty/NULL, INTEGER, BIGINT, DOUBLE,
BOOLEAN, including opposite BIGINT extremes), CSV file/stream and Parquet, with row-table controls. VECTOR Parquet
is described by the separate [VECTOR interchange contract](vector-parquet.md)
tracked by [#693](https://github.com/alopex-db/alopex/issues/693).
A related range-hint overflow during re-import is also corrected: the unsigned
distance between signed integer extrema is computed without signed subtraction.

This correction does not introduce a new bounded-memory export contract:
the existing columnar scan and Parquet writer materialize rows in memory.

## Exporting with an older binary

Published v0.8.15 columnar `COPY TO PARQUET` can silently produce an empty file
from a nonempty table. Updating the destination reader cannot recover those
missing rows. Keep the original database until a reopened destination has been
compared with its authoritative contents.

The [#692 comparison](https://github.com/alopex-db/alopex/issues/692) verified
this limited workaround with published 0.8.15/0.8.16 wheels: SELECT two scalar
rows from the columnar source, explicitly write their CSV fields in the same
column order, create the matching schema in the new database, then import,
close/reopen, and compare every value. The observed data contained INTEGER,
TEXT and DOUBLE values **without NULL or empty TEXT**. For that scope:

```python
import csv
import io

rows = old_db.execute_sql("SELECT id, note, score FROM col ORDER BY id")
text = io.StringIO(newline="")
writer = csv.writer(text)
writer.writerow(["id", "note", "score"])
for row in rows:
    writer.writerow([row["id"], row["note"], row["score"]])
# new_db already has the matching table schema.
count = new_db.copy_from_csv(
    "col", io.BytesIO(text.getvalue().encode("utf-8")), header=True
)
assert count == len(rows)
# Flush, close, reopen new_db, then compare its ordered SELECT with rows.
```

This evidence does not establish a general migration path for arbitrary types,
NULL/empty distinctions, reserved NULL marker text, or VECTOR. Do not extend
that claim to old native columnar CSV/writer output, which was not measured in
that comparison. The separately verified old native Parquet workaround for
scalar **row tables** is described in the CSV policy; it is not a workaround
for columnar tables. Direct opening of an old database remains a separate
compatibility path.
