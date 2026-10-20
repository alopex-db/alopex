# CSV NULL and text values in v0.8.17

The same CSV contract applies to SQL `COPY table TO`, `COPY (SELECT ...) TO`,
`COPY FROM`, Rust CSV reader/writer methods, and Python CSV streams.

| CSV field | Value read into a TEXT column |
| --- | --- |
| `\N` (unquoted) | SQL NULL |
| `"\N"` | The two-character TEXT `\N` |
| Empty field or `""` | Empty TEXT |
| `NULL` (unquoted, case-insensitive, optionally surrounded by whitespace) | SQL NULL, preserving the legacy input convention |
| `"NULL"` or `" null "` | The exact quoted TEXT |

The writer emits SQL NULL as unquoted `\N`. It quotes empty TEXT, text equal
to `\N`, text that would match the legacy NULL convention, and fields containing
commas, quotes, CR, or LF. Quotes inside a quoted field are doubled. Backslashes
are not escapes. TEXT whitespace and Unicode are retained. Numeric and other
typed fields continue to use their type parsers; quoting `"NULL"` does not make
it a valid integer.

The reader retains quoting information while using the existing CSV parser.
It buffers a record and parser read-ahead, not the entire input file. Header
handling, CR/LF/CRLF, multiline fields, UTF-8 validation, and the existing final
unquoted VECTOR-field convention remain supported.

## Existing CSV files

Existing ordinary CSV fields and empty TEXT fields retain their meaning.
Unquoted `NULL` still means SQL NULL. The following inputs change intentionally:

- Unquoted `\N` is now reserved for SQL NULL. Quote it as `"\N"` when it is TEXT.
- Quoted `"NULL"` now preserves TEXT rather than becoming SQL NULL.
- TEXT retains its original leading/trailing whitespace rather than trimming it.

New exports require a reader that implements this NULL/quoting contract.
Published v0.8.15/v0.8.16 CSV readers do not recognize `\N` as SQL NULL;
reading a new export with an old binary is not a supported NULL-preserving
round trip.

The old writer emitted both SQL NULL and empty TEXT as the same empty field.
A new reader cannot reconstruct which original value produced such a field.
Those files must be re-exported from an authoritative database if the distinction
matters. Updating a library cannot repair already-exported bytes, and does not
change the behavior of published v0.8.15/v0.8.16 binaries.

## Exporting from an older published version

When a scalar **row table** must be exported with an old binary, the verified
alternative is native Parquet export/import, or a typed logical export.
The [#691 comparison](https://github.com/alopex-db/alopex/issues/691) used public
0.8.15 and 0.8.16 wheels and retained NULL, empty TEXT, and 64-bit integers in
three rows through Parquet. It separately verified a typed logical export.
This evidence is specific to that data and path; columnar-table export and
VECTOR Parquet are tracked in #692 and #693.

```sql
-- Export a scalar row table using the old binary.
COPY scalars TO '/absolute/path/scalars.parquet' WITH (FORMAT PARQUET);
-- Recreate the original schema in the new database, then import.
COPY scalars FROM '/absolute/path/scalars.parquet' WITH (FORMAT PARQUET);
```

Preserve the original schema and compare values after closing and reopening
the destination database. Direct opening of an old database is a separate
compatibility path; this CSV correction does not require every user to export
and re-import data.
