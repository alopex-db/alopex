# SQL mutation contracts in v0.8.11

v0.8.11 adds single-node constraints, joined DML, `COPY`, and sequences to the
same transactional SQL path used by Rust, Python, and the CLI. The release
verifier runs the public Python demo at
`scripts/demo/v0811/demo_sql_mutations.py` against the published package.

## Capability summary

| Area | Supported contract | Deliberate boundary |
|---|---|---|
| Constraints | `CHECK`, single/composite `FOREIGN KEY`, `NO ACTION`, `RESTRICT`, `CASCADE`, `SET NULL`; catalog persistence and `information_schema` | `DEFERRABLE` is rejected; cascade depth is capped at 64 |
| DML | `RETURNING`, `ON CONFLICT DO NOTHING/UPDATE`, `MERGE`, `UPDATE ... FROM`, `DELETE ... USING` | A target row matched more than once is rejected atomically; triggers are not implemented |
| COPY | Embedded/CLI local CSV and Parquet files; CLI CSV `STDIN`/`STDOUT`; application-owned Rust readers/writers and Python `BinaryIO`; atomic file replacement | JSON and unknown formats are rejected; Parquet is file-only; HTTP/gRPC SQL rejects every local path and process-stdio COPY target |
| Sequences | `CREATE/ALTER/DROP SEQUENCE`, `NEXTVAL`, `CURRVAL`, `SERIAL`, `GENERATED ... AS IDENTITY`, bounds, cycle, ownership | Allocation is transactional, so rollback permits reuse; `CACHE` is a persisted hint and does not preallocate; `CURRVAL` reports the durable last committed allocation rather than PostgreSQL session-local state; distributed allocation is outside v0.8.11 |

All four areas use statement atomicity. A failed constraint, multiple-match
`MERGE`, unsupported `COPY` format, or exhausted non-cycling sequence leaves
the statement's target data unchanged.

## Example

```sql
CREATE TABLE parent (id BIGINT PRIMARY KEY, quota BIGINT CHECK (quota >= 0));
CREATE TABLE child (
  id BIGINT PRIMARY KEY,
  parent_id BIGINT,
  FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE
);

CREATE TABLE jobs (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  label TEXT UNIQUE
);
INSERT INTO jobs (label) VALUES ('first') RETURNING id, label;
INSERT INTO jobs (label) VALUES ('first') ON CONFLICT DO NOTHING RETURNING id;

COPY jobs TO '/tmp/jobs.parquet' WITH (FORMAT PARQUET);
```

Applications inspect constraints through `information_schema.table_constraints`.
Rust applications inspect sequence objects and ownership through
`Database::list_sequences`; Python applications use `Database.list_sequences()`.

## Operational contract

- Alopex validates local `COPY` paths against the configured allow-list and
  writes through a temporary sibling before rename. A failed export removes
  the temporary file and preserves the previous destination.
- Rust applications use `Database::copy_from_csv_reader` and
  `Database::copy_to_csv_writer`; Python applications use `copy_from_csv` and
  `copy_to_csv` with binary file-like objects. The CLI uses SQL `STDIN` and
  `STDOUT`. HTTP/gRPC SQL rejects COPY until a dedicated authenticated client
  upload/download API exists.
- Alopex accepts CSV headers through `HEADER TRUE`. Schema/type failures abort
  the import transaction; Alopex does not retain accepted prefix rows.
- Alopex drops an identity or serial-owned sequence with its table. Explicitly
  owned sequences follow the same lifecycle.
- The formal model `formal/tla/sql/SqlMutationLifecycle.tla` checks statement
  atomicity, bounded cascades, COPY publish/crash behavior, and sequence
  allocation/rollback behavior.

Run the source checks with:

```bash
cargo test -p alopex-embedded --features lane_ci --test constraints
cargo test -p alopex-embedded --features lane_ci --test advanced_dml
cargo test -p alopex-embedded --features lane_ci --test copy_sql
cargo test -p alopex-embedded --features lane_ci --test sequence
```
