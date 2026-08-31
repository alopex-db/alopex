# SQL full-text search

Alopex v0.8.10 provides deterministic local full-text search over `TEXT`.
`TO_TSVECTOR`, `TO_TSQUERY`, `PLAINTO_TSQUERY`, `WEBSEARCH_TO_TSQUERY`,
`TS_RANK`, and `TS_HEADLINE` expose the scalar surface. `FTS_SEARCH` exposes
ranked row search and uses a matching `USING FTS` index when one exists.
The v0.8.10 scalar contract represents `tsvector` and `tsquery` values as
canonical `TEXT`; callers pass the output of the constructor functions to
`TS_RANK` and `TS_HEADLINE`.

## Quick start

```sql
CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO docs VALUES (1, 'the quick brown fox'), (2, 'database search');

SELECT row_id, document, rank, headline
FROM FTS_SEARCH('docs', 'body', 'quick');

CREATE INDEX docs_body_fts ON docs(body) USING FTS;
```

`FTS_SEARCH(table, column, query [, config])` returns the internal `row_id`, source
document, matched-token ratio, and a `<b>...</b>` headline. Results have the
same semantics before and after index creation. Inserts, updates, deletes,
index drops, and index rebuilds update the index transactionally.
The optional configuration defaults to `simple`; the executor uses only an
index with the same configuration and otherwise performs an equivalent scan.

## Token and query contract

- `simple` lowercases Unicode alphanumeric/underscore runs. CJK text is
  supported as deterministic Unicode runs; v0.8.10 does not claim dictionary
  segmentation or morphological analysis.
- `english` adds deterministic ASCII suffix reduction. The supported
  configurations are deliberately closed to `simple` and `english`.
- `TO_TSQUERY` supports `&`, `|`, `!`, `<->`, parentheses, and the `:*`
  prefix suffix. `PLAINTO_TSQUERY` joins words with `&`.
  `WEBSEARCH_TO_TSQUERY` supports quoted phrases, `-term`, and `OR`.
- FTS index format version `1` stores one posting per distinct normalized term
  and row. The tokenizer and version are part of the persisted-index contract.

Documents are limited to 1 MiB and 65,536 tokens. Queries are limited to
4 KiB, 1,024 terms, and nesting depth 64. `FTS_SEARCH` returns at most 100,000
rows. Malformed queries and limit violations fail before partial results are
returned.

## Compatibility and distribution

Existing tables need no migration. Applications may create an FTS index
online and compare `FTS_SEARCH` results before and after creation; dropping the
index returns the same call to scan execution. Alopex v0.8.10 classifies all
full-text scalar functions and `FTS_SEARCH` as local-only in the distributed
read catalog, so a remote route fails before transport rather than silently
changing semantics.

The runnable SQL fixture is
[`scripts/demo/v08/demo_full_text_search.sql`](../scripts/demo/v08/demo_full_text_search.sql).
