# FM-SQL-DML-001 implementation scenarios

| Model transition or invariant | Implementation evidence |
| --- | --- |
| `RejectMultipleMatch` | `crates/alopex-embedded/tests/advanced_dml.rs::merge_rejects_multiple_source_matches_atomically` |
| `Conflict`, `Retry`, `RejectedOrConflictedDoesNotPublish` | `crates/alopex-embedded/tests/advanced_dml.rs::concurrent_advanced_dml_conflict_does_not_publish_stale_rows` |
| PostgreSQL/DuckDB public behavior | `scripts/reference_tests/sql_v0811_advanced_dml.py` and `scripts/reference_tests/sql_v0811_advanced_dml.json` |
