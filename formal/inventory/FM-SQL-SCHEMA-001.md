# FM-SQL-SCHEMA-001 inventory

- Model: `formal/tla/sql/SchemaEvolutionLifecycle.tla`
- Configuration: `formal/tla/sql/SchemaEvolutionLifecycle.cfg`
- Owner: SQL schema evolution transaction boundary
- Properties: ALTER and TRUNCATE commit together; rollback/crash discards partial migration; reopen cannot expose an overlay; a referenced base relation cannot be dropped
