# FM-SQL-METADATA-001 inventory

- Model: `formal/tla/sql/MetadataVisibility.tla`
- Configuration: `formal/tla/sql/MetadataVisibility.cfg`
- Owner: portable SQL metadata visibility boundary
- Properties: rollback clears transaction DDL; temporary objects are not durable; distributed metadata fails closed without partial output
