# FM-SQL-PREP-001 implementation scenarios

| Model transition or invariant | Implementation evidence |
| --- | --- |
| `Bind`, complete-binding guard, one-based indices | `prepared_statement_rejects_missing_and_non_value_parameters` |
| `Reset`, rebind, repeated `Execute`, `Finalize` | `prepared_statement_supports_null_rebind_reset_and_finalize` |
| `SchemaChange`, reparse, retry | `prepared_statement_reparses_after_schema_change_and_can_retry` |
| Transaction-bound execution | `session_prepared_statement_uses_the_active_transaction` |
| Value/SQL-structure boundary and thread transfer | `prepared_statement_is_send_and_text_binding_cannot_change_sql_structure` |
| Parser wire representation | `positional_bind_parameters_cross_the_nim_messagepack_boundary` |

The embedded integration suite and release verifier exercise the public API while TLC checks every reachable state of the bounded lifecycle model.
