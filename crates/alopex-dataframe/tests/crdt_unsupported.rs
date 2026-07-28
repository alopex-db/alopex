use alopex_dataframe::{DataFrameError, Result};

fn preflight(operation: &str) -> Result<()> {
    // DataFrame has no CRDT namespace or operation.  Reusing the existing
    // operation preflight keeps the request outside logical planning and
    // execution, rather than adding a distributed fallback.
    DataFrameError::preflight_dataframe_operation(operation)
}

#[test]
fn crdt_dataframe_operation_names_are_rejected_before_execution() {
    for operation in [
        "crdt_counter_create",
        "crdt_counter_increment",
        "crdt_set_add",
        "crdt_set_contains",
        "crdt_merge",
        "crdt_reconcile",
        "crdt_recover",
        "crdt_retire",
        "crdt_cancel",
    ] {
        let error = preflight(operation).expect_err("CRDT DataFrame operation is unsupported");
        assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
        assert!(error.to_string().contains(operation));
    }
}

#[test]
fn existing_dataframe_operation_preflight_is_unchanged() {
    assert!(preflight("cse").is_ok());
    assert!(preflight("concat").is_ok());
}
