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
        "crdt_counter_increment",
        "crdt_counter_decrement",
        "crdt_counter_read",
        "crdt_set_create",
        "crdt_set_read",
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
fn counter_create_has_a_stable_pre_execution_unsupported_classification() {
    let operation = "crdt_counter_create";
    let error = preflight(operation).expect_err("Counter create is not a DataFrame operation");
    assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
    assert_eq!(
        error.to_string(),
        "invalid operation: dataframe CRDT operation 'crdt_counter_create' is unsupported: pre_execution_unsupported"
    );
}

#[test]
fn counter_read_has_a_stable_pre_execution_unsupported_classification() {
    let operation = "crdt_counter_read";
    let error = preflight(operation).expect_err("Counter read is not a DataFrame operation");
    assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
    assert_eq!(
        error.to_string(),
        "invalid operation: dataframe CRDT operation 'crdt_counter_read' is unsupported: pre_execution_unsupported"
    );
}

#[test]
fn counter_increment_has_a_stable_pre_execution_unsupported_classification() {
    let operation = "crdt_counter_increment";
    let error = preflight(operation).expect_err("Counter increment is not a DataFrame operation");
    assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
    assert_eq!(
        error.to_string(),
        "invalid operation: dataframe CRDT operation 'crdt_counter_increment' is unsupported: pre_execution_unsupported"
    );
}

#[test]
fn counter_decrement_has_a_stable_pre_execution_unsupported_classification() {
    let operation = "crdt_counter_decrement";
    let error = preflight(operation).expect_err("Counter decrement is not a DataFrame operation");
    assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
    assert_eq!(
        error.to_string(),
        "invalid operation: dataframe CRDT operation 'crdt_counter_decrement' is unsupported: pre_execution_unsupported"
    );
}

#[test]
fn set_create_has_a_stable_pre_execution_unsupported_classification() {
    let operation = "crdt_set_create";
    let error = preflight(operation).expect_err("Set create is not a DataFrame operation");
    assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
    assert_eq!(
        error.to_string(),
        "invalid operation: dataframe CRDT operation 'crdt_set_create' is unsupported: pre_execution_unsupported"
    );
}

#[test]
fn set_read_has_a_stable_pre_execution_unsupported_classification() {
    let operation = "crdt_set_read";
    let error = preflight(operation).expect_err("Set read is not a DataFrame operation");
    assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
    assert_eq!(
        error.to_string(),
        "invalid operation: dataframe CRDT operation 'crdt_set_read' is unsupported: pre_execution_unsupported"
    );
}

#[test]
fn set_add_has_a_stable_pre_execution_unsupported_classification() {
    let operation = "crdt_set_add";
    let error = preflight(operation).expect_err("Set add is not a DataFrame operation");
    assert!(matches!(error, DataFrameError::InvalidOperation { .. }));
    assert_eq!(
        error.to_string(),
        "invalid operation: dataframe CRDT operation 'crdt_set_add' is unsupported: pre_execution_unsupported"
    );
}

#[test]
fn existing_dataframe_operation_preflight_is_unchanged() {
    assert!(preflight("cse").is_ok());
    assert!(preflight("concat").is_ok());
}
