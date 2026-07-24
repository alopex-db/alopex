use alopex_dataframe::DataFrameError;

const I09B_REGISTER: [(&str, &str, &str); 6] = [
    ("cast", "planned", "pre_execution_unsupported"),
    ("pivot", "planned", "pre_execution_unsupported"),
    ("unpivot", "planned", "pre_execution_unsupported"),
    ("window", "planned", "pre_execution_unsupported"),
    ("cse", "released", "supported"),
    ("concat", "released", "supported"),
];

#[derive(Debug)]
struct StatusRow {
    operation: &'static str,
    roadmap_status: &'static str,
    source_status: &'static str,
    passed: bool,
}

#[cfg_attr(not(feature = "lane_ci"), ignore)]
#[test]
fn i09b_planned_and_released_operations_have_explicit_preflight_statuses() {
    let rows: Vec<_> = I09B_REGISTER
        .into_iter()
        .map(|(operation, roadmap_status, source_status)| {
            let result = DataFrameError::preflight_dataframe_operation(operation);
            let passed = match source_status {
                "supported" => result.is_ok(),
                "pre_execution_unsupported" => matches!(
                    result,
                    Err(DataFrameError::InvalidOperation { message })
                        if message.contains(operation)
                            && message.contains("planned")
                            && message.contains("pre_execution_unsupported")
                ),
                _ => false,
            };
            StatusRow {
                operation,
                roadmap_status,
                source_status,
                passed,
            }
        })
        .collect();

    let actual: Vec<_> = rows
        .iter()
        .map(|row| (row.operation, row.roadmap_status, row.source_status))
        .collect();
    assert_eq!(
        actual, I09B_REGISTER,
        "the I-09b operation register drifted"
    );
    for row in rows {
        assert!(
            row.passed,
            "{} must remain {} ({})",
            row.operation, row.source_status, row.roadmap_status
        );
    }

    let unknown = DataFrameError::preflight_dataframe_operation("unknown").unwrap_err();
    assert!(matches!(unknown, DataFrameError::InvalidOperation { .. }));
    assert!(unknown.to_string().contains("unknown dataframe operation"));
}
