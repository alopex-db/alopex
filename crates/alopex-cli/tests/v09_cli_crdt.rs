use std::process::Command as ProcessCommand;

use alopex_cli::cli::Cli;
use clap::Parser;
use tempfile::tempdir;

const COUNTER_CREATE: [&str; 24] = [
    "--in-memory",
    "--output",
    "json",
    "crdt",
    "counter",
    "create",
    "--object-id",
    "counter-a",
    "--cluster-id",
    "cluster-a",
    "--table-id",
    "7",
    "--range-id",
    "range-a",
    "--schema-version",
    "1",
    "--data-epoch",
    "9",
    "--request-id",
    "request-a",
    "--operation-id",
    "operation-a",
    "--update-version",
    "12",
];

const COUNTER_READ: [&str; 23] = [
    "--output",
    "json",
    "crdt",
    "counter",
    "read",
    "--object-id",
    "counter-a",
    "--cluster-id",
    "cluster-a",
    "--table-id",
    "7",
    "--range-id",
    "range-a",
    "--schema-version",
    "1",
    "--data-epoch",
    "9",
    "--request-id",
    "request-read",
    "--operation-id",
    "operation-read",
    "--update-version",
    "12",
];

#[test]
fn i27_cli_counter_create_preserves_canonical_outcome_and_negative_value() {
    let mut args = COUNTER_CREATE.to_vec();
    args.extend(["--initial-value", "-4"]);
    let parsed = Cli::try_parse_from(std::iter::once("alopex").chain(args.iter().copied()));
    assert!(
        parsed.is_ok(),
        "Counter create must be a registered CLI operation"
    );

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(args)
        .output()
        .expect("run alopex Counter create");
    assert!(
        output.status.success(),
        "Counter create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let rows: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("Counter create must emit JSON");
    let outcome = rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("JSON output must contain one canonical outcome");
    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-a");
    assert_eq!(outcome["range"]["cluster_id"], "cluster-a");
    assert_eq!(outcome["range"]["table_id"], 7);
    assert_eq!(outcome["range"]["range_id"], "range-a");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["value_type"], "counter");
    assert_eq!(outcome["value"]["value"], -4);
    assert_eq!(outcome["idempotency"]["operation_id"], "operation-a");
}

#[test]
fn i27_cli_counter_create_requires_request_and_operation_identity() {
    let missing_request = Cli::try_parse_from([
        "alopex",
        "crdt",
        "counter",
        "create",
        "--object-id",
        "counter-a",
        "--cluster-id",
        "cluster-a",
        "--table-id",
        "7",
        "--range-id",
        "range-a",
        "--schema-version",
        "1",
        "--data-epoch",
        "9",
        "--operation-id",
        "operation-a",
        "--update-version",
        "12",
        "--initial-value",
        "0",
    ]);
    assert!(
        missing_request.is_err(),
        "mutations must require --request-id"
    );
}

#[test]
fn i27_cli_counter_read_preserves_canonical_outcome_without_mutating_the_projection() {
    let data_dir = tempdir().expect("temporary Counter data directory");
    let data_dir = data_dir.path().to_str().expect("UTF-8 data path");
    let create_args = [
        "--data-dir",
        data_dir,
        "--output",
        "json",
        "crdt",
        "counter",
        "create",
        "--object-id",
        "counter-a",
        "--cluster-id",
        "cluster-a",
        "--table-id",
        "7",
        "--range-id",
        "range-a",
        "--schema-version",
        "1",
        "--data-epoch",
        "9",
        "--request-id",
        "request-create",
        "--operation-id",
        "operation-create",
        "--update-version",
        "12",
        "--initial-value",
        "-4",
    ];
    let create = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(create_args)
        .output()
        .expect("run Counter create before read");
    assert!(
        create.status.success(),
        "Counter create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let mut args = vec!["--data-dir", data_dir];
    args.extend(COUNTER_READ);
    let parsed = Cli::try_parse_from(std::iter::once("alopex").chain(args.iter().copied()));
    assert!(
        parsed.is_ok(),
        "Counter read must be a registered CLI operation"
    );

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(args)
        .output()
        .expect("run Counter read");
    assert!(
        output.status.success(),
        "Counter read failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("Counter read must emit JSON");
    let outcome = rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("JSON output must contain one canonical Counter read outcome");
    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-a");
    assert_eq!(outcome["range"]["cluster_id"], "cluster-a");
    assert_eq!(outcome["range"]["table_id"], 7);
    assert_eq!(outcome["range"]["range_id"], "range-a");
    assert_eq!(outcome["request_id"], "request-read");
    assert_eq!(outcome["operation_id"], "operation-read");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["value_type"], "counter");
    assert_eq!(outcome["value"]["initial_value"], -4);
    assert_eq!(outcome["value"]["value"], -4);
    assert_eq!(outcome["idempotency"]["first_outcome"], "counter_read");
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);
}
