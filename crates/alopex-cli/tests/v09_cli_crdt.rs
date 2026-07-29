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

const COUNTER_INCREMENT: [&str; 23] = [
    "--output",
    "json",
    "crdt",
    "counter",
    "increment",
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
    "request-increment",
    "--operation-id",
    "operation-increment",
    "--update-version",
    "12",
];

const COUNTER_DECREMENT: [&str; 23] = [
    "--output",
    "json",
    "crdt",
    "counter",
    "decrement",
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
    "request-decrement",
    "--operation-id",
    "operation-decrement",
    "--update-version",
    "12",
];

const SET_CREATE: [&str; 24] = [
    "--in-memory",
    "--output",
    "json",
    "crdt",
    "set",
    "create",
    "--object-id",
    "set-a",
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
    "request-set-a",
    "--operation-id",
    "operation-set-a",
    "--update-version",
    "12",
];

const SET_READ: [&str; 23] = [
    "--output",
    "json",
    "crdt",
    "set",
    "read",
    "--object-id",
    "set-a",
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
    "request-set-read",
    "--operation-id",
    "operation-set-read",
    "--update-version",
    "12",
];

#[test]
fn i27_cli_set_create_preserves_canonical_empty_membership() {
    let parsed = Cli::try_parse_from(std::iter::once("alopex").chain(SET_CREATE));
    assert!(
        parsed.is_ok(),
        "Set create must be a registered CLI operation"
    );
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(SET_CREATE)
        .output()
        .expect("run Set create");
    assert!(
        output.status.success(),
        "Set create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).expect("Set create JSON");
    let outcome = rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("one Set outcome");
    assert_eq!(outcome["object_type"], "set");
    assert_eq!(outcome["object_id"], "set-a");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["value_type"], "set");
    assert_eq!(outcome["value"]["members"], serde_json::json!([]));
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);
}

#[test]
fn i27_cli_set_read_preserves_canonical_membership_without_mutating_the_projection() {
    let data_dir = tempdir().expect("temporary Set data directory");
    let data_dir = data_dir.path().to_str().expect("UTF-8 data path");
    let mut create_args = vec!["--data-dir", data_dir];
    create_args.extend(
        SET_CREATE
            .iter()
            .copied()
            .filter(|argument| *argument != "--in-memory"),
    );
    let create = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(create_args)
        .output()
        .expect("run Set create before read");
    assert!(
        create.status.success(),
        "Set create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let mut args = vec!["--data-dir", data_dir];
    args.extend(SET_READ);
    assert!(Cli::try_parse_from(std::iter::once("alopex").chain(args.iter().copied())).is_ok());
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(args)
        .output()
        .expect("run Set read");
    assert!(
        output.status.success(),
        "Set read failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).expect("Set read JSON");
    let outcome = rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("one Set read outcome");
    assert_eq!(outcome["object_type"], "set");
    assert_eq!(outcome["object_id"], "set-a");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["members"], serde_json::json!([]));
    assert_eq!(outcome["value"]["member_versions"], serde_json::json!({}));
    assert_eq!(outcome["idempotency"]["first_outcome"], "set_read");
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);
}

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

#[test]
fn i27_cli_counter_increment_preserves_canonical_outcome_and_replays_once() {
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
        .expect("run Counter create before increment");
    assert!(
        create.status.success(),
        "Counter create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let mut args = vec!["--data-dir", data_dir];
    args.extend(COUNTER_INCREMENT);
    args.extend(["--delta", "3"]);
    let parsed = Cli::try_parse_from(std::iter::once("alopex").chain(args.iter().copied()));
    assert!(
        parsed.is_ok(),
        "Counter increment must be a registered CLI operation"
    );

    let increment = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(&args)
        .output()
        .expect("run Counter increment");
    assert!(
        increment.status.success(),
        "Counter increment failed: {}",
        String::from_utf8_lossy(&increment.stderr)
    );
    let rows: serde_json::Value =
        serde_json::from_slice(&increment.stdout).expect("Counter increment must emit JSON");
    let outcome = rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("JSON output must contain one canonical Counter increment outcome");
    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-a");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["value_type"], "counter");
    assert_eq!(outcome["value"]["initial_value"], -4);
    assert_eq!(outcome["value"]["accepted_delta_total"], 3);
    assert_eq!(outcome["value"]["value"], -1);
    assert_eq!(outcome["idempotency"]["first_outcome"], "counter_committed");
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);

    let replay = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(&args)
        .output()
        .expect("replay Counter increment");
    assert!(
        replay.status.success(),
        "Counter increment replay failed: {}",
        String::from_utf8_lossy(&replay.stderr)
    );
    let replay_rows: serde_json::Value =
        serde_json::from_slice(&replay.stdout).expect("Counter increment replay must emit JSON");
    let replay_outcome = replay_rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("JSON output must contain one replay outcome");
    assert_eq!(replay_outcome["value"]["value"], -1);
    assert_eq!(replay_outcome["idempotency"]["duplicate_count"], 1);
}

#[test]
fn i27_cli_counter_decrement_preserves_canonical_outcome_and_replays_once() {
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
        .expect("run Counter create before decrement");
    assert!(
        create.status.success(),
        "Counter create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let mut args = vec!["--data-dir", data_dir];
    args.extend(COUNTER_DECREMENT);
    args.extend(["--delta", "3"]);
    let parsed = Cli::try_parse_from(std::iter::once("alopex").chain(args.iter().copied()));
    assert!(
        parsed.is_ok(),
        "Counter decrement must be a registered CLI operation"
    );

    let decrement = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(&args)
        .output()
        .expect("run Counter decrement");
    assert!(
        decrement.status.success(),
        "Counter decrement failed: {}",
        String::from_utf8_lossy(&decrement.stderr)
    );
    let rows: serde_json::Value =
        serde_json::from_slice(&decrement.stdout).expect("Counter decrement must emit JSON");
    let outcome = rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("JSON output must contain one canonical Counter decrement outcome");
    assert_eq!(outcome["object_type"], "counter");
    assert_eq!(outcome["object_id"], "counter-a");
    assert_eq!(outcome["state"], "committed");
    assert_eq!(outcome["routing"]["kind"], "local_only");
    assert_eq!(outcome["value"]["value_type"], "counter");
    assert_eq!(outcome["value"]["initial_value"], -4);
    assert_eq!(outcome["value"]["accepted_delta_total"], -3);
    assert_eq!(outcome["value"]["value"], -7);
    assert_eq!(outcome["idempotency"]["first_outcome"], "counter_committed");
    assert_eq!(outcome["idempotency"]["duplicate_count"], 0);

    let replay = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(&args)
        .output()
        .expect("replay Counter decrement");
    assert!(
        replay.status.success(),
        "Counter decrement replay failed: {}",
        String::from_utf8_lossy(&replay.stderr)
    );
    let replay_rows: serde_json::Value =
        serde_json::from_slice(&replay.stdout).expect("Counter decrement replay must emit JSON");
    let replay_outcome = replay_rows
        .as_array()
        .and_then(|rows| rows.first())
        .expect("JSON output must contain one replay outcome");
    assert_eq!(replay_outcome["value"]["value"], -7);
    assert_eq!(replay_outcome["idempotency"]["duplicate_count"], 1);
}
