use std::process::Command as ProcessCommand;

use alopex_cli::cli::Cli;
use clap::Parser;
use serde_json::{json, Value};
use tempfile::tempdir;

fn command_args(
    data_dir: &str,
    object_kind: &str,
    action: &str,
    request_id: &str,
    operation_id: &str,
    update_version: u64,
) -> Vec<String> {
    vec![
        "--data-dir".into(),
        data_dir.into(),
        "--output".into(),
        "json".into(),
        "crdt".into(),
        object_kind.into(),
        action.into(),
        "--object-id".into(),
        format!("f2-cli-{object_kind}"),
        "--cluster-id".into(),
        "cluster-f2-cli".into(),
        "--table-id".into(),
        "7".into(),
        "--range-id".into(),
        "range-f2-cli".into(),
        "--schema-version".into(),
        "1".into(),
        "--data-epoch".into(),
        "9".into(),
        "--request-id".into(),
        request_id.into(),
        "--operation-id".into(),
        operation_id.into(),
        "--update-version".into(),
        update_version.to_string(),
    ]
}

fn invoke(label: &str, args: &[String]) -> Value {
    let parsed =
        Cli::try_parse_from(std::iter::once("alopex").chain(args.iter().map(String::as_str)));
    assert!(parsed.is_ok(), "{label} arguments must parse: {parsed:?}");

    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args(args)
        .output()
        .expect("run CLI command");
    assert!(
        output.status.success(),
        "{label} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: Value = serde_json::from_slice(&output.stdout).expect("JSON output");
    rows.as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .expect("one canonical outcome")
}

fn run_and_replay(
    label: &str,
    args: &[String],
    object_type: &str,
    replay_duplicate_count: u64,
) -> Value {
    let first = invoke(label, args);
    assert_eq!(first["object_type"], object_type, "{label} object type");
    assert_eq!(first["state"], "committed", "{label} state");
    assert_eq!(first["routing"]["kind"], "local_only", "{label} routing");
    assert_eq!(
        first["idempotency"]["duplicate_count"], 0,
        "{label} first duplicate count"
    );

    let replay = invoke(&format!("{label} replay"), args);
    assert_eq!(
        replay["idempotency"]["duplicate_count"], replay_duplicate_count,
        "{label} replay duplicate count"
    );
    assert_eq!(replay["value"], first["value"], "{label} replay value");
    first
}

#[test]
fn f2_cli_register_covers_all_crdt_commands_options_output_and_exit_classes() {
    let data_dir = tempdir().expect("data directory");
    let data_dir = data_dir.path().to_str().expect("UTF-8 data directory");

    let mut counter_create = command_args(
        data_dir,
        "counter",
        "create",
        "f2-counter-create-request",
        "f2-counter-create-operation",
        0,
    );
    counter_create.extend(["--initial-value".into(), "-4".into()]);
    let counter_create = run_and_replay("Counter create", &counter_create, "counter", 1);
    assert_eq!(counter_create["value"]["value"], -4);

    let counter_read = command_args(
        data_dir,
        "counter",
        "read",
        "f2-counter-read-request",
        "f2-counter-read-operation",
        0,
    );
    let counter_read = run_and_replay("Counter read", &counter_read, "counter", 0);
    assert_eq!(counter_read["value"]["value"], -4);

    let mut counter_increment = command_args(
        data_dir,
        "counter",
        "increment",
        "f2-counter-increment-request",
        "f2-counter-increment-operation",
        1,
    );
    counter_increment.extend(["--delta".into(), "3".into()]);
    let counter_increment = run_and_replay("Counter increment", &counter_increment, "counter", 1);
    assert_eq!(counter_increment["value"]["value"], -1);

    let mut counter_decrement = command_args(
        data_dir,
        "counter",
        "decrement",
        "f2-counter-decrement-request",
        "f2-counter-decrement-operation",
        2,
    );
    counter_decrement.extend(["--delta".into(), "3".into()]);
    let counter_decrement = run_and_replay("Counter decrement", &counter_decrement, "counter", 1);
    assert_eq!(counter_decrement["value"]["value"], -4);

    let set_create = command_args(
        data_dir,
        "set",
        "create",
        "f2-set-create-request",
        "f2-set-create-operation",
        0,
    );
    let set_create = run_and_replay("Set create", &set_create, "set", 1);
    assert_eq!(set_create["value"]["members"], json!([]));

    let set_read = command_args(
        data_dir,
        "set",
        "read",
        "f2-set-read-request",
        "f2-set-read-operation",
        0,
    );
    let set_read = run_and_replay("Set read", &set_read, "set", 0);
    assert_eq!(set_read["value"]["members"], json!([]));

    let mut set_add = command_args(
        data_dir,
        "set",
        "add",
        "f2-set-add-request",
        "00000000-0000-0000-0000-000000000904",
        1,
    );
    set_add.extend(["--member".into(), "alice".into()]);
    let set_add = run_and_replay("Set add", &set_add, "set", 1);
    assert_eq!(set_add["value"]["members"], json!(["alice"]));

    let mut set_contains = command_args(
        data_dir,
        "set",
        "contains",
        "f2-set-contains-request",
        "f2-set-contains-operation",
        0,
    );
    set_contains.extend(["--member".into(), "alice".into()]);
    let set_contains = run_and_replay("Set contains", &set_contains, "set", 0);
    assert_eq!(set_contains["value"]["members"], json!(["alice"]));

    let set_list = command_args(
        data_dir,
        "set",
        "list",
        "f2-set-list-request",
        "f2-set-list-operation",
        0,
    );
    let set_list = run_and_replay("Set list", &set_list, "set", 0);
    assert_eq!(set_list["value"]["members"], json!(["alice"]));

    let mut set_remove = command_args(
        data_dir,
        "set",
        "remove",
        "f2-set-remove-request",
        "00000000-0000-0000-0000-000000000905",
        2,
    );
    set_remove.extend(["--member".into(), "alice".into()]);
    let set_remove = run_and_replay("Set remove", &set_remove, "set", 1);
    assert_eq!(set_remove["value"]["members"], json!([]));
}

#[test]
fn f2_cli_missing_identity_is_a_usage_failure() {
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_alopex"))
        .args([
            "--in-memory",
            "crdt",
            "counter",
            "create",
            "--object-id",
            "f2-invalid-counter",
            "--cluster-id",
            "cluster-f2-cli",
            "--table-id",
            "7",
            "--range-id",
            "range-f2-cli",
            "--schema-version",
            "1",
            "--data-epoch",
            "9",
            "--operation-id",
            "f2-invalid-operation",
            "--update-version",
            "0",
            "--initial-value",
            "0",
        ])
        .output()
        .expect("run invalid CLI command");
    assert!(!output.status.success(), "missing --request-id must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--request-id"),
        "usage error must identify the missing option"
    );
}
