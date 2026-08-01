use alopex_cli::cli::Cli;
use clap::{Command, CommandFactory, Parser};

const INHERITED_LOCAL_COMMANDS: [&str; 55] = [
    "profile create",
    "profile list",
    "profile show",
    "profile delete",
    "profile set-default",
    "kv get",
    "kv put",
    "kv delete",
    "kv list",
    "vector search",
    "vector upsert",
    "vector delete",
    "hnsw create",
    "hnsw stats",
    "hnsw drop",
    "columnar scan",
    "columnar stats",
    "columnar list",
    "columnar ingest",
    "columnar index create",
    "columnar index list",
    "columnar index drop",
    "server status",
    "server metrics",
    "server health",
    "server join",
    "server leave",
    "server compaction trigger",
    "server cluster metadata show",
    "server cluster members list",
    "server cluster members replace",
    "server cluster ranges list",
    "server cluster ranges show",
    "server cluster ranges register",
    "server cluster ranges update",
    "server cluster ranges retire",
    "server cluster placement get",
    "server cluster placement set",
    "server cluster placement replace",
    "server cluster read-policy get",
    "server cluster read-policy set",
    "server cluster schema owner get",
    "server cluster schema owner set",
    "server cluster schema rollout start",
    "server cluster schema rollout status",
    "server cluster recovery status",
    "server cluster recovery restore",
    "server cluster upgrade status",
    "server cluster upgrade start",
    "lifecycle archive",
    "lifecycle restore",
    "lifecycle backup",
    "lifecycle export",
    "version",
    "completions",
];

fn command_path_exists(root: &Command, path: &str) -> bool {
    let mut current = root;
    for component in path.split(' ') {
        let Some(next) = current
            .get_subcommands()
            .find(|command| command.get_name() == component)
        else {
            return false;
        };
        current = next;
    }
    true
}

#[test]
fn inherited_cli_register_stays_local_and_has_no_transaction_recovery_commands() {
    let command = Cli::command();
    for path in INHERITED_LOCAL_COMMANDS {
        assert!(
            command_path_exists(&command, path),
            "inherited API-CI command missing: {path}"
        );
    }

    for absent in [
        "kv txn status",
        "kv txn recover",
        "kv txn cancel",
        "sql status",
        "sql recover",
        "sql cancel",
    ] {
        assert!(
            !command_path_exists(&command, absent),
            "v0.9 must not invent a distributed transaction command: {absent}"
        );
    }

    Cli::try_parse_from(["alopex", "--in-memory", "kv", "put", "compat", "value"])
        .expect("legacy local KV command parses without a profile or distributed selection");
    Cli::try_parse_from([
        "alopex",
        "--in-memory",
        "vector",
        "search",
        "--index",
        "idx",
        "--query",
        "[1.0]",
    ])
    .expect("legacy local vector command remains in the parser contract");
}
