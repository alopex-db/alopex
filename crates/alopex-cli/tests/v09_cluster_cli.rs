use alopex_cli::cli::{Cli, ClusterCommand, Command, ServerCommand};
use clap::{error::ErrorKind, CommandFactory, Parser};

const CLUSTER_AREAS: [&str; 8] = [
    "metadata",
    "members",
    "ranges",
    "placement",
    "read-policy",
    "schema",
    "recovery",
    "upgrade",
];
const READ_OPTIONS: [&str; 2] = ["request-id", "expected-version"];
const TARGETED_READ_OPTIONS: [&str; 3] = ["request-id", "expected-version", "target"];
const MUTATION_OPTIONS: [&str; 4] = ["request-id", "expected-version", "target", "confirm"];

fn command_at<'a>(root: &'a clap::Command, path: &[&str]) -> &'a clap::Command {
    path.iter().fold(root, |command, segment| {
        command
            .find_subcommand(segment)
            .unwrap_or_else(|| panic!("missing cluster command path: {}", path.join(" ")))
    })
}

fn longs(command: &clap::Command) -> Vec<&str> {
    command
        .get_arguments()
        .filter_map(|argument| argument.get_long())
        .collect()
}

#[test]
fn i20_cluster_cli_register_and_idempotency_inputs_are_fixed() {
    let root = Cli::command();
    let cluster = command_at(&root, &["server", "cluster"]);
    assert_eq!(
        cluster
            .get_subcommands()
            .map(|command| command.get_name())
            .collect::<Vec<_>>(),
        CLUSTER_AREAS
    );

    for path in [
        &["server", "cluster", "metadata", "show"][..],
        &["server", "cluster", "members", "list"],
        &["server", "cluster", "ranges", "list"],
        &["server", "cluster", "read-policy", "get"],
        &["server", "cluster", "schema", "owner", "get"],
        &["server", "cluster", "schema", "rollout", "status"],
        &["server", "cluster", "recovery", "status"],
        &["server", "cluster", "upgrade", "status"],
    ] {
        assert_eq!(longs(command_at(&root, path)), READ_OPTIONS, "{path:?}");
    }
    for path in [
        &["server", "cluster", "ranges", "show"][..],
        &["server", "cluster", "placement", "get"],
    ] {
        assert_eq!(
            longs(command_at(&root, path)),
            TARGETED_READ_OPTIONS,
            "{path:?}"
        );
    }
    for path in [
        &["server", "cluster", "members", "replace"][..],
        &["server", "cluster", "ranges", "register"],
        &["server", "cluster", "ranges", "update"],
        &["server", "cluster", "ranges", "retire"],
        &["server", "cluster", "placement", "set"],
        &["server", "cluster", "placement", "replace"],
        &["server", "cluster", "read-policy", "set"],
        &["server", "cluster", "schema", "owner", "set"],
        &["server", "cluster", "schema", "rollout", "start"],
        &["server", "cluster", "recovery", "restore"],
        &["server", "cluster", "upgrade", "start"],
    ] {
        assert_eq!(longs(command_at(&root, path)), MUTATION_OPTIONS, "{path:?}");
    }

    let replay_args = [
        "alopex",
        "server",
        "cluster",
        "ranges",
        "register",
        "--request-id",
        "range-register-9",
        "--expected-version",
        "8",
        "--target",
        r#"{"range_id":"orders/0"}"#,
        "--confirm",
    ];
    let first = Cli::try_parse_from(replay_args).expect("first operation must parse");
    let replay = Cli::try_parse_from(replay_args).expect("replay must retain the request id");
    for cli in [first, replay] {
        assert!(matches!(
            cli.command,
            Some(Command::Server {
                command: Some(ServerCommand::Cluster {
                    command: ClusterCommand::Ranges {
                        command: alopex_cli::cli::ClusterRangesCommand::Register { request },
                    },
                }),
            }) if request.operation.request_id == "range-register-9"
                && request.operation.expected_version == Some(8)
                && request.target == r#"{"range_id":"orders/0"}"#
                && request.confirm
        ));
    }

    let missing_confirmation = Cli::try_parse_from([
        "alopex",
        "server",
        "cluster",
        "members",
        "replace",
        "--request-id",
        "members-replace-9",
        "--target",
        "{}",
    ])
    .expect_err("cluster mutation must require explicit confirmation");
    assert_eq!(
        missing_confirmation.kind(),
        ErrorKind::MissingRequiredArgument
    );
}
