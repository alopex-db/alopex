use alopex_cli::cli::{Cli, Command, OutputFormat, SqlCommand};
use clap::{CommandFactory, Parser};

const COLUMNAR_COMMANDS: [&str; 5] = ["scan", "stats", "list", "ingest", "index"];
const COLUMNAR_INDEX_COMMANDS: [&str; 3] = ["create", "list", "drop"];
const SERVER_COMMANDS: [&str; 7] = [
    "status",
    "metrics",
    "health",
    "join",
    "leave",
    "compaction",
    "cluster",
];
const LIFECYCLE_COMMANDS: [&str; 4] = ["archive", "restore", "backup", "export"];

fn names(command: &clap::Command) -> Vec<&str> {
    command
        .get_subcommands()
        .map(|subcommand| subcommand.get_name())
        .collect()
}

fn longs(command: &clap::Command) -> Vec<&str> {
    command
        .get_arguments()
        .filter_map(|argument| argument.get_long())
        .collect()
}

#[test]
fn i19_cli_columnar_server_lifecycle_output_and_auth_registers_are_fixed() {
    let root = Cli::command();

    let columnar = root.find_subcommand("columnar").expect("columnar command");
    assert_eq!(names(columnar), COLUMNAR_COMMANDS);
    assert_eq!(
        longs(columnar.find_subcommand("scan").expect("columnar scan")),
        ["segment", "progress"]
    );
    assert_eq!(
        longs(columnar.find_subcommand("stats").expect("columnar stats")),
        ["segment"]
    );
    assert_eq!(
        longs(columnar.find_subcommand("ingest").expect("columnar ingest")),
        [
            "file",
            "table",
            "delimiter",
            "header",
            "compression",
            "row-group-size",
        ]
    );
    let index = columnar.find_subcommand("index").expect("columnar index");
    assert_eq!(names(index), COLUMNAR_INDEX_COMMANDS);
    assert_eq!(
        longs(
            index
                .find_subcommand("create")
                .expect("columnar index create")
        ),
        ["segment", "column", "type"]
    );

    let server = root.find_subcommand("server").expect("server command");
    assert_eq!(names(server), SERVER_COMMANDS);
    assert_eq!(
        names(server.find_subcommand("compaction").expect("compaction")),
        ["trigger"]
    );

    let lifecycle = root
        .find_subcommand("lifecycle")
        .expect("lifecycle command");
    assert_eq!(names(lifecycle), LIFECYCLE_COMMANDS);
    let restore = lifecycle
        .find_subcommand("restore")
        .expect("lifecycle restore");
    assert_eq!(longs(restore), ["source"]);
    assert_eq!(names(restore), ["status"]);
    assert_eq!(
        longs(restore.find_subcommand("status").expect("restore status")),
        ["handle"]
    );
    let backup = lifecycle
        .find_subcommand("backup")
        .expect("lifecycle backup");
    assert_eq!(names(backup), ["status"]);
    assert_eq!(
        longs(backup.find_subcommand("status").expect("backup status")),
        ["handle"]
    );

    for (value, expected) in [
        ("table", OutputFormat::Table),
        ("json", OutputFormat::Json),
        ("jsonl", OutputFormat::Jsonl),
        ("csv", OutputFormat::Csv),
        ("tsv", OutputFormat::Tsv),
    ] {
        let cli = Cli::try_parse_from(["alopex", "--output", value, "server", "status"])
            .expect("documented output format must parse");
        assert_eq!(cli.output_format(), expected);
    }

    let cli = Cli::try_parse_from(["alopex", "--insecure", "sql", "--tui", "SELECT 1"])
        .expect("the explicit insecure/TUI boundary must stay parseable");
    assert!(cli.insecure);
    assert!(matches!(
        cli.command,
        Some(Command::Sql(SqlCommand { tui: true, .. }))
    ));
}
