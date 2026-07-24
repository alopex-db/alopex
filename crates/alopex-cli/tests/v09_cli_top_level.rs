use alopex_cli::cli::Cli;
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};

const TOP_LEVEL: [&str; 10] = [
    "profile",
    "kv",
    "sql",
    "vector",
    "hnsw",
    "columnar",
    "server",
    "lifecycle",
    "version",
    "completions",
];

const GLOBAL_OPTIONS: [&str; 11] = [
    "data-dir",
    "profile",
    "in-memory",
    "output",
    "limit",
    "quiet",
    "verbose",
    "insecure",
    "thread-mode",
    "batch",
    "yes",
];

#[test]
fn i16_cli_top_level_commands_global_options_help_and_errors_have_fixed_rows() {
    let mut command = Cli::command();
    let commands: Vec<_> = command
        .get_subcommands()
        .map(|subcommand| subcommand.get_name())
        .collect();
    assert_eq!(commands, TOP_LEVEL, "the I-16 command register drifted");

    let options: Vec<_> = command
        .get_arguments()
        .filter_map(|argument| argument.get_long())
        .filter(|long| GLOBAL_OPTIONS.contains(long))
        .collect();
    assert_eq!(
        options, GLOBAL_OPTIONS,
        "the I-16 global option register drifted"
    );

    let help = command.render_long_help().to_string();
    for name in TOP_LEVEL {
        assert!(help.contains(name), "help must advertise {name}");
    }
    for option in GLOBAL_OPTIONS {
        assert!(
            help.contains(&format!("--{option}")),
            "help must advertise --{option}"
        );
    }

    let invalid_output = Cli::try_parse_from(["alopex", "--output", "xml", "version"])
        .expect_err("unknown output must fail before command execution");
    assert_eq!(invalid_output.kind(), ErrorKind::InvalidValue);

    let conflicting_mode =
        Cli::try_parse_from(["alopex", "--data-dir", "./db", "--in-memory", "version"])
            .expect_err("conflicting persistence modes must fail before command execution");
    assert_eq!(conflicting_mode.kind(), ErrorKind::ArgumentConflict);
}
