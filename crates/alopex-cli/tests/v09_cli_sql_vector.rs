use alopex_cli::cli::Cli;
use clap::CommandFactory;

const SQL_OPTIONS: [&str; 8] = [
    "file",
    "fetch-size",
    "max-rows",
    "deadline",
    "request-id",
    "read-mode",
    "routing-report",
    "tui",
];
const VECTOR_COMMANDS: [&str; 3] = ["search", "upsert", "delete"];
const HNSW_COMMANDS: [&str; 3] = ["create", "stats", "drop"];

fn longs(command: &clap::Command) -> Vec<&str> {
    command
        .get_arguments()
        .filter_map(|argument| argument.get_long())
        .collect()
}

#[test]
fn i18_sql_vector_hnsw_commands_and_options_have_fixed_registers() {
    let root = Cli::command();
    let sql = root.find_subcommand("sql").expect("sql command");
    assert_eq!(longs(sql), SQL_OPTIONS);

    let vector = root.find_subcommand("vector").expect("vector command");
    assert_eq!(
        vector
            .get_subcommands()
            .map(|command| command.get_name())
            .collect::<Vec<_>>(),
        VECTOR_COMMANDS
    );
    assert_eq!(
        longs(vector.find_subcommand("search").expect("vector search")),
        ["index", "query", "k", "progress"]
    );
    assert_eq!(
        longs(vector.find_subcommand("upsert").expect("vector upsert")),
        ["index", "key", "vector"]
    );
    assert_eq!(
        longs(vector.find_subcommand("delete").expect("vector delete")),
        ["index", "key"]
    );

    let hnsw = root.find_subcommand("hnsw").expect("hnsw command");
    assert_eq!(
        hnsw.get_subcommands()
            .map(|command| command.get_name())
            .collect::<Vec<_>>(),
        HNSW_COMMANDS
    );
    assert_eq!(
        longs(hnsw.find_subcommand("create").expect("hnsw create")),
        ["dim", "metric"]
    );
}
