use alopex_cli::cli::Cli;
use clap::CommandFactory;

const PROFILE_COMMANDS: [&str; 5] = ["create", "list", "show", "delete", "set-default"];
const KV_COMMANDS: [&str; 5] = ["get", "put", "delete", "list", "txn"];
const KV_TXN_COMMANDS: [&str; 6] = ["begin", "get", "put", "delete", "commit", "rollback"];

fn subcommand_names(command: &clap::Command) -> Vec<&str> {
    command
        .get_subcommands()
        .map(|subcommand| subcommand.get_name())
        .collect()
}

fn long_options(command: &clap::Command) -> Vec<&str> {
    command
        .get_arguments()
        .filter_map(|argument| argument.get_long())
        .collect()
}

#[test]
fn i17_profile_and_kv_commands_options_have_fixed_registers() {
    let root = Cli::command();
    let profile = root.find_subcommand("profile").expect("profile command");
    assert_eq!(subcommand_names(profile), PROFILE_COMMANDS);
    let profile_create = profile.find_subcommand("create").expect("profile create");
    assert_eq!(long_options(profile_create), ["data-dir"]);

    let kv = root.find_subcommand("kv").expect("kv command");
    assert_eq!(subcommand_names(kv), KV_COMMANDS);
    let kv_list = kv.find_subcommand("list").expect("kv list");
    assert_eq!(long_options(kv_list), ["prefix"]);

    let txn = kv.find_subcommand("txn").expect("kv txn");
    assert_eq!(subcommand_names(txn), KV_TXN_COMMANDS);
    assert_eq!(
        long_options(txn.find_subcommand("begin").expect("txn begin")),
        ["timeout-secs", "request-id"]
    );
    for action in ["get", "put", "delete", "commit", "rollback"] {
        assert!(
            long_options(txn.find_subcommand(action).expect("txn action")).contains(&"txn-id"),
            "{action} must require a transaction identity"
        );
        assert!(
            long_options(txn.find_subcommand(action).expect("txn action")).contains(&"request-id"),
            "{action} must accept a stable retry identity"
        );
    }
}
