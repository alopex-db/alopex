mod common;

#[test]
fn inventory_requires_each_workspace_and_development_crate_to_be_classified() {
    let fixture = common::fixture();
    let (_, clean_errors) = fixture
        .inventory
        .verify(&fixture.source, &fixture.artifacts_root);
    assert!(clean_errors.is_empty(), "{clean_errors:?}");

    let mut declaration = fixture.inventory.clone();
    declaration
        .classifications
        .retain(|classification| classification.crate_name != "alopex-tools");
    let (_, errors) = declaration.verify(&fixture.source, &fixture.artifacts_root);
    assert!(errors
        .iter()
        .any(|error| error.code == "artifact_scope_ambiguous"));
}
