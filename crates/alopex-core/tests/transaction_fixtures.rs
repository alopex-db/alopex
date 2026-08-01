//! Fixed F4 transaction fixtures for local storage semantics.
//!
//! The manifest is intentionally immutable and has no clock, random, or
//! representation-derived fields.  Distributed fixture execution lives in
//! `alopex-cluster`; this file proves the local-only half of the same register.

use std::collections::BTreeSet;

use alopex_core::{Error, KVStore, KVTransaction, MemoryKV, TxnMode};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FixtureManifest {
    schema_version: u32,
    fixtures: Vec<Fixture>,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    id: String,
    transaction_id: String,
    request_id: String,
    ranges: Vec<String>,
    expected_state: String,
    coverage: Vec<String>,
}

fn manifest() -> FixtureManifest {
    serde_json::from_str(include_str!("../../../tests/fixtures/f4_transactions.json"))
        .expect("F4 fixture manifest must be valid JSON")
}

fn fixture<'a>(manifest: &'a FixtureManifest, id: &str) -> &'a Fixture {
    manifest
        .fixtures
        .iter()
        .find(|fixture| fixture.id == id)
        .unwrap_or_else(|| panic!("missing fixture {id}"))
}

fn key(name: &str) -> Vec<u8> {
    format!("f4:{name}").into_bytes()
}

#[test]
fn f4_manifest_is_a_complete_fixed_transaction_register() {
    let manifest = manifest();
    assert_eq!(manifest.schema_version, 1);

    let expected = BTreeSet::from([
        "F4-TXN-LOCAL-01",
        "F4-TXN-SINGLE-01",
        "F4-TXN-MULTI-01",
        "F4-TXN-ROLLBACK-01",
        "F4-TXN-TIMEOUT-01",
        "F4-TXN-CONFLICT-01",
        "F4-TXN-EPOCH-01",
        "F4-TXN-PENDING-01",
    ]);
    let actual = manifest
        .fixtures
        .iter()
        .map(|fixture| fixture.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected, "F4 register must be exact and unique");
    assert_eq!(actual.len(), manifest.fixtures.len());

    for fixture in &manifest.fixtures {
        assert!(fixture.transaction_id.starts_with("f4-"));
        assert!(fixture.request_id.starts_with("f4-"));
        assert!(!fixture.ranges.is_empty());
        assert!(!fixture.expected_state.is_empty());
        assert!(!fixture.coverage.is_empty());
    }
}

#[test]
fn local_fixture_preserves_snapshot_read_your_writes_atomicity_and_rollback() {
    let manifest = manifest();
    let local = fixture(&manifest, "F4-TXN-LOCAL-01");
    let rollback = fixture(&manifest, "F4-TXN-ROLLBACK-01");
    assert_eq!(local.expected_state, "committed");
    assert!(local.coverage.contains(&"local_only".to_owned()));
    assert!(local.coverage.contains(&"snapshot".to_owned()));
    assert!(local.coverage.contains(&"read_your_writes".to_owned()));
    assert!(rollback.coverage.contains(&"rollback".to_owned()));

    let store = MemoryKV::new();
    let mut seed = store.begin(TxnMode::ReadWrite).expect("seed begin");
    seed.put(key("visible"), b"base".to_vec())
        .expect("seed put");
    seed.commit_self().expect("seed commit");

    let mut transaction = store.begin(TxnMode::ReadWrite).expect("fixture begin");
    transaction
        .put(key("range-a"), b"prepared-a".to_vec())
        .expect("stage range a");
    transaction
        .put(key("range-b"), b"prepared-b".to_vec())
        .expect("stage range b");
    assert_eq!(
        transaction.get(&key("range-a")).expect("read own write"),
        Some(b"prepared-a".to_vec())
    );

    let mut outside = store.begin(TxnMode::ReadOnly).expect("outside begin");
    assert_eq!(
        outside.get(&key("visible")).unwrap(),
        Some(b"base".to_vec())
    );
    assert_eq!(outside.get(&key("range-a")).unwrap(), None);
    assert_eq!(outside.get(&key("range-b")).unwrap(), None);
    outside.rollback_self().expect("outside rollback");

    transaction.commit_self().expect("atomic local commit");
    let mut committed = store.begin(TxnMode::ReadOnly).expect("committed read");
    assert_eq!(
        committed.get(&key("range-a")).unwrap(),
        Some(b"prepared-a".to_vec())
    );
    assert_eq!(
        committed.get(&key("range-b")).unwrap(),
        Some(b"prepared-b".to_vec())
    );
    committed.rollback_self().expect("committed reader close");

    let mut aborted = store.begin(TxnMode::ReadWrite).expect("abort begin");
    aborted
        .put(key("never-visible"), b"discarded".to_vec())
        .unwrap();
    aborted
        .rollback_self()
        .expect("abort discards staged write");
    let mut verify = store.begin(TxnMode::ReadOnly).expect("rollback verify");
    assert_eq!(verify.get(&key("never-visible")).unwrap(), None);
}

#[test]
fn conflict_fixture_preserves_the_committed_winner_without_partial_write() {
    let manifest = manifest();
    let conflict = fixture(&manifest, "F4-TXN-CONFLICT-01");
    assert_eq!(conflict.expected_state, "terminal_failure");
    assert!(conflict.coverage.contains(&"conflict".to_owned()));
    assert!(conflict
        .coverage
        .contains(&"no_partial_visibility".to_owned()));

    let store = MemoryKV::new();
    let mut seed = store.begin(TxnMode::ReadWrite).expect("seed begin");
    seed.put(key("conflict"), b"v0".to_vec()).unwrap();
    seed.commit_self().unwrap();

    let mut stale = store.begin(TxnMode::ReadWrite).expect("stale begin");
    assert_eq!(stale.get(&key("conflict")).unwrap(), Some(b"v0".to_vec()));
    stale.put(key("conflict"), b"stale".to_vec()).unwrap();
    stale
        .put(key("unrelated"), b"must-not-appear".to_vec())
        .unwrap();

    let mut winner = store.begin(TxnMode::ReadWrite).expect("winner begin");
    winner.put(key("conflict"), b"winner".to_vec()).unwrap();
    winner.commit_self().expect("winner commit");
    assert!(matches!(stale.commit_self(), Err(Error::TxnConflict)));

    let mut verify = store.begin(TxnMode::ReadOnly).expect("verify begin");
    assert_eq!(
        verify.get(&key("conflict")).unwrap(),
        Some(b"winner".to_vec())
    );
    assert_eq!(verify.get(&key("unrelated")).unwrap(), None);
}
