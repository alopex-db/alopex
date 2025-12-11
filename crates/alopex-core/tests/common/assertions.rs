use alopex_core::{KVStore, KVTransaction, MemoryKV, Result as CoreResult, TxnMode};
use std::collections::HashMap;

/// 書き込み/削除の整合性を追跡する簡易チェッカー。
pub struct IntegrityChecker {
    writes: HashMap<Vec<u8>, usize>,
    deletes: HashMap<Vec<u8>, usize>,
}

impl IntegrityChecker {
    pub fn new() -> Self {
        Self {
            writes: HashMap::new(),
            deletes: HashMap::new(),
        }
    }

    pub fn record_write(&mut self, key: &[u8]) {
        *self.writes.entry(key.to_vec()).or_default() += 1;
    }

    pub fn record_delete(&mut self, key: &[u8]) {
        *self.deletes.entry(key.to_vec()).or_default() += 1;
    }

    pub fn verify(&self) -> bool {
        for (k, w) in &self.writes {
            let d = self.deletes.get(k).copied().unwrap_or(0);
            if d > *w {
                return false;
            }
        }
        true
    }
}

/// メモリリークがないことを確認（before/afterのMemoryStatsを比較）。
pub fn assert_no_memory_leak(before: usize, after: usize) {
    assert!(
        after <= before,
        "memory leak detected: before={} after={}",
        before,
        after
    );
}

/// ストレージリークがないことを確認（WAL/SSTサイズの増加を許容範囲でチェック）。
pub fn assert_no_storage_leak(db_path: &std::path::Path, max_growth_bytes: u64) -> CoreResult<()> {
    let wal_meta = std::fs::metadata(db_path)?;
    let sst_meta = std::fs::metadata(db_path.with_extension("sst")).ok();
    let growth = wal_meta.len() + sst_meta.map(|m| m.len()).unwrap_or(0);
    assert!(
        growth <= max_growth_bytes,
        "storage leak: grew {} bytes (limit {})",
        growth,
        max_growth_bytes
    );
    Ok(())
}

/// KVの基本的な存在確認。
pub fn assert_kv_contains(path: &std::path::Path, key: &[u8], expected: Option<Vec<u8>>) {
    let store = MemoryKV::open(path).expect("open store");
    let mut txn = store.begin(TxnMode::ReadOnly).expect("txn");
    let got = txn.get(&key.to_vec()).expect("get");
    assert_eq!(got, expected);
}
