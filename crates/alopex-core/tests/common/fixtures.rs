use alopex_core::{KVStore, KVTransaction, MemoryKV, Result as CoreResult, TxnMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// テスト環境（TempDir + DBパス）。
pub struct TestEnvironment {
    pub temp_dir: TempDir,
    pub db_path: PathBuf,
}

impl TestEnvironment {
    pub fn new(name: &str) -> CoreResult<Self> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join(format!("{name}.wal"));
        Ok(Self { temp_dir, db_path })
    }
}

/// ストアを開く。
pub fn open_store(path: &Path) -> CoreResult<MemoryKV> {
    MemoryKV::open(path)
}

/// テストデータ生成。
pub fn generate_test_data(count: usize, value_size: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|i| {
            let key = format!("key_{:08}", i).into_bytes();
            let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
            (key, value)
        })
        .collect()
}

/// ファイル破損を注入（単純に先頭を書き換える）。
pub fn corrupt_file(path: &Path, bytes: usize) -> CoreResult<()> {
    let mut data = Vec::new();
    {
        let mut f = OpenOptions::new().read(true).open(path)?;
        f.read_to_end(&mut data)?;
    }
    if data.is_empty() {
        return Ok(());
    }
    for i in 0..bytes.min(data.len()) {
        data[i] = 0;
    }
    let mut f = OpenOptions::new().write(true).truncate(true).open(path)?;
    f.write_all(&data)?;
    Ok(())
}

/// 簡易整合性チェック用データ投入。
pub fn seed_store(path: &Path, count: usize) -> CoreResult<()> {
    let store = MemoryKV::open(path)?;
    let mut txn = store.begin(TxnMode::ReadWrite)?;
    for i in 0..count {
        let key = format!("k{i:04}").into_bytes();
        let val = format!("v{i:04}").into_bytes();
        txn.put(key, val)?;
    }
    txn.commit_self()?;
    Ok(())
}
