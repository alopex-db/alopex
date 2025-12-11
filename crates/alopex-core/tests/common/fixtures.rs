#[cfg(feature = "test-hooks")]
use super::fault_injection::FaultInjector;
#[cfg(feature = "test-hooks")]
use alopex_core::{CrashSimulator, IoHooks};
use alopex_core::{KVStore, KVTransaction, MemoryKV, Result as CoreResult, TxnMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "test-hooks")]
use std::sync::Arc;
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

/// ストアを障害注入フック付きで開く（test-hooks有効時のみ）。
#[cfg(feature = "test-hooks")]
pub fn open_store_with_fault_injector(
    path: &Path,
    injector: Arc<dyn FaultInjector>,
) -> CoreResult<MemoryKV> {
    struct InjectorAdapter {
        inner: Arc<dyn FaultInjector>,
    }

    impl IoHooks for InjectorAdapter {
        fn before_wal_write(&self, data: &[u8]) -> std::io::Result<()> {
            self.inner.before_write(data)
        }

        fn after_wal_write(&self, _data: &[u8]) -> std::io::Result<()> {
            Ok(())
        }

        fn before_fsync(&self) -> std::io::Result<()> {
            self.inner.before_fsync()
        }

        fn after_fsync(&self) -> std::io::Result<()> {
            Ok(())
        }

        fn before_sst_write(&self, data: &[u8]) -> std::io::Result<()> {
            self.inner.before_write(data)
        }

        fn on_compaction_start(&self) {
            let _ = self.inner.before_fsync();
        }

        fn on_compaction_end(&self) {}
    }

    let hooks = Arc::new(InjectorAdapter { inner: injector });
    MemoryKV::open_with_io_hooks(path, hooks)
}

/// ストアをクラッシュシミュレーター付きで開く（test-hooks有効時のみ）。
#[cfg(feature = "test-hooks")]
pub fn open_store_with_crash_sim(
    path: &Path,
    crash_sim: Arc<CrashSimulator>,
) -> CoreResult<MemoryKV> {
    MemoryKV::open_with_crash_hooks(path, crash_sim)
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
