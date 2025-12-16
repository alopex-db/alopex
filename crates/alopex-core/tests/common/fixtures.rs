#[cfg(feature = "test-hooks")]
use super::fault_injection::FaultInjector;
#[cfg(feature = "test-hooks")]
use alopex_core::{CrashSimulator, IoHooks};
use alopex_core::kv::AnyKV;
use alopex_core::lsm::wal::{SyncMode, WalConfig};
use alopex_core::lsm::LsmKVConfig;
use alopex_core::{KVStore, KVTransaction, MemoryKV, Result as CoreResult, StorageFactory, StorageMode, TxnMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::env;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StressStorageMode {
    Memory,
    Disk,
}

impl StressStorageMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
        }
    }
}

/// 有効なストレージモード一覧を返す。
///
/// - `STRESS_STORAGE_MODE=memory|disk|both`（未指定は both）
pub fn selected_storage_modes() -> Vec<StressStorageMode> {
    match env::var("STRESS_STORAGE_MODE")
        .unwrap_or_else(|_| "both".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "memory" => vec![StressStorageMode::Memory],
        "disk" => vec![StressStorageMode::Disk],
        "both" | "" => vec![StressStorageMode::Memory, StressStorageMode::Disk],
        other => panic!("invalid STRESS_STORAGE_MODE={other} (expected memory|disk|both)"),
    }
}

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

pub fn storage_root_for_mode(base_wal_path: &Path, mode: StressStorageMode) -> PathBuf {
    match mode {
        StressStorageMode::Memory => base_wal_path.to_path_buf(),
        StressStorageMode::Disk => {
            let parent = base_wal_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            let stem = base_wal_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("db");
            parent.join(format!("{stem}.lsm"))
        }
    }
}

pub fn wal_path_for_mode(base_wal_path: &Path, mode: StressStorageMode) -> PathBuf {
    match mode {
        StressStorageMode::Memory => base_wal_path.to_path_buf(),
        StressStorageMode::Disk => storage_root_for_mode(base_wal_path, mode).join("lsm.wal"),
    }
}

/// 永続化なしの共有ストアを生成する（Disk の場合は TempDir を返す）。
pub fn new_shared_store_for_mode(
    mode: StressStorageMode,
) -> CoreResult<(Arc<AnyKV>, Option<TempDir>)> {
    match mode {
        StressStorageMode::Memory => Ok((Arc::new(AnyKV::Memory(MemoryKV::new())), None)),
        StressStorageMode::Disk => {
            let dir = TempDir::new()?;
            let cfg = LsmKVConfig {
                wal: WalConfig {
                    sync_mode: SyncMode::NoSync,
                    ..WalConfig::default()
                },
                ..LsmKVConfig::default()
            };
            let store = StorageFactory::create(StorageMode::Disk {
                path: dir.path().to_path_buf(),
                config: Some(cfg),
            })?;
            Ok((Arc::new(store), Some(dir)))
        }
    }
}

/// ストアを開く。
pub fn open_store_for_mode(base_wal_path: &Path, mode: StressStorageMode) -> CoreResult<AnyKV> {
    match mode {
        StressStorageMode::Memory => Ok(AnyKV::Memory(MemoryKV::open(base_wal_path)?)),
        StressStorageMode::Disk => {
            let root = storage_root_for_mode(base_wal_path, mode);
            std::fs::create_dir_all(&root)?;
            let cfg = LsmKVConfig {
                wal: WalConfig {
                    sync_mode: SyncMode::NoSync,
                    ..WalConfig::default()
                },
                ..LsmKVConfig::default()
            };
            // 並列テストで同一ディレクトリを同時 open すると WAL 初期化が競合するため、
            // 1プロセス内の簡易ロックで初期化を直列化する。
            let lock_path = root.join(".init.lock");
            let ready_path = root.join(".init.ready");
            let initializer = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(_) => true,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
                Err(e) => return Err(e.into()),
            };
            if !initializer {
                let start = Instant::now();
                while !ready_path.exists() && start.elapsed() < Duration::from_secs(5) {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            StorageFactory::create(StorageMode::Disk {
                path: root,
                config: Some(cfg),
            })
            .map(|store| {
                if initializer {
                    let _ = std::fs::write(&ready_path, b"");
                    let _ = std::fs::remove_file(&lock_path);
                }
                store
            })
        }
    }
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
