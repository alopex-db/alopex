//! Windows product-level regression for reopening an already-fenced large container.

use super::*;
use crate::lsm::sstable::CompressionType;
use crate::storage::format::{FileHeader, HEADER_SIZE};

const DEFAULT_FENCE_BYTES: u64 = 64 * 1024 * 1024;
const VALUE_BYTES: usize = 33 * 1024 * 1024;

fn config() -> LsmKVConfig {
    LsmKVConfig {
        wal: WalConfig {
            segment_size: 40 * 1024 * 1024,
            max_segments: 4,
            sync_mode: SyncMode::EveryWrite,
        },
        checkpoint: CheckpointConfig {
            auto_checkpoint: false,
            ..Default::default()
        },
        prune_sidecar_on_drop: false,
        ..Default::default()
    }
}

fn write(store: &LsmKV, key: u8, value: u8) {
    let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
    tx.put(vec![key], vec![value; VALUE_BYTES]).unwrap();
    tx.commit_self().unwrap();
}

fn value(store: &LsmKV, key: u8) -> Option<Value> {
    store
        .begin(TxnMode::ReadOnly)
        .unwrap()
        .get(&vec![key])
        .unwrap()
}

fn header(path: &Path) -> FileHeader {
    let mut bytes = [0; HEADER_SIZE];
    File::open(path).unwrap().read_exact(&mut bytes).unwrap();
    FileHeader::from_bytes(&bytes).unwrap()
}

#[test]
fn windows_reopens_fenced_container_above_default_copy_budget() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("large.alopex.d");
    let container = dir.path().join("large.alopex");
    let mut cfg = config();
    // Keep the physical container above 64 MiB on every CI feature set.
    cfg.sstable.compression = CompressionType::None;
    {
        let (store, _) = LsmKV::open_with_config(&data, cfg.clone()).unwrap();
        write(&store, 0, 7);
        write(&store, 1, 9);
        store.converge().unwrap();
    }
    let container_bytes = fs::metadata(&container).unwrap().len();
    assert!(container_bytes > DEFAULT_FENCE_BYTES);
    assert_eq!(header(&container).version, FileVersion::CURRENT);

    let reopen_options = LsmOverflowConfig::new(
        SpoolLimits::new(512, 4096, 7).unwrap(),
        OperationLimits::new(512, 8, 64).unwrap(),
        16384,
        7,
        4096,
    )
    .with_inline_bytes(1024)
    .with_activation_bytes(256 * 1024 * 1024);
    assert_eq!(reopen_options.container_fence_bytes, DEFAULT_FENCE_BYTES);
    {
        // Only the first fence is granted a copy budget equal to the actual file.
        let first_fence = reopen_options.with_container_fence_bytes(container_bytes);
        let (store, _) = LsmKV::open_with_overflow(&data, cfg.clone(), first_fence).unwrap();
        assert_eq!(value(&store, 0), Some(vec![7; VALUE_BYTES]));
        assert_eq!(value(&store, 1), Some(vec![9; VALUE_BYTES]));
    }
    assert_eq!(header(&container).version, FileVersion::WAL_OVERFLOW);

    // The already-published data must remain readable with the default budget.
    let (store, _) = LsmKV::open_with_overflow(&data, cfg, reopen_options).unwrap();
    assert_eq!(value(&store, 0), Some(vec![7; VALUE_BYTES]));
    assert_eq!(value(&store, 1), Some(vec![9; VALUE_BYTES]));
}
