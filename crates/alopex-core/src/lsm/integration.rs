//! LsmKV の統合テスト。
//!
//! - `crud`: 大量データでの CRUD/scan の健全性確認
//! - `recovery`: WAL リプレイによるクラッシュリカバリの確認（簡易）

pub mod crud {
    use crate::kv::{KVStore, KVTransaction};
    use crate::lsm::wal::{SyncMode, WalConfig};
    use crate::lsm::{LsmKV, LsmKVConfig, MemTableConfig};
    use crate::types::TxnMode;

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                // 1M キー規模のテストでも「チェックポイント無しで WAL が満杯にならない」容量を確保する。
                // 目安: 1M * (key+value+overhead) ≒ 数十 MB。
                segment_size: 1024 * 1024,
                max_segments: 64,
                sync_mode: SyncMode::NoSync,
            },
            memtable: MemTableConfig {
                // テスト中に無駄に flush しない程度に大きく。
                flush_threshold: 1_000_000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn key_for(i: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(2 + 8);
        k.extend_from_slice(b"k:");
        k.extend_from_slice(&i.to_be_bytes());
        k
    }

    fn value_for(i: u64) -> Vec<u8> {
        i.to_le_bytes().to_vec()
    }

    fn keys_to_write() -> usize {
        // 1M キー以上を既定とし、環境変数で調整可能にする（CI/ローカル双方を想定）。
        std::env::var("ALOPEX_LSM_CRUD_KEYS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(1_000_000)
    }

    #[test]
    fn large_crud_roundtrip_with_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_config();
        let n = keys_to_write();

        // 大量 insert（バッチコミットで write_set を肥大化させない）。
        {
            let store = LsmKV::open_with_config(dir.path(), cfg.clone()).expect("open");
            let mut i = 0usize;
            let batch = 1_000usize;
            while i < n {
                let end = usize::min(i + batch, n);
                let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
                for j in i..end {
                    let key = key_for(j as u64);
                    tx.put(key, value_for(j as u64)).unwrap();
                }
                tx.commit_self().unwrap();
                i = end;
            }
        }

        // reopen して読み取れること（WAL replay を含む）。
        let store = LsmKV::open_with_config(dir.path(), cfg.clone()).expect("reopen");

        // point get（サンプル）
        {
            let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
            for &idx in &[0usize, n / 2, n - 1] {
                let got = ro.get(&key_for(idx as u64)).unwrap();
                assert_eq!(got, Some(value_for(idx as u64)));
            }
        }

        // scan_prefix（件数と順序の最低限の検証）
        {
            let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
            let iter = ro.scan_prefix(b"k:").unwrap();
            let mut count = 0usize;
            for (expected, (k, v)) in iter.enumerate() {
                assert!(k.starts_with(b"k:"));
                let suffix: [u8; 8] = k[2..10].try_into().expect("fixed key layout");
                let i = u64::from_be_bytes(suffix);
                assert_eq!(i, expected as u64);
                assert_eq!(v, value_for(i));
                count += 1;
            }
            assert_eq!(count, n);
        }

        // update + delete（サブセット）
        {
            // update: 100 個だけ上書き
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            for i in 0..100u64 {
                tx.put(key_for(i), b"updated".to_vec()).unwrap();
            }
            tx.commit_self().unwrap();

            // delete: 10 個おきに削除（0,10,20,...）
            let mut i = 0usize;
            let batch = 10_000usize;
            while i < n {
                let end = usize::min(i + batch, n);
                let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
                for j in i..end {
                    if j % 10 == 0 {
                        tx.delete(key_for(j as u64)).unwrap();
                    }
                }
                tx.commit_self().unwrap();
                i = end;
            }
        }

        // reopen 後も整合が保たれること
        let store = LsmKV::open_with_config(dir.path(), cfg).expect("reopen after updates");
        {
            let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
            // updated: 0..100（ただし delete される 0,10,... は消える）
            for i in 0..100u64 {
                let got = ro.get(&key_for(i)).unwrap();
                if i.is_multiple_of(10) {
                    assert_eq!(got, None);
                } else {
                    assert_eq!(got, Some(b"updated".to_vec()));
                }
            }
            // 末尾付近の削除確認（削除対象/非対象）
            for &idx in &[n - 1, n - 2, n - 10] {
                let got = ro.get(&key_for(idx as u64)).unwrap();
                if idx % 10 == 0 {
                    assert_eq!(got, None);
                } else {
                    assert_eq!(got, Some(value_for(idx as u64)));
                }
            }
        }
    }
}

pub mod recovery {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    use crate::kv::{KVStore, KVTransaction};
    use crate::lsm::wal::{SyncMode, WalConfig, WalEntry, WalReader, WalSectionHeader};
    use crate::lsm::{LsmKV, LsmKVConfig, MemTableConfig};
    use crate::types::TxnMode;

    fn test_config() -> LsmKVConfig {
        LsmKVConfig {
            wal: WalConfig {
                segment_size: 4096,
                max_segments: 2,
                sync_mode: SyncMode::NoSync,
            },
            memtable: MemTableConfig {
                flush_threshold: 1_000_000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn reopen_replays_wal_and_ignores_corrupt_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_config();

        // 1) 正常に 1 件コミット（WAL に確実に残る）
        {
            let store = LsmKV::open_with_config(dir.path(), cfg.clone()).expect("open");
            let mut tx = store.begin(TxnMode::ReadWrite).unwrap();
            tx.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
            tx.commit_self().unwrap();
        }

        // 2) クラッシュ相当: セクションヘッダだけ進めて「未書き込みの末尾」を作る
        let wal_path = dir.path().join("lsm.wal");
        let bogus = WalEntry::put(999, b"k2".to_vec(), b"v2".to_vec());
        let bogus_len = bogus.encode().unwrap().len() as u64;

        // 変更前ヘッダを読みつつ、整合が崩れない範囲で end_offset を進める。
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_path)
                .unwrap();

            let mut hdr = [0u8; 16];
            file.seek(SeekFrom::Start(0)).unwrap();
            file.read_exact(&mut hdr).unwrap();
            let mut section = WalSectionHeader::from_bytes(&hdr);
            section.end_offset = section.end_offset.saturating_add(bogus_len);
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&section.to_bytes()).unwrap();
            file.flush().unwrap();
        }

        // 3) reopen: reader は末尾で停止しつつ、先頭のエントリは復元される
        let store = LsmKV::open_with_config(dir.path(), cfg.clone()).expect("reopen");
        let mut ro = store.begin(TxnMode::ReadOnly).unwrap();
        assert_eq!(ro.get(&b"k1".to_vec()).unwrap(), Some(b"v1".to_vec()));
        assert_eq!(ro.get(&b"k2".to_vec()).unwrap(), None);

        // reader 側でも stop_reason が出る（ただしエラーにはしない）
        let mut reader = WalReader::open(&wal_path, cfg.wal.clone()).unwrap();
        let replay = reader.replay().unwrap();
        assert!(replay.stop_reason.is_some());
        assert_eq!(replay.entries.len(), 1);
    }
}
