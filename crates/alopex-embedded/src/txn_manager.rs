//! Transaction manager for CLI-driven KV transactions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alopex_core::kv::any::AnyKVTransaction;
use alopex_core::{KVStore, KVTransaction, TxnMode};

use crate::{Database, Error, Result};

const TXN_META_PREFIX: &[u8] = b"__alopex_txn_meta__:";
const TXN_WRITE_PREFIX: &[u8] = b"__alopex_txn_write__:";
const TXN_WRITE_DELETE: u8 = 0;
const TXN_WRITE_PUT: u8 = 1;

/// Metadata for a persisted KV transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionInfo {
    /// Transaction identifier.
    pub txn_id: String,
    /// Persisted transaction start time.
    pub started_at: SystemTime,
    /// Timeout duration in seconds.
    pub timeout_secs: u64,
    /// Whether the transaction is expired.
    pub is_expired: bool,
}

#[derive(Debug, Clone, Copy)]
struct TxnMeta {
    started_at_secs: u64,
    timeout_secs: u64,
}

enum TxnWrite {
    Put(Vec<u8>),
    Delete,
}

/// Manages persisted KV transactions for CLI usage.
pub struct TransactionManager;

impl TransactionManager {
    /// Begins a new transaction with the given timeout and returns its ID.
    pub fn begin_with_timeout(db: &Database, timeout: Duration) -> Result<String> {
        let txn_id = generate_txn_id();
        let meta = TxnMeta {
            started_at_secs: current_timestamp_secs(),
            timeout_secs: timeout.as_secs(),
        };
        let mut txn = db.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        txn.put(txn_meta_key(&txn_id), encode_meta(meta))
            .map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)?;
        Ok(txn_id)
    }

    /// Retrieves persisted transaction metadata.
    pub fn get_info(db: &Database, txn_id: &str) -> Result<TransactionInfo> {
        let mut txn = db.store.begin(TxnMode::ReadOnly).map_err(Error::Core)?;
        let result = load_meta(&mut txn, txn_id).map(|meta| {
            let started_at = UNIX_EPOCH + Duration::from_secs(meta.started_at_secs);
            let is_expired = is_expired_from_meta(meta, current_timestamp_secs());
            TransactionInfo {
                txn_id: txn_id.to_string(),
                started_at,
                timeout_secs: meta.timeout_secs,
                is_expired,
            }
        });
        let commit_result = txn.commit_self().map_err(Error::Core);
        match (result, commit_result) {
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Ok(info), Ok(())) => Ok(info),
        }
    }

    /// Checks whether a transaction has expired.
    pub fn is_expired(db: &Database, txn_id: &str) -> Result<bool> {
        let mut txn = db.store.begin(TxnMode::ReadOnly).map_err(Error::Core)?;
        let result = load_meta(&mut txn, txn_id)
            .map(|meta| is_expired_from_meta(meta, current_timestamp_secs()));
        let commit_result = txn.commit_self().map_err(Error::Core);
        match (result, commit_result) {
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    /// Retrieves a key within the specified transaction.
    pub fn get(db: &Database, txn_id: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut txn = db.store.begin(TxnMode::ReadOnly).map_err(Error::Core)?;
        let result = (|| {
            let _ = load_meta(&mut txn, txn_id)?;
            if let Some(raw) = txn.get(&txn_write_key(txn_id, key)).map_err(Error::Core)? {
                return Ok(match decode_write(txn_id, &raw)? {
                    TxnWrite::Put(value) => Some(value),
                    TxnWrite::Delete => None,
                });
            }
            txn.get(&key.to_vec()).map_err(Error::Core)
        })();
        let commit_result = txn.commit_self().map_err(Error::Core);
        match (result, commit_result) {
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    /// Stages a put operation within the specified transaction.
    pub fn put(db: &Database, txn_id: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let mut txn = db.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        let _ = load_meta(&mut txn, txn_id)?;
        let encoded = encode_write(TxnWrite::Put(value.to_vec()));
        txn.put(txn_write_key(txn_id, key), encoded)
            .map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)?;
        Ok(())
    }

    /// Stages a delete operation within the specified transaction.
    pub fn delete(db: &Database, txn_id: &str, key: &[u8]) -> Result<()> {
        let mut txn = db.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        let _ = load_meta(&mut txn, txn_id)?;
        let encoded = encode_write(TxnWrite::Delete);
        txn.put(txn_write_key(txn_id, key), encoded)
            .map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)?;
        Ok(())
    }

    /// Commits staged writes and finalizes the transaction.
    pub fn commit(db: &Database, txn_id: &str) -> Result<()> {
        let mut txn = db.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        let _ = load_meta(&mut txn, txn_id)?;
        let prefix = txn_write_prefix(txn_id);
        let staged: Vec<(Vec<u8>, Vec<u8>)> =
            txn.scan_prefix(&prefix).map_err(Error::Core)?.collect();
        for (staged_key, raw) in &staged {
            let user_key = extract_user_key(txn_id, staged_key)?;
            match decode_write(txn_id, raw)? {
                TxnWrite::Put(value) => {
                    txn.put(user_key, value).map_err(Error::Core)?;
                }
                TxnWrite::Delete => {
                    txn.delete(user_key).map_err(Error::Core)?;
                }
            }
        }
        for (staged_key, _) in staged {
            txn.delete(staged_key).map_err(Error::Core)?;
        }
        txn.delete(txn_meta_key(txn_id)).map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)?;
        Ok(())
    }

    /// Rolls back staged writes and removes transaction metadata.
    pub fn rollback(db: &Database, txn_id: &str) -> Result<()> {
        let mut txn = db.store.begin(TxnMode::ReadWrite).map_err(Error::Core)?;
        let _ = load_meta(&mut txn, txn_id)?;
        let prefix = txn_write_prefix(txn_id);
        let staged: Vec<(Vec<u8>, Vec<u8>)> =
            txn.scan_prefix(&prefix).map_err(Error::Core)?.collect();
        for (staged_key, _) in staged {
            txn.delete(staged_key).map_err(Error::Core)?;
        }
        txn.delete(txn_meta_key(txn_id)).map_err(Error::Core)?;
        txn.commit_self().map_err(Error::Core)?;
        Ok(())
    }
}

fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_txn_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("txn-{}-{}", nanos, std::process::id())
}

fn txn_meta_key(txn_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(TXN_META_PREFIX.len() + txn_id.len());
    key.extend_from_slice(TXN_META_PREFIX);
    key.extend_from_slice(txn_id.as_bytes());
    key
}

fn txn_write_prefix(txn_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(TXN_WRITE_PREFIX.len() + txn_id.len() + 1);
    key.extend_from_slice(TXN_WRITE_PREFIX);
    key.extend_from_slice(txn_id.as_bytes());
    key.push(b':');
    key
}

fn txn_write_key(txn_id: &str, key: &[u8]) -> Vec<u8> {
    let mut full = txn_write_prefix(txn_id);
    full.extend_from_slice(key);
    full
}

fn encode_meta(meta: TxnMeta) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&meta.started_at_secs.to_le_bytes());
    payload.extend_from_slice(&meta.timeout_secs.to_le_bytes());
    payload
}

fn decode_meta(txn_id: &str, raw: &[u8]) -> Result<TxnMeta> {
    if raw.len() < 16 {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "transaction metadata invalid: {}",
            txn_id
        ))));
    }
    let started_at_secs = u64::from_le_bytes(raw[0..8].try_into().unwrap());
    let timeout_secs = u64::from_le_bytes(raw[8..16].try_into().unwrap());
    Ok(TxnMeta {
        started_at_secs,
        timeout_secs,
    })
}

fn load_meta(txn: &mut AnyKVTransaction<'_>, txn_id: &str) -> Result<TxnMeta> {
    let Some(raw) = txn.get(&txn_meta_key(txn_id)).map_err(Error::Core)? else {
        return Err(Error::InvalidTransactionId(txn_id.to_string()));
    };
    decode_meta(txn_id, &raw)
}

fn is_expired_from_meta(meta: TxnMeta, now_secs: u64) -> bool {
    now_secs.saturating_sub(meta.started_at_secs) >= meta.timeout_secs
}

fn encode_write(entry: TxnWrite) -> Vec<u8> {
    match entry {
        TxnWrite::Put(value) => {
            let mut payload = Vec::with_capacity(1 + value.len());
            payload.push(TXN_WRITE_PUT);
            payload.extend_from_slice(&value);
            payload
        }
        TxnWrite::Delete => vec![TXN_WRITE_DELETE],
    }
}

fn decode_write(txn_id: &str, raw: &[u8]) -> Result<TxnWrite> {
    let Some((&tag, rest)) = raw.split_first() else {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "transaction write entry invalid: {}",
            txn_id
        ))));
    };
    match tag {
        TXN_WRITE_PUT => Ok(TxnWrite::Put(rest.to_vec())),
        TXN_WRITE_DELETE => Ok(TxnWrite::Delete),
        _ => Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "transaction write entry invalid: {}",
            txn_id
        )))),
    }
}

fn extract_user_key(txn_id: &str, staged_key: &[u8]) -> Result<Vec<u8>> {
    let prefix = txn_write_prefix(txn_id);
    if !staged_key.starts_with(&prefix) {
        return Err(Error::Core(alopex_core::Error::InvalidFormat(format!(
            "transaction write key invalid: {}",
            txn_id
        ))));
    }
    Ok(staged_key[prefix.len()..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_db() -> Database {
        Database::open_in_memory().unwrap()
    }

    #[test]
    fn test_txn_put_get_commit() {
        let db = create_test_db();
        let txn_id = TransactionManager::begin_with_timeout(&db, Duration::from_secs(60)).unwrap();

        TransactionManager::put(&db, &txn_id, b"alpha", b"beta").unwrap();
        let value = TransactionManager::get(&db, &txn_id, b"alpha").unwrap();
        assert_eq!(value, Some(b"beta".to_vec()));

        TransactionManager::commit(&db, &txn_id).unwrap();

        let mut verify_txn = db.begin(TxnMode::ReadOnly).unwrap();
        let stored = verify_txn.get(b"alpha").unwrap();
        verify_txn.commit().unwrap();
        assert_eq!(stored, Some(b"beta".to_vec()));

        let err = TransactionManager::get(&db, &txn_id, b"alpha").unwrap_err();
        assert!(matches!(err, Error::InvalidTransactionId(_)));
    }

    #[test]
    fn test_txn_rollback_discards_writes() {
        let db = create_test_db();
        let txn_id = TransactionManager::begin_with_timeout(&db, Duration::from_secs(60)).unwrap();

        TransactionManager::put(&db, &txn_id, b"key", b"value").unwrap();
        TransactionManager::rollback(&db, &txn_id).unwrap();

        let mut verify_txn = db.begin(TxnMode::ReadOnly).unwrap();
        let stored = verify_txn.get(b"key").unwrap();
        verify_txn.commit().unwrap();
        assert!(stored.is_none());

        let err = TransactionManager::get(&db, &txn_id, b"key").unwrap_err();
        assert!(matches!(err, Error::InvalidTransactionId(_)));
    }

    #[test]
    fn test_txn_delete_marks_missing() {
        let db = create_test_db();
        {
            let mut seed = db.begin(TxnMode::ReadWrite).unwrap();
            seed.put(b"drop-me", b"payload").unwrap();
            seed.commit().unwrap();
        }

        let txn_id = TransactionManager::begin_with_timeout(&db, Duration::from_secs(60)).unwrap();
        TransactionManager::delete(&db, &txn_id, b"drop-me").unwrap();
        let value = TransactionManager::get(&db, &txn_id, b"drop-me").unwrap();
        assert!(value.is_none());
        TransactionManager::rollback(&db, &txn_id).unwrap();
    }

    #[test]
    fn test_txn_is_expired() {
        let db = create_test_db();
        let txn_id = TransactionManager::begin_with_timeout(&db, Duration::from_secs(0)).unwrap();
        assert!(TransactionManager::is_expired(&db, &txn_id).unwrap());
    }
}
