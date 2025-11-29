//! 短命データ（Intent/Lock）のモデルとGC設定。

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Intentセクション。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentSection {
    /// Intentエントリ一覧。
    pub entries: Vec<IntentEntry>,
}

/// Intentエントリ。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentEntry {
    /// 対象キー。
    pub key: Vec<u8>,
    /// トランザクションID。
    pub txn_id: u64,
    /// 開始タイムスタンプ。
    pub start_ts: u64,
    /// Intent種別。
    pub intent_type: IntentType,
    /// 書き込み予定値。
    pub value: Option<Vec<u8>>,
}

/// Intent種別。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IntentType {
    /// Put intent。
    Put = 0,
    /// Delete intent。
    Delete = 1,
}

/// Lockセクション。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockSection {
    /// Lockエントリ一覧。
    pub entries: Vec<LockEntry>,
}

/// Lockエントリ。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockEntry {
    /// 対象キー。
    pub key: Vec<u8>,
    /// ロック保持トランザクションID。
    pub txn_id: u64,
    /// ロック種別。
    pub lock_type: LockType,
    /// 取得タイムスタンプ。
    pub acquired_ts: u64,
    /// TTL（ミリ秒）。
    pub ttl_ms: u64,
}

/// Lock種別。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LockType {
    /// 共有ロック。
    Shared = 0,
    /// 排他ロック。
    Exclusive = 1,
}

/// Intent/LockデータのGC設定。
#[derive(Debug, Clone)]
pub struct EphemeralDataGcConfig {
    /// Intent flush間隔。
    pub intent_flush_interval: Duration,
    /// Lock flush間隔。
    pub lock_flush_interval: Duration,
    /// GCトリガー（エントリ数）。
    pub gc_trigger_entries: usize,
    /// GCトリガー（バイト数）。
    pub gc_trigger_bytes: u64,
    /// Intent保持期間。
    pub intent_retention: Duration,
    /// Lock保持期間。
    pub lock_retention: Duration,
}

impl Default for EphemeralDataGcConfig {
    fn default() -> Self {
        Self {
            intent_flush_interval: Duration::from_secs(1),
            lock_flush_interval: Duration::from_secs(1),
            gc_trigger_entries: 1000,
            gc_trigger_bytes: 1 * 1024 * 1024,
            intent_retention: Duration::from_secs(0),
            lock_retention: Duration::from_secs(0),
        }
    }
}
