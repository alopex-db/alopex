//! Raftログセクションのモデル。

use serde::{Deserialize, Serialize};

/// Raftログセクション。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogSection {
    /// 最終適用インデックス。
    pub last_applied_index: u64,
    /// 最終適用term。
    pub last_applied_term: u64,
    /// ログエントリ一覧。
    pub entries: Vec<RaftLogEntry>,
}

/// Raftログエントリ。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    /// ログインデックス。
    pub index: u64,
    /// term。
    pub term: u64,
    /// エントリタイプ。
    pub entry_type: RaftEntryType,
    /// 任意データ（上位レイヤーコマンド）。
    pub data: Vec<u8>,
}

/// Raftエントリタイプ。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftEntryType {
    /// 通常コマンド。
    Normal = 0,
    /// 構成変更。
    ConfigChange = 1,
    /// スナップショット。
    Snapshot = 2,
}
