//! 外部で構築されたセクションをそのまま取り込むためのモデル。

use crate::storage::compression::CompressionAlgorithm;
use crate::storage::format::{FormatError, SectionType};

/// セクションがカバーするキー範囲（[start, end)）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    /// 開始キー（包含）。
    pub start: Vec<u8>,
    /// 終了キー（排他）。
    pub end: Vec<u8>,
}

impl KeyRange {
    /// start < end の妥当性を確認する。
    pub fn validate(&self) -> Result<(), FormatError> {
        if self.start >= self.end {
            return Err(FormatError::InvalidKeyRange {
                start: self.start.clone(),
                end: self.end.clone(),
            });
        }
        Ok(())
    }

    /// 範囲が重複しているか判定する。
    pub fn overlaps(&self, other: &Self) -> bool {
        !(self.end <= other.start || other.end <= self.start)
    }
}

/// 外部ビルド済みセクションのインジェスト情報。
#[derive(Debug, Clone)]
pub struct ExternalSectionIngest {
    /// 圧縮済みセクションデータ（再圧縮しない）。
    pub section_data: Vec<u8>,
    /// 圧縮アルゴリズム（セクション作成時の実際の方式）。
    pub compression: CompressionAlgorithm,
    /// 解凍後サイズ（バイト）。
    pub uncompressed_length: u64,
    /// オプション: 取り込み時に解凍検証を行うか。
    pub validate_uncompressed: bool,
    /// セクションタイプ。
    pub section_type: SectionType,
    /// 競合検出用のキー範囲。
    pub key_range: KeyRange,
}

impl ExternalSectionIngest {
    /// ヘルパーコンストラクタ。
    pub fn new(
        section_data: Vec<u8>,
        compression: CompressionAlgorithm,
        uncompressed_length: u64,
        section_type: SectionType,
        key_range: KeyRange,
    ) -> Self {
        Self {
            section_data,
            compression,
            uncompressed_length,
            validate_uncompressed: false,
            section_type,
            key_range,
        }
    }

    /// 検証フラグ付きのコンストラクタ。
    pub fn new_with_validation(
        section_data: Vec<u8>,
        compression: CompressionAlgorithm,
        uncompressed_length: u64,
        section_type: SectionType,
        key_range: KeyRange,
    ) -> Self {
        Self {
            section_data,
            compression,
            uncompressed_length,
            validate_uncompressed: true,
            section_type,
            key_range,
        }
    }
}
