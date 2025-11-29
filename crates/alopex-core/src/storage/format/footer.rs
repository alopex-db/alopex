//! ファイルフッター定義とシリアライズ/デシリアライズ処理。

use std::convert::TryInto;
use std::mem;

use crate::storage::format::{FormatError, FOOTER_SIZE, REVERSE_MAGIC};

/// ファイルフッター（64バイト固定）。
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileFooter {
    /// セクションインデックスのファイル内オフセット。
    pub section_index_offset: u64,
    /// メタデータセクションのオフセット。
    pub metadata_section_offset: u64,
    /// データセクション数。
    pub data_section_count: u32,
    /// アライメント用パディング。
    pub _padding: u32,
    /// 総行数。
    pub total_rows: u64,
    /// 総KVバイト数。
    pub total_kv_bytes: u64,
    /// ファイルサイズ。
    pub file_size: u64,
    /// WALのシーケンス番号。
    pub wal_sequence_number: u64,
    /// フッター自身のチェックサム（offset 0-55 に対して計算）。
    pub footer_checksum: u32,
    /// 逆マジックナンバー ("XPLA")。
    pub reverse_magic: [u8; 4],
}

const _: () = assert!(mem::size_of::<FileFooter>() == FOOTER_SIZE);

impl FileFooter {
    /// フッターサイズ（バイト）。
    pub const SIZE: usize = FOOTER_SIZE;

    /// 新しいフッターを生成する。チェックサムは別途 [`Self::compute_and_set_checksum`] で計算する。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        section_index_offset: u64,
        metadata_section_offset: u64,
        data_section_count: u32,
        total_rows: u64,
        total_kv_bytes: u64,
        file_size: u64,
        wal_sequence_number: u64,
    ) -> Self {
        Self {
            section_index_offset,
            metadata_section_offset,
            data_section_count,
            _padding: 0,
            total_rows,
            total_kv_bytes,
            file_size,
            wal_sequence_number,
            footer_checksum: 0,
            reverse_magic: REVERSE_MAGIC,
        }
    }

    /// バイト列からフッターを復元する。
    pub fn from_bytes(bytes: &[u8; FOOTER_SIZE]) -> Result<Self, FormatError> {
        let mut reverse_magic = [0u8; 4];
        reverse_magic.copy_from_slice(&bytes[60..64]);
        let footer = Self {
            section_index_offset: u64::from_le_bytes(
                bytes[0..8].try_into().expect("fixed slice length"),
            ),
            metadata_section_offset: u64::from_le_bytes(
                bytes[8..16].try_into().expect("fixed slice length"),
            ),
            data_section_count: u32::from_le_bytes(
                bytes[16..20].try_into().expect("fixed slice length"),
            ),
            _padding: u32::from_le_bytes(bytes[20..24].try_into().expect("fixed slice length")),
            total_rows: u64::from_le_bytes(bytes[24..32].try_into().expect("fixed slice length")),
            total_kv_bytes: u64::from_le_bytes(
                bytes[32..40].try_into().expect("fixed slice length"),
            ),
            file_size: u64::from_le_bytes(bytes[40..48].try_into().expect("fixed slice length")),
            wal_sequence_number: u64::from_le_bytes(
                bytes[48..56].try_into().expect("fixed slice length"),
            ),
            footer_checksum: u32::from_le_bytes(
                bytes[56..60].try_into().expect("fixed slice length"),
            ),
            reverse_magic,
        };
        footer.validate()?;
        Ok(footer)
    }

    /// フッターをバイト列にシリアライズする。
    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut bytes = [0u8; FOOTER_SIZE];
        bytes[0..8].copy_from_slice(&self.section_index_offset.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.metadata_section_offset.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.data_section_count.to_le_bytes());
        bytes[20..24].copy_from_slice(&self._padding.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.total_rows.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.total_kv_bytes.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.file_size.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.wal_sequence_number.to_le_bytes());
        bytes[56..60].copy_from_slice(&self.footer_checksum.to_le_bytes());
        bytes[60..64].copy_from_slice(&self.reverse_magic);
        bytes
    }

    /// フッターチェックサムを計算して設定する。
    pub fn compute_and_set_checksum(&mut self) {
        let mut bytes = self.to_bytes();
        // チェックサムフィールド自身は計算対象外。
        bytes[56..60].fill(0);
        let checksum = crc32fast::hash(&bytes[..56]);
        self.footer_checksum = checksum;
    }

    /// 逆マジックとチェックサムを検証する。
    pub fn validate(&self) -> Result<(), FormatError> {
        if self.reverse_magic != REVERSE_MAGIC {
            return Err(FormatError::IncompleteWrite);
        }

        let mut bytes = self.to_bytes();
        bytes[56..60].fill(0);
        let found = crc32fast::hash(&bytes[..56]) as u64;
        let expected = self.footer_checksum as u64;
        if expected == found {
            Ok(())
        } else {
            Err(FormatError::ChecksumMismatch { expected, found })
        }
    }
}
