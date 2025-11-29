//! セクションエントリとセクションインデックスの定義。

use std::convert::TryInto;
use std::mem;

use crate::storage::compression::CompressionAlgorithm;
use crate::storage::format::{FormatError, SECTION_ENTRY_SIZE};

/// セクションタイプ。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionType {
    /// メタデータセクション。
    Metadata = 0x00,
    /// SSTableセクション。
    SSTable = 0x01,
    /// ベクターインデックスセクション。
    VectorIndex = 0x02,
    /// カラムナーセグメントセクション。
    ColumnarSegment = 0x03,
    /// 大型値セクション。
    LargeValue = 0x04,
    /// インテントセクション。
    Intent = 0x05,
    /// ロックセクション。
    Lock = 0x06,
    /// Raftログセクション。
    RaftLog = 0x07,
}

impl TryFrom<u8> for SectionType {
    type Error = FormatError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Metadata),
            0x01 => Ok(Self::SSTable),
            0x02 => Ok(Self::VectorIndex),
            0x03 => Ok(Self::ColumnarSegment),
            0x04 => Ok(Self::LargeValue),
            0x05 => Ok(Self::Intent),
            0x06 => Ok(Self::Lock),
            0x07 => Ok(Self::RaftLog),
            _ => Err(FormatError::IncompleteWrite),
        }
    }
}

/// セクションエントリ（40バイト）。
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionEntry {
    /// セクションタイプ。
    pub section_type: SectionType,
    /// 圧縮アルゴリズム。
    pub compression: CompressionAlgorithm,
    /// パディング（常にゼロ）。
    pub _padding: [u8; 2],
    /// セクションID。
    pub section_id: u32,
    /// ファイル内オフセット。
    pub offset: u64,
    /// 圧縮後サイズ（ファイル上のバイト数）。
    pub compressed_length: u64,
    /// 解凍後サイズ。
    pub uncompressed_length: u64,
    /// 圧縮後データに対するチェックサム。
    pub checksum: u32,
    /// パディング（常にゼロ）。
    pub _padding2: [u8; 4],
}

const _: () = assert!(mem::size_of::<SectionEntry>() == SECTION_ENTRY_SIZE);

impl SectionEntry {
    /// エントリサイズ（バイト）。
    pub const SIZE: usize = SECTION_ENTRY_SIZE;

    /// 新規エントリを生成する。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        section_type: SectionType,
        compression: CompressionAlgorithm,
        section_id: u32,
        offset: u64,
        compressed_length: u64,
        uncompressed_length: u64,
        checksum: u32,
    ) -> Self {
        Self {
            section_type,
            compression,
            _padding: [0u8; 2],
            section_id,
            offset,
            compressed_length,
            uncompressed_length,
            checksum,
            _padding2: [0u8; 4],
        }
    }
}

/// セクションインデックス。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionIndex {
    /// セクション数。
    pub count: u32,
    /// エントリ一覧。
    pub entries: Vec<SectionEntry>,
}

impl Default for SectionIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl SectionIndex {
    /// エントリの固定サイズ。
    pub const ENTRY_SIZE: usize = SECTION_ENTRY_SIZE;

    /// 空のインデックスを作成する。
    pub fn new() -> Self {
        Self {
            count: 0,
            entries: Vec::new(),
        }
    }

    /// エントリを追加する。
    pub fn add_entry(&mut self, entry: SectionEntry) {
        self.entries.push(entry);
        self.count = self.entries.len() as u32;
    }

    /// タイプでフィルタする。
    pub fn filter_by_type(&self, section_type: SectionType) -> Vec<&SectionEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.section_type == section_type)
            .collect()
    }

    /// IDで検索する。
    pub fn find_by_id(&self, section_id: u32) -> Option<&SectionEntry> {
        self.entries
            .iter()
            .find(|entry| entry.section_id == section_id)
    }

    /// バイト列からデシリアライズする。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < 4 {
            return Err(FormatError::IncompleteWrite);
        }

        let count = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .expect("fixed slice length for count"),
        );
        let expected_size = 4 + count as usize * SECTION_ENTRY_SIZE;
        if bytes.len() < expected_size {
            return Err(FormatError::IncompleteWrite);
        }

        let mut entries = Vec::with_capacity(count as usize);
        let mut cursor = 4;
        for _ in 0..count {
            let section_type = SectionType::try_from(bytes[cursor])?;
            let compression = parse_compression(bytes[cursor + 1])?;

            let padding = [bytes[cursor + 2], bytes[cursor + 3]];
            if padding != [0, 0] {
                return Err(FormatError::IncompleteWrite);
            }

            let section_id = u32::from_le_bytes(
                bytes[cursor + 4..cursor + 8]
                    .try_into()
                    .expect("fixed slice length for section_id"),
            );
            let offset = u64::from_le_bytes(
                bytes[cursor + 8..cursor + 16]
                    .try_into()
                    .expect("fixed slice length for offset"),
            );
            let compressed_length = u64::from_le_bytes(
                bytes[cursor + 16..cursor + 24]
                    .try_into()
                    .expect("fixed slice length for compressed_length"),
            );
            let uncompressed_length = u64::from_le_bytes(
                bytes[cursor + 24..cursor + 32]
                    .try_into()
                    .expect("fixed slice length for uncompressed_length"),
            );
            let checksum = u32::from_le_bytes(
                bytes[cursor + 32..cursor + 36]
                    .try_into()
                    .expect("fixed slice length for checksum"),
            );
            let padding2: [u8; 4] = bytes[cursor + 36..cursor + 40]
                .try_into()
                .expect("fixed slice length for padding2");
            if padding2 != [0; 4] {
                return Err(FormatError::IncompleteWrite);
            }

            entries.push(SectionEntry {
                section_type,
                compression,
                _padding: [0u8; 2],
                section_id,
                offset,
                compressed_length,
                uncompressed_length,
                checksum,
                _padding2: [0u8; 4],
            });
            cursor += SECTION_ENTRY_SIZE;
        }

        Ok(Self { count, entries })
    }

    /// バイト列へシリアライズする。
    pub fn to_bytes(&self) -> Vec<u8> {
        let count = self.entries.len() as u32;
        let mut bytes = Vec::with_capacity(self.serialized_size());
        bytes.extend_from_slice(&count.to_le_bytes());

        for entry in &self.entries {
            bytes.push(entry.section_type as u8);
            bytes.push(entry.compression as u8);
            bytes.extend_from_slice(&[0u8; 2]);
            bytes.extend_from_slice(&entry.section_id.to_le_bytes());
            bytes.extend_from_slice(&entry.offset.to_le_bytes());
            bytes.extend_from_slice(&entry.compressed_length.to_le_bytes());
            bytes.extend_from_slice(&entry.uncompressed_length.to_le_bytes());
            bytes.extend_from_slice(&entry.checksum.to_le_bytes());
            bytes.extend_from_slice(&[0u8; 4]);
        }

        bytes
    }

    /// シリアライズ後のサイズ（バイト）を返す。
    pub fn serialized_size(&self) -> usize {
        4 + self.entries.len() * SECTION_ENTRY_SIZE
    }
}

fn parse_compression(byte: u8) -> Result<CompressionAlgorithm, FormatError> {
    match byte {
        0 => Ok(CompressionAlgorithm::None),
        1 => Ok(CompressionAlgorithm::Snappy),
        2 => Ok(CompressionAlgorithm::Zstd),
        3 => Ok(CompressionAlgorithm::Lz4),
        other => Err(FormatError::UnsupportedCompression { algorithm: other }),
    }
}
