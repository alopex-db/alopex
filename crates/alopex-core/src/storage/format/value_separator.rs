//! 大型値の分離と参照管理。

use crate::storage::checksum;
use crate::storage::format::{FormatError, SectionEntry, SectionType};

/// 大型値分離の設定。
#[derive(Debug, Clone)]
pub struct ValueSeparationConfig {
    /// このサイズを超える値は分離対象（バイト）。
    pub threshold_bytes: usize,
    /// 分離した値を圧縮するか。
    pub compress_large_values: bool,
}

impl Default for ValueSeparationConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 1024, // 1KB
            compress_large_values: false,
        }
    }
}

/// 値の参照方法。
#[derive(Debug, Clone)]
pub enum ValueRef {
    /// インライン値。
    Inline(Vec<u8>),
    /// 分離された大型値への参照。
    Separated(LargeValuePointer),
}

/// 大型値セクション内の位置を示すポインタ。
#[derive(Debug, Clone, Copy)]
pub struct LargeValuePointer {
    /// 参照先セクションID。
    pub section_id: u32,
    /// セクション内のオフセット。
    pub offset: u64,
    /// 値の長さ。
    pub length: u64,
    /// 値のチェックサム（CRC32）。
    pub checksum: u32,
}

/// 大型値セクションを構築・参照するヘルパー。
#[derive(Debug, Default)]
pub struct ValueSeparator {
    large_values: Vec<(Vec<u8>, u32)>, // (data, checksum)
    raw_len: u64,
}

impl ValueSeparator {
    /// 値を分離するか判定し、参照を返す。
    pub fn process_value(&mut self, value: Vec<u8>, config: &ValueSeparationConfig) -> ValueRef {
        if value.len() <= config.threshold_bytes {
            ValueRef::Inline(value)
        } else {
            let checksum =
                checksum::compute(&value, checksum::ChecksumAlgorithm::Crc32).unwrap_or(0) as u32;
            let offset = self.raw_len;
            let length = value.len() as u64;
            // rawデータでは8バイトの長さプレフィックスを付与する。
            self.raw_len = self.raw_len.saturating_add(8 + length);
            self.large_values.push((value, checksum));
            ValueRef::Separated(LargeValuePointer {
                section_id: 0,
                offset,
                length,
                checksum,
            })
        }
    }

    /// これまで蓄積した大型値を1つのLargeValueセクションとしてバイト列に構築し、
    /// セクションエントリとポインタを返す。
    pub fn build_large_value_section(
        &self,
        section_id: u32,
        config: &ValueSeparationConfig,
    ) -> Result<(SectionEntry, Vec<u8>, Vec<LargeValuePointer>), FormatError> {
        let mut offsets = Vec::with_capacity(self.large_values.len());
        let mut raw = Vec::new();

        for (value, checksum) in &self.large_values {
            let offset = raw.len() as u64;
            raw.extend_from_slice(&value.len().to_le_bytes());
            raw.extend_from_slice(value);
            offsets.push((offset, *checksum, value.len() as u64));
        }

        let algorithm = if config.compress_large_values {
            crate::storage::compression::CompressionAlgorithm::Snappy
        } else {
            crate::storage::compression::CompressionAlgorithm::None
        };
        let compressed = crate::storage::compression::compress(&raw, algorithm)?;
        let section_checksum =
            checksum::compute(&compressed, checksum::ChecksumAlgorithm::Crc32)? as u32;

        let entry = SectionEntry::new(
            SectionType::LargeValue,
            algorithm,
            section_id,
            0, // 呼び出し側で埋める
            compressed.len() as u64,
            raw.len() as u64,
            section_checksum,
        );

        let pointers = offsets
            .into_iter()
            .map(|(offset, checksum, len)| LargeValuePointer {
                section_id,
                offset,
                length: len,
                checksum,
            })
            .collect();

        Ok((entry, compressed, pointers))
    }

    /// 生成済みのポインタを呼び出し側が保持するValueRefに反映するヘルパー。
    pub fn hydrate_pointers(&self, pointers: &[LargeValuePointer], refs: &mut [ValueRef]) {
        for value_ref in refs {
            if let ValueRef::Separated(ptr) = value_ref {
                if let Some(new_ptr) = pointers.iter().find(|p| {
                    p.offset == ptr.offset && p.length == ptr.length && p.checksum == ptr.checksum
                }) {
                    *ptr = *new_ptr;
                }
            }
        }
    }

    /// ポインタを解決して値を返す。呼び出し側でセクションデータを提供する。
    pub fn resolve_pointer(
        &self,
        pointer: &LargeValuePointer,
        section_data: &[u8],
    ) -> Result<Vec<u8>, FormatError> {
        let mut cursor = pointer.offset as usize;
        if cursor + 8 > section_data.len() {
            return Err(FormatError::IncompleteWrite);
        }
        let len = u64::from_le_bytes(
            section_data[cursor..cursor + 8]
                .try_into()
                .expect("slice length"),
        ) as usize;
        cursor += 8;
        if cursor + len > section_data.len() {
            return Err(FormatError::IncompleteWrite);
        }
        let data = section_data[cursor..cursor + len].to_vec();
        checksum::verify(
            &data,
            checksum::ChecksumAlgorithm::Crc32,
            pointer.checksum as u64,
        )?;
        Ok(data)
    }
}
