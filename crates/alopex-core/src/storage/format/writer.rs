//! ファイルライターのビルダーパターン実装。
//!
//! ネイティブ環境向けに一時ファイル + アトミックリネームで安全にファイルを構築する。

#![cfg(not(target_arch = "wasm32"))]

use std::fs::{remove_file, rename, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::storage::checksum;
use crate::storage::compression;
use crate::storage::format::{
    ExternalSectionIngest, FileFlags, FileFooter, FileHeader, FileVersion, FormatError, KeyRange,
    SectionEntry, SectionIndex, SectionType, FOOTER_SIZE, HEADER_SIZE,
};

/// `.alopex` ファイルを書き出すライター。
pub struct AlopexFileWriter {
    output_path: PathBuf,
    temp_path: PathBuf,
    writer: BufWriter<std::fs::File>,
    current_offset: u64,
    section_entries: Vec<SectionEntry>,
    header: FileHeader,
    total_rows: u64,
    total_kv_bytes: u64,
    wal_sequence_number: u64,
    /// セクションIDとキー範囲の対応（外部インジェストや明示登録されたもの）。
    section_key_ranges: Vec<(SectionType, KeyRange, u32)>,
}

impl AlopexFileWriter {
    /// 新規ライターを作成し、ヘッダーを書き込む。
    pub fn new(
        output_path: PathBuf,
        version: FileVersion,
        flags: FileFlags,
    ) -> Result<Self, FormatError> {
        let temp_path = output_path.with_extension("alopex.tmp");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|_| FormatError::IncompleteWrite)?;
        let mut writer = BufWriter::new(file);

        let header = FileHeader::new(version, flags);
        let header_bytes = header.to_bytes();
        writer
            .write_all(&header_bytes)
            .map_err(|_| FormatError::IncompleteWrite)?;

        Ok(Self {
            output_path,
            temp_path,
            writer,
            current_offset: HEADER_SIZE as u64,
            section_entries: Vec::new(),
            header,
            total_rows: 0,
            total_kv_bytes: 0,
            wal_sequence_number: 0,
            section_key_ranges: Vec::new(),
        })
    }

    /// 統計情報を更新する。
    pub fn update_stats(&mut self, rows: u64, kv_bytes: u64) {
        self.total_rows = self.total_rows.saturating_add(rows);
        self.total_kv_bytes = self.total_kv_bytes.saturating_add(kv_bytes);
    }

    /// WALシーケンス番号を設定する。
    pub fn set_wal_sequence_number(&mut self, seq: u64) {
        self.wal_sequence_number = seq;
    }

    /// セクションを追加する（ヘッダーのデフォルト圧縮設定を使用するか無圧縮）。
    ///
    /// キー範囲が判明している場合は `add_section_with_compression_and_range` を使用し、
    /// 重複検証用に範囲を登録することを推奨。
    pub fn add_section(
        &mut self,
        section_type: SectionType,
        data: &[u8],
        compress: bool,
    ) -> Result<u32, FormatError> {
        let chosen_alg = if compress {
            // ヘッダーデフォルトの圧縮アルゴリズムをそのまま使用。
            self.header.compression_algorithm
        } else {
            // 非圧縮を明示。
            compression::CompressionAlgorithm::None
        };
        self.add_section_with_compression_and_range(section_type, data, chosen_alg, None)
    }

    /// 圧縮アルゴリズムを明示指定してセクションを追加する。
    pub fn add_section_with_compression(
        &mut self,
        section_type: SectionType,
        data: &[u8],
        compression_alg: compression::CompressionAlgorithm,
    ) -> Result<u32, FormatError> {
        self.add_section_with_compression_and_range(section_type, data, compression_alg, None)
    }

    /// 圧縮アルゴリズムとキー範囲を指定してセクションを追加する。
    ///
    /// キー範囲を渡すと自動で重複検証用に登録される。
    pub fn add_section_with_compression_and_range(
        &mut self,
        section_type: SectionType,
        data: &[u8],
        compression_alg: compression::CompressionAlgorithm,
        key_range: Option<KeyRange>,
    ) -> Result<u32, FormatError> {
        let compressed = compression::compress(data, compression_alg)?;
        let checksum = checksum::compute(&compressed, self.header.checksum_algorithm)?;

        let section_id = self.section_entries.len() as u32;
        let entry = SectionEntry::new(
            section_type,
            compression_alg,
            section_id,
            self.current_offset,
            compressed.len() as u64,
            data.len() as u64,
            checksum as u32,
        );

        self.writer
            .write_all(&compressed)
            .map_err(|_| FormatError::IncompleteWrite)?;
        self.current_offset = self.current_offset.saturating_add(compressed.len() as u64);

        self.section_entries.push(entry);
        if let Some(range) = key_range {
            self.register_section_key_range(section_type, section_id, range)?;
        }
        Ok(section_id)
    }

    /// メタデータセクションを追加する（現状はバイト列で受け取り、後続タスクでモデル対応予定）。
    pub fn add_metadata_section_bytes(&mut self, data: &[u8]) -> Result<u32, FormatError> {
        self.add_section(SectionType::Metadata, data, false)
    }

    /// 外部で圧縮済みのセクションを再圧縮せずに取り込む。
    pub fn ingest_external_section(
        &mut self,
        section: ExternalSectionIngest,
    ) -> Result<u32, FormatError> {
        section.key_range.validate()?;
        self.ensure_compression_supported(section.compression)?;
        if section.validate_uncompressed {
            let decompressed = compression::decompress(&section.section_data, section.compression)?;
            if decompressed.len() as u64 != section.uncompressed_length {
                return Err(FormatError::IngestValidationFailed {
                    message: "uncompressed length mismatch",
                });
            }
        }
        self.validate_key_range(section.section_type, &section.key_range)?;
        let checksum =
            checksum::compute(&section.section_data, self.header.checksum_algorithm)? as u32;
        let section_id = self.section_entries.len() as u32;
        let entry = SectionEntry::new(
            section.section_type,
            section.compression,
            section_id,
            self.current_offset,
            section.section_data.len() as u64,
            section.uncompressed_length,
            checksum,
        );

        self.writer
            .write_all(&section.section_data)
            .map_err(|_| FormatError::IncompleteWrite)?;
        self.current_offset = self
            .current_offset
            .saturating_add(section.section_data.len() as u64);

        self.section_entries.push(entry);
        self.section_key_ranges
            .push((section.section_type, section.key_range, section_id));
        Ok(section_id)
    }

    /// 既存セクション（通常書き込み済みなど）のキー範囲を手動登録する。
    pub fn register_section_key_range(
        &mut self,
        section_type: SectionType,
        section_id: u32,
        key_range: KeyRange,
    ) -> Result<(), FormatError> {
        key_range.validate()?;
        self.section_key_ranges
            .push((section_type, key_range, section_id));
        Ok(())
    }

    /// 既存の同一セクションタイプとキー範囲が重複しないか検証する。
    fn validate_key_range(
        &self,
        section_type: SectionType,
        range: &KeyRange,
    ) -> Result<(), FormatError> {
        range.validate()?;
        for (ty, existing, section_id) in &self.section_key_ranges {
            if *ty == section_type && range.overlaps(existing) {
                return Err(FormatError::KeyRangeOverlap {
                    start: range.start.clone(),
                    end: range.end.clone(),
                    section_id: *section_id,
                });
            }
        }
        Ok(())
    }

    /// 要求された圧縮アルゴリズムがビルドで有効かを確認する。
    fn ensure_compression_supported(
        &self,
        algorithm: compression::CompressionAlgorithm,
    ) -> Result<(), FormatError> {
        match algorithm {
            compression::CompressionAlgorithm::None | compression::CompressionAlgorithm::Snappy => {
                Ok(())
            }
            compression::CompressionAlgorithm::Zstd => {
                #[cfg(not(feature = "compression-zstd"))]
                {
                    Err(FormatError::UnsupportedCompression {
                        algorithm: algorithm as u8,
                    })
                }
                #[cfg(feature = "compression-zstd")]
                {
                    Ok(())
                }
            }
            compression::CompressionAlgorithm::Lz4 => {
                #[cfg(not(feature = "compression-lz4"))]
                {
                    Err(FormatError::UnsupportedCompression {
                        algorithm: algorithm as u8,
                    })
                }
                #[cfg(feature = "compression-lz4")]
                {
                    Ok(())
                }
            }
        }
    }

    /// ファイルをファイナライズし、フッターを書き込んでアトミックリネームする。
    pub fn finalize(mut self) -> Result<(), FormatError> {
        // セクションインデックスを書き込み
        let mut section_index = SectionIndex::new();
        for entry in &self.section_entries {
            section_index.add_entry(*entry);
        }
        let section_index_bytes = section_index.to_bytes();
        let section_index_offset = self.current_offset;
        self.writer
            .write_all(&section_index_bytes)
            .map_err(|_| FormatError::IncompleteWrite)?;
        self.current_offset = self
            .current_offset
            .saturating_add(section_index_bytes.len() as u64);

        // メタデータセクションのオフセットを検索（最初のMetadataを採用）
        let metadata_section_offset = self
            .section_entries
            .iter()
            .find(|e| e.section_type == SectionType::Metadata)
            .map(|e| e.offset)
            .unwrap_or(0);

        // データセクション数（Metadata以外）
        let data_section_count = self
            .section_entries
            .iter()
            .filter(|e| e.section_type != SectionType::Metadata)
            .count() as u32;

        // フッター作成
        let file_size = self.current_offset.saturating_add(FOOTER_SIZE as u64);
        let mut footer = FileFooter::new(
            section_index_offset,
            metadata_section_offset,
            data_section_count,
            self.total_rows,
            self.total_kv_bytes,
            file_size,
            self.wal_sequence_number,
        );
        footer.compute_and_set_checksum();
        let footer_bytes = footer.to_bytes();

        // フッター書き込み
        self.writer
            .write_all(&footer_bytes)
            .map_err(|_| FormatError::IncompleteWrite)?;
        self.writer
            .flush()
            .map_err(|_| FormatError::IncompleteWrite)?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|_| FormatError::IncompleteWrite)?;

        // アトミックリネーム
        rename(&self.temp_path, &self.output_path).map_err(|_| FormatError::IncompleteWrite)?;
        Ok(())
    }

    /// 中断し、一時ファイルを削除する。
    pub fn abort(self) -> Result<(), FormatError> {
        remove_file(&self.temp_path).map_err(|_| FormatError::IncompleteWrite)
    }
}
