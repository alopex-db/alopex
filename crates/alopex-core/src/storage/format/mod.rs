//! # Alopex Unified Data File Format
//!
//! Alopex DB が共通で読み書きする `.alopex` バイナリファイル形式の
//! 定数・型・エラーをまとめ、Native/WASM 両ターゲットで同一フォーマットを提供する。
//!
//! ## 主な構造体
//! - [`FileHeader`], [`FileFooter`] — マジック・バージョン・統計・チェックサムを保持
//! - [`SectionEntry`], [`SectionIndex`] — セクションメタデータ（圧縮方式・オフセット・長さ）
//! - [`AlopexFileWriter`] (native) — ヘッダー書き込み → セクション追加 → フッター確定 → アトミックリネーム
//! - [`AlopexFileReader`] (native/wasm) — ヘッダー/フッター/インデックスを検証し、圧縮後データのチェックサムを照合
//! - [`ValueSeparator`] — 大型値を専用セクションに分離するユーティリティ
//!
//! ## フォーマット特性
//! - バイトオーダー: Little Endian 固定
//! - セクションデータのチェックサムは「圧縮後バイト列」に対して計算
//! - バージョン互換性: `file.version` が [`FileVersion::CURRENT`] を超える場合、 [`FormatError::IncompatibleVersion`] を返す
//! - 圧縮: None / Snappy（デフォルト）/ Zstd / LZ4 （feature で有効化）
//! - ターゲット: x86_64 / ARM64 / WASM（WASMは Snappy/None のみをデフォルトサポート）
//!
//! ## 典型的な書き込みフロー（native）
//! ```rust,no_run
//! use alopex_core::storage::format::{
//!     AlopexFileWriter, FileFlags, FileVersion, SectionType, FormatError,
//! };
//!
//! fn write_file() -> Result<(), FormatError> {
//!     let mut writer = AlopexFileWriter::new(
//!         "example.alopex".into(),
//!         FileVersion::CURRENT,
//!         FileFlags(0),
//!     )?;
//!     // 非圧縮メタデータ
//!     writer.add_section(SectionType::Metadata, b"meta-v1", false)?;
//!     // デフォルト圧縮（Snappy）でSSTableを書き込む
//!     writer.add_section(SectionType::SSTable, b"sstable-bytes", true)?;
//!     writer.finalize()?;
//!     Ok(())
//! }
//! ```
//!
//! ## 典型的な読み取りフロー（native）
//! ```rust,no_run
//! use alopex_core::storage::format::{AlopexFileReader, FileReader, FileSource, FormatError};
//!
//! fn read_and_validate() -> Result<(), FormatError> {
//!     let reader = AlopexFileReader::open(FileSource::Path("example.alopex".into()))?;
//!     reader.validate_all()?; // ヘッダー/フッター/各セクションの整合性を検証
//!     let meta = reader.read_section(0)?; // 解凍済みバイト列
//!     let data_raw = reader.read_section_raw(1)?; // 圧縮後バイト列
//!     assert!(!meta.is_empty());
//!     assert!(!data_raw.is_empty());
//!     Ok(())
//! }
//! ```
//!
//! ## WASM 読み取りフローの留意点
//! - `full_load_threshold_bytes` 未満のサイズは全体をバッファにロード。
//! - 閾値超過時は IndexedDB などの範囲ローダーでストリーミング読み取り。
//! - デフォルトで Snappy/None のみをサポートし、Zstd/LZ4 はビルドサイズ・メモリ上限の理由で無効化。

pub mod backpressure;
pub mod footer;
pub mod header;
pub mod ingest;
pub mod models;
pub mod reader;
pub mod section;
pub mod value_separator;
#[cfg(not(target_arch = "wasm32"))]
pub mod writer;

pub use backpressure::{CompactionDebtTracker, WriteThrottleConfig};
pub use footer::FileFooter;
pub use header::{FileFlags, FileHeader};
pub use ingest::{ExternalSectionIngest, KeyRange};
pub use models::{
    bincode_config, ColumnDefinition, EphemeralDataGcConfig, IndexDefinition, IntentEntry,
    IntentSection, IntentType, LockEntry, LockSection, LockType, Metadata, RaftEntryType,
    RaftLogEntry, RaftLogSection, RangeMetadata, TableSchema, VectorIndexMetadata, VectorMetric,
};
#[cfg(not(target_arch = "wasm32"))]
pub use reader::AlopexFileReader;
#[cfg(target_arch = "wasm32")]
pub use reader::{AlopexFileReader, WasmReaderConfig};
pub use reader::{FileReader, FileSource, PrefetchFuture};
pub use section::{SectionEntry, SectionIndex, SectionType};
pub use value_separator::{LargeValuePointer, ValueRef, ValueSeparationConfig, ValueSeparator};
#[cfg(not(target_arch = "wasm32"))]
pub use writer::AlopexFileWriter;

use thiserror::Error;

/// ファイル先頭のマジックナンバー ("ALPX")。
pub const MAGIC: [u8; 4] = *b"ALPX";
/// フッター末尾の逆マジックナンバー ("XPLA")。
pub const REVERSE_MAGIC: [u8; 4] = *b"XPLA";

/// ヘッダー領域の固定サイズ（バイト数）。
pub const HEADER_SIZE: usize = 64;
/// フッター領域の固定サイズ（バイト数）。
pub const FOOTER_SIZE: usize = 64;
/// SectionEntry（メタデータ1件）の固定サイズ（バイト数）。
pub const SECTION_ENTRY_SIZE: usize = 40;

/// 形式のメジャー/マイナー/パッチバージョン（初期値: v0.1.0）。
pub const VERSION_MAJOR: u16 = 0;
/// 現行マイナーバージョン。
pub const VERSION_MINOR: u16 = 1;
/// 現行パッチバージョン。
pub const VERSION_PATCH: u16 = 0;

/// ファイルバージョン（6バイト）。
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileVersion {
    /// メジャーバージョン。
    pub major: u16,
    /// マイナーバージョン。
    pub minor: u16,
    /// パッチバージョン。
    pub patch: u16,
}

impl FileVersion {
    /// 定数からバージョンを生成するヘルパー。
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// 現行バージョン定数。
    pub const CURRENT: Self = Self::new(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);
}

/// 統一データファイル形式のエラー型。
#[repr(C)]
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FormatError {
    /// マジックナンバーが一致しない。
    #[error("Invalid magic number: expected ALPX, found {found:?}")]
    InvalidMagic {
        /// ファイルから読み取ったマジックナンバー。
        found: [u8; 4],
    },

    /// ファイルバージョンがリーダーより新しく互換でない。
    #[error(
        "Incompatible version: file version {file:?} is newer than reader version {reader:?}. Please upgrade Alopex DB."
    )]
    IncompatibleVersion {
        /// ファイルに記録されたバージョン。
        file: FileVersion,
        /// リーダー（実行バイナリ）がサポートするバージョン。
        reader: FileVersion,
    },

    /// セクションのチェックサム不一致。
    #[error("Section {section_id} is corrupted: expected checksum {expected:#x}, found {found:#x}. The section may be damaged.")]
    CorruptedSection {
        /// 対象セクションID。
        section_id: u32,
        /// 期待されるチェックサム値。
        expected: u32,
        /// 実際に計算されたチェックサム値。
        found: u32,
    },

    /// フッターが欠損/不正で書き込みが完了していない。
    #[error("File appears to be incomplete (missing or invalid footer). This may indicate a crash during write.")]
    IncompleteWrite,

    /// External ingest 時のキー範囲重複。
    #[error("Key range [{start:?}, {end:?}) overlaps with existing section {section_id}")]
    KeyRangeOverlap {
        /// 追加しようとした開始キー（包含）。
        start: Vec<u8>,
        /// 追加しようとした終了キー（排他）。
        end: Vec<u8>,
        /// 衝突した既存セクションのID。
        section_id: u32,
    },

    /// ビルドでサポートしていない圧縮アルゴリズムが要求された。
    #[error("Compression algorithm {algorithm} is not supported in this build")]
    UnsupportedCompression {
        /// 要求された圧縮アルゴリズムの識別子。
        algorithm: u8,
    },

    /// 圧縮処理に失敗した。
    #[error("Compression failed for algorithm {algorithm}")]
    CompressionFailed {
        /// 対象アルゴリズム。
        algorithm: u8,
    },

    /// 解凍処理に失敗した。
    #[error("Decompression failed for algorithm {algorithm}")]
    DecompressionFailed {
        /// 対象アルゴリズム。
        algorithm: u8,
    },

    /// 未サポートのチェックサムアルゴリズム。
    #[error("Checksum algorithm {algorithm} is not supported in this build")]
    UnsupportedChecksum {
        /// 対象アルゴリズム。
        algorithm: u8,
    },

    /// ポインタ情報が不正または未解決。
    #[error("Invalid pointer: section_id={section_id}, offset={offset}, length={length}")]
    InvalidPointer {
        /// セクションID（未解決時は0）。
        section_id: u32,
        /// オフセット。
        offset: u64,
        /// 長さ。
        length: u64,
    },

    /// チェックサム不一致。
    #[error("Checksum mismatch: expected {expected:#x}, found {found:#x}")]
    ChecksumMismatch {
        /// 期待値。
        expected: u64,
        /// 実測値。
        found: u64,
    },

    /// キー範囲が不正（start >= end など）。
    #[error("Invalid key range: start={start:?}, end={end:?}")]
    InvalidKeyRange {
        /// 範囲開始（包含）。
        start: Vec<u8>,
        /// 範囲終了（排他）。
        end: Vec<u8>,
    },

    /// 外部セクションの検証に失敗。
    #[error("Invalid external section: {message}")]
    IngestValidationFailed {
        /// 検証失敗理由。
        message: &'static str,
    },
}
