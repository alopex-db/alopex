//! 統一データファイル形式で共有する定数とエラー型。
//!
//! ヘッダー/フッター/セクションのサイズやマジックナンバーは全プラットフォームで
//! 同一となるため、ここで集約して管理する。

pub mod backpressure;
pub mod footer;
pub mod header;
pub mod reader;
pub mod section;
pub mod value_separator;
#[cfg(not(target_arch = "wasm32"))]
pub mod writer;

pub use backpressure::{CompactionDebtTracker, WriteThrottleConfig};
pub use footer::FileFooter;
pub use header::{FileFlags, FileHeader};
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

    /// チェックサム不一致。
    #[error("Checksum mismatch: expected {expected:#x}, found {found:#x}")]
    ChecksumMismatch {
        /// 期待値。
        expected: u64,
        /// 実測値。
        found: u64,
    },
}
