//! ファイルリーダーの共通トレイトと入力ソース定義。
//!
//! この段階ではインターフェースのみを提供し、各プラットフォームごとの実装は
//! 別タスク (3.1 Native / 3.2 WASM) で追加する。

use std::pin::Pin;

use crate::storage::checksum;
use crate::storage::compression;
use crate::storage::format::{
    FileFooter, FileHeader, FileVersion, FormatError, SectionEntry, SectionIndex, FOOTER_SIZE,
    HEADER_SIZE,
};

#[cfg(not(target_arch = "wasm32"))]
use memmap2::Mmap;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};

#[cfg(target_arch = "wasm32")]
use std::vec::Vec;

/// ファイルソース抽象化。
pub enum FileSource {
    /// ファイルパス（Native用）。
    #[cfg(not(target_arch = "wasm32"))]
    Path(PathBuf),
    /// バイトバッファ（WASM用）。
    #[cfg(target_arch = "wasm32")]
    Buffer(Vec<u8>),
    /// IndexedDBキー（WASM + feature）。
    #[cfg(all(target_arch = "wasm32", feature = "wasm-indexeddb"))]
    IndexedDb {
        db_name: String,
        key: String,
        /// ファイル全体のサイズ（バイト）。
        length: u64,
        /// 範囲読み込みローダー。
        loader: Box<dyn RangeLoader>,
    },
}

/// prefetch_sections の戻り値に用いるFuture型。
#[cfg(not(target_arch = "wasm32"))]
pub type PrefetchFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<(), FormatError>> + Send + 'a>>;
/// prefetch_sections の戻り値に用いるFuture型（WASM版、Send制約なし）。
#[cfg(target_arch = "wasm32")]
pub type PrefetchFuture<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<(), FormatError>> + 'a>>;

/// WASM向けの範囲読み込みローダー。
#[cfg(target_arch = "wasm32")]
pub trait RangeLoader: Send + Sync {
    /// `[offset, offset+length)` のバイト列を取得する。
    fn load_range(&self, offset: u64, length: u64) -> Result<Vec<u8>, FormatError>;
}

/// メモリ上のバッファから範囲読み込みするデフォルト実装。
#[cfg(target_arch = "wasm32")]
pub struct BufferRangeLoader {
    data: Vec<u8>,
}

#[cfg(target_arch = "wasm32")]
impl BufferRangeLoader {
    /// 新規バッファローダーを作成する。
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

#[cfg(target_arch = "wasm32")]
impl RangeLoader for BufferRangeLoader {
    fn load_range(&self, offset: u64, length: u64) -> Result<Vec<u8>, FormatError> {
        let offset = offset as usize;
        let length = length as usize;
        let end = offset
            .checked_add(length)
            .ok_or(FormatError::IncompleteWrite)?;
        if end > self.data.len() {
            return Err(FormatError::IncompleteWrite);
        }
        Ok(self.data[offset..end].to_vec())
    }
}

/// プラットフォーム共通のファイルリーダートレイト。
pub trait FileReader {
    /// ファイルを開き、ヘッダー/フッター/セクションインデックスを初期化する。
    fn open(source: FileSource) -> Result<Self, FormatError>
    where
        Self: Sized;

    /// ヘッダーへの参照を返す。
    fn header(&self) -> &FileHeader;

    /// フッターへの参照を返す。
    fn footer(&self) -> &FileFooter;

    /// セクションインデックスへの参照を返す。
    fn section_index(&self) -> &SectionIndex;

    /// 指定セクションを解凍済みバイト列で読み取る。
    fn read_section(&self, section_id: u32) -> Result<Vec<u8>, FormatError>;

    /// 指定セクションを圧縮状態のまま読み取る。
    fn read_section_raw(&self, section_id: u32) -> Result<Vec<u8>, FormatError>;

    /// 指定セクションのチェックサムを検証する（圧縮後データを対象）。
    fn validate_section(&self, section_id: u32) -> Result<(), FormatError>;

    /// 全セクションの整合性を検証する。
    fn validate_all(&self) -> Result<(), FormatError>;

    /// 指定セクションを事前読み込みする（WASMではIndexedDBからの範囲読み込みを想定）。
    fn prefetch_sections<'a>(&'a self, section_ids: &'a [u32]) -> PrefetchFuture<'a>;
}

/// ネイティブ向けファイルリーダー（mmap）。
#[cfg(not(target_arch = "wasm32"))]
pub struct AlopexFileReader {
    mmap: Mmap,
    header: FileHeader,
    footer: FileFooter,
    section_index: SectionIndex,
}

#[cfg(not(target_arch = "wasm32"))]
impl AlopexFileReader {
    fn map_file(path: &Path) -> Result<Mmap, FormatError> {
        let file = File::open(path).map_err(|_| FormatError::IncompleteWrite)?;
        unsafe { Mmap::map(&file).map_err(|_| FormatError::IncompleteWrite) }
    }

    fn read_footer(mmap: &Mmap) -> Result<FileFooter, FormatError> {
        if mmap.len() < FOOTER_SIZE {
            return Err(FormatError::IncompleteWrite);
        }
        let start = mmap.len() - FOOTER_SIZE;
        let mut buf = [0u8; FOOTER_SIZE];
        buf.copy_from_slice(&mmap[start..]);
        FileFooter::from_bytes(&buf)
    }

    fn read_header(mmap: &Mmap) -> Result<FileHeader, FormatError> {
        if mmap.len() < HEADER_SIZE {
            return Err(FormatError::IncompleteWrite);
        }
        let mut buf = [0u8; HEADER_SIZE];
        buf.copy_from_slice(&mmap[..HEADER_SIZE]);
        let header = FileHeader::from_bytes(&buf)?;
        header.check_compatibility(&FileVersion::CURRENT)?;
        Ok(header)
    }

    fn read_section_index(mmap: &Mmap, footer: &FileFooter) -> Result<SectionIndex, FormatError> {
        let offset = footer.section_index_offset as usize;
        if offset >= mmap.len() {
            return Err(FormatError::IncompleteWrite);
        }
        // まずcountを読むために最低4バイトが必要。
        if mmap.len() < offset + 4 {
            return Err(FormatError::IncompleteWrite);
        }
        let count = u32::from_le_bytes(
            mmap[offset..offset + 4]
                .try_into()
                .expect("slice length checked"),
        );
        let expected = 4usize + count as usize * SectionEntry::SIZE;
        if mmap.len() < offset + expected {
            return Err(FormatError::IncompleteWrite);
        }
        SectionIndex::from_bytes(&mmap[offset..offset + expected])
    }

    fn entry(&self, section_id: u32) -> Result<&SectionEntry, FormatError> {
        self.section_index
            .find_by_id(section_id)
            .ok_or(FormatError::IncompleteWrite)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl FileReader for AlopexFileReader {
    fn open(source: FileSource) -> Result<Self, FormatError> {
        let path = match source {
            FileSource::Path(p) => p,
        };
        let mmap = Self::map_file(&path)?;
        let footer = Self::read_footer(&mmap)?;
        let section_index = Self::read_section_index(&mmap, &footer)?;
        let header = Self::read_header(&mmap)?;
        Ok(Self {
            mmap,
            header,
            footer,
            section_index,
        })
    }

    fn header(&self) -> &FileHeader {
        &self.header
    }

    fn footer(&self) -> &FileFooter {
        &self.footer
    }

    fn section_index(&self) -> &SectionIndex {
        &self.section_index
    }

    fn read_section(&self, section_id: u32) -> Result<Vec<u8>, FormatError> {
        let entry = self.entry(section_id)?;
        let raw = self.read_section_raw(section_id)?;
        if raw.len() as u64 != entry.compressed_length {
            return Err(FormatError::IncompleteWrite);
        }
        checksum::verify(&raw, self.header.checksum_algorithm, entry.checksum as u64)?;
        let decompressed = compression::decompress(&raw, entry.compression)?;
        if decompressed.len() as u64 != entry.uncompressed_length {
            return Err(FormatError::IncompleteWrite);
        }
        Ok(decompressed)
    }

    fn read_section_raw(&self, section_id: u32) -> Result<Vec<u8>, FormatError> {
        let entry = self.entry(section_id)?;
        let offset = entry.offset as usize;
        let end = offset
            .checked_add(entry.compressed_length as usize)
            .ok_or(FormatError::IncompleteWrite)?;
        if end > self.mmap.len() || end - offset != entry.compressed_length as usize {
            return Err(FormatError::IncompleteWrite);
        }
        Ok(self.mmap[offset..end].to_vec())
    }

    fn validate_section(&self, section_id: u32) -> Result<(), FormatError> {
        let entry = self.entry(section_id)?;
        let raw = self.read_section_raw(section_id)?;
        checksum::verify(&raw, self.header.checksum_algorithm, entry.checksum as u64)
    }

    fn validate_all(&self) -> Result<(), FormatError> {
        for entry in &self.section_index.entries {
            self.validate_section(entry.section_id)?;
        }
        Ok(())
    }

    fn prefetch_sections<'a>(&'a self, _section_ids: &'a [u32]) -> PrefetchFuture<'a> {
        // mmap上でアクセスしてページをウォームアップする。
        let section_ids = _section_ids.to_vec();
        Box::pin(async move {
            for id in section_ids {
                let _ = self.read_section_raw(id)?;
            }
            Ok(())
        })
    }
}

/// WASM向けの読み取り挙動設定。
#[cfg(target_arch = "wasm32")]
pub struct WasmReaderConfig {
    /// このサイズ未満なら全体をバッファにロードする。
    pub full_load_threshold_bytes: usize,
    /// 大容量ファイル用の範囲ローダー（IndexedDB等）。
    pub range_loader: Option<Box<dyn RangeLoader>>,
}

#[cfg(target_arch = "wasm32")]
impl Default for WasmReaderConfig {
    fn default() -> Self {
        Self {
            full_load_threshold_bytes: 100 * 1024 * 1024, // 100MB
            range_loader: None,
        }
    }
}

/// WASM向けファイルリーダー（バッファ/IndexedDB）。
#[cfg(target_arch = "wasm32")]
pub struct AlopexFileReader {
    buffer: Option<Vec<u8>>,
    loader: Option<Box<dyn RangeLoader>>,
    length: u64,
    header: FileHeader,
    footer: FileFooter,
    section_index: SectionIndex,
    config: WasmReaderConfig,
}

#[cfg(target_arch = "wasm32")]
impl AlopexFileReader {
    /// コンフィグ付きでファイルを開く。
    pub fn open_with_config(
        source: FileSource,
        config: WasmReaderConfig,
    ) -> Result<Self, FormatError> {
        match source {
            FileSource::Buffer(buf) => Self::from_buffer(buf, config),
            #[cfg(feature = "wasm-indexeddb")]
            FileSource::IndexedDb {
                length,
                db_name: _,
                key: _,
                loader,
            } => {
                let mut cfg = config;
                cfg.range_loader = Some(loader);
                Self::from_indexed_db(length, cfg)
            }
        }
    }

    fn from_buffer(buffer: Vec<u8>, config: WasmReaderConfig) -> Result<Self, FormatError> {
        // 閾値超過時、range_loaderがあればIndexedDB経路へフォールバック。
        if buffer.len() > config.full_load_threshold_bytes {
            if let Some(loader) = config.range_loader {
                return Self::from_indexed_db(
                    buffer.len() as u64,
                    WasmReaderConfig {
                        range_loader: Some(loader),
                        ..config
                    },
                );
            } else {
                return Err(FormatError::IncompleteWrite);
            }
        }

        if buffer.len() < HEADER_SIZE + FOOTER_SIZE {
            return Err(FormatError::IncompleteWrite);
        }

        let footer = Self::read_footer(&buffer)?;
        let section_index = Self::read_section_index(&buffer, &footer)?;
        let header = Self::read_header(&buffer)?;

        Ok(Self {
            buffer: Some(buffer),
            loader: None,
            length: (HEADER_SIZE + FOOTER_SIZE + section_index.serialized_size()) as u64,
            header,
            footer,
            section_index,
            config,
        })
    }

    #[cfg(feature = "wasm-indexeddb")]
    fn from_indexed_db(length: u64, mut config: WasmReaderConfig) -> Result<Self, FormatError> {
        let loader = config
            .range_loader
            .take()
            .ok_or(FormatError::IncompleteWrite)?;

        // サイズが閾値未満なら全体をロードしてバッファ経路に切替
        if (length as usize) <= config.full_load_threshold_bytes {
            let full = loader.load_range(0, length)?;
            return Self::from_buffer(
                full,
                WasmReaderConfig {
                    range_loader: Some(loader),
                    ..config
                },
            );
        }

        // フッターを末尾から取得
        let footer_start = length
            .checked_sub(FOOTER_SIZE as u64)
            .ok_or(FormatError::IncompleteWrite)?;
        let footer_bytes = loader.load_range(footer_start, FOOTER_SIZE as u64)?;
        let footer_array: [u8; FOOTER_SIZE] = footer_bytes
            .try_into()
            .map_err(|_| FormatError::IncompleteWrite)?;
        let footer = FileFooter::from_bytes(&footer_array)?;

        // セクションインデックスを取得
        let index_offset = footer.section_index_offset;
        // 最低でもcount + entries分だけ読む
        let count_bytes = loader.load_range(index_offset, 4)?;
        let count_arr: [u8; 4] = count_bytes
            .try_into()
            .map_err(|_| FormatError::IncompleteWrite)?;
        let count = u32::from_le_bytes(count_arr);
        let total_size = 4 + count as usize * SectionEntry::SIZE;
        let index_bytes = loader.load_range(index_offset, total_size as u64)?;
        let section_index = SectionIndex::from_bytes(&index_bytes)?;

        // ヘッダー取得
        let header_bytes = loader.load_range(0, HEADER_SIZE as u64)?;
        let header_array: [u8; HEADER_SIZE] = header_bytes
            .try_into()
            .map_err(|_| FormatError::IncompleteWrite)?;
        let header = FileHeader::from_bytes(&header_array)?;
        header.check_compatibility(&FileVersion::CURRENT)?;

        Ok(Self {
            buffer: None,
            loader: Some(loader),
            length,
            header,
            footer,
            section_index,
            config,
        })
    }

    fn read_footer(buffer: &[u8]) -> Result<FileFooter, FormatError> {
        if buffer.len() < FOOTER_SIZE {
            return Err(FormatError::IncompleteWrite);
        }
        let start = buffer.len() - FOOTER_SIZE;
        let mut buf = [0u8; FOOTER_SIZE];
        buf.copy_from_slice(&buffer[start..]);
        FileFooter::from_bytes(&buf)
    }

    fn read_header(buffer: &[u8]) -> Result<FileHeader, FormatError> {
        if buffer.len() < HEADER_SIZE {
            return Err(FormatError::IncompleteWrite);
        }
        let mut buf = [0u8; HEADER_SIZE];
        buf.copy_from_slice(&buffer[..HEADER_SIZE]);
        let header = FileHeader::from_bytes(&buf)?;
        header.check_compatibility(&FileVersion::CURRENT)?;
        Ok(header)
    }

    fn read_section_index(buffer: &[u8], footer: &FileFooter) -> Result<SectionIndex, FormatError> {
        let offset = footer.section_index_offset as usize;
        if buffer.len() < offset + 4 {
            return Err(FormatError::IncompleteWrite);
        }
        let count = u32::from_le_bytes(
            buffer[offset..offset + 4]
                .try_into()
                .expect("slice length checked"),
        );
        let expected = 4usize + count as usize * SectionEntry::SIZE;
        if buffer.len() < offset + expected {
            return Err(FormatError::IncompleteWrite);
        }
        SectionIndex::from_bytes(&buffer[offset..offset + expected])
    }

    fn entry(&self, section_id: u32) -> Result<&SectionEntry, FormatError> {
        self.section_index
            .find_by_id(section_id)
            .ok_or(FormatError::IncompleteWrite)
    }
}

#[cfg(target_arch = "wasm32")]
impl FileReader for AlopexFileReader {
    fn open(source: FileSource) -> Result<Self, FormatError>
    where
        Self: Sized,
    {
        Self::open_with_config(source, WasmReaderConfig::default())
    }

    fn header(&self) -> &FileHeader {
        &self.header
    }

    fn footer(&self) -> &FileFooter {
        &self.footer
    }

    fn section_index(&self) -> &SectionIndex {
        &self.section_index
    }

    fn read_section(&self, section_id: u32) -> Result<Vec<u8>, FormatError> {
        let entry = self.entry(section_id)?;
        let raw = self.read_section_raw(section_id)?;
        if raw.len() as u64 != entry.compressed_length {
            return Err(FormatError::IncompleteWrite);
        }
        checksum::verify(&raw, self.header.checksum_algorithm, entry.checksum as u64)?;
        let decompressed = compression::decompress(&raw, entry.compression)?;
        if decompressed.len() as u64 != entry.uncompressed_length {
            return Err(FormatError::IncompleteWrite);
        }
        Ok(decompressed)
    }

    fn read_section_raw(&self, section_id: u32) -> Result<Vec<u8>, FormatError> {
        let entry = self.entry(section_id)?;
        match &self.buffer {
            Some(buf) => {
                let offset = entry.offset as usize;
                let end = offset
                    .checked_add(entry.compressed_length as usize)
                    .ok_or(FormatError::IncompleteWrite)?;
                if end > buf.len() || end - offset != entry.compressed_length as usize {
                    return Err(FormatError::IncompleteWrite);
                }
                Ok(buf[offset..end].to_vec())
            }
            None => {
                let loader = self.loader.as_ref().ok_or(FormatError::IncompleteWrite)?;
                loader.load_range(entry.offset, entry.compressed_length)
            }
        }
    }

    fn validate_section(&self, section_id: u32) -> Result<(), FormatError> {
        let entry = self.entry(section_id)?;
        let raw = self.read_section_raw(section_id)?;
        checksum::verify(&raw, self.header.checksum_algorithm, entry.checksum as u64)
    }

    fn validate_all(&self) -> Result<(), FormatError> {
        for entry in &self.section_index.entries {
            self.validate_section(entry.section_id)?;
        }
        Ok(())
    }

    fn prefetch_sections<'a>(&'a self, _section_ids: &'a [u32]) -> PrefetchFuture<'a> {
        let section_ids = _section_ids.to_vec();
        Box::pin(async move {
            for id in section_ids {
                let _ = self.read_section_raw(id)?;
            }
            Ok(())
        })
    }
}
