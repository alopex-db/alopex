#![cfg(target_arch = "wasm32")]

use alopex_core::storage::format::{
    BufferRangeLoader, FileFooter, FileHeader, FileSource, FileVersion, SectionIndex, SectionType,
    WasmReaderConfig, HEADER_SIZE,
};
use std::sync::{Arc, Mutex};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

struct MockRangeLoader {
    inner: BufferRangeLoader,
    log: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl MockRangeLoader {
    fn new(data: Vec<u8>, log: Arc<Mutex<Vec<(u64, u64)>>>) -> Self {
        Self {
            inner: BufferRangeLoader::new(data),
            log,
        }
    }
}

impl alopex_core::storage::format::RangeLoader for MockRangeLoader {
    fn load_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, alopex_core::storage::format::FormatError> {
        self.log.lock().unwrap().push((offset, length));
        self.inner.load_range(offset, length)
    }
}

/// 最小限の.alopexファイルをバッファで構築する（ヘッダー + 空インデックス + フッター）。
fn build_minimal_file() -> Vec<u8> {
    let header = FileHeader::new(
        FileVersion::CURRENT,
        alopex_core::storage::format::FileFlags(0),
    );
    let section_index = SectionIndex::new();
    let index_bytes = section_index.to_bytes();
    let section_index_offset = HEADER_SIZE as u64;
    let metadata_offset = 0u64;
    let data_section_count = 0u32;
    let mut footer = FileFooter::new(
        section_index_offset,
        metadata_offset,
        data_section_count,
        0,
        0,
        (HEADER_SIZE + index_bytes.len() + FileFooter::SIZE) as u64,
        0,
    );
    footer.compute_and_set_checksum();

    let mut file = Vec::with_capacity(HEADER_SIZE + index_bytes.len() + FileFooter::SIZE);
    file.extend_from_slice(&header.to_bytes());
    file.extend_from_slice(&index_bytes);
    file.extend_from_slice(&footer.to_bytes());
    file
}

#[wasm_bindgen_test]
fn below_threshold_uses_buffer() {
    let file = build_minimal_file();
    let cfg = WasmReaderConfig {
        full_load_threshold_bytes: file.len(),
        range_loader: None,
    };
    let reader = alopex_core::storage::format::AlopexFileReader::open_with_config(
        FileSource::Buffer(file),
        cfg,
    )
    .expect("buffer path should succeed");
    assert_eq!(reader.section_index().entries.len(), 0);
}

#[wasm_bindgen_test]
fn above_threshold_without_loader_errors() {
    let file = build_minimal_file();
    let cfg = WasmReaderConfig {
        full_load_threshold_bytes: file.len() - 1,
        range_loader: None,
    };
    let err = alopex_core::storage::format::AlopexFileReader::open_with_config(
        FileSource::Buffer(file),
        cfg,
    )
    .expect_err("should require loader when exceeding threshold");
    assert!(matches!(
        err,
        alopex_core::storage::format::FormatError::IncompleteWrite
    ));
}

#[wasm_bindgen_test]
fn above_threshold_with_loader_reads_ranges() {
    let file = build_minimal_file();
    let len = file.len();
    let log: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(vec![]));
    let loader = MockRangeLoader::new(file.clone(), log.clone());
    let cfg = WasmReaderConfig {
        full_load_threshold_bytes: len - 1, // force range path
        range_loader: Some(Box::new(loader)),
    };
    let reader = alopex_core::storage::format::AlopexFileReader::open_with_config(
        FileSource::Buffer(file),
        cfg,
    )
    .expect("range loader path should succeed");
    assert_eq!(reader.section_index().entries.len(), 0);
    let calls = log.lock().unwrap().clone();
    // 逆マジック/フッター読み出し
    assert!(calls.iter().any(|(off, len)| {
        *len == alopex_core::storage::format::FOOTER_SIZE as u64
            && *off + *len as u64 == reader.footer().file_size
    }));
    // インデックス読み出し（ヘッダーサイズオフセット）
    assert!(calls.iter().any(|(off, _)| *off == HEADER_SIZE as u64));
    // ヘッダー読み出し
    assert!(calls
        .iter()
        .any(|(off, len)| *off == 0 && *len == HEADER_SIZE as u64));
}

#[wasm_bindgen_test]
fn threshold_boundary_cases() {
    let file = build_minimal_file();
    let len = file.len();

    // below threshold (simulate 99MB < 100MB)
    let cfg_below = WasmReaderConfig {
        full_load_threshold_bytes: len,
        range_loader: None,
    };
    alopex_core::storage::format::AlopexFileReader::open_with_config(
        FileSource::Buffer(file.clone()),
        cfg_below,
    )
    .expect("below threshold should buffer");

    // at threshold (simulate 100MB)
    let cfg_at = WasmReaderConfig {
        full_load_threshold_bytes: len - 1 + 1, // same as len
        range_loader: None,
    };
    alopex_core::storage::format::AlopexFileReader::open_with_config(
        FileSource::Buffer(file.clone()),
        cfg_at,
    )
    .expect("at threshold should buffer");

    // above threshold (simulate 101MB) requires loader
    let log: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(vec![]));
    let loader = MockRangeLoader::new(file.clone(), log.clone());
    let cfg_above = WasmReaderConfig {
        full_load_threshold_bytes: len - 1, // smaller than len to force loader
        range_loader: Some(Box::new(loader)),
    };
    alopex_core::storage::format::AlopexFileReader::open_with_config(
        FileSource::Buffer(file),
        cfg_above,
    )
    .expect("above threshold should use loader");
}
