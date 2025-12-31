#![cfg(not(target_arch = "wasm32"))]

use alopex_core::storage::compression::CompressionAlgorithm;
#[cfg(not(target_arch = "wasm32"))]
use alopex_core::storage::format::AlopexFileWriter;
use alopex_core::storage::format::{
    AlopexFileReader, FileFlags, FileReader, FileSource, FileVersion, FormatError, SectionType,
    HEADER_SIZE,
};
use std::env;

#[cfg(not(target_arch = "wasm32"))]
use tempfile::tempdir;

const V0_1_GOLDEN: &[u8] = include_bytes!("data/compatibility/v0_1.alopex");

#[cfg(not(target_arch = "wasm32"))]
fn generate_v0_1_test_file() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().expect("create tempdir");
    let path = dir.path().join("compat_v0_1.alopex");
    std::fs::write(&path, V0_1_GOLDEN).expect("write golden file");
    (dir, path)
}

#[cfg(target_arch = "wasm32")]
fn generate_v0_1_test_bytes() -> Vec<u8> {
    V0_1_GOLDEN.to_vec()
}

#[cfg(not(target_arch = "wasm32"))]
fn mutate_version(path: &std::path::Path, file_version: FileVersion) -> std::path::PathBuf {
    let mut bytes = std::fs::read(path).expect("read file");
    let mut header = [0u8; HEADER_SIZE];
    header.copy_from_slice(&bytes[..HEADER_SIZE]);
    let mut header =
        alopex_core::storage::format::FileHeader::from_bytes(&header).expect("parse header");
    header.version = file_version;
    let serialized = header.to_bytes();
    bytes[..HEADER_SIZE].copy_from_slice(&serialized);

    let bumped_path = path.with_file_name("newer_version.alopex");
    std::fs::write(&bumped_path, bytes).expect("write bumped file");
    bumped_path
}

#[cfg(target_arch = "wasm32")]
fn mutate_version_bytes(mut bytes: Vec<u8>, file_version: FileVersion) -> Vec<u8> {
    let mut header_bytes = [0u8; HEADER_SIZE];
    header_bytes.copy_from_slice(&bytes[..HEADER_SIZE]);
    let mut header =
        alopex_core::storage::format::FileHeader::from_bytes(&header_bytes).expect("parse header");
    header.version = file_version;
    let serialized = header.to_bytes();
    bytes[..HEADER_SIZE].copy_from_slice(&serialized);
    bytes
}

#[cfg(not(target_arch = "wasm32"))]
fn compression_from_env() -> CompressionAlgorithm {
    match env::var("ALOPEX_COMPAT_COMPRESSION")
        .unwrap_or_else(|_| "snappy".to_string())
        .as_str()
    {
        "none" => CompressionAlgorithm::None,
        "zstd" => CompressionAlgorithm::Zstd,
        "lz4" => CompressionAlgorithm::Lz4,
        _ => CompressionAlgorithm::Snappy,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::match_like_matches_macro)]
fn compression_available(alg: CompressionAlgorithm) -> bool {
    match alg {
        CompressionAlgorithm::Zstd => cfg!(feature = "compression-zstd"),
        CompressionAlgorithm::Lz4 => cfg!(feature = "compression-lz4"),
        _ => true,
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn reads_v0_1_golden_file() {
    let (_dir, path) = generate_v0_1_test_file();
    let reader = AlopexFileReader::open(FileSource::Path(path.clone())).expect("open v0.1 file");
    assert_eq!(reader.header().version, FileVersion::new(0, 1, 0));
    reader.validate_all().expect("validate sections");
    assert_eq!(reader.section_index().entries.len(), 2);
    assert_eq!(reader.read_section(0).unwrap(), b"compat-meta-v0.1");
    assert_eq!(reader.read_section(1).unwrap(), b"compat-data-v0.1");
    // ensure footer layout aligns with index offsets
    assert!(
        reader.footer().section_index_offset >= alopex_core::storage::format::HEADER_SIZE as u64
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn newer_version_is_rejected() {
    let (_dir, path) = generate_v0_1_test_file();
    let bumped = mutate_version(
        &path,
        FileVersion::new(FileVersion::CURRENT.major + 1, 0, 0),
    );
    let err = match AlopexFileReader::open(FileSource::Path(bumped)) {
        Ok(_) => panic!("should reject newer"),
        Err(e) => e,
    };
    match err {
        FormatError::IncompatibleVersion { file, reader } => {
            assert!(
                file > reader,
                "file {:?} should be newer than reader {:?}",
                file,
                reader
            );
        }
        other => panic!("unexpected error: {:?}", other),
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn roundtrip_with_requested_compression() {
    let alg = compression_from_env();
    if !compression_available(alg) {
        eprintln!("skip compression {alg:?} because feature is disabled");
        return;
    }
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("compat_env.alopex");
    let mut writer =
        AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).expect("writer");
    writer
        .add_section(SectionType::Metadata, b"env-meta", false)
        .expect("write metadata");
    writer
        .add_section_with_compression(SectionType::SSTable, b"env-data", alg)
        .expect("write data");
    writer.finalize().expect("finalize");

    let reader = AlopexFileReader::open(FileSource::Path(path)).expect("open generated file");
    reader.validate_all().expect("validate generated file");
    assert_eq!(reader.header().version, FileVersion::CURRENT);
    assert_eq!(reader.read_section(1).unwrap(), b"env-data");
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn reads_v0_1_golden_file() {
        let buffer = generate_v0_1_test_bytes();
        let reader = AlopexFileReader::open(FileSource::Buffer(buffer)).expect("open v0.1 file");
        assert_eq!(reader.header().version, FileVersion::new(0, 1, 0));
        reader.validate_all().expect("validate sections");
        assert_eq!(reader.section_index().entries.len(), 2);
        assert_eq!(reader.read_section(0).unwrap(), b"compat-meta-v0.1");
        assert_eq!(reader.read_section(1).unwrap(), b"compat-data-v0.1");
    }

    #[wasm_bindgen_test]
    fn newer_version_is_rejected() {
        let buffer = generate_v0_1_test_bytes();
        let bumped = mutate_version_bytes(
            buffer,
            FileVersion::new(FileVersion::CURRENT.major + 1, 0, 0),
        );
        let err = match AlopexFileReader::open(FileSource::Buffer(bumped)) {
            Ok(_) => panic!("should reject newer"),
            Err(e) => e,
        };
        match err {
            FormatError::IncompatibleVersion { file, reader } => assert!(file > reader),
            other => panic!("unexpected error: {:?}", other),
        }
    }
}
