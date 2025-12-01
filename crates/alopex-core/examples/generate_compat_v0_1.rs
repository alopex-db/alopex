//! Generate the v0.1 golden compatibility file used in tests.
//! This is a helper utility and is not invoked during CI.
//! Run from the workspace root:
//! `cargo run -p alopex-core --example generate_compat_v0_1`

#![cfg(not(target_arch = "wasm32"))]

use std::fs;
use std::path::PathBuf;

use alopex_core::storage::compression::CompressionAlgorithm;
use alopex_core::storage::format::{
    AlopexFileWriter, FileFlags, FileHeader, FileVersion, SectionType, HEADER_SIZE,
};

fn main() {
    let output = PathBuf::from("crates/alopex-core/tests/data/compatibility/v0_1.alopex");
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).expect("create compatibility data dir");
    }

    // Build a deterministic file with one metadata and one SSTable section.
    let mut writer = AlopexFileWriter::new(output.clone(), FileVersion::new(0, 1, 0), FileFlags(0))
        .expect("create writer");
    writer
        .add_section(SectionType::Metadata, b"compat-meta-v0.1", false)
        .expect("write metadata");
    writer
        .add_section_with_compression(
            SectionType::SSTable,
            b"compat-data-v0.1",
            CompressionAlgorithm::Snappy,
        )
        .expect("write sstable");
    writer.finalize().expect("finalize file");

    // Normalize timestamps for reproducibility.
    let mut bytes = fs::read(&output).expect("read generated file");
    let mut header_bytes = [0u8; HEADER_SIZE];
    header_bytes.copy_from_slice(&bytes[..HEADER_SIZE]);
    let mut header = FileHeader::from_bytes(&header_bytes).expect("parse header");
    header.created_at = 0;
    header.modified_at = 0;
    let normalized = header.to_bytes();
    bytes[..HEADER_SIZE].copy_from_slice(&normalized);
    fs::write(&output, bytes).expect("write normalized file");

    println!("Generated compatibility fixture: {}", output.display());
}
