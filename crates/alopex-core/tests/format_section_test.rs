use alopex_core::storage::compression::CompressionAlgorithm;
use alopex_core::storage::format::{SectionEntry, SectionIndex, SectionType, SECTION_ENTRY_SIZE};

#[test]
fn section_entry_size_and_padding_zero() {
    let entry = SectionEntry::new(
        SectionType::SSTable,
        CompressionAlgorithm::Snappy,
        1,
        128,
        64,
        128,
        0xdead_beef,
    );
    let mut index = SectionIndex::new();
    index.add_entry(entry);
    let bytes = index.to_bytes();
    assert_eq!(bytes.len(), 4 + SECTION_ENTRY_SIZE);
    // padding bytes are zeroed
    assert_eq!(&bytes[4 + 2..4 + 4], &[0, 0]);
    assert_eq!(&bytes[4 + 36..4 + 40], &[0, 0, 0, 0]);
}

#[test]
fn section_entry_roundtrip() {
    let entry = SectionEntry::new(
        SectionType::LargeValue,
        CompressionAlgorithm::None,
        7,
        4096,
        1024,
        2048,
        0x12345678,
    );
    let mut index = SectionIndex::new();
    index.add_entry(entry);
    let bytes = index.to_bytes();
    let parsed_index = SectionIndex::from_bytes(&bytes).expect("roundtrip");
    assert_eq!(parsed_index.entries.len(), 1);
    assert_eq!(parsed_index.entries[0], entry);
}

#[test]
fn section_index_add_filter_find_and_size() {
    let mut index = SectionIndex::new();
    let e1 = SectionEntry::new(
        SectionType::Metadata,
        CompressionAlgorithm::None,
        0,
        64,
        32,
        32,
        1,
    );
    let e2 = SectionEntry::new(
        SectionType::SSTable,
        CompressionAlgorithm::Snappy,
        1,
        96,
        64,
        96,
        2,
    );
    index.add_entry(e1);
    index.add_entry(e2);
    assert_eq!(index.count, 2);

    let filtered = index.filter_by_type(SectionType::SSTable);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].section_id, 1);

    let found = index.find_by_id(0).expect("entry 0");
    assert_eq!(found.section_type, SectionType::Metadata);

    let size = index.serialized_size();
    assert_eq!(size, 4 + 2 * SECTION_ENTRY_SIZE);
}

#[test]
fn section_index_empty_state() {
    let index = SectionIndex::new();
    assert_eq!(index.count, 0);
    assert!(index.entries.is_empty());
    assert_eq!(index.serialized_size(), 4); // only count field
    let bytes = index.to_bytes();
    assert_eq!(bytes, 0u32.to_le_bytes());
    let parsed = SectionIndex::from_bytes(&bytes).expect("parse empty");
    assert_eq!(parsed.count, 0);
    assert!(parsed.entries.is_empty());
}

#[test]
fn section_index_all_section_types_roundtrip() {
    let variants = [
        SectionType::Metadata,
        SectionType::SSTable,
        SectionType::VectorIndex,
        SectionType::ColumnarSegment,
        SectionType::LargeValue,
        SectionType::Intent,
        SectionType::Lock,
        SectionType::RaftLog,
    ];
    let mut index = SectionIndex::new();
    for (i, ty) in variants.iter().enumerate() {
        let entry = SectionEntry::new(
            *ty,
            CompressionAlgorithm::None,
            i as u32,
            (i * 100) as u64,
            16,
            32,
            (i as u32) + 1,
        );
        index.add_entry(entry);
    }
    let bytes = index.to_bytes();
    let parsed = SectionIndex::from_bytes(&bytes).expect("parse");
    assert_eq!(parsed.entries.len(), variants.len());
    for (i, ty) in variants.iter().enumerate() {
        assert_eq!(parsed.entries[i].section_type, *ty);
    }
}
