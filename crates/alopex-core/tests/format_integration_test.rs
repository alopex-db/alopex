use alopex_core::storage::format::{
    AlopexFileReader, AlopexFileWriter, FileFlags, FileReader, FileSource, FileVersion, SectionType,
};
use tempfile::tempdir;

#[test]
fn file_roundtrip_metadata_and_sstable() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("roundtrip.alopex");

    let version = FileVersion::new(0, 1, 0);
    let flags = FileFlags(FileFlags::VALUE_SEPARATED);
    let metadata_bytes = b"meta-v1";
    let sstable_bytes = b"sstable-payload-123";

    // write
    {
        let mut writer = AlopexFileWriter::new(path.clone(), version, flags).unwrap();
        let meta_id = writer
            .add_section(SectionType::Metadata, metadata_bytes, false)
            .unwrap();
        assert_eq!(meta_id, 0);
        let data_id = writer
            .add_section(SectionType::SSTable, sstable_bytes, true)
            .unwrap();
        assert_eq!(data_id, 1);
        writer.finalize().unwrap();
    }

    // read
    let reader = AlopexFileReader::open(FileSource::Path(path)).unwrap();
    reader.validate_all().unwrap();

    assert_eq!(reader.section_index().entries.len(), 2);
    let header = reader.header();
    assert_eq!(header.version, version);
    assert_eq!(header.flags.is_value_separated(), true);

    // metadata
    let meta = reader.read_section(0).unwrap();
    assert_eq!(meta, metadata_bytes);
    // sstable (compressed) should roundtrip
    let data = reader.read_section(1).unwrap();
    assert_eq!(data, sstable_bytes);

    // footer counters
    let footer = reader.footer();
    assert_eq!(footer.data_section_count, 1);
    let entries = &reader.section_index().entries;
    let meta_entry = entries
        .iter()
        .find(|e| e.section_type == SectionType::Metadata)
        .unwrap();
    assert_eq!(footer.metadata_section_offset, meta_entry.offset);
    let last_entry = entries.last().unwrap();
    let expected_index_offset = last_entry.offset + last_entry.compressed_length;
    assert_eq!(footer.section_index_offset, expected_index_offset);
}
