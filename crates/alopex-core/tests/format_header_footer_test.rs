use alopex_core::storage::format::{
    FileFlags, FileFooter, FileHeader, FileVersion, FormatError, FOOTER_SIZE, HEADER_SIZE,
};
use proptest::prelude::*;

fn header_with_version(version: FileVersion, flags: FileFlags) -> FileHeader {
    let mut header = FileHeader::new(version, flags);
    header.version = version;
    header
}

#[test]
fn header_roundtrip_and_magic_ok() {
    let flags = FileFlags(FileFlags::ENCRYPTED | FileFlags::VALUE_SEPARATED);
    let header = header_with_version(FileVersion::new(0, 1, 0), flags);
    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), HEADER_SIZE);
    let parsed = FileHeader::from_bytes(&bytes).expect("should parse");
    assert_eq!(parsed.magic, header.magic);
    assert_eq!(parsed.version, header.version);
    assert_eq!(parsed.flags.bits(), header.flags.bits());
    assert_eq!(parsed.checksum_algorithm, header.checksum_algorithm);
    assert_eq!(parsed.compression_algorithm, header.compression_algorithm);
}

#[test]
fn header_invalid_magic_is_detected() {
    let mut bytes = [0u8; HEADER_SIZE];
    bytes[0..4].copy_from_slice(b"BADM");
    let err = FileHeader::from_bytes(&bytes).expect_err("should fail");
    assert!(matches!(err, FormatError::InvalidMagic { .. }));
}

proptest! {
    #[test]
    fn header_version_compatibility_matches_order(
        file_major in 0u16..3,
        file_minor in 0u16..3,
        file_patch in 0u16..3,
        reader_major in 0u16..3,
        reader_minor in 0u16..3,
        reader_patch in 0u16..3,
    ) {
        let file_v = FileVersion::new(file_major, file_minor, file_patch);
        let reader_v = FileVersion::new(reader_major, reader_minor, reader_patch);
        let header = header_with_version(file_v, FileFlags(0));
        let res = header.check_compatibility(&reader_v);
        if file_v > reader_v {
            let incompatible = match res {
                Err(FormatError::IncompatibleVersion { file, reader }) => {
                    file == file_v && reader == reader_v
                }
                _ => false,
            };
            prop_assert!(incompatible);
        } else {
            prop_assert!(res.is_ok());
        }
    }
}

fn footer_sample() -> FileFooter {
    let mut footer = FileFooter::new(128, 64, 3, 10, 2048, 4096, 7);
    footer.compute_and_set_checksum();
    footer
}

#[test]
fn footer_roundtrip_and_checksum_valid() {
    let footer = footer_sample();
    let bytes = footer.to_bytes();
    assert_eq!(bytes.len(), FOOTER_SIZE);
    let parsed = FileFooter::from_bytes(&bytes).expect("should parse");
    assert_eq!(parsed, footer);
}

#[test]
fn footer_checksum_mismatch_detected() {
    let footer = footer_sample();
    let mut bytes = footer.to_bytes();
    // flip one byte outside checksum field to corrupt data
    bytes[0] ^= 0xFF;
    let err = FileFooter::from_bytes(&bytes).expect_err("should detect checksum mismatch");
    assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
}

#[test]
fn footer_reverse_magic_invalid_detected() {
    let mut footer = footer_sample();
    footer.reverse_magic = *b"BAD!";
    let mut bytes = footer.to_bytes();
    // ensure checksum matches altered magic to isolate reverse magic check
    bytes[60..64].copy_from_slice(&footer.reverse_magic);
    let err = FileFooter::from_bytes(&bytes).expect_err("should detect invalid reverse magic");
    assert!(matches!(err, FormatError::IncompleteWrite));
}
