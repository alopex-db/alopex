use alopex_core::storage::checksum::{compute, verify, ChecksumAlgorithm};
use alopex_core::storage::compression::{compress, decompress, CompressionAlgorithm};
use alopex_core::storage::format::FormatError;
use proptest::prelude::*;

#[test]
fn snappy_roundtrip() {
    let data = b"hello compression";
    let compressed = compress(data, CompressionAlgorithm::Snappy).expect("compress");
    let decompressed = decompress(&compressed, CompressionAlgorithm::Snappy).expect("decompress");
    assert_eq!(decompressed, data);
}

#[test]
fn none_roundtrip_returns_same_bytes() {
    let data = b"no compression";
    let compressed = compress(data, CompressionAlgorithm::None).expect("compress");
    assert_eq!(compressed, data);
    let decompressed = decompress(&compressed, CompressionAlgorithm::None).expect("decompress");
    assert_eq!(decompressed, data);
}

#[cfg(feature = "compression-zstd")]
#[test]
fn zstd_roundtrip() {
    let data = b"zstd compress me";
    let compressed = compress(data, CompressionAlgorithm::Zstd).expect("compress");
    let decompressed = decompress(&compressed, CompressionAlgorithm::Zstd).expect("decompress");
    assert_eq!(decompressed, data);
}

#[cfg(not(feature = "compression-zstd"))]
#[test]
fn zstd_unsupported_without_feature() {
    let data = b"zstd unsupported";
    let err = compress(data, CompressionAlgorithm::Zstd).expect_err("should be unsupported");
    assert!(matches!(err, FormatError::UnsupportedCompression { .. }));
}

#[cfg(feature = "compression-lz4")]
#[test]
fn lz4_roundtrip() {
    let data = b"lz4 compress me";
    let compressed = compress(data, CompressionAlgorithm::Lz4).expect("compress");
    let decompressed = decompress(&compressed, CompressionAlgorithm::Lz4).expect("decompress");
    assert_eq!(decompressed, data);
}

#[cfg(not(feature = "compression-lz4"))]
#[test]
fn lz4_unsupported_without_feature() {
    let data = b"lz4 unsupported";
    let err = compress(data, CompressionAlgorithm::Lz4).expect_err("should be unsupported");
    assert!(matches!(err, FormatError::UnsupportedCompression { .. }));
}

proptest! {
    #[test]
fn crc32_detects_bit_flip(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let checksum = compute(&bytes, ChecksumAlgorithm::Crc32).expect("compute");
        // Flip one bit if data non-empty; if empty, checksum still should verify and flipped version fails
        let mut corrupted = bytes.clone();
        if !corrupted.is_empty() {
            corrupted[0] ^= 0x01;
        } else {
            corrupted.push(0x01);
        }
        let ok = verify(&bytes, ChecksumAlgorithm::Crc32, checksum);
        prop_assert!(ok.is_ok());
        let err = verify(&corrupted, ChecksumAlgorithm::Crc32, checksum).unwrap_err();
        let is_mismatch = matches!(err, FormatError::ChecksumMismatch { .. });
        prop_assert!(is_mismatch);
    }
}

#[cfg(not(feature = "compression-zstd"))]
#[test]
fn decompress_zstd_unsupported_without_feature() {
    let data = b"zstd data";
    let err = decompress(data, CompressionAlgorithm::Zstd).expect_err("unsupported");
    assert!(matches!(err, FormatError::UnsupportedCompression { .. }));
}

#[cfg(not(feature = "compression-lz4"))]
#[test]
fn decompress_lz4_unsupported_without_feature() {
    let data = b"lz4 data";
    let err = decompress(data, CompressionAlgorithm::Lz4).expect_err("unsupported");
    assert!(matches!(err, FormatError::UnsupportedCompression { .. }));
}

#[test]
fn checksum_is_on_compressed_bytes() {
    let raw = b"compress then checksum";
    let compressed = compress(raw, CompressionAlgorithm::Snappy).expect("compress");
    let checksum = compute(&compressed, ChecksumAlgorithm::Crc32).expect("compute");
    // verifying on compressed data should pass
    verify(&compressed, ChecksumAlgorithm::Crc32, checksum).expect("verify");
    // verifying on raw should fail
    let err = verify(raw, ChecksumAlgorithm::Crc32, checksum).unwrap_err();
    assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
}

#[cfg(feature = "checksum-xxh64")]
#[test]
fn xxh64_compute_and_verify_when_feature_enabled() {
    let data = b"xxh64 enabled";
    let checksum = compute(data, ChecksumAlgorithm::Xxh64).expect("compute");
    verify(data, ChecksumAlgorithm::Xxh64, checksum).expect("verify");
}

#[cfg(not(feature = "checksum-xxh64"))]
#[test]
fn xxh64_unsupported_when_feature_disabled() {
    let data = b"xxh64 disabled";
    let err = compute(data, ChecksumAlgorithm::Xxh64).expect_err("unsupported");
    assert!(matches!(err, FormatError::UnsupportedChecksum { .. }));
}
