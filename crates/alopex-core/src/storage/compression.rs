//! 圧縮アルゴリズムの抽象化とユーティリティ。
//!
//! Snappy は常に利用可能、Zstd/LZ4 は feature で有効化する。

use crate::storage::format::FormatError;

#[cfg(feature = "compression-zstd")]
use std::io::Cursor;

/// 圧縮アルゴリズム識別子。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// 圧縮なし。
    None = 0,
    /// Snappy 圧縮。
    Snappy = 1,
    /// Zstandard 圧縮（`compression-zstd` feature）。
    Zstd = 2,
    /// LZ4 圧縮（`compression-lz4` feature）。
    Lz4 = 3,
}

/// データを指定されたアルゴリズムで圧縮する。
///
/// Feature が無効なアルゴリズムを要求された場合は
/// [`FormatError::UnsupportedCompression`] を返す。
pub fn compress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>, FormatError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Snappy => {
            snap::raw::Encoder::new().compress_vec(data).map_err(|_| {
                FormatError::CompressionFailed {
                    algorithm: algorithm as u8,
                }
            })
        }
        CompressionAlgorithm::Zstd => {
            #[cfg(feature = "compression-zstd")]
            {
                zstd::stream::encode_all(Cursor::new(data), 0).map_err(|_| {
                    FormatError::CompressionFailed {
                        algorithm: algorithm as u8,
                    }
                })
            }
            #[cfg(not(feature = "compression-zstd"))]
            {
                Err(FormatError::UnsupportedCompression {
                    algorithm: algorithm as u8,
                })
            }
        }
        CompressionAlgorithm::Lz4 => {
            #[cfg(feature = "compression-lz4")]
            {
                // Enable content size in the block header so decompression does not require
                // the caller to supply the original length.
                lz4::block::compress(data, None, true).map_err(|_| FormatError::CompressionFailed {
                    algorithm: algorithm as u8,
                })
            }
            #[cfg(not(feature = "compression-lz4"))]
            {
                Err(FormatError::UnsupportedCompression {
                    algorithm: algorithm as u8,
                })
            }
        }
    }
}

/// データを指定されたアルゴリズムで解凍する。
///
/// Feature が無効なアルゴリズムを要求された場合は
/// [`FormatError::UnsupportedCompression`] を返す。
pub fn decompress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>, FormatError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Snappy => {
            snap::raw::Decoder::new().decompress_vec(data).map_err(|_| {
                FormatError::DecompressionFailed {
                    algorithm: algorithm as u8,
                }
            })
        }
        CompressionAlgorithm::Zstd => {
            #[cfg(feature = "compression-zstd")]
            {
                zstd::stream::decode_all(Cursor::new(data)).map_err(|_| {
                    FormatError::DecompressionFailed {
                        algorithm: algorithm as u8,
                    }
                })
            }
            #[cfg(not(feature = "compression-zstd"))]
            {
                Err(FormatError::UnsupportedCompression {
                    algorithm: algorithm as u8,
                })
            }
        }
        CompressionAlgorithm::Lz4 => {
            #[cfg(feature = "compression-lz4")]
            {
                lz4::block::decompress(data, None).map_err(|_| FormatError::DecompressionFailed {
                    algorithm: algorithm as u8,
                })
            }
            #[cfg(not(feature = "compression-lz4"))]
            {
                Err(FormatError::UnsupportedCompression {
                    algorithm: algorithm as u8,
                })
            }
        }
    }
}

// =============================================================================
// CompressionV2 - Columnar Storage Compression Interface
// =============================================================================

use serde::{Deserialize, Serialize};

/// V2 Compression algorithm with level support.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionV2 {
    /// No compression.
    #[default]
    None,
    /// LZ4 compression.
    Lz4,
    /// Zstandard compression with configurable level (1-22).
    Zstd {
        /// Compression level (1-22, higher = better compression but slower).
        level: i32,
    },
}

impl CompressionV2 {
    /// Check if this compression algorithm is available (feature flag enabled).
    pub fn is_available(&self) -> bool {
        match self {
            CompressionV2::None => true,
            CompressionV2::Lz4 => cfg!(feature = "compression-lz4"),
            CompressionV2::Zstd { .. } => cfg!(feature = "compression-zstd"),
        }
    }

    /// Get a human-readable name for the compression algorithm.
    pub fn name(&self) -> &'static str {
        match self {
            CompressionV2::None => "none",
            CompressionV2::Lz4 => "lz4",
            CompressionV2::Zstd { .. } => "zstd",
        }
    }
}

/// Compressor trait for V2 compression implementations.
pub trait Compressor: Send + Sync {
    /// Compress data and return the compressed bytes.
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FormatError>;

    /// Decompress data given the expected uncompressed size.
    fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>, FormatError>;

    /// Get the compression type.
    fn compression_type(&self) -> CompressionV2;
}

/// No-op compressor that passes data through unchanged.
pub struct NoneCompressor;

impl Compressor for NoneCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FormatError> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>, FormatError> {
        Ok(data.to_vec())
    }

    fn compression_type(&self) -> CompressionV2 {
        CompressionV2::None
    }
}

/// LZ4 compressor using the existing LZ4 implementation.
#[cfg(feature = "compression-lz4")]
pub struct Lz4Compressor;

#[cfg(feature = "compression-lz4")]
impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FormatError> {
        lz4::block::compress(data, None, true).map_err(|_| FormatError::CompressionFailed {
            algorithm: CompressionAlgorithm::Lz4 as u8,
        })
    }

    fn decompress(&self, data: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>, FormatError> {
        lz4::block::decompress(data, None).map_err(|_| FormatError::DecompressionFailed {
            algorithm: CompressionAlgorithm::Lz4 as u8,
        })
    }

    fn compression_type(&self) -> CompressionV2 {
        CompressionV2::Lz4
    }
}

/// Zstandard compressor with configurable compression level.
#[cfg(feature = "compression-zstd")]
pub struct ZstdCompressor {
    level: i32,
}

#[cfg(feature = "compression-zstd")]
impl ZstdCompressor {
    /// Create a new Zstd compressor with the specified level (1-22).
    /// Level is clamped to valid range.
    pub fn new(level: i32) -> Self {
        Self {
            level: level.clamp(1, 22),
        }
    }
}

#[cfg(feature = "compression-zstd")]
impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, FormatError> {
        zstd::stream::encode_all(Cursor::new(data), self.level).map_err(|_| {
            FormatError::CompressionFailed {
                algorithm: CompressionAlgorithm::Zstd as u8,
            }
        })
    }

    fn decompress(&self, data: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>, FormatError> {
        zstd::stream::decode_all(Cursor::new(data)).map_err(|_| FormatError::DecompressionFailed {
            algorithm: CompressionAlgorithm::Zstd as u8,
        })
    }

    fn compression_type(&self) -> CompressionV2 {
        CompressionV2::Zstd { level: self.level }
    }
}

/// Create a compressor for the given compression type.
///
/// Returns `FormatError::UnsupportedCompression` if the required feature flag is disabled.
pub fn create_compressor(compression: CompressionV2) -> Result<Box<dyn Compressor>, FormatError> {
    match compression {
        CompressionV2::None => Ok(Box::new(NoneCompressor)),

        CompressionV2::Lz4 => {
            #[cfg(feature = "compression-lz4")]
            {
                Ok(Box::new(Lz4Compressor))
            }
            #[cfg(not(feature = "compression-lz4"))]
            {
                Err(FormatError::UnsupportedCompression {
                    algorithm: CompressionAlgorithm::Lz4 as u8,
                })
            }
        }

        CompressionV2::Zstd { level } => {
            #[cfg(feature = "compression-zstd")]
            {
                Ok(Box::new(ZstdCompressor::new(level)))
            }
            #[cfg(not(feature = "compression-zstd"))]
            {
                let _ = level; // Suppress unused warning
                Err(FormatError::UnsupportedCompression {
                    algorithm: CompressionAlgorithm::Zstd as u8,
                })
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn test_none_compressor_roundtrip() {
        let compressor = NoneCompressor;
        let data = b"hello, world!";

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data);

        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn test_lz4_compress_decompress() {
        let compressor = Lz4Compressor;
        let data = b"hello, world! this is a test string for lz4 compression.";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn test_lz4_compressor_via_factory() {
        let compressor = create_compressor(CompressionV2::Lz4).unwrap();
        let data = b"test data for lz4";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(compressor.compression_type(), CompressionV2::Lz4);
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_zstd_compress_decompress_levels() {
        let data = b"hello, world! this is a test string for zstd compression at various levels.";

        for level in [1, 3, 9, 15, 22] {
            let compressor = ZstdCompressor::new(level);
            let compressed = compressor.compress(data).unwrap();
            let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
            assert_eq!(decompressed, data, "Failed at level {}", level);
        }
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_zstd_compressor_via_factory() {
        let compressor = create_compressor(CompressionV2::Zstd { level: 3 }).unwrap();
        let data = b"test data for zstd";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(
            compressor.compression_type(),
            CompressionV2::Zstd { level: 3 }
        );
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_zstd_level_clamping() {
        // Level below minimum
        let compressor = ZstdCompressor::new(-5);
        assert_eq!(
            compressor.compression_type(),
            CompressionV2::Zstd { level: 1 }
        );

        // Level above maximum
        let compressor = ZstdCompressor::new(100);
        assert_eq!(
            compressor.compression_type(),
            CompressionV2::Zstd { level: 22 }
        );
    }

    #[test]
    fn test_none_compressor_via_factory() {
        let compressor = create_compressor(CompressionV2::None).unwrap();
        let data = b"test data";

        let compressed = compressor.compress(data).unwrap();
        let decompressed = compressor.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(compressor.compression_type(), CompressionV2::None);
    }

    #[cfg(not(feature = "compression-lz4"))]
    #[test]
    fn test_unsupported_lz4_compression_error() {
        let result = create_compressor(CompressionV2::Lz4);
        assert!(result.is_err());
        match result {
            Err(FormatError::UnsupportedCompression { algorithm }) => {
                assert_eq!(algorithm, CompressionAlgorithm::Lz4 as u8);
            }
            _ => panic!("Expected UnsupportedCompression error"),
        }
    }

    #[cfg(not(feature = "compression-zstd"))]
    #[test]
    fn test_unsupported_zstd_compression_error() {
        let result = create_compressor(CompressionV2::Zstd { level: 3 });
        assert!(result.is_err());
        match result {
            Err(FormatError::UnsupportedCompression { algorithm }) => {
                assert_eq!(algorithm, CompressionAlgorithm::Zstd as u8);
            }
            _ => panic!("Expected UnsupportedCompression error"),
        }
    }

    #[test]
    fn test_compression_v2_is_available() {
        // None is always available
        assert!(CompressionV2::None.is_available());

        // LZ4 depends on feature flag
        #[cfg(feature = "compression-lz4")]
        assert!(CompressionV2::Lz4.is_available());
        #[cfg(not(feature = "compression-lz4"))]
        assert!(!CompressionV2::Lz4.is_available());

        // Zstd depends on feature flag
        #[cfg(feature = "compression-zstd")]
        assert!(CompressionV2::Zstd { level: 3 }.is_available());
        #[cfg(not(feature = "compression-zstd"))]
        assert!(!CompressionV2::Zstd { level: 3 }.is_available());
    }

    #[test]
    fn test_compression_v2_name() {
        assert_eq!(CompressionV2::None.name(), "none");
        assert_eq!(CompressionV2::Lz4.name(), "lz4");
        assert_eq!(CompressionV2::Zstd { level: 5 }.name(), "zstd");
    }

    #[test]
    fn test_compression_v2_default() {
        assert_eq!(CompressionV2::default(), CompressionV2::None);
    }
}
