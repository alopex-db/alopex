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
                lz4::block::compress(data, None, false).map_err(|_| {
                    FormatError::CompressionFailed {
                        algorithm: algorithm as u8,
                    }
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
