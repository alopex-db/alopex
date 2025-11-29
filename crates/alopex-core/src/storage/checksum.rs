//! チェックサムアルゴリズムの抽象化とユーティリティ。
//!
//! CRC32 は常に利用可能、XXH64 は `checksum-xxh64` feature で有効化する。

use crate::storage::format::FormatError;
use std::io::Read;

/// チェックサムアルゴリズム識別子。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    /// CRC32 (IEEE)。
    Crc32 = 0,
    /// XXH64（feature `checksum-xxh64` が必要）。
    Xxh64 = 1,
}

/// バイト列に対してチェックサムを計算する（圧縮後データを想定）。
pub fn compute(data: &[u8], algorithm: ChecksumAlgorithm) -> Result<u64, FormatError> {
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(data);
            Ok(hasher.finalize() as u64)
        }
        ChecksumAlgorithm::Xxh64 => {
            #[cfg(feature = "checksum-xxh64")]
            {
                let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
                hasher.update(data);
                Ok(hasher.digest())
            }
            #[cfg(not(feature = "checksum-xxh64"))]
            {
                Err(FormatError::UnsupportedChecksum {
                    algorithm: algorithm as u8,
                })
            }
        }
    }
}

/// ストリームに対してチェックサムを計算する（大容量データ向け）。
pub fn compute_stream<R: Read>(
    reader: &mut R,
    algorithm: ChecksumAlgorithm,
) -> Result<u64, FormatError> {
    const BUF_SIZE: usize = 16 * 1024;
    let mut buf = [0u8; BUF_SIZE];
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            let mut hasher = crc32fast::Hasher::new();
            loop {
                let read = reader
                    .read(&mut buf)
                    .map_err(|_| FormatError::ChecksumMismatch {
                        expected: 0,
                        found: 0,
                    })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buf[..read]);
            }
            Ok(hasher.finalize() as u64)
        }
        ChecksumAlgorithm::Xxh64 => {
            #[cfg(feature = "checksum-xxh64")]
            {
                let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
                loop {
                    let read =
                        reader
                            .read(&mut buf)
                            .map_err(|_| FormatError::ChecksumMismatch {
                                expected: 0,
                                found: 0,
                            })?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buf[..read]);
                }
                Ok(hasher.digest())
            }
            #[cfg(not(feature = "checksum-xxh64"))]
            {
                Err(FormatError::UnsupportedChecksum {
                    algorithm: algorithm as u8,
                })
            }
        }
    }
}

/// バイト列のチェックサムを検証する。計算値が一致しない場合は [`FormatError::ChecksumMismatch`] を返す。
pub fn verify(data: &[u8], algorithm: ChecksumAlgorithm, expected: u64) -> Result<(), FormatError> {
    let found = compute(data, algorithm)?;
    if found == expected {
        Ok(())
    } else {
        Err(FormatError::ChecksumMismatch { expected, found })
    }
}

/// ストリームのチェックサムを検証する。計算値が一致しない場合は [`FormatError::ChecksumMismatch`] を返す。
pub fn verify_stream<R: Read>(
    reader: &mut R,
    algorithm: ChecksumAlgorithm,
    expected: u64,
) -> Result<(), FormatError> {
    let found = compute_stream(reader, algorithm)?;
    if found == expected {
        Ok(())
    } else {
        Err(FormatError::ChecksumMismatch { expected, found })
    }
}
