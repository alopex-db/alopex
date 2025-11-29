//! ファイルヘッダー定義とシリアライズ/デシリアライズ処理。

use std::convert::TryInto;
use std::mem;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage::checksum::ChecksumAlgorithm;
use crate::storage::compression::CompressionAlgorithm;
use crate::storage::format::{FileVersion, FormatError, HEADER_SIZE, MAGIC};

/// ファイルフラグ（ビットフィールド）。
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FileFlags(pub u32);

impl FileFlags {
    /// 暗号化フラグ。
    pub const ENCRYPTED: u32 = 1 << 1;
    /// 値分離フラグ。
    pub const VALUE_SEPARATED: u32 = 1 << 2;

    /// フラグ値を取得。
    pub fn bits(&self) -> u32 {
        self.0
    }

    /// 暗号化フラグが有効か。
    pub fn is_encrypted(&self) -> bool {
        self.0 & Self::ENCRYPTED != 0
    }

    /// 値分離フラグが有効か。
    pub fn is_value_separated(&self) -> bool {
        self.0 & Self::VALUE_SEPARATED != 0
    }
}

/// ファイルヘッダー（64バイト固定）。
///
/// フィールド配置（リトルエンディアン）:
/// - 0..4:   magic
/// - 4..6:   version.major
/// - 6..8:   version.minor
/// - 8..10:  version.patch
/// - 10:     checksum_algorithm
/// - 11:     compression_algorithm
/// - 12..16: flags
/// - 16..24: created_at (µs)
/// - 24..32: modified_at (µs)
/// - 32..40: schema_version
/// - 40..64: reserved (ゼロ埋め)
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    /// マジックナンバー。
    pub magic: [u8; 4],
    /// ファイルバージョン。
    pub version: FileVersion,
    /// チェックサムアルゴリズム。
    pub checksum_algorithm: ChecksumAlgorithm,
    /// 圧縮アルゴリズム（デフォルト）。
    pub compression_algorithm: CompressionAlgorithm,
    /// グローバルフラグ。
    pub flags: FileFlags,
    /// 作成タイムスタンプ（µs）。
    pub created_at: u64,
    /// 最終更新タイムスタンプ（µs）。
    pub modified_at: u64,
    /// スキーマバージョン（アプリケーション依存）。
    pub schema_version: u64,
    /// 将来拡張のための予約領域。
    pub reserved: [u8; 24],
}

const _: () = assert!(mem::size_of::<FileHeader>() == HEADER_SIZE);

impl FileHeader {
    /// ヘッダーサイズ（バイト）。
    pub const SIZE: usize = HEADER_SIZE;

    /// 新規ヘッダーを生成する。
    ///
    /// タイムスタンプは現在時刻のマイクロ秒、圧縮アルゴリズムはSnappy、
    /// チェックサムはCRC32をデフォルトとする。
    pub fn new(version: FileVersion, flags: FileFlags) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        Self {
            magic: MAGIC,
            version,
            checksum_algorithm: ChecksumAlgorithm::Crc32,
            compression_algorithm: CompressionAlgorithm::Snappy,
            flags,
            created_at: now,
            modified_at: now,
            schema_version: 0,
            reserved: [0u8; 24],
        }
    }

    /// バイト列からヘッダーを復元する。
    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Result<Self, FormatError> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);

        let version = FileVersion {
            major: u16::from_le_bytes([bytes[4], bytes[5]]),
            minor: u16::from_le_bytes([bytes[6], bytes[7]]),
            patch: u16::from_le_bytes([bytes[8], bytes[9]]),
        };

        let checksum_algorithm = match bytes[10] {
            0 => ChecksumAlgorithm::Crc32,
            1 => ChecksumAlgorithm::Xxh64,
            other => return Err(FormatError::UnsupportedCompression { algorithm: other }),
        };

        let compression_algorithm = match bytes[11] {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Snappy,
            2 => CompressionAlgorithm::Zstd,
            3 => CompressionAlgorithm::Lz4,
            other => return Err(FormatError::UnsupportedCompression { algorithm: other }),
        };

        let flags = FileFlags(u32::from_le_bytes(
            bytes[12..16].try_into().expect("fixed slice length"),
        ));

        let created_at = u64::from_le_bytes(bytes[16..24].try_into().expect("fixed slice length"));
        let modified_at = u64::from_le_bytes(bytes[24..32].try_into().expect("fixed slice length"));
        let schema_version =
            u64::from_le_bytes(bytes[32..40].try_into().expect("fixed slice length"));

        let mut reserved = [0u8; 24];
        reserved.copy_from_slice(&bytes[40..64]);

        let header = Self {
            magic,
            version,
            checksum_algorithm,
            compression_algorithm,
            flags,
            created_at,
            modified_at,
            schema_version,
            reserved,
        };

        header.validate_magic()?;
        Ok(header)
    }

    /// ヘッダーをバイト列にシリアライズする。
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..6].copy_from_slice(&self.version.major.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.version.minor.to_le_bytes());
        bytes[8..10].copy_from_slice(&self.version.patch.to_le_bytes());
        bytes[10] = self.checksum_algorithm as u8;
        bytes[11] = self.compression_algorithm as u8;
        bytes[12..16].copy_from_slice(&self.flags.bits().to_le_bytes());
        bytes[16..24].copy_from_slice(&self.created_at.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.modified_at.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.schema_version.to_le_bytes());
        bytes[40..64].copy_from_slice(&self.reserved);
        bytes
    }

    /// マジックナンバーを検証する。
    pub fn validate_magic(&self) -> Result<(), FormatError> {
        if self.magic == MAGIC {
            Ok(())
        } else {
            Err(FormatError::InvalidMagic { found: self.magic })
        }
    }

    /// リーダーバージョンとの互換性を検証する。
    ///
    /// ファイルのバージョンがリーダーより新しい場合に [`FormatError::IncompatibleVersion`] を返す。
    pub fn check_compatibility(&self, reader_version: &FileVersion) -> Result<(), FormatError> {
        if self.version > *reader_version {
            Err(FormatError::IncompatibleVersion {
                file: self.version,
                reader: *reader_version,
            })
        } else {
            Ok(())
        }
    }
}
