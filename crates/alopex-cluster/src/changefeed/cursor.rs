use std::cmp::Ordering;

use sha2::{Digest, Sha256};

use crate::{ClusterId, FailureClass, RangeId};

use super::Checkpoint;

const CURSOR_VERSION: u8 = 1;
const BASE64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Stable identity material for a delivered event. The SHA-256 digest is
/// computed from a length-delimited binary form, so map order and transport
/// formatting cannot change an event identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIdentity {
    pub cluster_id: ClusterId,
    pub range_id: RangeId,
    pub generation: u64,
    pub epoch: u64,
    pub replay_id: String,
    pub payload_ordinal: u32,
}

impl EventIdentity {
    pub fn new(
        cluster_id: impl Into<ClusterId>,
        range_id: impl Into<RangeId>,
        generation: u64,
        epoch: u64,
        replay_id: impl Into<String>,
        payload_ordinal: u32,
    ) -> Result<Self, CursorError> {
        let identity = Self {
            cluster_id: cluster_id.into(),
            range_id: range_id.into(),
            generation,
            epoch,
            replay_id: replay_id.into(),
            payload_ordinal,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, CursorError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(
            self.cluster_id.as_str().len()
                + self.range_id.as_str().len()
                + self.replay_id.len()
                + 32,
        );
        put_string(&mut bytes, self.cluster_id.as_str())?;
        put_string(&mut bytes, self.range_id.as_str())?;
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&self.epoch.to_be_bytes());
        put_string(&mut bytes, &self.replay_id)?;
        bytes.extend_from_slice(&self.payload_ordinal.to_be_bytes());
        Ok(bytes)
    }

    /// The immutable public event id. Duplicate delivery of the same source
    /// position uses the same identity; a different ordinal does not.
    pub fn event_id(&self) -> Result<String, CursorError> {
        Ok(hex_digest(self.canonical_bytes()?))
    }

    fn validate(&self) -> Result<(), CursorError> {
        require_non_empty("cluster_id", self.cluster_id.as_str())?;
        require_non_empty("range_id", self.range_id.as_str())?;
        require_non_empty("replay_id", &self.replay_id)
    }
}

/// Versioned base64url representation of a `Checkpoint`. It is a wire
/// boundary only: ownership, predecessor continuity, and retention decisions
/// remain responsibilities of the later resume planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCursor {
    checkpoint: Checkpoint,
}

impl CheckpointCursor {
    pub fn new(checkpoint: Checkpoint) -> Result<Self, CursorError> {
        checkpoint
            .validate()
            .map_err(CursorError::from_model_error)?;
        Ok(Self { checkpoint })
    }

    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Encodes exactly one versioned checkpoint with URL-safe, unpadded base64.
    pub fn encode(&self) -> Result<String, CursorError> {
        Ok(encode_base64url(&self.canonical_bytes()?))
    }

    /// Decodes one complete cursor. Padding, malformed base64url characters,
    /// an unknown version, and any trailing or truncated field are rejected as
    /// `invalid_checkpoint`, never recovered as a successful empty resume.
    pub fn decode(value: &str) -> Result<Self, CursorError> {
        let bytes = decode_base64url(value)?;
        let mut reader = CursorReader::new(&bytes);
        let version = reader.u8()?;
        if version != CURSOR_VERSION {
            return Err(CursorError::UnsupportedVersion { found: version });
        }
        let feed_id = reader.string()?;
        let range_id = reader.string()?;
        let generation = reader.u64()?;
        let commit_position = reader.u64()?;
        let payload_ordinal = reader.u32()?;
        let epoch = reader.u64()?;
        let retention_deadline = match reader.u8()? {
            0 => None,
            1 => Some(reader.u64()?),
            marker => return Err(CursorError::InvalidRetentionMarker { marker }),
        };
        if !reader.is_empty() {
            return Err(CursorError::TrailingBytes);
        }
        let checkpoint = Checkpoint::new(
            feed_id,
            range_id,
            generation,
            commit_position,
            payload_ordinal,
            epoch,
            retention_deadline,
        )
        .map_err(CursorError::from_model_error)?;
        Self::new(checkpoint)
    }

    /// Decodes a cursor only when it belongs to the exact feed and range that
    /// requested resume. A valid cursor for another feed is invalid input for
    /// this request.
    pub fn decode_for(
        value: &str,
        expected_feed_id: &str,
        expected_range_id: &RangeId,
    ) -> Result<Self, CursorError> {
        let cursor = Self::decode(value)?;
        if cursor.checkpoint.feed_id != expected_feed_id {
            return Err(CursorError::FeedMismatch);
        }
        if &cursor.checkpoint.range_id != expected_range_id {
            return Err(CursorError::RangeMismatch);
        }
        Ok(cursor)
    }

    /// Returns whether this candidate lies in `(checkpoint, +∞)`. Equality is
    /// deliberately false so an at-least-once duplicate keeps its event id but
    /// is not advanced as a new position.
    pub fn is_strictly_after(&self, checkpoint: &Checkpoint) -> Result<bool, CursorError> {
        checkpoint
            .validate()
            .map_err(CursorError::from_model_error)?;
        if self.checkpoint.feed_id != checkpoint.feed_id {
            return Err(CursorError::FeedMismatch);
        }
        if self.checkpoint.range_id != checkpoint.range_id {
            return Err(CursorError::RangeMismatch);
        }
        Ok(self.position().cmp(&CheckpointPosition::from(checkpoint)) == Ordering::Greater)
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, CursorError> {
        self.checkpoint
            .validate()
            .map_err(CursorError::from_model_error)?;
        let mut bytes = Vec::with_capacity(
            self.checkpoint.feed_id.len() + self.checkpoint.range_id.as_str().len() + 40,
        );
        bytes.push(CURSOR_VERSION);
        put_string(&mut bytes, &self.checkpoint.feed_id)?;
        put_string(&mut bytes, self.checkpoint.range_id.as_str())?;
        bytes.extend_from_slice(&self.checkpoint.generation.to_be_bytes());
        bytes.extend_from_slice(&self.checkpoint.commit_position.to_be_bytes());
        bytes.extend_from_slice(&self.checkpoint.payload_ordinal.to_be_bytes());
        bytes.extend_from_slice(&self.checkpoint.epoch.to_be_bytes());
        match self.checkpoint.retention_deadline {
            Some(deadline) => {
                bytes.push(1);
                bytes.extend_from_slice(&deadline.to_be_bytes());
            }
            None => bytes.push(0),
        }
        Ok(bytes)
    }

    fn position(&self) -> CheckpointPosition {
        CheckpointPosition::from(&self.checkpoint)
    }
}

/// Ordering key used by the cursor codec. Integer fields are compared in the
/// same unsigned big-endian order used in the binary cursor, while range id
/// remains explicit instead of implying table or cluster global order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CheckpointPosition {
    pub range_id: RangeId,
    pub generation: u64,
    pub epoch: u64,
    pub commit_position: u64,
    pub payload_ordinal: u32,
}

impl From<&Checkpoint> for CheckpointPosition {
    fn from(checkpoint: &Checkpoint) -> Self {
        Self {
            range_id: checkpoint.range_id.clone(),
            generation: checkpoint.generation,
            epoch: checkpoint.epoch,
            commit_position: checkpoint.commit_position,
            payload_ordinal: checkpoint.payload_ordinal,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("event identity {field} must not be empty")]
    EmptyIdentity { field: &'static str },
    #[error("event identity field is too long for canonical encoding")]
    IdentityFieldTooLong,
    #[error("cursor uses unsupported version {found}")]
    UnsupportedVersion { found: u8 },
    #[error("cursor is not unpadded base64url")]
    InvalidBase64url,
    #[error("cursor is truncated")]
    Truncated,
    #[error("cursor has trailing bytes")]
    TrailingBytes,
    #[error("cursor retention marker {marker} is invalid")]
    InvalidRetentionMarker { marker: u8 },
    #[error("cursor checkpoint is invalid: {0}")]
    InvalidCheckpoint(String),
    #[error("cursor belongs to another feed")]
    FeedMismatch,
    #[error("cursor belongs to another range")]
    RangeMismatch,
}

impl CursorError {
    /// Every decoder failure uses the approved public failure mapping.
    pub const fn failure_class(&self) -> FailureClass {
        FailureClass::InvalidRequest
    }

    pub const fn reason_code(&self) -> &'static str {
        "invalid_checkpoint"
    }

    fn from_model_error(error: impl std::fmt::Display) -> Self {
        Self::InvalidCheckpoint(error.to_string())
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), CursorError> {
    if value.is_empty() {
        Err(CursorError::EmptyIdentity { field })
    } else {
        Ok(())
    }
}

fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), CursorError> {
    let length = u32::try_from(value.len()).map_err(|_| CursorError::IdentityFieldTooLong)?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn encode_base64url(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity((bytes.len() * 4).div_ceil(3));
    let mut offset = 0;
    while offset + 3 <= bytes.len() {
        let block = u32::from(bytes[offset]) << 16
            | u32::from(bytes[offset + 1]) << 8
            | u32::from(bytes[offset + 2]);
        for shift in [18, 12, 6, 0] {
            encoded.push(BASE64URL[((block >> shift) & 0x3f) as usize] as char);
        }
        offset += 3;
    }
    match bytes.len() - offset {
        0 => {}
        1 => {
            let block = u32::from(bytes[offset]) << 16;
            encoded.push(BASE64URL[((block >> 18) & 0x3f) as usize] as char);
            encoded.push(BASE64URL[((block >> 12) & 0x3f) as usize] as char);
        }
        2 => {
            let block = u32::from(bytes[offset]) << 16 | u32::from(bytes[offset + 1]) << 8;
            encoded.push(BASE64URL[((block >> 18) & 0x3f) as usize] as char);
            encoded.push(BASE64URL[((block >> 12) & 0x3f) as usize] as char);
            encoded.push(BASE64URL[((block >> 6) & 0x3f) as usize] as char);
        }
        _ => unreachable!("remainder is at most two bytes"),
    }
    encoded
}

fn decode_base64url(value: &str) -> Result<Vec<u8>, CursorError> {
    if value.is_empty() || value.contains('=') || value.len() % 4 == 1 {
        return Err(CursorError::InvalidBase64url);
    }
    let values = value
        .bytes()
        .map(decode_base64url_char)
        .collect::<Result<Vec<_>, _>>()?;
    let remainder = values.len() % 4;
    let full_length = values.len() - remainder;
    let mut decoded = Vec::with_capacity((values.len() * 3) / 4);
    for chunk in values[..full_length].chunks_exact(4) {
        let block = u32::from(chunk[0]) << 18
            | u32::from(chunk[1]) << 12
            | u32::from(chunk[2]) << 6
            | u32::from(chunk[3]);
        decoded.extend_from_slice(&[(block >> 16) as u8, (block >> 8) as u8, block as u8]);
    }
    match remainder {
        0 => {}
        2 => {
            if values[full_length + 1] & 0x0f != 0 {
                return Err(CursorError::InvalidBase64url);
            }
            let block =
                u32::from(values[full_length]) << 18 | u32::from(values[full_length + 1]) << 12;
            decoded.push((block >> 16) as u8);
        }
        3 => {
            if values[full_length + 2] & 0x03 != 0 {
                return Err(CursorError::InvalidBase64url);
            }
            let block = u32::from(values[full_length]) << 18
                | u32::from(values[full_length + 1]) << 12
                | u32::from(values[full_length + 2]) << 6;
            decoded.extend_from_slice(&[(block >> 16) as u8, (block >> 8) as u8]);
        }
        _ => return Err(CursorError::InvalidBase64url),
    }
    Ok(decoded)
}

fn decode_base64url_char(value: u8) -> Result<u8, CursorError> {
    match value {
        b'A'..=b'Z' => Ok(value - b'A'),
        b'a'..=b'z' => Ok(value - b'a' + 26),
        b'0'..=b'9' => Ok(value - b'0' + 52),
        b'-' => Ok(62),
        b'_' => Ok(63),
        _ => Err(CursorError::InvalidBase64url),
    }
}

struct CursorReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CursorReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, CursorError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CursorError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed length"),
        ))
    }

    fn u64(&mut self) -> Result<u64, CursorError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed length"),
        ))
    }

    fn string(&mut self) -> Result<String, CursorError> {
        let length = usize::try_from(self.u32()?).map_err(|_| CursorError::Truncated)?;
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| CursorError::InvalidBase64url)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CursorError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CursorError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CursorError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}
