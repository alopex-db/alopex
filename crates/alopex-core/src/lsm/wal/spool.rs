//! Bounded framing for an immutable overflow spool.
//!
//! This low-level codec does not commit a database transaction. A sealed spool
//! needs a separately durable WAL reference before recovery may apply it. The
//! live LSM writer still uses WAL v2; activation and reference replay are separate.

use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"ALPXOVFL";
const SEAL_MAGIC: &[u8; 8] = b"ALPXSEAL";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 56;
const FRAME_LEN: usize = 12;
const SEAL_LEN: usize = 16;
/// Hard upper bound on a codec's single payload buffer.
pub const MAX_FRAGMENT_BYTES: u32 = 1024 * 1024;

/// Independently supplied limits, never inferred from an untrusted file.
#[derive(Clone, Copy, Debug)]
pub struct SpoolLimits {
    payload_bytes: u64,
    file_bytes: u64,
    fragment_bytes: u32,
}

impl SpoolLimits {
    /// Set maximum payload, complete file size, and temporary fragment buffer.
    pub fn new(payload_bytes: u64, file_bytes: u64, fragment_bytes: u32) -> Result<Self> {
        if payload_bytes == 0
            || file_bytes == 0
            || fragment_bytes == 0
            || fragment_bytes > MAX_FRAGMENT_BYTES
        {
            return Err(invalid("invalid spool limits"));
        }
        Ok(Self {
            payload_bytes,
            file_bytes,
            fragment_bytes,
        })
    }

    fn check(&self, payload: u64, fragment: u32) -> Result<u32> {
        let (count, file_bytes) = layout(payload, fragment)?;
        if payload > self.payload_bytes
            || fragment > self.fragment_bytes
            || file_bytes > self.file_bytes
        {
            return Err(std::io::Error::new(std::io::ErrorKind::StorageFull,
                format!("overflow spool exceeds budget: payload={payload}, file={file_bytes}, fragment={fragment}")).into());
        }
        Ok(count)
    }
}

/// Identity supplied by a future commit reference, not a filesystem path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpoolIdentity {
    transaction: [u8; 16],
    lsn: u64,
}

impl SpoolIdentity {
    /// Construct a nonzero transaction identity and commit LSN.
    pub fn new(transaction: [u8; 16], lsn: u64) -> Result<Self> {
        if transaction == [0; 16] || lsn == 0 {
            return Err(invalid("zero spool transaction identity or LSN"));
        }
        Ok(Self { transaction, lsn })
    }
    /// Transaction identity bytes.
    pub fn transaction(&self) -> [u8; 16] {
        self.transaction
    }
    /// Commit sequence number.
    pub fn lsn(&self) -> u64 {
        self.lsn
    }
}

/// Expected spool identity, framing, and checksum to bind into a commit record.
/// CRC32 detects accidental corruption; it is not cryptographic authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpoolDescriptor {
    identity: SpoolIdentity,
    payload_bytes: u64,
    fragment_bytes: u32,
    checksum: u32,
}

impl SpoolDescriptor {
    /// Reconstruct an expected descriptor from an independently validated record.
    pub fn from_parts(
        identity: SpoolIdentity,
        payload_bytes: u64,
        fragment_bytes: u32,
        checksum: u32,
    ) -> Result<Self> {
        layout(payload_bytes, fragment_bytes)?;
        Ok(Self {
            identity,
            payload_bytes,
            fragment_bytes,
            checksum,
        })
    }
    /// Identity of the complete transaction stream.
    pub fn identity(&self) -> SpoolIdentity {
        self.identity
    }
    /// Encoded operation stream length, without physical framing.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
    /// Maximum fragment payload length used by this spool.
    pub fn fragment_bytes(&self) -> u32 {
        self.fragment_bytes
    }
    /// CRC32 of the header and all complete physical fragments.
    pub fn checksum(&self) -> u32 {
        self.checksum
    }
    /// Exact file bytes to reserve, including framing and seal.
    pub fn file_bytes(&self) -> Result<u64> {
        layout(self.payload_bytes, self.fragment_bytes).map(|(_, bytes)| bytes)
    }
}

fn invalid(message: &str) -> Error {
    Error::InvalidFormat(message.into())
}

fn layout(payload: u64, fragment: u32) -> Result<(u32, u64)> {
    if payload == 0 || fragment == 0 || fragment > MAX_FRAGMENT_BYTES {
        return Err(invalid("invalid overflow spool shape"));
    }
    // Division followed by a remainder avoids overflowing payload + fragment - 1.
    let count =
        payload / u64::from(fragment) + u64::from(!payload.is_multiple_of(u64::from(fragment)));
    let count = u32::try_from(count).map_err(|_| invalid("too many spool fragments"))?;
    let bytes = u64::from(count)
        .checked_mul(FRAME_LEN as u64)
        .and_then(|overhead| overhead.checked_add(payload))
        .and_then(|bytes| bytes.checked_add((HEADER_LEN + SEAL_LEN) as u64))
        .ok_or_else(|| invalid("overflow spool file length overflow"))?;
    Ok((count, bytes))
}

fn header(identity: SpoolIdentity, payload: u64, fragment: u32, count: u32) -> [u8; HEADER_LEN] {
    let mut bytes = [0; HEADER_LEN];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8..10].copy_from_slice(&VERSION.to_le_bytes());
    bytes[12..28].copy_from_slice(&identity.transaction);
    bytes[28..36].copy_from_slice(&identity.lsn.to_le_bytes());
    bytes[36..44].copy_from_slice(&payload.to_le_bytes());
    bytes[44..48].copy_from_slice(&fragment.to_le_bytes());
    bytes[48..52].copy_from_slice(&count.to_le_bytes());
    let crc = crc32fast::hash(&bytes[..52]);
    bytes[52..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn frame_crc(prefix: &[u8], payload: &[u8]) -> u32 {
    let mut crc = crc32fast::Hasher::new();
    crc.update(prefix);
    crc.update(payload);
    crc.finalize()
}

fn require_eof(reader: &mut impl Read) -> Result<()> {
    let mut byte = [0];
    loop {
        match reader.read(&mut byte) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err(invalid("trailing bytes in overflow spool stream")),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err.into()),
        }
    }
}

/// Encode exactly `payload_bytes` from a stream with one bounded buffer.
///
/// Budget/shape errors occur before writing anything. An I/O or input-length
/// error may leave partial output; the caller must keep that output uncommitted.
/// This function does not fsync, rename, or publish a WAL reference.
pub fn write_spool(
    input: &mut impl Read,
    output: &mut impl Write,
    identity: SpoolIdentity,
    payload_bytes: u64,
    fragment_bytes: u32,
    limits: SpoolLimits,
) -> Result<SpoolDescriptor> {
    let count = limits.check(payload_bytes, fragment_bytes)?;
    let header = header(identity, payload_bytes, fragment_bytes, count);
    output.write_all(&header)?;
    let mut whole = crc32fast::Hasher::new();
    whole.update(&header);
    let mut buffer = vec![0; fragment_bytes as usize];
    let mut remaining = payload_bytes;
    for ordinal in 0..count {
        let length = remaining.min(u64::from(fragment_bytes)) as usize;
        let payload = &mut buffer[..length];
        input.read_exact(payload)?;
        let mut frame = [0; FRAME_LEN];
        frame[..4].copy_from_slice(&ordinal.to_le_bytes());
        frame[4..8].copy_from_slice(&(length as u32).to_le_bytes());
        let crc = frame_crc(&frame[..8], payload);
        frame[8..].copy_from_slice(&crc.to_le_bytes());
        output.write_all(&frame)?;
        output.write_all(payload)?;
        whole.update(&frame);
        whole.update(payload);
        remaining -= length as u64;
    }
    require_eof(input)?;
    let checksum = whole.finalize();
    let mut seal = [0; SEAL_LEN];
    seal[..8].copy_from_slice(SEAL_MAGIC);
    seal[8..12].copy_from_slice(&checksum.to_le_bytes());
    let crc = crc32fast::hash(&seal[..12]);
    seal[12..].copy_from_slice(&crc.to_le_bytes());
    output.write_all(&seal)?;
    SpoolDescriptor::from_parts(identity, payload_bytes, fragment_bytes, checksum)
}

/// Verify a complete spool against its independently supplied descriptor.
/// No payload is delivered on this pass, and trailing bytes are rejected.
pub fn verify_spool(
    reader: &mut impl Read,
    expected: &SpoolDescriptor,
    limits: SpoolLimits,
) -> Result<()> {
    visit(reader, expected, limits, &mut |_| Ok(()))
}

/// Validate completely, seek back, then deliver bounded fragments in order.
///
/// The caller must own an immutable, exclusively opened file and keep all replay
/// state private until this returns success. A second-pass I/O error or visitor
/// error can occur after some callbacks; discard that private state on failure.
/// This method neither publishes database state nor makes data durable.
pub fn read_verified_spool<R: Read + Seek>(
    reader: &mut R,
    expected: &SpoolDescriptor,
    limits: SpoolLimits,
    mut visitor: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let start = reader.stream_position()?;
    verify_spool(reader, expected, limits)?;
    reader.seek(SeekFrom::Start(start))?;
    visit(reader, expected, limits, &mut visitor)
}

fn visit(
    reader: &mut impl Read,
    expected: &SpoolDescriptor,
    limits: SpoolLimits,
    visitor: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let count = limits.check(expected.payload_bytes, expected.fragment_bytes)?;
    let mut bytes = [0; HEADER_LEN];
    reader.read_exact(&mut bytes)?;
    if &bytes[..8] != MAGIC || u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != VERSION {
        return Err(invalid("unsupported overflow spool magic or version"));
    }
    if bytes[10..12] != [0, 0] {
        return Err(invalid("unsupported overflow spool flags"));
    }
    if crc32fast::hash(&bytes[..52]) != u32::from_le_bytes(bytes[52..].try_into().unwrap()) {
        return Err(Error::ChecksumMismatch);
    }
    // Comparing a canonical expected header rejects hostile lengths, count,
    // identity and LSN before allocating any buffer based on file contents.
    if bytes
        != header(
            expected.identity,
            expected.payload_bytes,
            expected.fragment_bytes,
            count,
        )
    {
        return Err(invalid(
            "overflow spool does not match its commit descriptor",
        ));
    }
    let mut whole = crc32fast::Hasher::new();
    whole.update(&bytes);
    let mut buffer = vec![0; expected.fragment_bytes as usize];
    let mut remaining = expected.payload_bytes;
    for ordinal in 0..count {
        let length = remaining.min(u64::from(expected.fragment_bytes)) as usize;
        let mut frame = [0; FRAME_LEN];
        reader.read_exact(&mut frame)?;
        if u32::from_le_bytes(frame[..4].try_into().unwrap()) != ordinal
            || u32::from_le_bytes(frame[4..8].try_into().unwrap()) != length as u32
        {
            return Err(invalid("invalid overflow spool fragment order or length"));
        }
        let payload = &mut buffer[..length];
        reader.read_exact(payload)?;
        if frame_crc(&frame[..8], payload) != u32::from_le_bytes(frame[8..].try_into().unwrap()) {
            return Err(Error::ChecksumMismatch);
        }
        whole.update(&frame);
        whole.update(payload);
        visitor(payload)?;
        remaining -= length as u64;
    }
    let mut seal = [0; SEAL_LEN];
    reader.read_exact(&mut seal)?;
    if &seal[..8] != SEAL_MAGIC {
        return Err(invalid("missing overflow spool seal"));
    }
    if crc32fast::hash(&seal[..12]) != u32::from_le_bytes(seal[12..].try_into().unwrap())
        || u32::from_le_bytes(seal[8..12].try_into().unwrap()) != expected.checksum
        || whole.finalize() != expected.checksum
    {
        return Err(Error::ChecksumMismatch);
    }
    require_eof(reader)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
