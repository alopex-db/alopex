//! Bounded operation payloads for overflow spools, independent of fragment boundaries.
//!
//! Encoding borrows the write set. Decoding holds at most one operation; the sink
//! must remain private until `finish` and the enclosing WAL recovery both succeed.
//! This codec does not enable overflow commits in `LsmKV`.

use std::collections::{btree_map, BTreeMap};
use std::io::{self, Read};

use super::{WalBatchOp, WalOpType};
use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"ALPXOPS1";
const STREAM_HEADER: usize = 16;
const OP_HEADER: usize = 17;

/// Independent limits supplied by the owning transaction/recovery layer.
#[derive(Clone, Copy, Debug)]
pub struct OperationLimits {
    payload_bytes: u64,
    key_bytes: usize,
    value_bytes: usize,
}

impl OperationLimits {
    /// Bound the entire encoded payload and each decoded key/value allocation.
    /// Sink memory and the original write set need separate owner-level budgets.
    pub fn new(payload_bytes: u64, key_bytes: usize, value_bytes: usize) -> Result<Self> {
        if payload_bytes < STREAM_HEADER as u64 || key_bytes.checked_add(value_bytes).is_none() {
            return Err(invalid("invalid operation stream limits"));
        }
        Ok(Self {
            payload_bytes,
            key_bytes,
            value_bytes,
        })
    }

    fn check_lengths(self, key: u64, value: u64) -> Result<(usize, usize)> {
        let key = usize::try_from(key).map_err(|_| invalid("key length overflow"))?;
        let value = usize::try_from(value).map_err(|_| invalid("value length overflow"))?;
        if key > self.key_bytes || value > self.value_bytes {
            return Err(capacity("operation exceeds key/value budget"));
        }
        Ok((key, value))
    }

    pub(crate) fn check_payload(self, bytes: u64) -> Result<()> {
        if bytes > self.payload_bytes {
            return Err(capacity("operation stream exceeds payload budget"));
        }
        Ok(())
    }

    pub(crate) fn check_single_operation(self, key: usize, value: usize) -> Result<u64> {
        self.check_lengths(key as u64, value as u64)?;
        let payload = (STREAM_HEADER as u64 + OP_HEADER as u64)
            .checked_add(key as u64)
            .and_then(|n| n.checked_add(value as u64))
            .ok_or_else(|| invalid("operation stream length overflow"))?;
        if payload > self.payload_bytes {
            return Err(capacity("operation stream exceeds payload budget"));
        }
        Ok(payload)
    }
}

fn capacity(message: &str) -> Error {
    Error::WalCapacityExceeded {
        reason: message.into(),
    }
}

fn invalid(message: &str) -> Error {
    Error::InvalidFormat(message.into())
}

/// A `Read` adapter over a borrowed write set, with a fixed 17-byte scratch header.
/// Construction validates all sizes before the caller creates a spool file.
pub struct OperationStream<'a> {
    operations: btree_map::Iter<'a, Vec<u8>, Option<Vec<u8>>>,
    header: [u8; OP_HEADER],
    header_len: usize,
    key: &'a [u8],
    value: &'a [u8],
    part: usize,
    offset: usize,
    payload_bytes: u64,
}

impl<'a> OperationStream<'a> {
    /// Borrow keys and values without cloning the batch or encoding it into a Vec.
    pub fn new(
        writes: &'a BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        limits: OperationLimits,
    ) -> Result<Self> {
        let mut payload_bytes = STREAM_HEADER as u64;
        for (key, value) in writes {
            let value_len = value.as_ref().map_or(0, Vec::len);
            limits.check_lengths(key.len() as u64, value_len as u64)?;
            payload_bytes = payload_bytes
                .checked_add(OP_HEADER as u64)
                .and_then(|n| n.checked_add(key.len() as u64))
                .and_then(|n| n.checked_add(value_len as u64))
                .ok_or_else(|| invalid("operation stream length overflow"))?;
        }
        if payload_bytes > limits.payload_bytes {
            return Err(capacity("operation stream exceeds payload budget"));
        }
        let mut header = [0; OP_HEADER];
        header[..8].copy_from_slice(MAGIC);
        header[8..16].copy_from_slice(&(writes.len() as u64).to_le_bytes());
        Ok(Self {
            operations: writes.iter(),
            header,
            header_len: STREAM_HEADER,
            key: &[],
            value: &[],
            part: 0,
            offset: 0,
            payload_bytes,
        })
    }

    /// Exact payload length to reserve in the spool descriptor.
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }
}

impl Read for OperationStream<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let mut written = 0;
        while written < output.len() {
            if self.part == 3 {
                let Some((key, value)) = self.operations.next() else {
                    break;
                };
                self.header[0] = u8::from(value.is_none());
                self.header[1..9].copy_from_slice(&(key.len() as u64).to_le_bytes());
                self.header[9..17]
                    .copy_from_slice(&(value.as_ref().map_or(0, Vec::len) as u64).to_le_bytes());
                self.header_len = OP_HEADER;
                self.key = key;
                self.value = value.as_deref().unwrap_or(&[]);
                self.part = 0;
            }
            let source = match self.part {
                0 => &self.header[..self.header_len],
                1 => self.key,
                _ => self.value,
            };
            let count = (source.len() - self.offset).min(output.len() - written);
            output[written..written + count]
                .copy_from_slice(&source[self.offset..self.offset + count]);
            written += count;
            self.offset += count;
            if self.offset == source.len() {
                self.part += 1;
                self.offset = 0;
            }
        }
        Ok(written)
    }
}

#[derive(Debug)]
enum State {
    Header,
    Operation,
    Payload { delete: bool, key: usize },
    Done,
    Failed,
}

/// Incremental decoder for fragments delivered by verified spool recovery.
/// On any error discard the sink and decoder; a failed decoder cannot be reused.
pub struct OperationDecoder {
    limits: OperationLimits,
    remaining_bytes: u64,
    remaining_ops: u64,
    state: State,
    pending: Vec<u8>,
    target: usize,
}

impl OperationDecoder {
    /// Bind the expected size from the validated WAL reference before any allocation.
    pub fn new(payload_bytes: u64, limits: OperationLimits) -> Result<Self> {
        limits.check_payload(payload_bytes)?;
        if payload_bytes < STREAM_HEADER as u64 {
            return Err(invalid("invalid operation payload length"));
        }
        Ok(Self {
            limits,
            remaining_bytes: payload_bytes,
            remaining_ops: 0,
            state: State::Header,
            pending: Vec::new(),
            target: STREAM_HEADER,
        })
    }

    /// Consume one arbitrary fragment. Successfully delivered operations are still
    /// provisional: discard all of them if any later fragment or `finish` fails.
    pub fn push(
        &mut self,
        fragment: &[u8],
        mut sink: impl FnMut(WalBatchOp) -> Result<()>,
    ) -> Result<()> {
        let result = self.consume(fragment, &mut sink);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }

    fn consume(
        &mut self,
        mut fragment: &[u8],
        sink: &mut impl FnMut(WalBatchOp) -> Result<()>,
    ) -> Result<()> {
        if matches!(self.state, State::Failed) {
            return Err(invalid("operation decoder failed"));
        }
        if fragment.len() as u64 > self.remaining_bytes {
            return Err(invalid("operation stream trailing bytes"));
        }
        while !fragment.is_empty() || self.pending.len() == self.target {
            if matches!(self.state, State::Done) {
                return Err(invalid("operation stream trailing bytes"));
            }
            let take = fragment.len().min(self.target - self.pending.len());
            self.pending.extend_from_slice(&fragment[..take]);
            self.remaining_bytes -= take as u64;
            fragment = &fragment[take..];
            if self.pending.len() != self.target {
                break;
            }
            match self.state {
                State::Header => {
                    if &self.pending[..8] != MAGIC {
                        return Err(invalid("unsupported operation stream"));
                    }
                    self.remaining_ops =
                        u64::from_le_bytes(self.pending[8..16].try_into().unwrap());
                    if self.remaining_ops > self.remaining_bytes / OP_HEADER as u64 {
                        return Err(invalid("operation count exceeds remaining payload"));
                    }
                    self.state = if self.remaining_ops == 0 {
                        State::Done
                    } else {
                        State::Operation
                    };
                    self.target = OP_HEADER;
                    self.pending.clear();
                }
                State::Operation => {
                    let tag = self.pending[0];
                    let key = u64::from_le_bytes(self.pending[1..9].try_into().unwrap());
                    let value = u64::from_le_bytes(self.pending[9..17].try_into().unwrap());
                    if tag > 1 || (tag == 1 && value != 0) {
                        return Err(invalid("invalid operation tag/value"));
                    }
                    let (key, value) = self.limits.check_lengths(key, value)?;
                    let bytes = key
                        .checked_add(value)
                        .ok_or_else(|| invalid("operation length overflow"))?;
                    let later_headers = (self.remaining_ops - 1)
                        .checked_mul(OP_HEADER as u64)
                        .ok_or_else(|| invalid("operation count overflow"))?;
                    if (bytes as u64)
                        .checked_add(later_headers)
                        .is_none_or(|n| n > self.remaining_bytes)
                    {
                        return Err(invalid("operation exceeds remaining payload"));
                    }
                    self.pending.clear();
                    self.target = bytes;
                    self.state = State::Payload {
                        delete: tag == 1,
                        key,
                    };
                }
                State::Payload { delete, key } => {
                    let mut key_bytes = std::mem::take(&mut self.pending);
                    let value = key_bytes.split_off(key);
                    sink(WalBatchOp {
                        op_type: if delete {
                            WalOpType::Delete
                        } else {
                            WalOpType::Put
                        },
                        key: key_bytes,
                        value: if delete { None } else { Some(value) },
                    })?;
                    self.remaining_ops -= 1;
                    self.state = if self.remaining_ops == 0 {
                        State::Done
                    } else {
                        State::Operation
                    };
                    self.target = OP_HEADER;
                }
                State::Done | State::Failed => unreachable!(),
            }
        }
        Ok(())
    }

    /// Accept only an exact, complete stream. This does not publish the sink.
    pub fn finish(self) -> Result<()> {
        if !matches!(self.state, State::Done) || self.remaining_bytes != 0 {
            return Err(invalid("incomplete operation stream"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
