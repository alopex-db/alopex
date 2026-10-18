use std::cell::Cell;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use brotli_decompressor::{
    Allocator, BrotliDecompressStream, BrotliResult, BrotliState, HuffmanCode, SliceWrapper,
    StandardAlloc,
};
use bytes::Bytes;
use parquet::basic::{Compression, Encoding, Type};
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::statistics;
use parquet::format::{PageHeader, PageLocation, PageType};
use parquet::thrift::TSerializable;
use thrift::protocol::TCompactInputProtocol;

use super::super::preflight;

fn invalid() -> ParquetError {
    ParquetError::General("invalid parquet page declaration".into())
}

fn nonnegative(value: i32) -> Result<usize> {
    usize::try_from(value).map_err(|_| invalid())
}

struct ScratchReservation<'a> {
    reserved: &'a AtomicU64,
    bytes: Cell<u64>,
}

impl<'a> ScratchReservation<'a> {
    fn new(reserved: &'a AtomicU64, bytes: u64, limit: u64, codec: &str) -> Result<Self> {
        reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|&used| used <= limit)
            })
            .map_err(|_| {
                ParquetError::External(Box::new(crate::executor::ExecutorError::ResourceExhausted {
                    message: format!("READ_PARQUET {codec} workspace requires {bytes} additional bytes (limit {limit})"),
                }))
            })?;
        Ok(Self {
            reserved,
            bytes: Cell::new(bytes),
        })
    }
}

impl Drop for ScratchReservation<'_> {
    fn drop(&mut self) {
        self.reserved.fetch_sub(self.bytes.get(), Ordering::Relaxed);
    }
}

fn decode_zstd(input: &[u8], output: &mut [u8], limit: u64, reserved: &AtomicU64) -> Result<()> {
    use zstd::zstd_safe::zstd_sys;

    #[repr(C, align(8))]
    #[derive(Clone)]
    struct Word {
        _bytes: [u8; 8],
    }

    // SAFETY: This query has no arguments and allocates no context.
    let bytes = unsafe { zstd_sys::ZSTD_estimateDCtxSize() };
    let words = bytes.checked_add(7).ok_or_else(invalid)? / 8;
    let bytes = words
        .checked_mul(8)
        .filter(|&bytes| bytes != 0)
        .ok_or_else(invalid)?;
    let _reservation = ScratchReservation::new(reserved, bytes as u64, limit, "ZSTD")?;
    let mut workspace = vec![Word { _bytes: [0; 8] }; words];
    // SAFETY: The 8-byte-aligned workspace stays live and fixed through both C calls.
    // The static context never mallocs or resizes, and is not passed to freeDCtx.
    let valid = unsafe {
        let context = zstd_sys::ZSTD_initStaticDCtx(workspace.as_mut_ptr().cast(), bytes);
        !context.is_null() && {
            let decoded = zstd_sys::ZSTD_decompressDCtx(
                context,
                output.as_mut_ptr().cast(),
                output.len(),
                input.as_ptr().cast(),
                input.len(),
            );
            zstd_sys::ZSTD_isError(decoded) == 0 && decoded == output.len()
        }
    };
    if !valid {
        return Err(invalid());
    }
    Ok(())
}

struct BrotliAllocation<'a> {
    inner: StandardAlloc,
    limit: u64,
    reserved: &'a AtomicU64,
    pre_reserved: &'a Cell<u64>,
    rejected: &'a Cell<Option<usize>>,
}

impl<T> Allocator<T> for BrotliAllocation<'_>
where
    StandardAlloc: Allocator<T>,
{
    type AllocatedMemory = <StandardAlloc as Allocator<T>>::AllocatedMemory;

    fn alloc_cell(&mut self, count: usize) -> Self::AllocatedMemory {
        if self.rejected.get().is_none() {
            let bytes = count.checked_mul(std::mem::size_of::<T>());
            if let Some(bytes) = bytes.filter(|&bytes| bytes as u64 <= self.limit) {
                let prepaid = self.pre_reserved.get().min(bytes as u64);
                let additional = bytes as u64 - prepaid;
                match self
                    .reserved
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                        used.checked_add(additional)
                            .filter(|&used| used <= self.limit)
                    }) {
                    Ok(_) => {
                        self.pre_reserved.set(self.pre_reserved.get() - prepaid);
                        return <StandardAlloc as Allocator<T>>::alloc_cell(&mut self.inner, count);
                    }
                    Err(used) => self.rejected.set(Some(
                        usize::try_from(used.saturating_add(additional)).unwrap_or(usize::MAX),
                    )),
                }
            } else {
                self.rejected.set(Some(bytes.unwrap_or(usize::MAX)));
            }
        }
        // All three allocators stay rejected so dependent tables cannot outlive a failed request.
        <StandardAlloc as Allocator<T>>::alloc_cell(&mut self.inner, 0)
    }

    fn free_cell(&mut self, memory: Self::AllocatedMemory) {
        let bytes = std::mem::size_of_val(memory.slice());
        <StandardAlloc as Allocator<T>>::free_cell(&mut self.inner, memory);
        self.reserved.fetch_sub(bytes as u64, Ordering::Relaxed);
    }
}

fn brotli_budget_error(bytes: usize, limit: u64) -> ParquetError {
    ParquetError::External(Box::new(
        crate::executor::ExecutorError::ResourceExhausted {
            message: format!("READ_PARQUET Brotli requires {bytes} bytes (limit {limit})"),
        },
    ))
}

fn decode_brotli(input: &[u8], output: &mut [u8], limit: u64, reserved: &AtomicU64) -> Result<()> {
    // Brotli 4.0.3's largest fixed Huffman request is 3 * 1080 slots.
    let minimum = 3 * 1080 * std::mem::size_of::<HuffmanCode>();
    if minimum as u64 > limit {
        return Err(brotli_budget_error(minimum, limit));
    }
    // Fixed tables include an unchecked allocation; reserve all three before entering the decoder.
    let fixed = ScratchReservation::new(
        reserved,
        ((1080 + 3240 + 3240) * std::mem::size_of::<HuffmanCode>()) as u64,
        limit,
        "Brotli",
    )?;
    let zero = Cell::new(0);
    let rejected = Cell::new(None);
    let allocator = || BrotliAllocation {
        inner: StandardAlloc::default(),
        limit,
        reserved,
        pre_reserved: &zero,
        rejected: &rejected,
    };
    let mut huffman = allocator();
    huffman.pre_reserved = &fixed.bytes;
    let mut state = BrotliState::new(allocator(), allocator(), huffman);
    let mut available_in = input.len();
    let mut available_out = output.len();
    let mut input_offset = 0;
    let mut output_offset = 0;
    let mut total_out = 0;
    // Full input lets the decoder shrink the ringbuffer for small final blocks.
    let result = BrotliDecompressStream(
        &mut available_in,
        &mut input_offset,
        input,
        &mut available_out,
        &mut output_offset,
        output,
        &mut total_out,
        &mut state,
    );
    if let Some(bytes) = rejected.get() {
        return Err(brotli_budget_error(bytes, limit));
    }
    if !matches!(result, BrotliResult::ResultSuccess) || output_offset != output.len() {
        return Err(invalid());
    }
    Ok(())
}

enum Position {
    Sequential {
        offset: u64,
        end: u64,
    },
    Indexed {
        pages: VecDeque<PageLocation>,
        total_rows: usize,
    },
}

struct Pending {
    header: PageHeader,
    data: u64,
    next: u64,
    rows: Option<usize>,
}

pub(super) struct BoundedPages {
    file: Arc<File>,
    position: Position,
    pending: Option<Pending>,
    physical_type: Type,
    limit: u64,
    chunk_start: u64,
    chunk_end: u64,
    compression: Compression,
    reserved_bytes: Arc<AtomicU64>,
}

impl BoundedPages {
    pub(super) fn new(
        file: Arc<File>,
        column: &ColumnChunkMetaData,
        total_rows: usize,
        locations: Option<Vec<PageLocation>>,
        limit: u64,
        reserved_bytes: Arc<AtomicU64>,
    ) -> Result<Self> {
        let chunk_start = u64::try_from(
            column
                .dictionary_page_offset()
                .unwrap_or(column.data_page_offset()),
        )
        .map_err(|_| invalid())?;
        let chunk_end = chunk_start
            .checked_add(u64::try_from(column.compressed_size()).map_err(|_| invalid())?)
            .ok_or_else(invalid)?;
        let position = if let Some(locations) = locations {
            let mut pages: VecDeque<_> = locations.into();
            if let Some(offset) = column.dictionary_page_offset() {
                let size = column
                    .data_page_offset()
                    .checked_sub(offset)
                    .ok_or_else(invalid)?;
                pages.push_front(PageLocation::new(
                    offset,
                    i32::try_from(size).map_err(|_| invalid())?,
                    -1,
                ));
            }
            Position::Indexed { pages, total_rows }
        } else {
            Position::Sequential {
                offset: chunk_start,
                end: chunk_end,
            }
        };
        Ok(Self {
            file,
            position,
            pending: None,
            physical_type: column.column_type(),
            limit,
            chunk_start,
            chunk_end,
            compression: column.compression(),
            reserved_bytes,
        })
    }

    fn span(&self) -> Result<Option<(u64, u64, Option<usize>)>> {
        match &self.position {
            Position::Sequential { offset, end } => {
                if offset == end {
                    return Ok(None);
                }
                if offset > end {
                    return Err(invalid());
                }
                Ok(Some((*offset, *end, None)))
            }
            Position::Indexed { pages, total_rows } => {
                let Some(page) = pages.front() else {
                    return Ok(None);
                };
                let start = u64::try_from(page.offset).map_err(|_| invalid())?;
                let end = start
                    .checked_add(u64::try_from(page.compressed_page_size).map_err(|_| invalid())?)
                    .ok_or_else(invalid)?;
                let rows = if page.first_row_index == -1 {
                    None
                } else {
                    let first = usize::try_from(page.first_row_index).map_err(|_| invalid())?;
                    let next = pages
                        .get(1)
                        .map(|p| usize::try_from(p.first_row_index).map_err(|_| invalid()))
                        .transpose()?
                        .unwrap_or(*total_rows);
                    Some(next.checked_sub(first).ok_or_else(invalid)?)
                };
                Ok(Some((start, end, rows)))
            }
        }
    }

    fn advance(&mut self, next: u64) {
        match &mut self.position {
            Position::Sequential { offset, .. } => *offset = next,
            Position::Indexed { pages, .. } => {
                pages.pop_front();
            }
        }
    }

    fn prepare(&mut self) -> Result<bool> {
        if self.pending.is_some() {
            return Ok(true);
        }
        loop {
            let Some((start, end, rows)) = self.span()? else {
                return Ok(false);
            };
            if start < self.chunk_start || end > self.chunk_end {
                return Err(invalid());
            }
            let mut input = super::super::input::InputFile::from(Arc::clone(&self.file)).cursor();
            let bytes = preflight::read_page_header_bytes(&mut input, start, end, self.limit)
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            let data = start.checked_add(bytes.len() as u64).ok_or_else(invalid)?;
            let mut protocol = TCompactInputProtocol::new(Cursor::new(bytes));
            let header = PageHeader::read_from_in_protocol(&mut protocol)?;
            let size = nonnegative(header.compressed_page_size)?;
            let next = data
                .checked_add(size as u64)
                .filter(|next| *next <= end)
                .ok_or_else(invalid)?;
            if matches!(self.position, Position::Indexed { .. }) && next != end {
                return Err(invalid());
            }
            if header.type_ == PageType::INDEX_PAGE {
                self.advance(next);
                continue;
            }
            // Validate counts before peek exposes them to Arrow's allocation decisions.
            match header.type_ {
                PageType::DICTIONARY_PAGE => {
                    nonnegative(
                        header
                            .dictionary_page_header
                            .as_ref()
                            .ok_or_else(invalid)?
                            .num_values,
                    )?;
                }
                PageType::DATA_PAGE => {
                    nonnegative(
                        header
                            .data_page_header
                            .as_ref()
                            .ok_or_else(invalid)?
                            .num_values,
                    )?;
                }
                PageType::DATA_PAGE_V2 => {
                    let values = header.data_page_header_v2.as_ref().ok_or_else(invalid)?;
                    let count = nonnegative(values.num_values)?;
                    if nonnegative(values.num_nulls)? > count
                        || nonnegative(values.num_rows)? > count
                    {
                        return Err(invalid());
                    }
                    nonnegative(values.definition_levels_byte_length)?;
                    nonnegative(values.repetition_levels_byte_length)?;
                }
                _ => return Err(invalid()),
            }
            if matches!(self.position, Position::Indexed { .. })
                && (rows.is_none() != (header.type_ == PageType::DICTIONARY_PAGE))
            {
                return Err(invalid());
            }
            PageMetadata::try_from(&header)?;
            self.pending = Some(Pending {
                header,
                data,
                next,
                rows,
            });
            return Ok(true);
        }
    }

    fn read_payload(&self, pending: &Pending) -> Result<Bytes> {
        let compressed_size = nonnegative(pending.header.compressed_page_size)?;
        let output_size = nonnegative(pending.header.uncompressed_page_size)?;
        if compressed_size as u64 > self.limit || output_size as u64 > self.limit {
            return Err(invalid());
        }
        let mut input = super::super::input::InputFile::from(Arc::clone(&self.file)).cursor();
        input.seek(SeekFrom::Start(pending.data))?;
        let mut compressed = vec![0; compressed_size];
        input.read_exact(&mut compressed)?;
        if let Some(crc) = pending.header.crc
            && crc32fast::hash(&compressed) != crc as u32
        {
            return Err(ParquetError::General("Page CRC checksum mismatch".into()));
        }
        let (offset, is_compressed) = match pending.header.data_page_header_v2.as_ref() {
            Some(header) => (
                nonnegative(header.definition_levels_byte_length)?
                    .checked_add(nonnegative(header.repetition_levels_byte_length)?)
                    .ok_or_else(invalid)?,
                header.is_compressed.unwrap_or(true),
            ),
            None => (0, true),
        };
        if offset > compressed_size || offset > output_size {
            return Err(invalid());
        }
        if !is_compressed {
            if compressed_size != output_size {
                return Err(invalid());
            }
            return Ok(Bytes::from(compressed));
        }
        let mut output = vec![0; output_size];
        output[..offset].copy_from_slice(&compressed[..offset]);
        match self.compression {
            Compression::GZIP(_) => {
                let mut decoder = flate2::bufread::MultiGzDecoder::new(&compressed[offset..]);
                decoder.read_exact(&mut output[offset..])?;
                let mut extra = [0];
                if decoder.read(&mut extra)? != 0 {
                    return Err(ParquetError::General(
                        "decoded parquet output exceeds its declared size".into(),
                    ));
                }
            }
            Compression::ZSTD(_) if compressed_size != offset || output_size != offset => {
                decode_zstd(
                    &compressed[offset..],
                    &mut output[offset..],
                    self.limit,
                    &self.reserved_bytes,
                )?;
            }
            Compression::ZSTD(_) => {}
            Compression::BROTLI(_) if compressed_size != offset || output_size != offset => {
                decode_brotli(
                    &compressed[offset..],
                    &mut output[offset..],
                    self.limit,
                    &self.reserved_bytes,
                )?;
            }
            Compression::BROTLI(_) => {}
            _ => return Err(invalid()),
        }
        Ok(Bytes::from(output))
    }

    fn page(&self, header: PageHeader, buf: Bytes) -> Result<Page> {
        match header.type_ {
            PageType::DICTIONARY_PAGE => {
                let header = header.dictionary_page_header.ok_or_else(invalid)?;
                let count = nonnegative(header.num_values)?;
                let width = match self.physical_type {
                    Type::BOOLEAN => 1,
                    Type::INT32 | Type::FLOAT => 4,
                    Type::INT64 | Type::DOUBLE => 8,
                    Type::INT96 => std::mem::size_of::<parquet::data_type::Int96>(),
                    Type::BYTE_ARRAY => std::mem::size_of::<parquet::data_type::ByteArray>(),
                    Type::FIXED_LEN_BYTE_ARRAY => {
                        std::mem::size_of::<parquet::data_type::FixedLenByteArray>()
                    }
                };
                if count.checked_mul(width).ok_or_else(invalid)? as u64 > self.limit {
                    return Err(invalid());
                }
                Ok(Page::DictionaryPage {
                    buf,
                    num_values: u32::try_from(count).map_err(|_| invalid())?,
                    encoding: Encoding::try_from(header.encoding)?,
                    is_sorted: header.is_sorted.unwrap_or(false),
                })
            }
            PageType::DATA_PAGE => {
                let header = header.data_page_header.ok_or_else(invalid)?;
                Ok(Page::DataPage {
                    buf,
                    num_values: u32::try_from(header.num_values).map_err(|_| invalid())?,
                    encoding: Encoding::try_from(header.encoding)?,
                    def_level_encoding: Encoding::try_from(header.definition_level_encoding)?,
                    rep_level_encoding: Encoding::try_from(header.repetition_level_encoding)?,
                    statistics: statistics::from_thrift(self.physical_type, header.statistics)?,
                })
            }
            PageType::DATA_PAGE_V2 => {
                let header = header.data_page_header_v2.ok_or_else(invalid)?;
                Ok(Page::DataPageV2 {
                    buf,
                    num_values: u32::try_from(header.num_values).map_err(|_| invalid())?,
                    encoding: Encoding::try_from(header.encoding)?,
                    num_nulls: u32::try_from(header.num_nulls).map_err(|_| invalid())?,
                    num_rows: u32::try_from(header.num_rows).map_err(|_| invalid())?,
                    def_levels_byte_len: u32::try_from(header.definition_levels_byte_length)
                        .map_err(|_| invalid())?,
                    rep_levels_byte_len: u32::try_from(header.repetition_levels_byte_length)
                        .map_err(|_| invalid())?,
                    is_compressed: header.is_compressed.unwrap_or(true),
                    statistics: statistics::from_thrift(self.physical_type, header.statistics)?,
                })
            }
            _ => Err(invalid()),
        }
    }
}

impl Iterator for BoundedPages {
    type Item = Result<Page>;
    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for BoundedPages {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        if !self.prepare()? {
            return Ok(None);
        }
        let pending = self.pending.take().ok_or_else(invalid)?;
        let buf = self.read_payload(&pending)?;
        self.advance(pending.next);
        Ok(Some(self.page(pending.header, buf)?))
    }
    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        if !self.prepare()? {
            return Ok(None);
        }
        let pending = self.pending.as_ref().ok_or_else(invalid)?;
        if matches!(self.position, Position::Indexed { .. }) {
            Ok(Some(PageMetadata {
                num_rows: pending.rows,
                num_levels: None,
                is_dict: pending.header.type_ == PageType::DICTIONARY_PAGE,
            }))
        } else {
            Ok(Some(PageMetadata::try_from(&pending.header)?))
        }
    }
    fn skip_next_page(&mut self) -> Result<()> {
        if self.prepare()? {
            let pending = self.pending.take().ok_or_else(invalid)?;
            self.advance(pending.next);
        }
        Ok(())
    }
    fn at_record_boundary(&mut self) -> Result<bool> {
        match self.position {
            Position::Indexed { .. } => Ok(true),
            Position::Sequential { .. } => Ok(self.peek_next_page()?.is_none()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_scratch_is_released_after_success_and_failure() {
        let input = zstd::bulk::compress(&[7; 8], 1).unwrap();
        let mut concatenated = zstd::bulk::compress(&[7; 4], 1).unwrap();
        concatenated.extend(zstd::bulk::compress(&[7; 4], 1).unwrap());
        let mut skippable = vec![0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 1, 2, 3];
        skippable.extend_from_slice(&input);
        let mut corrupt = input.clone();
        corrupt[0] ^= 0xff;
        let reserved = AtomicU64::new(128);
        let context = zstd::zstd_safe::DCtx::create().sizeof() as u64;
        for (data, length, valid) in [
            (input.as_slice(), 8, true),
            (concatenated.as_slice(), 8, true),
            (skippable.as_slice(), 8, true),
            (corrupt.as_slice(), 8, false),
            (&input[..input.len() - 1], 8, false),
            (input.as_slice(), 7, false),
            (input.as_slice(), 9, false),
        ] {
            let mut output = vec![0; length];
            assert_eq!(
                decode_zstd(data, &mut output, context + 128, &reserved).is_ok(),
                valid
            );
            assert_eq!(reserved.load(Ordering::Relaxed), 128);
            if valid {
                assert_eq!(output, [7; 8]);
            }
        }
        assert!(decode_zstd(&input, &mut [0; 8], context + 127, &reserved).is_err());
        assert_eq!(reserved.load(Ordering::Relaxed), 128);

        let mut codec = parquet::compression::create_codec(
            Compression::BROTLI(Default::default()),
            &parquet::compression::CodecOptions::default(),
        )
        .unwrap()
        .unwrap();
        let mut input = Vec::new();
        codec.compress(&[7; 8], &mut input).unwrap();
        for (data, length, valid) in [
            (input.as_slice(), 8, true),
            (&input[..0], 8, false),
            (&input[..1], 8, false),
            (&input[..input.len() - 1], 8, false),
            (input.as_slice(), 7, false),
            (input.as_slice(), 9, false),
        ] {
            let mut output = vec![0; length];
            assert_eq!(
                decode_brotli(data, &mut output, 6 * 1024 * 1024, &reserved).is_ok(),
                valid
            );
            assert_eq!(reserved.load(Ordering::Relaxed), 128);
            if valid {
                assert_eq!(output, [7; 8]);
            }
        }
        assert!(decode_brotli(&input, &mut [0; 8], 30_240 + 127, &reserved).is_err());
        assert_eq!(reserved.load(Ordering::Relaxed), 128);

        let reserved = AtomicU64::new(8);
        let rejected = Cell::new(None);
        let zero = Cell::new(0);
        let mut allocator = BrotliAllocation {
            inner: StandardAlloc::default(),
            limit: 8,
            reserved: &reserved,
            pre_reserved: &zero,
            rejected: &rejected,
        };
        let failed = <BrotliAllocation as Allocator<u32>>::alloc_cell(&mut allocator, 1);
        assert!(failed.slice().is_empty());
        reserved.store(0, Ordering::Relaxed);
        let later = <BrotliAllocation as Allocator<HuffmanCode>>::alloc_cell(&mut allocator, 1);
        assert!(later.slice().is_empty());
        <BrotliAllocation as Allocator<u32>>::free_cell(&mut allocator, failed);
        <BrotliAllocation as Allocator<HuffmanCode>>::free_cell(&mut allocator, later);
        assert_eq!(reserved.load(Ordering::Relaxed), 0);
        assert_eq!(rejected.get(), Some(12));
    }
}
