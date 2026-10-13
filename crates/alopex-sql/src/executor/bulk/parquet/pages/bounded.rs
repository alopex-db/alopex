use std::collections::VecDeque;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::Arc;

use brotli_decompressor::{
    Allocator, BrotliDecompressStream, BrotliResult, BrotliState, StandardAlloc,
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

struct BrotliBytes {
    inner: StandardAlloc,
    limit: u64,
    rejected: Option<usize>,
}

impl Allocator<u8> for BrotliBytes {
    type AllocatedMemory = <StandardAlloc as Allocator<u8>>::AllocatedMemory;

    fn alloc_cell(&mut self, count: usize) -> Self::AllocatedMemory {
        // The pinned decoder checks empty u8 allocations before accessing them.
        if count as u64 > self.limit {
            self.rejected = Some(count);
            return self.inner.alloc_cell(0);
        }
        self.inner.alloc_cell(count)
    }

    fn free_cell(&mut self, memory: Self::AllocatedMemory) {
        self.inner.free_cell(memory);
    }
}

fn decode_brotli(input: &[u8], output: &mut [u8], limit: u64) -> Result<()> {
    let mut state = BrotliState::new(
        BrotliBytes {
            inner: StandardAlloc::default(),
            limit,
            rejected: None,
        },
        StandardAlloc::default(),
        StandardAlloc::default(),
    );
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
    if let Some(bytes) = state.alloc_u8.rejected {
        return Err(ParquetError::External(Box::new(
            crate::executor::ExecutorError::ResourceExhausted {
                message: format!("READ_PARQUET Brotli requires {bytes} bytes (limit {limit})"),
            },
        )));
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
}

impl BoundedPages {
    pub(super) fn new(
        file: Arc<File>,
        column: &ColumnChunkMetaData,
        total_rows: usize,
        locations: Option<Vec<PageLocation>>,
        limit: u64,
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
            let mut input = self.file.try_clone()?;
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
        let mut input = self.file.try_clone()?;
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
                let decoded = zstd::bulk::Decompressor::new()?
                    .decompress_to_buffer(&compressed[offset..], &mut output[offset..])?;
                if decoded != output_size - offset {
                    return Err(invalid());
                }
            }
            Compression::ZSTD(_) => {}
            Compression::BROTLI(_) if compressed_size != offset || output_size != offset => {
                decode_brotli(&compressed[offset..], &mut output[offset..], self.limit)?;
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
