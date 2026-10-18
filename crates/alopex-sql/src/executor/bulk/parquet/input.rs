use std::fs::File;
use std::io::{self, BufReader, Read};
use std::sync::Arc;

use bytes::Bytes;
use parquet::errors::Result;
use parquet::file::reader::{ChunkReader, Length};

pub(super) struct InputFile(Arc<File>);

impl InputFile {
    pub(super) fn raw_file(&self) -> &Arc<File> {
        &self.0
    }

    pub(super) const fn allocation_bytes() -> usize {
        std::mem::size_of::<(usize, usize, File)>() + std::mem::size_of::<(usize, usize, Self)>()
    }
}

impl From<File> for InputFile {
    fn from(file: File) -> Self {
        Self(Arc::new(file))
    }
}

impl Length for InputFile {
    fn len(&self) -> u64 {
        self.0.len()
    }
}

impl ChunkReader for InputFile {
    type T = BufReader<PositionedRead>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        // Some PageReader paths inspect only headers. Keep their read-ahead small;
        // larger valid headers refill the buffer without a size restriction.
        Ok(BufReader::with_capacity(
            512,
            PositionedRead {
                file: Arc::clone(&self.0),
                position: start,
            },
        ))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let mut bytes = vec![0; length];
        PositionedRead {
            file: Arc::clone(&self.0),
            position: start,
        }
        .read_exact(&mut bytes)?;
        Ok(bytes.into())
    }
}

pub(super) struct PositionedRead {
    file: Arc<File>,
    position: u64,
}

impl Read for PositionedRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        #[cfg(unix)]
        let count = {
            use std::os::unix::fs::FileExt;
            self.file.read_at(buffer, self.position)?
        };
        #[cfg(windows)]
        let count = {
            use std::os::windows::fs::FileExt;
            self.file.seek_read(buffer, self.position)?
        };
        #[cfg(not(any(unix, windows)))]
        let count = {
            use std::io::{Seek, SeekFrom};
            let mut file = self.file.try_clone()?;
            file.seek(SeekFrom::Start(self.position))?;
            file.read(buffer)?
        };
        self.position = self.position.checked_add(count as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "parquet input offset overflow")
        })?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use parquet::file::reader::{ChunkReader, Length};

    use super::InputFile;

    #[test]
    fn page_header_reads_do_not_prefetch_large_payloads() {
        use std::io::Cursor;
        use std::sync::Arc;

        use parquet::basic::{Repetition, Type};
        use parquet::data_type::{ByteArray, ByteArrayType};
        use parquet::file::properties::{EnabledStatistics, WriterProperties};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::format::PageHeader;
        use parquet::schema::types;
        use parquet::thrift::TSerializable;
        use thrift::protocol::TCompactInputProtocol;

        for statistics in [EnabledStatistics::None, EnabledStatistics::Page] {
            let field = Arc::new(
                types::Type::primitive_type_builder("value", Type::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            );
            let schema = Arc::new(
                types::Type::group_type_builder("fixture")
                    .with_fields(vec![field])
                    .build()
                    .unwrap(),
            );
            let properties = WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_statistics_enabled(statistics)
                .set_max_statistics_size(64 * 1024)
                .set_statistics_truncate_length(None)
                .build();
            let mut bytes = Vec::new();
            let mut writer =
                SerializedFileWriter::new(&mut bytes, schema, Arc::new(properties)).unwrap();
            let mut group = writer.next_row_group().unwrap();
            let mut column = group.next_column().unwrap().unwrap();
            column
                .typed::<ByteArrayType>()
                .write_batch(&[ByteArray::from(vec![b'x'; 16 * 1024])], None, None)
                .unwrap();
            column.close().unwrap();
            group.close().unwrap();
            let footer = writer.close().unwrap();
            let start = footer.row_groups[0].columns[0]
                .meta_data
                .as_ref()
                .unwrap()
                .data_page_offset as usize;
            let mut cursor = Cursor::new(&bytes[start..]);
            let expected =
                PageHeader::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut cursor))
                    .unwrap();
            let header_size = cursor.position() as usize;
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&bytes).unwrap();
            let source = InputFile::from(file);
            let mut input = source.get_read(start as u64).unwrap();
            let actual =
                PageHeader::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut input))
                    .unwrap();
            assert_eq!(actual, expected);
            let fetched = input.get_ref().position - start as u64;
            println!(
                "statistics={statistics:?} header_bytes={header_size} fetched_bytes={}",
                fetched
            );
            if statistics == EnabledStatistics::None {
                assert!(header_size < 128);
                assert!(
                    fetched <= 1024,
                    "header-only reads must avoid fetching a whole data payload"
                );
            } else {
                assert!(
                    header_size > 16 * 1024,
                    "large valid headers must refill the buffer"
                );
            }
            let mut payload = [0; 16];
            input.read_exact(&mut payload).unwrap();
            assert_eq!(
                &payload,
                &bytes[start + header_size..start + header_size + 16]
            );
        }
    }

    #[test]
    fn positioned_ranges_preserve_independent_offsets() {
        let data = (0..1024)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&data).unwrap();
        let input = InputFile::from(file);
        assert_eq!(input.len(), data.len() as u64);
        let mut first = input.get_read(0).unwrap();
        let mut second = input.get_read(37).unwrap();
        let mut bytes = [0; 16];
        first.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, &data[..16]);
        second.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, &data[37..53]);
        assert_eq!(input.get_bytes(61, 7).unwrap().as_ref(), &data[61..68]);
        first.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, &data[16..32]);
        second.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, &data[53..69]);
        assert!(input.get_bytes(0, 0).unwrap().is_empty());
        assert!(input.get_bytes((data.len() - 3) as u64, 4).is_err());
        assert_eq!(
            input
                .get_read(data.len() as u64)
                .unwrap()
                .read(&mut bytes)
                .unwrap(),
            0
        );
        assert_eq!(first.read(&mut []).unwrap(), 0);
    }
}
