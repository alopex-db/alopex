//! カラムナーセグメント用の Section 0x03 ラッパー。

use bincode::Options;

use crate::columnar::segment_v2::ColumnSegmentV2;
use crate::storage::format::{
    bincode_config, AlopexFileReader, AlopexFileWriter, FileReader, FormatError, SectionEntry,
    SectionType,
};

/// セクションタイプ定数（0x03）。
pub const SECTION_TYPE_COLUMNAR: u8 = 0x03;

/// カラムナーセクションの書き込みヘルパー。
pub struct ColumnarSectionWriter;

impl ColumnarSectionWriter {
    /// セグメントを Section 0x03 として追加し、セクションIDを返す。
    pub fn write_section(
        file: &mut AlopexFileWriter,
        segment: &ColumnSegmentV2,
    ) -> Result<u32, FormatError> {
        let bytes = bincode_config()
            .serialize(segment)
            .map_err(|_| FormatError::IncompleteWrite)?;
        file.add_section(SectionType::ColumnarSegment, &bytes, true)
    }
}

/// カラムナーセクションの読み取りヘルパー。
pub struct ColumnarSectionReader;

impl ColumnarSectionReader {
    /// SectionEntry を指定して ColumnSegmentV2 を復元する。
    pub fn read_section(
        file: &AlopexFileReader,
        entry: &SectionEntry,
    ) -> Result<ColumnSegmentV2, FormatError> {
        let bytes = file.read_section(entry.section_id)?;
        bincode_config()
            .deserialize(&bytes)
            .map_err(|_| FormatError::IncompleteWrite)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::encoding::{Column, LogicalType};
    use crate::columnar::segment_v2::{ColumnSchema, RecordBatch, Schema, SegmentWriterV2};
    use crate::storage::format::{FileFlags, FileReader, FileSource, FileVersion};
    use tempfile::tempdir;

    fn make_segment() -> ColumnSegmentV2 {
        let schema = Schema {
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    logical_type: LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
                ColumnSchema {
                    name: "val".into(),
                    logical_type: LogicalType::Int64,
                    nullable: false,
                    fixed_len: None,
                },
            ],
        };
        let batch = RecordBatch::new(
            schema,
            vec![
                Column::Int64(vec![1, 2, 3]),
                Column::Int64(vec![10, 20, 30]),
            ],
            vec![None, None],
        );
        let mut writer = SegmentWriterV2::new(Default::default());
        writer.write_batch(batch).unwrap();
        writer.finish().unwrap()
    }

    #[test]
    fn test_section_0x03_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.alopex");
        let mut writer =
            AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).unwrap();
        let segment = make_segment();
        let section_id = ColumnarSectionWriter::write_section(&mut writer, &segment).unwrap();
        writer.finalize().unwrap();

        let reader = AlopexFileReader::open(FileSource::Path(path)).unwrap();
        let entry = reader
            .section_index()
            .find_by_id(section_id)
            .expect("section entry");
        assert_eq!(entry.section_type, SectionType::ColumnarSegment);

        let recovered = ColumnarSectionReader::read_section(&reader, entry).unwrap();
        let reader_v2 = crate::columnar::segment_v2::SegmentReaderV2::open(Box::new(
            crate::columnar::segment_v2::InMemorySegmentSource::new(recovered.data.clone()),
        ))
        .unwrap();
        let batches = reader_v2.read_columns(&[0, 1]).unwrap();
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_alopex_file_with_columnar_section() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col2.alopex");
        let mut writer =
            AlopexFileWriter::new(path.clone(), FileVersion::CURRENT, FileFlags(0)).unwrap();
        let section_id =
            ColumnarSectionWriter::write_section(&mut writer, &make_segment()).unwrap();
        writer.finalize().unwrap();

        let reader = AlopexFileReader::open(FileSource::Path(path)).unwrap();
        let entries = reader
            .section_index()
            .filter_by_type(SectionType::ColumnarSegment);
        assert_eq!(entries.len(), 1);
        let entry = entries[0];
        assert_eq!(entry.section_id, section_id);

        let recovered = ColumnarSectionReader::read_section(&reader, entry).unwrap();
        let reader_v2 = crate::columnar::segment_v2::SegmentReaderV2::open(Box::new(
            crate::columnar::segment_v2::InMemorySegmentSource::new(recovered.data.clone()),
        ))
        .unwrap();
        let batches = reader_v2.read_columns(&[1]).unwrap();
        if let Column::Int64(vals) = &batches[0].columns[0] {
            assert_eq!(vals, &vec![10, 20, 30]);
        } else {
            panic!("expected int64 column");
        }
    }
}
