//! Segment V2 のデータモデル・ヘッダ定義と簡易Writer/Reader。
//!
//! セグメントのフォーマットメタデータ、RowGroup/Column チャンクの
//! オフセットテーブル、チェックサム設定を扱い、メモリバッファベースで
//! 読み書きできる最小限の実装を提供する。

use std::collections::HashSet;
use std::convert::TryFrom;
use std::io::{Cursor, Read};

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::columnar::encoding::{Column, LogicalType};
use crate::columnar::encoding_v2::{
    create_decoder, create_encoder, select_encoding, Bitmap, Decoder, Encoder, EncodingHints,
    EncodingV2,
};
use crate::columnar::error::{ColumnarError, Result};
use crate::storage::compression::{create_compressor, CompressionV2};

/// Segment V2 のマジックバイト。
pub const SEGMENT_MAGIC: &[u8; 4] = b"ALXC";
/// Segment V2 のフォーマットバージョン。
pub const SEGMENT_FORMAT_VERSION_V2: u16 = 2;
/// ヘッダの固定長（24バイト）。
pub const SEGMENT_HEADER_SIZE: usize = 24;
/// RowID のセグメントID割り当てビット数（上位 20bit）。
pub const ROW_ID_SEGMENT_BITS: u8 = 20;
/// RowID のセグメント内オフセットビット数（下位 44bit）。
pub const ROW_ID_OFFSET_BITS: u8 = 44;
const ROW_ID_OFFSET_MASK: u64 = (1u64 << ROW_ID_OFFSET_BITS) - 1;
const ROW_ID_SEGMENT_MASK: u64 = (1u64 << ROW_ID_SEGMENT_BITS) - 1;

/// RowID をエンコードする（segment_id << ROW_ID_OFFSET_BITS | local_offset）。
pub fn encode_row_id(segment_id: u64, local_offset: u64) -> Result<u64> {
    if segment_id > ROW_ID_SEGMENT_MASK {
        return Err(ColumnarError::InvalidFormat(format!(
            "segment_id overflow for RowID: {segment_id} > {ROW_ID_SEGMENT_MASK}"
        )));
    }
    if local_offset > ROW_ID_OFFSET_MASK {
        return Err(ColumnarError::InvalidFormat(format!(
            "row offset overflow for RowID: {local_offset} > {ROW_ID_OFFSET_MASK}"
        )));
    }
    Ok((segment_id << ROW_ID_OFFSET_BITS) | local_offset)
}

/// RowID から (segment_id, local_offset) をデコードする。
pub fn decode_row_id(row_id: u64) -> (u64, u64) {
    let segment_id = row_id >> ROW_ID_OFFSET_BITS;
    let local_offset = row_id & ROW_ID_OFFSET_MASK;
    (segment_id, local_offset)
}

/// チェックサムの適用範囲。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumScope {
    /// チェックサムなし。
    None = 0,
    /// フッターのみチェックサム付与。
    Footer = 1,
    /// 各チャンクにチェックサム付与。
    Chunk = 2,
}

impl From<ChecksumScope> for u8 {
    fn from(scope: ChecksumScope) -> Self {
        scope as u8
    }
}

impl TryFrom<u8> for ChecksumScope {
    type Error = ColumnarError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(ChecksumScope::None),
            1 => Ok(ChecksumScope::Footer),
            2 => Ok(ChecksumScope::Chunk),
            other => Err(ColumnarError::InvalidFormat(format!(
                "unknown checksum scope: {other}"
            ))),
        }
    }
}

/// セグメントヘッダ（固定長 24 バイト）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentHeader {
    /// マジック "ALXC"。
    pub magic: [u8; 4],
    /// フォーマットバージョン (u16)。
    pub format_version: u16,
    /// カラム数 (u16)。
    pub column_count: u16,
    /// 総行数 (u64)。
    pub row_count: u64,
    /// RowGroup のターゲット行数 (u32)。
    pub row_group_size: u32,
    /// チェックサムスコープ。
    pub checksum_scope: ChecksumScope,
    /// デフォルト圧縮形式。
    pub compression: CompressionV2,
    /// 予約フィールド（24 バイト揃えのためのパディング）。
    pub reserved: [u8; 2],
}

impl SegmentHeader {
    /// V2 デフォルト値を埋めたヘッダを生成する。
    pub fn new(
        column_count: u16,
        row_count: u64,
        row_group_size: u32,
        checksum_scope: ChecksumScope,
        compression: CompressionV2,
    ) -> Self {
        Self {
            magic: *SEGMENT_MAGIC,
            format_version: SEGMENT_FORMAT_VERSION_V2,
            column_count,
            row_count,
            row_group_size,
            checksum_scope,
            compression,
            reserved: [0u8; 2],
        }
    }
}

/// シリアライズヘッダをバイト配列へ書き込む。
fn write_header(header: &SegmentHeader, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&header.magic);
    buf.extend_from_slice(&header.format_version.to_le_bytes());
    buf.extend_from_slice(&header.column_count.to_le_bytes());
    buf.extend_from_slice(&header.row_count.to_le_bytes());
    buf.extend_from_slice(&header.row_group_size.to_le_bytes());
    buf.push(header.checksum_scope as u8);
    buf.push(match header.compression {
        CompressionV2::None => 0,
        CompressionV2::Lz4 => 1,
        CompressionV2::Zstd { .. } => 2,
    });
    buf.extend_from_slice(&header.reserved);
}

/// Column 長を返すヘルパー。
fn column_len(column: &Column) -> usize {
    match column {
        Column::Int64(v) => v.len(),
        Column::Float32(v) => v.len(),
        Column::Float64(v) => v.len(),
        Column::Bool(v) => v.len(),
        Column::Binary(v) => v.len(),
        Column::Fixed { values, .. } => values.len(),
    }
}

/// Column をスライスするヘルパー。
fn slice_column(column: &Column, start: usize, len: usize) -> Result<Column> {
    match column {
        Column::Int64(v) => Ok(Column::Int64(v[start..start + len].to_vec())),
        Column::Float32(v) => Ok(Column::Float32(v[start..start + len].to_vec())),
        Column::Float64(v) => Ok(Column::Float64(v[start..start + len].to_vec())),
        Column::Bool(v) => Ok(Column::Bool(v[start..start + len].to_vec())),
        Column::Binary(v) => Ok(Column::Binary(v[start..start + len].to_vec())),
        Column::Fixed { values, len: fixed } => Ok(Column::Fixed {
            len: *fixed,
            values: values[start..start + len].to_vec(),
        }),
    }
}

fn slice_bitmap(bitmap: &Bitmap, start: usize, len: usize) -> Bitmap {
    let mut v = Vec::with_capacity(len);
    for i in start..start + len {
        v.push(bitmap.get(i));
    }
    Bitmap::from_bools(&v)
}

/// エンコーディング選択用の簡易ヒントを構築する。
fn build_encoding_hints(column: &Column, bitmap: Option<&Bitmap>) -> EncodingHints {
    let mut hints = EncodingHints::default();
    let mut last: Option<i64> = None;
    let mut distinct_int = HashSet::new();
    let mut distinct_bool = HashSet::new();

    match column {
        Column::Int64(values) => {
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for (i, v) in values.iter().enumerate() {
                if let Some(bm) = bitmap {
                    if !bm.get(i) {
                        continue;
                    }
                }
                min = min.min(*v);
                max = max.max(*v);
                hints.total_count += 1;
                distinct_int.insert(*v);
                if let Some(prev) = last {
                    if prev > *v {
                        hints.is_sorted = false;
                    }
                } else {
                    hints.is_sorted = true;
                }
                last = Some(*v);
            }
            hints.distinct_count = distinct_int.len();
            if hints.total_count > 0 {
                hints.value_range = Some((max - min) as u64);
            }
        }
        Column::Float64(values) => {
            hints.total_count = values.len();
            hints.distinct_count = values.len(); // 粗い推定
            hints.is_sorted = values.windows(2).all(|w| {
                w[0].partial_cmp(&w[1])
                    .map(|o| o != std::cmp::Ordering::Greater)
                    .unwrap_or(true)
            });
        }
        Column::Float32(values) => {
            hints.total_count = values.len();
            hints.distinct_count = values.len();
            hints.is_sorted = values.windows(2).all(|w| {
                w[0].partial_cmp(&w[1])
                    .map(|o| o != std::cmp::Ordering::Greater)
                    .unwrap_or(true)
            });
        }
        Column::Bool(values) => {
            for (i, v) in values.iter().enumerate() {
                if let Some(bm) = bitmap {
                    if !bm.get(i) {
                        continue;
                    }
                }
                hints.total_count += 1;
                distinct_bool.insert(*v);
            }
            hints.distinct_count = distinct_bool.len();
            hints.is_sorted = true;
        }
        Column::Binary(values) => {
            hints.total_count = values.len();
            hints.distinct_count = values.len(); // 粗い推定
            hints.is_sorted = values.windows(2).all(|w| w[0] <= w[1]);
        }
        Column::Fixed { values, .. } => {
            hints.total_count = values.len();
            hints.distinct_count = values.len();
            hints.is_sorted = values.windows(2).all(|w| w[0] <= w[1]);
        }
    }

    hints
}

/// バイト列からヘッダを復元する。
fn read_header(bytes: &[u8]) -> Result<SegmentHeader> {
    if bytes.len() < SEGMENT_HEADER_SIZE {
        return Err(ColumnarError::InvalidFormat("header too short".into()));
    }
    let mut cur = Cursor::new(bytes);
    let mut magic = [0u8; 4];
    cur.read_exact(&mut magic)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read magic failed: {e}")))?;
    if &magic != SEGMENT_MAGIC {
        return Err(ColumnarError::InvalidFormat("invalid segment magic".into()));
    }
    let mut u16buf = [0u8; 2];
    cur.read_exact(&mut u16buf)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read version failed: {e}")))?;
    let format_version = u16::from_le_bytes(u16buf);

    let mut u16buf = [0u8; 2];
    cur.read_exact(&mut u16buf)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read column_count failed: {e}")))?;
    let column_count = u16::from_le_bytes(u16buf);

    let mut u64buf = [0u8; 8];
    cur.read_exact(&mut u64buf)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read row_count failed: {e}")))?;
    let row_count = u64::from_le_bytes(u64buf);

    let mut u32buf = [0u8; 4];
    cur.read_exact(&mut u32buf)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read row_group_size failed: {e}")))?;
    let row_group_size = u32::from_le_bytes(u32buf);

    let mut scope_byte = [0u8; 1];
    cur.read_exact(&mut scope_byte)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read checksum_scope failed: {e}")))?;
    let checksum_scope = ChecksumScope::try_from(scope_byte[0])?;

    let mut comp_byte = [0u8; 1];
    cur.read_exact(&mut comp_byte)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read compression failed: {e}")))?;
    let compression = match comp_byte[0] {
        0 => CompressionV2::None,
        1 => CompressionV2::Lz4,
        2 => CompressionV2::Zstd { level: 3 },
        other => {
            return Err(ColumnarError::InvalidFormat(format!(
                "unknown compression id: {other}"
            )))
        }
    };

    let mut reserved = [0u8; 2];
    cur.read_exact(&mut reserved)
        .map_err(|e| ColumnarError::InvalidFormat(format!("read reserved failed: {e}")))?;

    Ok(SegmentHeader {
        magic,
        format_version,
        column_count,
        row_count,
        row_group_size,
        checksum_scope,
        compression,
        reserved,
    })
}

/// セグメント内のカラムスキーマ。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// カラム名。
    pub name: String,
    /// 論理型。
    pub logical_type: LogicalType,
    /// NULL 許容フラグ。
    pub nullable: bool,
    /// 固定長型の場合のバイト長。
    pub fixed_len: Option<u32>,
}

/// セグメント全体のスキーマ。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    /// カラム定義一覧。
    pub columns: Vec<ColumnSchema>,
}

impl Schema {
    /// カラム数を返す。
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// セグメントメタデータ (V2)。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentMetaV2 {
    /// フォーマットバージョン (u16)。
    pub format_version: u16,
    /// セグメントのスキーマ。
    pub schema: Schema,
    /// 総行数。
    pub num_rows: u64,
    /// 作成タイムスタンプ (Unix epoch millis)。
    pub created_at: u64,
    /// 非圧縮サイズ。
    pub uncompressed_size: u64,
    /// 圧縮後サイズ。
    pub compressed_size: u64,
    /// RowGroup メタデータ一覧。
    pub row_groups: Vec<RowGroupMeta>,
}

/// RowGroup 単位のメタデータ。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowGroupMeta {
    /// RowGroup の開始行インデックス。
    pub row_start: u64,
    /// RowGroup 内の行数。
    pub row_count: u64,
    /// RowGroup 圧縮サイズ。
    pub compressed_size: u64,
    /// カラムチャンクメタデータ。
    pub column_chunks: Vec<ColumnChunkMeta>,
    /// checksum_scope = Chunk の場合のチェックサム。
    pub checksum: Option<u32>,
}

/// カラムチャンクメタデータ。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ColumnChunkMeta {
    /// カラムインデックス (schema 基準)。
    pub column_index: u16,
    /// 使用したエンコーディング。
    pub encoding: EncodingV2,
    /// 使用した圧縮。
    pub compression: CompressionV2,
    /// セグメント先頭からのオフセット。
    pub offset: u64,
    /// 圧縮後サイズ。
    pub compressed_size: u64,
    /// 非圧縮サイズ。
    pub uncompressed_size: u64,
    /// NULL の件数。
    pub null_count: u64,
    /// 辞書ページオフセット（辞書利用時のみ）。
    pub dictionary_offset: Option<u64>,
}

/// RowGroup テーブル（ランダムアクセス用オフセットテーブル）。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowGroupTable {
    /// RowGroup テーブルのエントリ一覧。
    pub entries: Vec<RowGroupTableEntry>,
}

/// RowGroup テーブルの 1 エントリ。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RowGroupTableEntry {
    /// RowGroup の開始行。
    pub row_start: u64,
    /// RowGroup 行数。
    pub row_count: u64,
    /// セグメント先頭からの RowGroup データ開始オフセット。
    pub data_offset: u64,
    /// RowGroup の圧縮サイズ合計。
    pub compressed_size: u64,
    /// RowGroup 内の各カラムチャンクの相対オフセット。
    pub column_chunk_offsets: Vec<ColumnChunkOffset>,
    /// checksum_scope = Chunk の場合のチェックサム。
    pub checksum: Option<u32>,
}

/// RowGroup 内のカラムチャンク位置。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ColumnChunkOffset {
    /// カラムインデックス。
    pub column_idx: u16,
    /// RowGroup データ先頭からのオフセット。
    pub offset: u64,
    /// カラムチャンクの圧縮サイズ。
    pub length: u64,
    /// カラムチャンクの非圧縮サイズ。
    pub uncompressed_length: u64,
    /// チャンクチェックサム (checksum_scope = Chunk の場合)。
    pub checksum: Option<u32>,
}

/// カラムディスクリプタ集合（列単位メタデータ）。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ColumnDescriptors {
    /// カラムディスクリプタ一覧。
    pub columns: Vec<ColumnDescriptor>,
}

/// 単一カラムのメタデータ。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ColumnDescriptor {
    /// カラムインデックス。
    pub column_idx: u16,
    /// 論理型。
    pub logical_type: LogicalType,
    /// 使用エンコーディング。
    pub encoding: EncodingV2,
    /// 使用圧縮。
    pub compression: CompressionV2,
    /// NULL 許容フラグ。
    pub nullable: bool,
    /// 固定長型のバイト長。
    pub fixed_len: Option<u32>,
    /// 辞書オフセット（辞書利用時）。
    pub dictionary_offset: Option<u64>,
    /// セグメント内でのデータ開始オフセット（最初の RowGroup）。
    pub data_offset: u64,
    /// 全 RowGroup 合計のデータ長。
    pub data_length: u64,
}

/// フッター（RowGroupTable と ColumnDescriptors をまとめたもの）。
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SegmentFooter {
    /// RowGroup テーブル。
    pub row_group_table: RowGroupTable,
    /// カラムディスクリプタ。
    pub column_descriptors: ColumnDescriptors,
}

/// レコードバッチ（単純な Schema + Column + null bitmap の組）。
#[derive(Clone, Debug)]
pub struct RecordBatch {
    /// スキーマ。
    pub schema: Schema,
    /// カラムデータ。
    pub columns: Vec<Column>,
    /// NULL ビットマップ。
    pub null_bitmaps: Vec<Option<Bitmap>>,
    /// 行ID（RowIDモードで利用）。
    pub row_ids: Option<Vec<u64>>,
}

impl RecordBatch {
    /// 新規作成。カラム数とビットマップ数がスキーマと一致することを前提にする。
    pub fn new(schema: Schema, columns: Vec<Column>, null_bitmaps: Vec<Option<Bitmap>>) -> Self {
        Self {
            schema,
            columns,
            null_bitmaps,
            row_ids: None,
        }
    }

    /// 行数を返す（先頭カラム長で代表）。
    pub fn num_rows(&self) -> usize {
        self.columns.first().map(column_len).unwrap_or_default()
    }

    /// RowID を埋め込んだ新しいバッチを返す。
    pub fn with_row_ids(mut self, row_ids: Option<Vec<u64>>) -> Self {
        self.row_ids = row_ids;
        self
    }
}

/// SegmentWriterV2 の構成。
#[derive(Clone, Debug)]
pub struct SegmentConfigV2 {
    /// RowGroup のターゲット行数。
    pub row_group_size: u64,
    /// デフォルト圧縮。
    pub compression: CompressionV2,
    /// チェックサムスコープ。
    pub checksum_scope: ChecksumScope,
    /// 圧縮後サイズの上限（デフォルト 16MiB）。
    pub max_row_group_bytes: u64,
}

impl Default for SegmentConfigV2 {
    fn default() -> Self {
        Self {
            row_group_size: 100_000,
            compression: CompressionV2::None,
            checksum_scope: ChecksumScope::Footer,
            max_row_group_bytes: 16 * 1024 * 1024,
        }
    }
}

/// セグメント出力（メモリバッファ）。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnSegmentV2 {
    /// ヘッダ。
    pub header: SegmentHeader,
    /// メタデータ。
    pub meta: SegmentMetaV2,
    /// セグメント全体のバイト列。
    pub data: Vec<u8>,
    /// RowID（セグメント内オフセットまたはエンコード済み）を保持する。無い場合は空。
    #[serde(default)]
    pub row_ids: Vec<u64>,
}

/// メモリバッファを入力とする SegmentSource 実装。
#[derive(Debug)]
pub struct InMemorySegmentSource {
    data: Vec<u8>,
}

impl InMemorySegmentSource {
    /// 新しいメモリソースを作成。
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

/// セグメント読み込み元を抽象化する。
pub trait SegmentSource: Send + Sync + std::fmt::Debug {
    /// 指定範囲を読み取る。
    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>>;
    /// 全体サイズを返す。
    fn total_size(&self) -> u64;
}

impl SegmentSource for InMemorySegmentSource {
    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let start = offset as usize;
        let end = start + len as usize;
        if end > self.data.len() {
            return Err(ColumnarError::InvalidFormat("range out of bounds".into()));
        }
        Ok(self.data[start..end].to_vec())
    }

    fn total_size(&self) -> u64 {
        self.data.len() as u64
    }
}

/// SegmentWriterV2 実装。
#[derive(Debug)]
pub struct SegmentWriterV2 {
    config: SegmentConfigV2,
    buffer: Vec<RecordBatch>,
}

impl SegmentWriterV2 {
    /// 新しい Writer を生成。
    pub fn new(config: SegmentConfigV2) -> Self {
        Self {
            config,
            buffer: Vec::new(),
        }
    }

    /// バッチを追加する。行数はスキーマと整合している前提。
    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.buffer.push(batch);
        Ok(())
    }

    /// セグメントを書き出し、ColumnSegmentV2 を返す。
    pub fn finish(mut self) -> Result<ColumnSegmentV2> {
        // すべて同じスキーマであることを前提に整合性チェック。
        let schema = self
            .buffer
            .first()
            .ok_or_else(|| ColumnarError::InvalidFormat("no batches".into()))?
            .schema
            .clone();
        for b in &self.buffer {
            if b.schema.column_count() != schema.column_count() {
                return Err(ColumnarError::InvalidFormat("schema mismatch".into()));
            }
        }

        // RowGroup 分割
        let row_group_size = self.config.row_group_size as usize;
        let mut row_groups: Vec<RecordBatch> = Vec::new();
        for batch in self.buffer.drain(..) {
            let rows = batch.num_rows();
            let mut offset = 0;
            while offset < rows {
                let end = usize::min(offset + row_group_size, rows);
                let mut cols = Vec::new();
                for col in &batch.columns {
                    cols.push(slice_column(col, offset, end - offset)?);
                }
                let mut bitmaps = Vec::new();
                for bm in &batch.null_bitmaps {
                    let sliced = bm.as_ref().map(|b| slice_bitmap(b, offset, end - offset));
                    bitmaps.push(sliced);
                }
                row_groups.push(RecordBatch::new(batch.schema.clone(), cols, bitmaps));
                offset = end;
            }
        }

        let total_rows: u64 = row_groups.iter().map(|rg| rg.num_rows() as u64).sum();

        // ヘッダとスキーマ
        let header = SegmentHeader::new(
            schema.column_count() as u16,
            total_rows,
            self.config.row_group_size as u32,
            self.config.checksum_scope,
            self.config.compression,
        );

        let schema_bytes =
            bincode::serialize(&schema).map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
        let schema_len = schema_bytes.len() as u32;

        // データ部とメタ
        let mut data = Vec::new();
        write_header(&header, &mut data);
        data.extend_from_slice(&schema_len.to_le_bytes());
        data.extend_from_slice(&schema_bytes);

        let mut row_group_table_entries = Vec::new();
        let mut column_descriptors = Vec::new();
        column_descriptors.resize(
            schema.column_count(),
            ColumnDescriptor {
                column_idx: 0,
                logical_type: LogicalType::Int64,
                encoding: EncodingV2::Plain,
                compression: self.config.compression,
                nullable: false,
                fixed_len: None,
                dictionary_offset: None,
                data_offset: 0,
                data_length: 0,
            },
        );

        let mut current_offset = data.len() as u64;
        let mut row_start = 0u64;
        let mut total_uncompressed = 0u64;
        let mut queue: std::collections::VecDeque<RecordBatch> = row_groups.into_iter().collect();

        let mut meta_row_groups = Vec::new();
        while let Some(rg) = queue.pop_front() {
            let mut rg_buffer = Vec::new();
            let mut rg_uncompressed_size = 0u64;
            let mut pending_chunks = Vec::new();

            for (col_idx, col) in rg.columns.iter().enumerate() {
                let null_bitmap = rg.null_bitmaps.get(col_idx).and_then(|b| b.as_ref());
                let hints = build_encoding_hints(col, null_bitmap);
                let encoding = select_encoding(schema.columns[col_idx].logical_type, &hints);
                let encoder: Box<dyn Encoder> = create_encoder(encoding);
                let encoded = encoder
                    .encode(col, null_bitmap)
                    .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
                let uncompressed_len = encoded.len() as u64;
                rg_uncompressed_size += uncompressed_len;

                let compressed = if let CompressionV2::None = self.config.compression {
                    encoded
                } else {
                    let compressor = create_compressor(self.config.compression)
                        .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
                    compressor
                        .compress(&encoded)
                        .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?
                };

                let chunk_offset = rg_buffer.len() as u64;
                let chunk_checksum = if self.config.checksum_scope == ChecksumScope::Chunk {
                    let mut hasher = Hasher::new();
                    hasher.update(&compressed);
                    Some(hasher.finalize())
                } else {
                    None
                };

                let chunk_len = compressed.len() as u64;
                rg_buffer.extend_from_slice(&compressed);

                pending_chunks.push((
                    col_idx,
                    encoding,
                    chunk_offset,
                    chunk_len,
                    uncompressed_len,
                    chunk_checksum,
                    null_bitmap.map(|b| b.null_count() as u64).unwrap_or(0),
                    compressed,
                ));
            }

            let rg_compressed_size = rg_buffer.len() as u64;

            if self.config.max_row_group_bytes > 0
                && rg_compressed_size > self.config.max_row_group_bytes
            {
                // 圧縮後サイズが上限を超えたら分割を試みる。1 行なら分割不可なのでエラー。
                if rg.num_rows() <= 1 {
                    return Err(ColumnarError::RowGroupTooLarge {
                        size: rg_compressed_size,
                        max: self.config.max_row_group_bytes,
                    });
                }
                let mid = rg.num_rows() / 2;
                let mut left_cols = Vec::new();
                let mut right_cols = Vec::new();
                for col in &rg.columns {
                    left_cols.push(slice_column(col, 0, mid)?);
                    right_cols.push(slice_column(col, mid, rg.num_rows() - mid)?);
                }
                let mut left_bm = Vec::new();
                let mut right_bm = Vec::new();
                for bm in &rg.null_bitmaps {
                    match bm {
                        Some(b) => {
                            left_bm.push(Some(slice_bitmap(b, 0, mid)));
                            right_bm.push(Some(slice_bitmap(b, mid, rg.num_rows() - mid)));
                        }
                        None => {
                            left_bm.push(None);
                            right_bm.push(None);
                        }
                    }
                }
                queue.push_front(RecordBatch::new(rg.schema.clone(), right_cols, right_bm));
                queue.push_front(RecordBatch::new(rg.schema.clone(), left_cols, left_bm));
                continue;
            }

            let rg_data_offset = current_offset;
            let mut column_chunk_offsets = Vec::new();
            let mut rg_column_chunks = Vec::new();
            let mut written = 0u64;
            for (
                col_idx,
                encoding,
                chunk_relative,
                chunk_len,
                uncompressed_len,
                chunk_checksum,
                null_count,
                compressed,
            ) in pending_chunks
            {
                let chunk_offset_abs = rg_data_offset + chunk_relative;
                column_chunk_offsets.push(ColumnChunkOffset {
                    column_idx: col_idx as u16,
                    offset: chunk_relative,
                    length: chunk_len,
                    uncompressed_length: uncompressed_len,
                    checksum: chunk_checksum,
                });

                rg_column_chunks.push(ColumnChunkMeta {
                    column_index: col_idx as u16,
                    encoding,
                    compression: self.config.compression,
                    offset: chunk_offset_abs,
                    compressed_size: chunk_len,
                    uncompressed_size: uncompressed_len,
                    null_count,
                    dictionary_offset: if encoding == EncodingV2::Dictionary {
                        Some(chunk_offset_abs)
                    } else {
                        None
                    },
                });

                // ColumnDescriptors の初期値設定
                let desc = &mut column_descriptors[col_idx];
                desc.column_idx = col_idx as u16;
                desc.logical_type = schema.columns[col_idx].logical_type;
                desc.encoding = encoding;
                desc.compression = self.config.compression;
                desc.nullable = schema.columns[col_idx].nullable;
                desc.fixed_len = schema.columns[col_idx].fixed_len;
                if meta_row_groups.is_empty() {
                    desc.data_offset = chunk_offset_abs;
                }
                desc.data_length += chunk_len;
                if encoding == EncodingV2::Dictionary {
                    desc.dictionary_offset = Some(chunk_offset_abs);
                }

                data.extend_from_slice(&compressed);
                written += chunk_len;
            }

            debug_assert_eq!(written, rg_compressed_size);

            // Chunk スコープでは ColumnChunk 側でチェックサムを保持するため、
            // RowGroup 単位のチェックサムは計算しない。
            let row_group_checksum = None;

            row_group_table_entries.push(RowGroupTableEntry {
                row_start,
                row_count: rg.num_rows() as u64,
                data_offset: rg_data_offset,
                compressed_size: rg_compressed_size,
                column_chunk_offsets,
                checksum: row_group_checksum,
            });

            meta_row_groups.push(RowGroupMeta {
                row_start,
                row_count: rg.num_rows() as u64,
                compressed_size: rg_compressed_size,
                column_chunks: rg_column_chunks,
                checksum: row_group_checksum,
            });

            current_offset += rg_compressed_size;
            row_start += rg.num_rows() as u64;
            total_uncompressed += rg_uncompressed_size;
        }

        let footer = SegmentFooter {
            row_group_table: RowGroupTable {
                entries: row_group_table_entries,
            },
            column_descriptors: ColumnDescriptors {
                columns: column_descriptors,
            },
        };

        let footer_payload =
            bincode::serialize(&footer).map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
        let footer_size = footer_payload.len() as u32;
        let mut hasher = Hasher::new();
        hasher.update(&footer_payload);
        let footer_checksum = hasher.finalize();

        data.extend_from_slice(&footer_payload);
        data.extend_from_slice(&footer_size.to_le_bytes());
        data.extend_from_slice(&footer_checksum.to_le_bytes());

        let meta = SegmentMetaV2 {
            format_version: header.format_version,
            schema: schema.clone(),
            num_rows: header.row_count,
            created_at: 0,
            uncompressed_size: total_uncompressed,
            compressed_size: data.len() as u64,
            row_groups: meta_row_groups,
        };

        Ok(ColumnSegmentV2 {
            header,
            meta,
            data,
            row_ids: Vec::new(),
        })
    }
}

/// SegmentReaderV2 実装。
#[derive(Debug)]
pub struct SegmentReaderV2 {
    header: SegmentHeader,
    schema: Schema,
    footer: SegmentFooter,
    source: Box<dyn SegmentSource>,
}

impl SegmentReaderV2 {
    /// セグメントをオープンしてヘッダ/フッター検証を行う。
    pub fn open(source: Box<dyn SegmentSource>) -> Result<Self> {
        let total_size = source.total_size();
        if total_size < (SEGMENT_HEADER_SIZE + 8) as u64 {
            return Err(ColumnarError::InvalidFormat("segment too small".into()));
        }

        // フッターサイズとチェックサムを取得
        let trailer = source.read_range(total_size - 8, 8)?;
        let footer_size = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as u64;
        let footer_checksum = u32::from_le_bytes(trailer[4..8].try_into().unwrap());

        if footer_size + 8 > total_size {
            return Err(ColumnarError::InvalidFormat("invalid footer size".into()));
        }

        let footer_start = total_size - 8 - footer_size;
        let footer_bytes = source.read_range(footer_start, footer_size)?;
        let mut hasher = Hasher::new();
        hasher.update(&footer_bytes);
        let computed = hasher.finalize();
        if computed != footer_checksum {
            return Err(ColumnarError::ChecksumMismatch);
        }

        let footer: SegmentFooter = bincode::deserialize(&footer_bytes)
            .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;

        // ヘッダとスキーマ読み込み
        let header_bytes = source.read_range(0, SEGMENT_HEADER_SIZE as u64)?;
        let header = read_header(&header_bytes)?;
        if header.format_version != SEGMENT_FORMAT_VERSION_V2 {
            return Err(ColumnarError::UnsupportedFormatVersion {
                found: header.format_version,
                expected: SEGMENT_FORMAT_VERSION_V2,
            });
        }

        let schema_len_bytes = source.read_range(SEGMENT_HEADER_SIZE as u64, 4)?;
        let schema_len = u32::from_le_bytes(schema_len_bytes.try_into().unwrap()) as u64;
        let schema_bytes = source.read_range(SEGMENT_HEADER_SIZE as u64 + 4, schema_len)?;
        let schema: Schema = bincode::deserialize(&schema_bytes)
            .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;

        Ok(Self {
            header,
            schema,
            footer,
            source,
        })
    }

    /// カラムの一部だけを読み取る（カラムプルーニング）。
    pub fn read_columns(&self, columns: &[usize]) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for idx in 0..self.footer.row_group_table.entries.len() {
            batches.push(self.read_row_group_by_index(columns, idx)?);
        }
        Ok(batches)
    }

    /// 指定RowGroupインデックスの指定カラムを読み取る。
    pub fn read_row_group_by_index(
        &self,
        columns: &[usize],
        rg_index: usize,
    ) -> Result<RecordBatch> {
        let entry = self
            .footer
            .row_group_table
            .entries
            .get(rg_index)
            .ok_or_else(|| ColumnarError::InvalidFormat("row group index out of bounds".into()))?;

        let mut cols = Vec::new();
        let mut bitmaps = Vec::new();
        for &col_idx in columns {
            let desc = self
                .footer
                .column_descriptors
                .columns
                .get(col_idx)
                .ok_or_else(|| ColumnarError::InvalidFormat("column index out of bounds".into()))?;
            let chunk_meta = entry
                .column_chunk_offsets
                .iter()
                .find(|c| c.column_idx as usize == col_idx)
                .ok_or_else(|| ColumnarError::InvalidFormat("missing chunk offset".into()))?;
            let chunk_bytes = self
                .source
                .read_range(entry.data_offset + chunk_meta.offset, chunk_meta.length)?;

            if self.header.checksum_scope == ChecksumScope::Chunk {
                if let Some(expected) = chunk_meta.checksum {
                    let mut hasher = Hasher::new();
                    hasher.update(&chunk_bytes);
                    let computed = hasher.finalize();
                    if expected != computed {
                        return Err(ColumnarError::ChecksumMismatch);
                    }
                }
                if let Some(expected_rg) = entry.checksum {
                    let mut hasher = Hasher::new();
                    let rg_bytes = self
                        .source
                        .read_range(entry.data_offset, entry.compressed_size)?;
                    hasher.update(&rg_bytes);
                    if hasher.finalize() != expected_rg {
                        return Err(ColumnarError::ChecksumMismatch);
                    }
                }
            }

            let decoder: Box<dyn Decoder> = create_decoder(desc.encoding);
            let decompressed = if let CompressionV2::None = desc.compression {
                chunk_bytes
            } else {
                let compressor = create_compressor(desc.compression)
                    .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?;
                compressor
                    .decompress(&chunk_bytes, chunk_meta.uncompressed_length as usize)
                    .map_err(|e| ColumnarError::InvalidFormat(e.to_string()))?
            };
            let (col, bitmap) =
                decoder.decode(&decompressed, entry.row_count as usize, desc.logical_type)?;
            cols.push(col);
            bitmaps.push(bitmap);
        }
        let projected_schema = Schema {
            columns: columns
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect(),
        };
        Ok(RecordBatch::new(projected_schema, cols, bitmaps))
    }

    /// RowGroup 単位で RecordBatch を順次読み取るイテレータ。
    pub fn iter_row_groups(&self) -> RowGroupIter<'_> {
        RowGroupIter {
            reader: self,
            index: 0,
        }
    }
}

/// RowGroup イテレータ。
#[derive(Debug)]
pub struct RowGroupIter<'a> {
    reader: &'a SegmentReaderV2,
    index: usize,
}

impl<'a> Iterator for RowGroupIter<'a> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.reader.footer.row_group_table.entries.len() {
            return None;
        }
        let idx = self.index;
        self.index += 1;
        let cols: Vec<usize> = (0..self.reader.schema.column_count()).collect();
        Some(self.reader.read_row_group_by_index(&cols, idx))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    fn simple_schema() -> Schema {
        Schema {
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
        }
    }

    fn make_batch(rows: &[(i64, i64)]) -> RecordBatch {
        let ids: Vec<i64> = rows.iter().map(|(a, _)| *a).collect();
        let vals: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
        RecordBatch::new(
            simple_schema(),
            vec![Column::Int64(ids), Column::Int64(vals)],
            vec![None, None],
        )
    }

    fn write_and_read(
        config: SegmentConfigV2,
        batches: Vec<RecordBatch>,
    ) -> Result<SegmentReaderV2> {
        let mut writer = SegmentWriterV2::new(config);
        for b in batches {
            writer.write_batch(b)?;
        }
        let segment = writer.finish()?;
        SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data)))
    }

    #[test]
    fn test_row_group_boundary() {
        let cfg = SegmentConfigV2 {
            row_group_size: 2,
            ..Default::default()
        };
        let reader = write_and_read(cfg, vec![make_batch(&[(1, 10), (2, 20), (3, 30)])]).unwrap();
        let batches = reader.read_columns(&[0, 1]).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn test_16mib_row_group_guard() {
        let cfg = SegmentConfigV2 {
            row_group_size: 10,
            max_row_group_bytes: 16,
            ..Default::default()
        };
        let oversized = RecordBatch::new(
            simple_schema(),
            vec![Column::Int64(vec![1, 2, 3]), Column::Int64(vec![4, 5, 6])],
            vec![None, None],
        );
        let mut writer = SegmentWriterV2::new(cfg);
        writer.write_batch(oversized).unwrap();
        let err = writer.finish().unwrap_err();
        assert!(matches!(err, ColumnarError::RowGroupTooLarge { .. }));
    }

    #[test]
    fn test_single_batch_too_large() {
        let cfg = SegmentConfigV2 {
            row_group_size: 100,
            max_row_group_bytes: 20,
            ..Default::default()
        };
        let batch = RecordBatch::new(
            simple_schema(),
            vec![
                Column::Int64((0..10).collect()),
                Column::Int64((0..10).collect()),
            ],
            vec![None, None],
        );
        let mut writer = SegmentWriterV2::new(cfg);
        writer.write_batch(batch).unwrap();
        let err = writer.finish().unwrap_err();
        assert!(matches!(err, ColumnarError::RowGroupTooLarge { .. }));
    }

    #[test]
    fn test_footer_checksum_validation() {
        let segment = {
            let mut writer = SegmentWriterV2::new(Default::default());
            writer.write_batch(make_batch(&[(1, 10)])).unwrap();
            writer.finish().unwrap()
        };
        let mut data = segment.data.clone();
        let len = data.len();
        data[len - 1] ^= 0xFF; // フッターのチェックサムを壊す
        let err = SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(data))).unwrap_err();
        assert!(matches!(err, ColumnarError::ChecksumMismatch));
    }

    #[test]
    fn test_multi_column_roundtrip() {
        let reader = write_and_read(
            Default::default(),
            vec![make_batch(&[(1, 10), (2, 20), (3, 30)])],
        )
        .unwrap();
        let batches = reader.read_columns(&[0, 1]).unwrap();
        assert_eq!(batches.len(), 1);
        if let Column::Int64(ids) = &batches[0].columns[0] {
            assert_eq!(ids, &vec![1, 2, 3]);
        } else {
            panic!("expected int64");
        }
    }

    #[test]
    fn test_offset_table_random_access() {
        let cfg = SegmentConfigV2 {
            row_group_size: 2,
            ..Default::default()
        };
        let reader =
            write_and_read(cfg, vec![make_batch(&[(1, 10), (2, 20), (3, 30), (4, 40)])]).unwrap();
        let batch = reader.read_row_group_by_index(&[1], 1).unwrap();
        assert_eq!(batch.num_rows(), 2);
        if let Column::Int64(vals) = &batch.columns[0] {
            assert_eq!(vals, &vec![30, 40]);
        } else {
            panic!("expected int64");
        }
    }

    #[test]
    fn test_format_version_rejection() {
        let segment = {
            let mut writer = SegmentWriterV2::new(Default::default());
            writer.write_batch(make_batch(&[(1, 1)])).unwrap();
            writer.finish().unwrap()
        };
        let mut bytes = segment.data.clone();
        // バージョンを 1 にする
        bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
        let err = SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(bytes))).unwrap_err();
        assert!(matches!(
            err,
            ColumnarError::UnsupportedFormatVersion { .. }
        ));
    }

    #[test]
    fn test_column_pruning() {
        let reader =
            write_and_read(Default::default(), vec![make_batch(&[(1, 10), (2, 20)])]).unwrap();
        let batches = reader.read_columns(&[1]).unwrap();
        assert_eq!(batches[0].columns.len(), 1);
        if let Column::Int64(vals) = &batches[0].columns[0] {
            assert_eq!(vals, &vec![10, 20]);
        } else {
            panic!("expected int64");
        }
    }

    #[test]
    fn test_chunk_checksum_validation() {
        let cfg = SegmentConfigV2 {
            checksum_scope: ChecksumScope::Chunk,
            ..Default::default()
        };
        let segment = {
            let mut writer = SegmentWriterV2::new(cfg);
            writer
                .write_batch(make_batch(&[(1, 10), (2, 20), (3, 30)]))
                .unwrap();
            writer.finish().unwrap()
        };
        let reader_ok =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(segment.data.clone())))
                .unwrap();
        let entry = &reader_ok.footer.row_group_table.entries[0];
        let chunk = &entry.column_chunk_offsets[0];
        let mut corrupted = segment.data.clone();
        let chunk_pos = (entry.data_offset + chunk.offset) as usize;
        corrupted[chunk_pos] ^= 0xAA;
        let reader =
            SegmentReaderV2::open(Box::new(InMemorySegmentSource::new(corrupted))).unwrap();
        let err = reader.read_columns(&[0]).unwrap_err();
        assert!(matches!(err, ColumnarError::ChecksumMismatch));
    }

    #[test]
    fn row_id_encode_decode_roundtrip() {
        let segment_id = 123u64;
        let offset = 456u64;
        let encoded = encode_row_id(segment_id, offset).expect("encode");
        let (decoded_seg, decoded_off) = decode_row_id(encoded);
        assert_eq!(decoded_seg, segment_id);
        assert_eq!(decoded_off, offset);
    }

    #[test]
    fn row_id_encode_rejects_overflow() {
        let overflow_segment = (1u64 << ROW_ID_SEGMENT_BITS) + 1;
        assert!(matches!(
            encode_row_id(overflow_segment, 0),
            Err(ColumnarError::InvalidFormat(_))
        ));

        let overflow_offset = (1u64 << ROW_ID_OFFSET_BITS) + 1;
        assert!(matches!(
            encode_row_id(0, overflow_offset),
            Err(ColumnarError::InvalidFormat(_))
        ));
    }
}
