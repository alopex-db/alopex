use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::fs::File;
use std::sync::Arc;

use alopex_core::kv::memory::MemoryKV;
use alopex_sql::catalog::MemoryCatalog;
use alopex_sql::dialect::AlopexDialect;
use alopex_sql::executor::query::execute_query_with_policy;
use alopex_sql::executor::{ExecutionResult, ExecutorError, MemoryPolicy, SpillPolicy};
use alopex_sql::parser::Parser;
use alopex_sql::planner::Planner;
use alopex_sql::storage::{SqlValue, TxnBridge};
use parquet::basic::Compression;
use parquet::data_type::Int64Type;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::thrift::{TCompactOutputProtocol, TSerializable};

const LIMIT: usize = 64 * 1024;
const DECLARED_PAGE_BYTES: i32 = 1024 * 1024;

// The separate test executable keeps this observer out of other integration tests.
struct ObservedAllocator;
thread_local! {
    static MAX_REQUEST: Cell<Option<usize>> = const { Cell::new(None) };
    static LIVE_ALLOCATIONS: RefCell<Option<LiveAllocations>> = const { RefCell::new(None) };
}

struct LiveAllocations {
    entries: [(usize, usize); 4096],
    active: bool,
    live: usize,
    peak: usize,
    exhausted: bool,
}

impl LiveAllocations {
    fn new() -> Self {
        Self {
            entries: [(0, 0); 4096],
            active: false,
            live: 0,
            peak: 0,
            exhausted: false,
        }
    }

    fn allocated(&mut self, pointer: usize, size: usize) {
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.0 == 0) {
            *entry = (pointer, size);
            self.live += size;
            self.peak = self.peak.max(self.live);
        } else {
            self.exhausted = true;
        }
    }

    fn deallocated(&mut self, pointer: usize) {
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.0 == pointer) {
            self.live -= entry.1;
            *entry = (0, 0);
        }
    }
}

fn observe_live(pointer: *mut u8, size: Option<usize>) {
    if pointer.is_null() {
        return;
    }
    let _ = LIVE_ALLOCATIONS.try_with(|observed| {
        if let Ok(mut observed) = observed.try_borrow_mut()
            && let Some(observed) = observed.as_mut().filter(|observed| observed.active)
        {
            match size {
                Some(size) => observed.allocated(pointer as usize, size),
                None => observed.deallocated(pointer as usize),
            }
        }
    });
}

fn observe(size: usize) {
    let _ = MAX_REQUEST.try_with(|maximum| {
        if let Some(previous) = maximum.get() {
            maximum.set(Some(previous.max(size)));
        }
    });
}

unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        observe(layout.size());
        let pointer = unsafe { System.alloc(layout) };
        observe_live(pointer, Some(layout.size()));
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        observe(layout.size());
        let pointer = unsafe { System.alloc_zeroed(layout) };
        observe_live(pointer, Some(layout.size()));
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        observe(size);
        let resized = unsafe { System.realloc(pointer, layout, size) };
        if !resized.is_null() {
            observe_live(pointer, None);
            observe_live(resized, Some(size));
        }
        resized
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        observe_live(pointer, None);
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

fn fixture(oversized: bool) -> tempfile::NamedTempFile {
    fixture_with_sizes(oversized.then_some((DECLARED_PAGE_BYTES, 10)))
}

fn fixture_with_sizes(sizes: Option<(i32, i32)>) -> tempfile::NamedTempFile {
    fixture_with_dictionary(sizes, false)
}

fn fixture_with_dictionary(sizes: Option<(i32, i32)>, dictionary: bool) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(parse_message_type("message fixture { REQUIRED INT64 id; }").unwrap());
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(dictionary)
            .build(),
    );
    let mut writer =
        SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties).unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&[7], None, None)
        .unwrap();
    column.close().unwrap();
    group.close().unwrap();
    let mut footer = writer.close().unwrap();

    if let Some((uncompressed, compressed)) = sizes {
        let mut bytes = std::fs::read(file.path()).unwrap();
        let footer_size =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                as usize;
        bytes.truncate(bytes.len() - 8 - footer_size);
        let chunk = &mut footer.row_groups[0].columns[0];
        let metadata = chunk.meta_data.as_mut().unwrap();
        let offset = metadata.data_page_offset as usize;
        // Compact Thrift: field 1 DATA_PAGE, then field 2 i32 with the eight-byte payload size.
        assert_eq!(
            &bytes[offset..offset + 6],
            &[0x15, 0x00, 0x15, 0x10, 0x15, 0x14]
        );
        let mut encoded = Vec::new();
        for (tag, size) in [(0x15, uncompressed), (0x15, compressed)] {
            encoded.push(tag);
            let mut value = ((size as u32) << 1) ^ ((size >> 31) as u32);
            while value >= 128 {
                encoded.push((value as u8 & 0x7f) | 0x80);
                value >>= 7;
            }
            encoded.push(value as u8);
        }
        let growth = encoded.len() as i64 - 4;
        bytes.splice(offset + 2..offset + 6, encoded);
        // Keep the chunk bounds accurate, but retain the small footer payload declaration.
        metadata.total_compressed_size += growth;
        metadata.total_uncompressed_size += growth;
        footer.row_groups[0].total_byte_size += growth;
        if let Some(size) = &mut footer.row_groups[0].total_compressed_size {
            *size += growth;
        }
        let chunk = &mut footer.row_groups[0].columns[0];
        chunk.offset_index_offset = None;
        chunk.offset_index_length = None;
        chunk.column_index_offset = None;
        chunk.column_index_length = None;
        let mut encoded_footer = Vec::new();
        footer
            .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded_footer))
            .unwrap();
        bytes.extend_from_slice(&encoded_footer);
        bytes.extend_from_slice(&(encoded_footer.len() as u32).to_le_bytes());
        bytes.extend_from_slice(b"PAR1");
        std::fs::write(file.path(), bytes).unwrap();
    }

    let reader = SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
    assert!(reader.metadata().row_group(0).total_byte_size() < LIMIT as i64);
    assert!(reader.metadata().row_group(0).column(0).uncompressed_size() < LIMIT as i64);
    assert!(file.as_file().metadata().unwrap().len() < 4096);
    file
}

fn execute_observed(
    file: &tempfile::NamedTempFile,
) -> (Result<ExecutionResult, ExecutorError>, usize) {
    execute_observed_after_planning(file, || {})
}

fn replace_footer(file: &tempfile::NamedTempFile, footer: &[u8]) {
    let mut bytes = std::fs::read(file.path()).unwrap();
    let size =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
    bytes.truncate(bytes.len() - 8 - size);
    bytes.extend_from_slice(footer);
    bytes.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    bytes.extend_from_slice(b"PAR1");
    std::fs::write(file.path(), bytes).unwrap();
}

fn execute_observed_after_planning(
    file: &tempfile::NamedTempFile,
    change: impl FnOnce(),
) -> (Result<ExecutionResult, ExecutorError>, usize) {
    execute_observed_with_alias(file, "", Some(LIMIT), change)
}

fn execute_observed_with_alias(
    file: &tempfile::NamedTempFile,
    alias: &str,
    limit: Option<usize>,
    change: impl FnOnce(),
) -> (Result<ExecutionResult, ExecutorError>, usize) {
    let sql = format!(
        "SELECT * FROM read_parquet('{}') {alias}",
        file.path().display()
    );
    execute_observed_sql(&sql, limit, change)
}

fn execute_observed_sql(
    sql: &str,
    limit: Option<usize>,
    change: impl FnOnce(),
) -> (Result<ExecutionResult, ExecutorError>, usize) {
    let bridge = TxnBridge::new(Arc::new(MemoryKV::new()));
    let catalog = MemoryCatalog::new();
    let statement = Parser::parse_sql(&AlopexDialect, sql).unwrap().remove(0);
    let plan = Planner::new(&catalog).plan(&statement).unwrap();
    change();
    let mut transaction = bridge.begin_write().unwrap();
    let policy = limit.map(|limit| MemoryPolicy::new(Some(limit as u64), SpillPolicy::FailFast));
    LIVE_ALLOCATIONS.with(|observed| {
        if let Some(observed) = observed.borrow_mut().as_mut() {
            observed.active = true;
        }
    });
    MAX_REQUEST.with(|maximum| maximum.set(Some(0)));
    let result = execute_query_with_policy(&mut transaction, &catalog, plan, policy.as_ref());
    let maximum = MAX_REQUEST.with(|maximum| maximum.replace(None).unwrap());
    LIVE_ALLOCATIONS.with(|observed| {
        if let Some(observed) = observed.borrow_mut().as_mut() {
            observed.active = false;
        }
    });
    (result, maximum)
}

fn metadata_footer(
    file: &tempfile::NamedTempFile,
    values: Vec<parquet::format::KeyValue>,
    column_metadata: bool,
) -> Vec<u8> {
    rewritten_metadata_footer(file, |footer| {
        footer.key_value_metadata = Some(values);
        if column_metadata {
            footer.row_groups[0].columns[0]
                .meta_data
                .as_mut()
                .unwrap()
                .key_value_metadata = footer.key_value_metadata.take();
        }
    })
}

fn rewritten_metadata_footer(
    file: &tempfile::NamedTempFile,
    change: impl FnOnce(&mut parquet::format::FileMetaData),
) -> Vec<u8> {
    use std::io::Cursor;
    use thrift::protocol::TCompactInputProtocol;

    let bytes = std::fs::read(file.path()).unwrap();
    let size =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
    let mut footer = parquet::format::FileMetaData::read_from_in_protocol(
        &mut TCompactInputProtocol::new(Cursor::new(&bytes[bytes.len() - 8 - size..])),
    )
    .unwrap();
    change(&mut footer);
    let mut encoded = Vec::new();
    footer
        .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded))
        .unwrap();
    assert!(encoded.len() < LIMIT);
    encoded
}

#[test]
fn issue577_created_by_metadata_budget_checked_before_allocation() {
    for (limit, width, rows, may_reject) in [
        (None, 24 * 1024, 512, false),
        (Some(LIMIT), 8, 1, false),
        (Some(LIMIT), 24 * 1024, 1, true),
        (Some(LIMIT), 8, 512, false),
        (Some(LIMIT), 24 * 1024, 512, true),
        (Some(LIMIT * 4), 24 * 1024, 512, false),
    ] {
        let file = row_count_fixture(rows);
        let encoded = rewritten_metadata_footer(&file, |footer| {
            footer.created_by = Some("x".repeat(width));
        });
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {
            replace_footer(&file, &encoded);
        });
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        println!(
            "created_by limit={limit:?} width={width} rows={rows} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
            observed.peak,
            result.is_err()
        );
        if let Some(limit) = limit {
            assert!(maximum <= limit);
            assert!(
                observed.peak <= limit,
                "retained metadata exceeded its budget"
            );
        }
        if may_reject && result.is_err() {
            assert!(matches!(
                result,
                Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected created_by metadata rows");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]; rows]);
        }
    }
}

#[test]
fn issue577_schema_names_budget_checked_before_allocation() {
    schema_names_budget_checked_before_allocation(false);
}

#[test]
fn issue577_schema_names_zero_children_budget_checked_before_allocation() {
    schema_names_budget_checked_before_allocation(true);
}

fn schema_names_budget_checked_before_allocation(zero_children: bool) {
    for (limit, width, rows, may_reject) in [
        (None, 8 * 1024, 512, false),
        (Some(LIMIT), 8, 1, false),
        (Some(LIMIT), 8 * 1024, 1, true),
        (Some(LIMIT), 8, 512, false),
        (Some(LIMIT), 8 * 1024, 512, true),
        (Some(LIMIT * 4), 8 * 1024, 512, false),
    ] {
        let file = row_count_fixture(rows);
        let encoded = rewritten_metadata_footer(&file, |footer| {
            let name = "x".repeat(width);
            footer.schema[1].name = name.clone();
            if zero_children {
                footer.schema[1].num_children = Some(0);
            }
            footer.row_groups[0].columns[0]
                .meta_data
                .as_mut()
                .unwrap()
                .path_in_schema[0] = name;
        });
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {
            replace_footer(&file, &encoded);
        });
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        println!(
            "schema names zero_children={zero_children} limit={limit:?} width={width} rows={rows} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
            observed.peak,
            result.is_err()
        );
        if let Some(limit) = limit {
            assert!(maximum <= limit);
            assert!(observed.peak <= limit, "schema names exceeded their budget");
        }
        if may_reject && result.is_err() {
            assert!(matches!(
                result,
                Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected schema metadata rows");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]; rows]);
        }
    }
}

#[test]
fn issue577_schema_names_tree_walk_preserves_controlled_errors() {
    for (schema, children, exceeds_budget) in [
        (
            "message fixture { REQUIRED group branch { REQUIRED INT64 a; REQUIRED group deep { REQUIRED INT64 b; } } REQUIRED INT64 c; }".to_owned(),
            None,
            false,
        ),
        (
            format!(
                "message fixture {{ REQUIRED group {} {{ REQUIRED INT64 a; REQUIRED INT64 b; REQUIRED INT64 c; REQUIRED INT64 d; }} REQUIRED INT64 e; }}",
                "x".repeat(8 * 1024)
            ),
            None,
            true,
        ),
        (
            "message fixture { REQUIRED group branch { REQUIRED INT64 a; } REQUIRED INT64 b; }".to_owned(),
            Some(3),
            false,
        ),
        (
            "message fixture { REQUIRED group branch { REQUIRED INT64 a; } REQUIRED INT64 b; }".to_owned(),
            Some(1),
            false,
        ),
        (
            "message fixture { REQUIRED group branch { REQUIRED INT64 a; } REQUIRED INT64 b; }".to_owned(),
            Some(-1),
            false,
        ),
    ] {
        let file = row_count_fixture(1);
        let encoded = rewritten_metadata_footer(&file, |footer| {
            footer.schema =
                parquet::schema::types::to_thrift(&parse_message_type(&schema).unwrap()).unwrap();
            if let Some(children) = children {
                footer.schema[0].num_children = Some(children);
            }
            footer.num_rows = 0;
            footer.row_groups.clear();
            footer.column_orders = None;
        });
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_after_planning(&file, || {
            replace_footer(&file, &encoded);
        });
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        assert!(maximum <= LIMIT);
        assert!(observed.peak <= LIMIT);
        if exceeds_budget {
            assert!(matches!(result, Err(ExecutorError::ResourceExhausted { .. })));
        } else {
            let Err(ExecutorError::BulkLoad(error)) = result else {
                panic!("expected controlled schema error");
            };
            if children.is_some() {
                assert!(error.contains("invalid parquet page or chunk declaration"));
            } else {
                // Nested columns remain outside READ_PARQUET's supported SQL type contract.
                assert!(error.contains("unsupported parquet/arrow type"));
            }
        }
    }
}

#[test]
fn issue577_metadata_workspace_sum_checked_before_allocation() {
    for (limit, width, rows, column_metadata, reject) in [
        (None, 24 * 1024, 1, false, false),
        (Some(LIMIT), 8, 1, false, false),
        (Some(LIMIT), 24 * 1024, 1, false, true),
        (Some(LIMIT * 4), 8, 1, false, false),
        (Some(LIMIT * 4), 24 * 1024, 1, false, false),
        (Some(LIMIT), 8, 512, false, false),
        (Some(LIMIT), 12 * 1024, 1, false, false),
        (Some(LIMIT), 12 * 1024, 512, false, true),
        (Some(LIMIT), 12 * 1024, 512, true, false),
        (Some(LIMIT * 4), 12 * 1024, 512, false, false),
        (None, 12 * 1024, 512, false, false),
    ] {
        let file = row_count_fixture(rows);
        let encoded = metadata_footer(
            &file,
            ["budget_a", "budget_b"]
                .into_iter()
                .map(|key| parquet::format::KeyValue {
                    key: key.into(),
                    value: Some("x".repeat(width)),
                })
                .collect(),
            column_metadata,
        );
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {
            replace_footer(&file, &encoded);
        });
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        println!(
            "metadata workspace limit={limit:?} width={width} rows={rows} column_metadata={column_metadata} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
            observed.peak,
            result.is_err()
        );
        if let Some(limit) = limit {
            assert!(maximum <= limit);
            assert!(
                observed.peak <= limit,
                "metadata combined workspace exceeded its budget"
            );
        }
        if reject {
            assert!(matches!(
                result,
                Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected normal metadata rows");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]; rows]);
        }
    }
}

#[test]
fn issue577_materialized_projection_preserves_values_and_names() {
    let file = row_count_fixture(512);
    for (projection, predicate, value, rows) in [
        ("*", "", 7, 512),
        ("id + 1 AS next", "WHERE id = 7", 8, 512),
        ("id + 1 AS next", "WHERE id = 8", 8, 0),
    ] {
        let sql = format!(
            "SELECT {projection} FROM read_parquet('{}') {predicate}",
            file.path().display()
        );
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_sql(&sql, Some(LIMIT), || {});
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        println!(
            "projection={projection} rows={rows} maximum_request={maximum} rust_live_peak_bytes={}",
            observed.peak
        );
        assert!(maximum <= LIMIT);
        assert!(observed.peak <= LIMIT);
        let ExecutionResult::Query(result) = result.unwrap() else {
            panic!("expected projected rows");
        };
        assert_eq!(result.rows, vec![vec![SqlValue::BigInt(value)]; rows]);
        assert_eq!(
            result.columns[0].name,
            if projection == "*" { "id" } else { "next" }
        );
    }
}

#[test]
fn issue577_metadata_map_capacity_checked_before_allocation() {
    for (limit, count, column_metadata, may_reject) in [
        (None, 513, false, false),
        (Some(LIMIT), 1, false, false),
        (Some(LIMIT), 513, false, true),
        (Some(LIMIT), 513, true, false),
        (Some(LIMIT * 4), 513, false, false),
    ] {
        let file = row_count_fixture(1);
        let encoded = metadata_footer(
            &file,
            (0..count)
                .map(|index| parquet::format::KeyValue {
                    key: format!("kv_{index:04}"),
                    value: Some("x".into()),
                })
                .collect(),
            column_metadata,
        );
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {
            replace_footer(&file, &encoded);
        });
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        println!(
            "metadata map limit={limit:?} count={count} column_metadata={column_metadata} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
            observed.peak,
            result.is_err()
        );
        if let Some(limit) = limit {
            assert!(maximum <= limit);
            assert!(observed.peak <= limit, "metadata map exceeded its budget");
        }
        if may_reject && result.is_err() {
            assert!(matches!(
                result,
                Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected normal metadata map rows");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]]);
        }
    }
}

#[test]
fn issue577_embedded_arrow_schema_preserves_results_and_errors() {
    for limit in [None, Some(LIMIT), Some(LIMIT * 4)] {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![7]))],
        )
        .unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(File::create(file.path()).unwrap(), schema, None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
        assert!(
            metadata
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .iter()
                .any(|value| value.key == "ARROW:schema")
        );
        drop(metadata);
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {});
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        if let Some(limit) = limit {
            assert!(maximum <= limit && observed.peak <= limit);
        }
        let ExecutionResult::Query(result) = result.unwrap() else {
            panic!("expected embedded schema rows");
        };
        assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]]);
    }
    for (limit, column_metadata, value, invalid_schema) in [
        (None, false, Some("x"), true),
        (Some(LIMIT), false, Some("x"), true),
        (Some(LIMIT * 4), false, Some("x"), true),
        (Some(LIMIT), true, Some("x"), false),
        (Some(LIMIT), false, None, false),
    ] {
        let file = row_count_fixture(1);
        let encoded = metadata_footer(
            &file,
            vec![parquet::format::KeyValue {
                key: "ARROW:schema".into(),
                value: value.map(str::to_owned),
            }],
            column_metadata,
        );
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {
            replace_footer(&file, &encoded);
        });
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        if let Some(limit) = limit {
            assert!(maximum <= limit && observed.peak <= limit);
        }
        if invalid_schema {
            assert!(matches!(result, Err(ExecutorError::BulkLoad(_))));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected ordinary column metadata rows");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]]);
        }
    }
}

#[test]
fn issue577_aggregate_observer_calibration() {
    LIVE_ALLOCATIONS.with(|observed| {
        let mut allocations = LiveAllocations::new();
        allocations.active = true;
        *observed.borrow_mut() = Some(allocations);
    });
    let first = std::hint::black_box(vec![0_u8; 1000]);
    let mut second = std::hint::black_box(vec![0_u8; 2000]);
    drop(first);
    second.reserve_exact(1000);
    LIVE_ALLOCATIONS.with(|observed| {
        let observed = observed.borrow();
        let observed = observed.as_ref().unwrap();
        assert_eq!(observed.live, second.capacity());
        assert_eq!(observed.peak, 3000);
    });
    drop(second);
    let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
    assert_eq!(observed.live, 0);
    assert!(!observed.exhausted);
}

#[test]
fn issue577_aggregate_columns_checked_before_allocation() {
    aggregate_checked_before_allocation(4, 64, 64, 64);
}

#[test]
fn issue577_aggregate_pages_checked_before_allocation() {
    aggregate_checked_before_allocation(1, 128, 32, 128);
}

#[test]
fn issue577_aggregate_groups_checked_before_allocation() {
    aggregate_checked_before_allocation(1, 128, 128, 32);
}

fn aggregate_checked_before_allocation(
    columns: usize,
    count: usize,
    page_rows: usize,
    group_rows: usize,
) {
    use parquet::data_type::{ByteArray, ByteArrayType};

    for (limit, width, oversized) in [
        (LIMIT, 8, false),
        (LIMIT, 256, true),
        (LIMIT * 2, 8, false),
        (LIMIT * 2, 512, true),
    ] {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let fields = (0..columns)
            .map(|index| format!("REQUIRED BYTE_ARRAY body{index};"))
            .collect::<Vec<_>>()
            .join(" ");
        let schema =
            Arc::new(parse_message_type(&format!("message fixture {{ {fields} }}")).unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .set_encoding(parquet::basic::Encoding::PLAIN)
                .set_data_page_row_count_limit(page_rows)
                .set_write_batch_size(1)
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        let values = vec![ByteArray::from(vec![0x42; width]); count];
        for values in values.chunks(group_rows) {
            let mut group = writer.next_row_group().unwrap();
            while let Some(mut column) = group.next_column().unwrap() {
                column
                    .typed::<ByteArrayType>()
                    .write_batch(values, None, None)
                    .unwrap();
                column.close().unwrap();
            }
            group.close().unwrap();
        }
        writer.close().unwrap();
        let reader = SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
        assert_eq!(reader.num_row_groups(), count.div_ceil(group_rows));
        for (index, values) in values.chunks(group_rows).enumerate() {
            let group = reader.get_row_group(index).unwrap();
            for column in 0..columns {
                let mut pages = group.get_column_page_reader(column).unwrap();
                let mut page_count = 0;
                while let Some(page) = pages.get_next_page().unwrap() {
                    assert!(page.buffer().len() <= limit);
                    page_count += 1;
                }
                assert_eq!(page_count, values.len().div_ceil(page_rows));
            }
        }
        drop(reader);
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", Some(limit), || {});
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted, "allocation observer capacity exceeded");
        println!(
            "aggregate limit={limit} columns={columns} rows={count} width={width} page_rows={page_rows} group_rows={group_rows} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
            observed.peak,
            result.is_err()
        );
        assert!(maximum <= limit);
        assert!(
            observed.peak <= limit,
            "aggregate allocation exceeded its budget"
        );
        if oversized {
            assert!(matches!(
                result,
                Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected normal aggregate rows");
            };
            assert_eq!(
                result.rows,
                vec![vec![SqlValue::Blob(vec![0x42; width]); columns]; count]
            );
        }
    }
}

#[test]
fn issue577_small_compressed_page_preserves_rows() {
    let file = fixture(false);
    let (result, maximum) = execute_observed(&file);
    let ExecutionResult::Query(result) = result.unwrap() else {
        panic!("expected the normal compressed page to produce a query result");
    };
    assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]]);
    println!(
        "normal fixture bytes={} maximum_request={maximum} limit={LIMIT}",
        file.as_file().metadata().unwrap().len()
    );
    assert!(maximum <= LIMIT);
}

#[test]
fn issue577_row_count_checked_before_decode() {
    use std::io::Cursor;
    use thrift::protocol::TCompactInputProtocol;

    for (file_rows, group_rows) in [(1, 100_001), (100_001, 1)] {
        let file = fixture(false);
        let (result, maximum) = execute_observed_with_alias(&file, "", None, || {
            let bytes = std::fs::read(file.path()).unwrap();
            let footer_size =
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                    as usize;
            let mut footer = parquet::format::FileMetaData::read_from_in_protocol(
                &mut TCompactInputProtocol::new(Cursor::new(
                    &bytes[bytes.len() - 8 - footer_size..],
                )),
            )
            .unwrap();
            footer.num_rows = file_rows;
            footer.row_groups[0].num_rows = group_rows;
            let mut encoded = Vec::new();
            footer
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded))
                .unwrap();
            replace_footer(&file, &encoded);
        });
        assert!(
            matches!(&result, Err(ExecutorError::ResourceExhausted { message })
                if message.contains("row limit")),
            "expected a row-limit error, got {result:?}"
        );
        println!(
            "row declaration file_rows={file_rows} group_rows={group_rows} maximum_request={maximum}"
        );
        assert!(maximum <= LIMIT);
    }
}

fn row_count_fixture(rows: usize) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(parse_message_type("message fixture { REQUIRED INT64 id; }").unwrap());
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(false)
            .build(),
    );
    let mut writer =
        SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties).unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    column
        .typed::<Int64Type>()
        .write_batch(&vec![7; rows], None, None)
        .unwrap();
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();
    file
}

#[test]
fn issue577_row_group_metadata_shared_budget_before_allocation() {
    row_group_metadata_shared_budget_before_allocation(false);
}

#[test]
fn issue577_row_group_metadata_sorting_shared_budget_before_allocation() {
    row_group_metadata_shared_budget_before_allocation(true);
}

fn row_group_metadata_shared_budget_before_allocation(sorted: bool) {
    for (limit, groups, rows, may_reject) in [
        (None, 32, 512, false),
        (Some(LIMIT), 1, 1, false),
        (Some(LIMIT), 1, 512, false),
        (Some(LIMIT), 4, 64, false),
        (Some(LIMIT), 5, 80, false),
        (Some(LIMIT), 32, 512, true),
        (Some(LIMIT * 4), 32, 512, false),
        (Some(LIMIT), 33, 528, true),
        (Some(LIMIT * 4), 33, 528, false),
    ] {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let schema =
            Arc::new(parse_message_type("message fixture { REQUIRED INT64 id; }").unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .set_sorting_columns(
                    sorted.then(|| vec![parquet::format::SortingColumn::new(0, false, false)]),
                )
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        for values in vec![7; rows].chunks(rows / groups) {
            let mut group = writer.next_row_group().unwrap();
            let mut column = group.next_column().unwrap().unwrap();
            column
                .typed::<Int64Type>()
                .write_batch(values, None, None)
                .unwrap();
            column.close().unwrap();
            group.close().unwrap();
        }
        writer.close().unwrap();
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {});
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        println!(
            "row group metadata sorted={sorted} limit={limit:?} groups={groups} rows={rows} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
            observed.peak,
            result.is_err()
        );
        if let Some(limit) = limit {
            assert!(maximum <= limit);
            assert!(
                observed.peak <= limit,
                "row group metadata exceeded shared budget"
            );
        }
        if may_reject && result.is_err() {
            assert!(matches!(
                result,
                Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected row group metadata rows");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]; rows]);
        }
    }
}

#[test]
fn issue577_row_limit_boundary_preserves_rows() {
    let file = row_count_fixture(100_000);
    let limit = 32 * 1024 * 1024;
    let (result, maximum) = execute_observed_with_alias(&file, "", Some(limit), || {});
    let ExecutionResult::Query(result) = result.unwrap() else {
        panic!("expected rows at the inclusive row limit");
    };
    assert_eq!(result.rows.len(), 100_000);
    assert!(result.rows.iter().all(|row| row == &[SqlValue::BigInt(7)]));
    println!(
        "row limit boundary rows={} maximum_request={maximum}",
        result.rows.len()
    );
    assert!(maximum <= limit);
}

#[test]
fn issue577_public_read_parquet_row_limit() {
    let file = row_count_fixture(100_001);
    MAX_REQUEST.with(|maximum| maximum.set(Some(0)));
    let result = alopex_sql::executor::bulk::read_parquet(file.path().to_str().unwrap());
    let maximum = MAX_REQUEST.with(|maximum| maximum.replace(None).unwrap());
    assert!(matches!(
        result,
        Err(ExecutorError::ResourceExhausted { .. })
    ));
    println!("public reader row limit maximum_request={maximum}");
    assert!(maximum <= LIMIT);
}

#[test]
fn issue577_streaming_reader_exceeds_materialized_row_limit() {
    use alopex_sql::catalog::{ColumnMetadata, TableMetadata};
    use alopex_sql::executor::bulk::{BulkReader, ParquetReader};
    use alopex_sql::planner::types::ResolvedType;

    let file = row_count_fixture(100_001);
    let table = TableMetadata::new(
        "copy_stream",
        vec![ColumnMetadata::new("id", ResolvedType::BigInt)],
    );
    let mut reader = ParquetReader::open(file.path().to_str().unwrap(), &table, false).unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next_batch(1024).unwrap() {
        assert!(batch.iter().all(|row| row == &[SqlValue::BigInt(7)]));
        rows += batch.len();
    }
    assert_eq!(rows, 100_001);
}

#[test]
fn issue577_delta_length_header_checked_before_allocation() {
    delta_header_checked_before_allocation(
        parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY,
        &[0x80, 1, 4, 0x80, 0x80, 8, 0],
    );
}

#[test]
fn issue577_delta_byte_array_prefix_checked_before_allocation() {
    delta_header_checked_before_allocation(
        parquet::basic::Encoding::DELTA_BYTE_ARRAY,
        &[0x80, 1, 4, 0x80, 0x80, 8, 0],
    );
}

#[test]
fn issue577_delta_byte_array_suffix_checked_before_allocation() {
    delta_header_checked_before_allocation(
        parquet::basic::Encoding::DELTA_BYTE_ARRAY,
        &[0x80, 1, 4, 1, 0, 0x80, 1, 4, 0x80, 0x80, 8, 0],
    );
}

fn delta_header_checked_before_allocation(encoding: parquet::basic::Encoding, count_input: &[u8]) {
    use parquet::data_type::{ByteArray, ByteArrayType};
    use parquet::file::properties::EnabledStatistics;
    use std::io::Cursor;
    use thrift::protocol::TCompactInputProtocol;

    let cases: [(&str, Option<&[u8]>); 4] = [
        ("normal", None),
        ("count", Some(count_input)),
        ("zero mini blocks", Some(&[0x80, 1, 0, 1, 0])),
        ("varint overflow", Some(&[0xff; 10])),
    ];
    for (case, replacement) in cases {
        let oversized = replacement.is_some();
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let schema =
            Arc::new(parse_message_type("message fixture { REQUIRED BYTE_ARRAY body; }").unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .set_encoding(encoding)
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<ByteArrayType>()
            .write_batch(&[ByteArray::from(vec![0x42])], None, None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
        let mut footer = writer.close().unwrap();
        let (result, maximum) = execute_observed_after_planning(&file, || {
            let Some(input) = replacement else {
                return;
            };
            let mut bytes = std::fs::read(file.path()).unwrap();
            let metadata = footer.row_groups[0].columns[0].meta_data.as_mut().unwrap();
            let offset = metadata.data_page_offset as usize;
            let mut header = parquet::format::PageHeader::read_from_in_protocol(
                &mut TCompactInputProtocol::new(Cursor::new(&bytes[offset..])),
            )
            .unwrap();
            header.uncompressed_page_size = input.len() as i32;
            header.compressed_page_size = input.len() as i32;
            let mut encoded_header = Vec::new();
            header
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded_header))
                .unwrap();
            let size = (encoded_header.len() + input.len()) as i64;
            metadata.total_compressed_size = size;
            metadata.total_uncompressed_size = size;
            let chunk = &mut footer.row_groups[0].columns[0];
            chunk.offset_index_offset = None;
            chunk.offset_index_length = None;
            chunk.column_index_offset = None;
            chunk.column_index_length = None;
            footer.row_groups[0].total_byte_size = size;
            footer.row_groups[0].total_compressed_size = Some(size);
            bytes.truncate(offset);
            bytes.extend_from_slice(&encoded_header);
            bytes.extend_from_slice(input);
            let mut encoded_footer = Vec::new();
            footer
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded_footer))
                .unwrap();
            bytes.extend_from_slice(&encoded_footer);
            bytes.extend_from_slice(&(encoded_footer.len() as u32).to_le_bytes());
            bytes.extend_from_slice(b"PAR1");
            assert!(bytes.len() < 4096);
            std::fs::write(file.path(), bytes).unwrap();
        });
        println!(
            "DELTA {encoding:?} control case={case} oversized={oversized} maximum_request={maximum}"
        );
        assert!(
            maximum <= LIMIT,
            "DELTA length header exceeded its allocation budget"
        );
        if oversized {
            assert!(matches!(
                result,
                Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected normal DELTA length rows");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::Blob(vec![0x42])]]);
        }
    }
}

#[test]
fn issue577_delta_length_nullable_pages_preserve_rows() {
    delta_nullable_pages_preserve_rows(parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY, 3);
}

#[test]
fn issue577_delta_byte_array_expansion_checked_before_allocation() {
    delta_expansion_checked_before_allocation(64, 8192, 64, 64);
}

#[test]
fn issue577_delta_byte_array_expansion_across_pages_checked_before_allocation() {
    delta_expansion_checked_before_allocation(96, 1024, 32, 96);
}

#[test]
fn issue577_delta_byte_array_expansion_across_groups_checked_before_allocation() {
    delta_expansion_checked_before_allocation(96, 1024, 96, 32);
}

fn delta_expansion_checked_before_allocation(
    large_count: usize,
    large_width: usize,
    page_rows: usize,
    group_rows: usize,
) {
    use parquet::data_type::{ByteArray, ByteArrayType};

    for (count, width, oversized) in [
        (4, 128, false),
        (large_count, 128, false),
        (large_count, large_width, true),
    ] {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let schema =
            Arc::new(parse_message_type("message fixture { REQUIRED BYTE_ARRAY body; }").unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .set_encoding(parquet::basic::Encoding::DELTA_BYTE_ARRAY)
                .set_data_page_row_count_limit(page_rows)
                .set_write_batch_size(1)
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        let value = ByteArray::from(vec![0x42; width]);
        let values = vec![value; count];
        for values in values.chunks(group_rows) {
            let mut group = writer.next_row_group().unwrap();
            let mut column = group.next_column().unwrap().unwrap();
            column
                .typed::<ByteArrayType>()
                .write_batch(values, None, None)
                .unwrap();
            column.close().unwrap();
            group.close().unwrap();
        }
        writer.close().unwrap();
        let fixture_bytes = file.as_file().metadata().unwrap().len();
        assert!(fixture_bytes < 16 * 1024);
        let reader = SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
        assert_eq!(reader.num_row_groups(), count.div_ceil(group_rows));
        for (index, values) in values.chunks(group_rows).enumerate() {
            let group = reader.get_row_group(index).unwrap();
            let mut pages = group.get_column_page_reader(0).unwrap();
            let mut page_count = 0;
            while pages.get_next_page().unwrap().is_some() {
                page_count += 1;
            }
            assert_eq!(page_count, values.len().div_ceil(page_rows));
        }
        drop(reader);
        let (result, maximum) = execute_observed(&file);
        println!(
            "DELTA expansion count={count} width={width} page_rows={page_rows} group_rows={group_rows} fixture_bytes={fixture_bytes} maximum_request={maximum}"
        );
        assert!(
            maximum <= LIMIT,
            "DELTA expansion exceeded its allocation budget"
        );
        if oversized {
            assert!(matches!(
                result,
                Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected normal expanded DELTA rows");
            };
            assert_eq!(
                result.rows,
                vec![vec![SqlValue::Blob(vec![0x42; width])]; count]
            );
        }
    }
}

#[test]
fn issue577_delta_byte_array_nullable_pages_preserve_rows() {
    for count in [3, 300] {
        delta_nullable_pages_preserve_rows(parquet::basic::Encoding::DELTA_BYTE_ARRAY, count);
    }
}

fn delta_nullable_pages_preserve_rows(encoding: parquet::basic::Encoding, count: usize) {
    use parquet::data_type::{ByteArray, ByteArrayType};
    use parquet::file::properties::WriterVersion;

    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        for compression in [
            Compression::UNCOMPRESSED,
            Compression::GZIP(Default::default()),
        ] {
            for all_null in [false, true] {
                let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
                let schema = Arc::new(
                    parse_message_type("message fixture { OPTIONAL BYTE_ARRAY body; }").unwrap(),
                );
                let properties = Arc::new(
                    WriterProperties::builder()
                        .set_writer_version(version)
                        .set_compression(compression)
                        .set_dictionary_enabled(false)
                        .set_encoding(encoding)
                        .build(),
                );
                let mut writer = SerializedFileWriter::new(
                    File::create(file.path()).unwrap(),
                    schema,
                    properties,
                )
                .unwrap();
                let mut group = writer.next_row_group().unwrap();
                let mut column = group.next_column().unwrap().unwrap();
                let expected: Vec<_> = (0..count)
                    .map(|index| {
                        vec![if all_null || index % 3 == 1 {
                            SqlValue::Null
                        } else {
                            SqlValue::Blob(format!("shared-prefix-{index:04}").into_bytes())
                        }]
                    })
                    .collect();
                let values: Vec<_> = expected
                    .iter()
                    .filter_map(|row| match &row[0] {
                        SqlValue::Blob(value) => Some(ByteArray::from(value.clone())),
                        _ => None,
                    })
                    .collect();
                let levels: Vec<_> = expected
                    .iter()
                    .map(|row| {
                        if matches!(row[0], SqlValue::Null) {
                            0
                        } else {
                            1
                        }
                    })
                    .collect();
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&values, Some(&levels), None)
                    .unwrap();
                column.close().unwrap();
                group.close().unwrap();
                writer.close().unwrap();
                let (result, maximum) = execute_observed_after_planning(&file, || {});
                println!(
                    "DELTA {encoding:?} nullable version={version:?} compression={compression:?} count={count} all_null={all_null} maximum_request={maximum}"
                );
                assert!(maximum <= LIMIT);
                let ExecutionResult::Query(result) = result.unwrap() else {
                    panic!("expected nullable DELTA length rows");
                };
                assert_eq!(result.rows, expected);
            }
        }
    }
}

#[test]
fn issue577_gzip_output_checked_before_capacity_growth() {
    codec_output_checked_before_capacity_growth(Compression::GZIP(Default::default()));
}

#[test]
fn issue577_snappy_output_checked_before_capacity_growth() {
    codec_output_checked_before_capacity_growth(Compression::SNAPPY);
}

#[test]
fn issue577_codec_brotli_output_checked_before_capacity_growth() {
    codec_output_checked_before_capacity_growth(Compression::BROTLI(Default::default()));
}

#[test]
fn issue577_brotli_workspace_sum_checked_before_allocation() {
    use parquet::data_type::{ByteArray, ByteArrayType};

    // The default 4 MiB window fits; wide output plus its workspace exceeds 6 MiB.
    let limit = 6 * 1024 * 1024;
    for (limit, width, reject) in [
        (limit, 8, false),
        (limit, LIMIT, true),
        (limit * 2, 8, false),
        (limit * 2, LIMIT, false),
    ] {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let schema =
            Arc::new(parse_message_type("message fixture { REQUIRED BYTE_ARRAY body; }").unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::BROTLI(Default::default()))
                .set_dictionary_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<ByteArrayType>()
            .write_batch(&vec![ByteArray::from(vec![0x42; width]); 32], None, None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
        writer.close().unwrap();
        assert!(file.as_file().metadata().unwrap().len() < 4096);
        LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
        let (result, maximum) = execute_observed_with_alias(&file, "", Some(limit), || {});
        let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
        assert!(!observed.exhausted);
        println!(
            "Brotli workspace limit={limit} width={width} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
            observed.peak,
            result.is_err()
        );
        assert!(maximum <= limit);
        assert!(
            observed.peak <= limit,
            "Brotli combined workspace exceeded its budget"
        );
        if reject {
            assert!(matches!(
                result,
                Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected normal Brotli rows");
            };
            assert_eq!(
                result.rows,
                vec![vec![SqlValue::Blob(vec![0x42; width])]; 32]
            );
        }
    }
}

fn replace_with_brotli_huffman_fixture(file: &tempfile::NamedTempFile) {
    use std::io::Cursor;
    use thrift::protocol::TCompactInputProtocol;

    let bytes = std::fs::read(file.path()).unwrap();
    let footer_size =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap()) as usize;
    let mut footer = parquet::format::FileMetaData::read_from_in_protocol(
        &mut TCompactInputProtocol::new(Cursor::new(&bytes[bytes.len() - 8 - footer_size..])),
    )
    .unwrap();
    let chunk = &mut footer.row_groups[0].columns[0];
    let metadata = chunk.meta_data.as_mut().unwrap();
    let offset = metadata.data_page_offset as usize;
    let mut header = parquet::format::PageHeader::read_from_in_protocol(
        &mut TCompactInputProtocol::new(Cursor::new(&bytes[offset..])),
    )
    .unwrap();
    // Window 10, eight output bytes, 256 literal trees; the tree data is incomplete.
    let input = [0xa1, 0x38, 0, 0, 0xc0, 0xff, 5, 0];
    header.uncompressed_page_size = 8;
    header.compressed_page_size = input.len() as i32;
    let mut encoded_header = Vec::new();
    header
        .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded_header))
        .unwrap();
    metadata.codec = parquet::format::CompressionCodec::BROTLI;
    metadata.total_compressed_size = (encoded_header.len() + input.len()) as i64;
    metadata.total_uncompressed_size = metadata.total_compressed_size;
    let total_size = metadata.total_compressed_size;
    chunk.offset_index_offset = None;
    chunk.offset_index_length = None;
    chunk.column_index_offset = None;
    chunk.column_index_length = None;
    footer.row_groups[0].total_byte_size = total_size;
    footer.row_groups[0].total_compressed_size = Some(total_size);
    let mut changed = bytes[..offset].to_vec();
    changed.extend_from_slice(&encoded_header);
    changed.extend_from_slice(&input);
    let mut encoded_footer = Vec::new();
    footer
        .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded_footer))
        .unwrap();
    changed.extend_from_slice(&encoded_footer);
    changed.extend_from_slice(&(encoded_footer.len() as u32).to_le_bytes());
    changed.extend_from_slice(b"PAR1");
    assert!(changed.len() < 4096);
    std::fs::write(file.path(), changed).unwrap();
}

#[test]
fn issue577_brotli_huffman_checked_before_allocation() {
    let file = fixture(false);
    let (result, maximum) =
        execute_observed_after_planning(&file, || replace_with_brotli_huffman_fixture(&file));
    assert!(matches!(
        result,
        Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
    ));
    println!("Brotli Huffman maximum_request={maximum} limit={LIMIT}");
    assert!(
        maximum <= LIMIT,
        "Huffman request exceeded the decoding budget"
    );
}

#[test]
fn issue577_brotli_minimum_workspace_checked_before_allocation() {
    let file = fixture(false);
    let limit = 8192;
    let (result, maximum) = execute_observed_with_alias(&file, "", Some(limit), || {
        replace_with_brotli_huffman_fixture(&file);
    });
    let error = result.unwrap_err();
    assert!(error.to_string().contains("Brotli requires 12960 bytes"));
    println!("Brotli minimum workspace maximum_request={maximum} limit={limit}");
    assert!(maximum <= limit);
}

#[test]
fn issue577_zstd_scratch_checked_before_allocation() {
    let context_bytes = zstd::zstd_safe::DCtx::create().sizeof();
    for (rows, page_rows, group_rows) in [(1, 1, 1), (64, 16, 64), (64, 64, 16)] {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let schema =
            Arc::new(parse_message_type("message fixture { REQUIRED INT64 id; }").unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_dictionary_enabled(false)
                .set_data_page_row_count_limit(page_rows)
                .set_write_batch_size(1)
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        for values in vec![7; rows].chunks(group_rows) {
            let mut group = writer.next_row_group().unwrap();
            let mut column = group.next_column().unwrap().unwrap();
            column
                .typed::<Int64Type>()
                .write_batch(values, None, None)
                .unwrap();
            column.close().unwrap();
            group.close().unwrap();
        }
        writer.close().unwrap();
        let row_slots = rows * (size_of::<Vec<SqlValue>>() + size_of::<SqlValue>());
        for (limit, reject) in [
            (None, false),
            (Some(context_bytes + LIMIT), false),
            (Some(context_bytes - 1), true),
            (Some(context_bytes + row_slots - 1), true),
        ] {
            LIVE_ALLOCATIONS.with(|observed| *observed.borrow_mut() = Some(LiveAllocations::new()));
            let (result, maximum) = execute_observed_with_alias(&file, "", limit, || {});
            let observed = LIVE_ALLOCATIONS.with(|observed| observed.borrow_mut().take().unwrap());
            assert!(!observed.exhausted);
            println!(
                "ZSTD scratch context_bytes={context_bytes} limit={limit:?} rows={rows} page_rows={page_rows} group_rows={group_rows} maximum_request={maximum} rust_live_peak_bytes={} rejected={}",
                observed.peak,
                result.is_err()
            );
            if let Some(limit) = limit {
                assert!(maximum <= limit);
                assert!(observed.peak <= limit);
            }
            if reject {
                assert!(matches!(
                    result,
                    Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
                ));
            } else {
                let ExecutionResult::Query(result) = result.unwrap() else {
                    panic!("expected normal ZSTD rows");
                };
                assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]; rows]);
            }
        }
    }
}

#[test]
fn issue577_codec_zstd_output_checked_before_capacity_growth() {
    codec_output_checked_before_capacity_growth(Compression::ZSTD(Default::default()));
}

#[test]
fn issue577_codec_lz4_raw_output_checked_before_capacity_growth() {
    codec_output_checked_before_capacity_growth(Compression::LZ4_RAW);
}

fn codec_test_limit(compression: Compression) -> usize {
    // ZSTD's native context must fit alongside the existing page/result allowance.
    if matches!(compression, Compression::ZSTD(_)) {
        zstd::zstd_safe::DCtx::create().sizeof() + LIMIT
    } else {
        LIMIT
    }
}

fn codec_output_checked_before_capacity_growth(compression: Compression) {
    use parquet::data_type::{ByteArray, ByteArrayType};
    use parquet::file::properties::EnabledStatistics;

    let limit = codec_test_limit(compression);
    for large in [false, true] {
        let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
        let schema =
            Arc::new(parse_message_type("message fixture { REQUIRED BYTE_ARRAY body; }").unwrap());
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(compression)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .build(),
        );
        let mut writer =
            SerializedFileWriter::new(File::create(file.path()).unwrap(), schema, properties)
                .unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        let payload = vec![0; if large { limit + 1 } else { 8 }];
        column
            .typed::<ByteArrayType>()
            .write_batch(&[ByteArray::from(payload)], None, None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
        let mut footer = writer.close().unwrap();
        let (result, maximum) = execute_observed_with_alias(&file, "", Some(limit), || {
            if !large {
                return;
            }
            let mut bytes = std::fs::read(file.path()).unwrap();
            let footer_size =
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                    as usize;
            bytes.truncate(bytes.len() - 8 - footer_size);
            let metadata = footer.row_groups[0].columns[0].meta_data.as_mut().unwrap();
            let offset = metadata.data_page_offset as usize;
            assert_eq!(&bytes[offset..offset + 3], &[0x15, 0, 0x15]);
            let mut end = offset + 3;
            while bytes[end] & 128 != 0 {
                end += 1;
            }
            // Preserve the encoded width and every physical offset while declaring eight bytes.
            let width = end - (offset + 3) + 1;
            assert!(width > 1);
            bytes[offset + 3] = 16 | 128;
            bytes[offset + 4..end].fill(128);
            bytes[end] = 0;
            metadata.total_uncompressed_size = 1024;
            footer.row_groups[0].total_byte_size = 1024;
            let mut encoded = Vec::new();
            footer
                .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded))
                .unwrap();
            assert!(encoded.len() < 4096);
            bytes.extend_from_slice(&encoded);
            bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            bytes.extend_from_slice(b"PAR1");
            assert!(bytes.len() < 4096);
            std::fs::write(file.path(), bytes).unwrap();
        });
        println!(
            "{compression:?} control large={large} limit={limit} maximum_request={maximum} result={result:?}"
        );
        if large {
            assert!(matches!(
                result,
                Err(ExecutorError::BulkLoad(_)) | Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            let ExecutionResult::Query(result) = result.unwrap() else {
                panic!("expected a normal compressed query result");
            };
            assert_eq!(result.rows, vec![vec![SqlValue::Blob(vec![0; 8])]]);
        }
        assert!(
            maximum <= limit,
            "{compression:?} output grew beyond the decoding budget"
        );
    }
}

#[test]
fn issue577_oversized_page_rejected_before_allocation() {
    let file = fixture(true);
    let (result, maximum) = execute_observed(&file);
    println!(
        "oversized fixture bytes={} maximum_request={maximum} limit={LIMIT} declared_page_bytes={DECLARED_PAGE_BYTES} result={result:?}",
        file.as_file().metadata().unwrap().len()
    );
    assert!(
        maximum <= LIMIT,
        "READ_PARQUET requested {maximum} bytes before rejecting a page under its {LIMIT}-byte budget"
    );
    assert!(
        matches!(result, Err(ExecutorError::ResourceExhausted { .. })),
        "expected a memory-policy rejection before page decompression, got {result:?}"
    );
}

#[test]
fn issue577_invalid_page_declarations_rejected_before_allocation() {
    for sizes in [(-1, 10), (8, -1), (8, 1024), (i32::MAX, 10), (8, i32::MAX)] {
        let file = fixture_with_sizes(Some(sizes));
        let (result, maximum) = execute_observed(&file);
        println!("sizes={sizes:?} maximum_request={maximum} result={result:?}");
        assert!(maximum <= LIMIT);
        if sizes.0 > LIMIT as i32 || sizes.1 > LIMIT as i32 {
            assert!(matches!(
                result,
                Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            assert!(matches!(result, Err(ExecutorError::BulkLoad(_))));
        }
    }
}

#[test]
fn issue577_malformed_headers_and_footer_rejected_before_allocation() {
    for case in [
        "varint overflow",
        "binary length",
        "missing stop",
        "footer length",
        "explicit field id overflow",
        "delta field id overflow",
    ] {
        let file = fixture(false);
        let (result, maximum) = execute_observed_after_planning(&file, || {
            let reader = SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
            let column = reader.metadata().row_group(0).column(0);
            let start = column.data_page_offset() as usize;
            let end = start + column.compressed_size() as usize;
            let mut bytes = std::fs::read(file.path()).unwrap();
            match case {
                "varint overflow" => {
                    bytes[start..end].fill(0xff);
                    bytes[start..start + 3].copy_from_slice(&[0x15, 0, 0x15]);
                }
                "binary length" => {
                    // An unknown binary field must be skipped without allocating its declaration.
                    bytes[start..start + 5].copy_from_slice(&[0x18, 0x80, 0x80, 0x80, 0x01]);
                }
                "missing stop" => bytes[start..end].fill(0x11),
                "footer length" => {
                    let trailer = bytes.len() - 8;
                    bytes[trailer..trailer + 4]
                        .copy_from_slice(&(DECLARED_PAGE_BYTES as u32).to_le_bytes());
                }
                "explicit field id overflow" => {
                    // Explicit compact IDs are i16, not arbitrary zigzag integers.
                    bytes[start..start + 5].copy_from_slice(&[0x05, 0x84, 0x80, 0x08, 0x00]);
                }
                "delta field id overflow" => {
                    bytes[start..start + 6].copy_from_slice(&[0x01, 0xfe, 0xff, 0x03, 0xf1, 0x00]);
                }
                _ => unreachable!(),
            }
            std::fs::write(file.path(), bytes).unwrap();
        });
        println!("case={case} maximum_request={maximum} result={result:?}");
        assert!(maximum <= LIMIT);
        if case == "footer length" {
            assert!(matches!(
                result,
                Err(ExecutorError::ResourceExhausted { .. })
            ));
        } else {
            assert!(matches!(result, Err(ExecutorError::BulkLoad(_))));
        }
    }
}

#[test]
fn issue577_dictionary_count_rejected_before_allocation() {
    let file = fixture_with_dictionary(None, true);
    let (result, maximum) = execute_observed(&file);
    let ExecutionResult::Query(result) = result.unwrap() else {
        panic!("expected a normal dictionary to produce a query result");
    };
    assert_eq!(result.rows, vec![vec![SqlValue::BigInt(7)]]);
    assert!(maximum <= LIMIT);
    for count in [-1, (LIMIT / std::mem::size_of::<i64>() + 1) as i32] {
        let file = fixture(false);
        let (result, maximum) = execute_observed_after_planning(&file, || {
            let reader = SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
            let start = reader.metadata().row_group(0).column(0).data_page_offset() as usize;
            let mut bytes = std::fs::read(file.path()).unwrap();
            // DICTIONARY_PAGE, small byte sizes, then DictionaryPageHeader.num_values.
            let mut header = vec![0x15, 0x04, 0x15, 0x10, 0x15, 0x14, 0x4c, 0x15];
            let mut value = ((count as u32) << 1) ^ ((count >> 31) as u32);
            while value >= 128 {
                header.push((value as u8 & 0x7f) | 0x80);
                value >>= 7;
            }
            header.extend_from_slice(&[value as u8, 0x15, 0x00, 0x00, 0x00]);
            bytes[start..start + header.len()].copy_from_slice(&header);
            std::fs::write(file.path(), bytes).unwrap();
        });
        println!("dictionary count={count} maximum_request={maximum} result={result:?}");
        assert!(maximum <= LIMIT);
        if count < 0 {
            assert!(matches!(result, Err(ExecutorError::BulkLoad(_))));
        } else {
            assert!(matches!(
                result,
                Err(ExecutorError::ResourceExhausted { .. })
            ));
        }
    }
}

#[test]
fn issue577_schema_change_precedes_page_and_row_budget_checks() {
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    for columns in [1, 2] {
        let file = fixture(false);
        let (result, maximum) = execute_observed_after_planning(&file, || {
            let fields = (0..columns)
                .map(|index| Field::new(format!("id{index}"), DataType::Int32, false))
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields));
            let arrays = (0..columns)
                .map(|_| Arc::new(arrow_array::Int32Array::from(vec![7; 1024])) as _)
                .collect();
            let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
            let mut writer =
                ArrowWriter::try_new(File::create(file.path()).unwrap(), schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let reader = SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
            let column = reader.metadata().row_group(0).column(0);
            let start = column
                .dictionary_page_offset()
                .unwrap_or(column.data_page_offset()) as usize;
            let mut bytes = std::fs::read(file.path()).unwrap();
            bytes[start] = 0;
            std::fs::write(file.path(), bytes).unwrap();
        });
        println!("schema columns={columns} maximum_request={maximum} result={result:?}");
        assert!(maximum <= LIMIT);
        assert!(matches!(result, Err(ExecutorError::SchemaMismatch { .. })));
    }
}

#[test]
fn issue577_footer_metadata_allocations_rejected_before_decode() {
    use parquet::format;
    use std::mem::size_of;

    // Each path uses explicit compact field IDs; intermediate lists have one struct.
    type MetadataCase = (&'static str, &'static [(u8, u8)], usize, u8);
    let cases: &[MetadataCase] = &[
        ("schema", &[(2, 9)], size_of::<format::SchemaElement>(), 12),
        ("row groups", &[(4, 9)], size_of::<format::RowGroup>(), 12),
        (
            "file key/value",
            &[(5, 9)],
            size_of::<format::KeyValue>(),
            12,
        ),
        (
            "column orders",
            &[(7, 9)],
            size_of::<format::ColumnOrder>(),
            12,
        ),
        (
            "columns",
            &[(4, 9), (1, 9)],
            size_of::<format::ColumnChunk>(),
            12,
        ),
        (
            "sorting",
            &[(4, 9), (4, 9)],
            size_of::<format::SortingColumn>(),
            12,
        ),
        (
            "encodings",
            &[(4, 9), (1, 9), (3, 12), (2, 9)],
            size_of::<format::Encoding>(),
            5,
        ),
        (
            "schema path",
            &[(4, 9), (1, 9), (3, 12), (3, 9)],
            size_of::<String>(),
            8,
        ),
        (
            "column key/value",
            &[(4, 9), (1, 9), (3, 12), (8, 9)],
            size_of::<format::KeyValue>(),
            12,
        ),
        (
            "encoding stats",
            &[(4, 9), (1, 9), (3, 12), (13, 9)],
            size_of::<format::PageEncodingStats>(),
            12,
        ),
        (
            "repetition histogram",
            &[(4, 9), (1, 9), (3, 12), (16, 12), (2, 9)],
            size_of::<i64>(),
            6,
        ),
        (
            "definition histogram",
            &[(4, 9), (1, 9), (3, 12), (16, 12), (3, 9)],
            size_of::<i64>(),
            6,
        ),
        (
            "encryption path",
            &[(4, 9), (1, 9), (8, 12), (2, 12), (1, 9)],
            size_of::<String>(),
            8,
        ),
        ("schema name", &[(2, 9), (4, 8)], 1, 8),
        ("created by", &[(6, 8)], 1, 8),
        ("unknown binary", &[(10, 8)], 1, 8),
    ];
    let mut failures = Vec::new();
    for &(name, path, width, element) in cases {
        let count = LIMIT.checked_div(width).map_or(17, |count| count + 1);
        let file = fixture(false);
        let (result, maximum) = execute_observed_after_planning(&file, || {
            let mut footer = vec![0x15, 0x02];
            for (index, &(id, kind)) in path.iter().enumerate() {
                footer.extend_from_slice(&[kind, id * 2]);
                if kind == 9 {
                    footer.push(if index + 1 == path.len() {
                        0xf0 | element
                    } else {
                        0x1c
                    });
                }
            }
            let mut value = count as u64;
            while value >= 128 {
                footer.push((value as u8 & 0x7f) | 0x80);
                value >>= 7;
            }
            footer.extend_from_slice(&[value as u8, 0, 0]);
            replace_footer(&file, &footer);
        });
        println!(
            "metadata={name} slots={count} width={width} declared={} maximum_request={maximum} result={result:?}",
            count * width
        );
        let expected = if width == 0 {
            matches!(result, Err(ExecutorError::BulkLoad(_)))
        } else {
            matches!(result, Err(ExecutorError::ResourceExhausted { .. }))
        };
        if maximum > LIMIT || !expected {
            failures.push(name);
        }
    }
    assert!(
        failures.is_empty(),
        "metadata preflight failures: {failures:?}"
    );
}

#[test]
fn issue577_invalid_metadata_wire_types_rejected_before_decode() {
    let cases: &[(&str, &[u8])] = &[
        ("scalar field type", &[0x18, 0]),
        ("list field type", &[0x15, 2, 0x18, 0]),
        ("list element type", &[0x15, 2, 0x19, 0x18, 0]),
        (
            "nested list element type",
            &[0x15, 2, 0x09, 8, 0x1c, 0x09, 2, 0x18, 0],
        ),
        ("truncated list", &[0x15, 2, 0x19, 0x2c, 0]),
        ("truncated unknown binary", &[0x15, 2, 0x98, 4, 0]),
        ("truncated unknown list", &[0x15, 2, 0x99, 0xfc, 16, 0]),
    ];
    for &(name, footer) in cases {
        let file = fixture(false);
        let (result, maximum) = execute_observed_after_planning(&file, || {
            replace_footer(&file, footer);
        });
        println!("invalid metadata={name} maximum_request={maximum} result={result:?}");
        assert!(maximum <= LIMIT);
        assert!(matches!(result, Err(ExecutorError::BulkLoad(_))));
    }
}

#[test]
fn issue577_page_reader_adapter_preserves_normal_pages() {
    use arrow_array::{Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::column::page::Page;
    use parquet::file::properties::WriterVersion;

    for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
        for dictionary in [false, true] {
            for compression in [
                Compression::UNCOMPRESSED,
                Compression::SNAPPY,
                Compression::GZIP(Default::default()),
                Compression::ZSTD(Default::default()),
                Compression::BROTLI(Default::default()),
            ] {
                let limit = codec_test_limit(compression);
                let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("label", DataType::LargeUtf8, true),
                ]));
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(vec![
                            Some(1),
                            None,
                            Some(3),
                            Some(4),
                            None,
                        ])),
                        Arc::new(LargeStringArray::from(vec![
                            Some("alpha"),
                            Some("repeat"),
                            None,
                            Some("repeat"),
                            Some("omega"),
                        ])),
                    ],
                )
                .unwrap();
                let properties = WriterProperties::builder()
                    .set_writer_version(version)
                    .set_dictionary_enabled(dictionary)
                    .set_compression(compression)
                    .set_max_row_group_size(3)
                    .set_data_page_row_count_limit(1)
                    .set_write_batch_size(1)
                    .build();
                let mut writer = ArrowWriter::try_new(
                    File::create(file.path()).unwrap(),
                    schema,
                    Some(properties),
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                let reference =
                    SerializedFileReader::new(File::open(file.path()).unwrap()).unwrap();
                assert_eq!(reference.num_row_groups(), 2);
                let mut pages = reference
                    .get_row_group(0)
                    .unwrap()
                    .get_column_page_reader(0)
                    .unwrap();
                let mut data_pages = 0;
                let mut dictionaries = 0;
                while let Some(page) = pages.get_next_page().unwrap() {
                    match page {
                        Page::DataPage { .. } => {
                            assert_eq!(version, WriterVersion::PARQUET_1_0);
                            data_pages += 1;
                        }
                        Page::DataPageV2 { .. } => {
                            assert_eq!(version, WriterVersion::PARQUET_2_0);
                            data_pages += 1;
                        }
                        Page::DictionaryPage { .. } => dictionaries += 1,
                    }
                }
                assert!(data_pages > 1);
                assert_eq!(dictionaries, usize::from(dictionary));

                let (result, maximum) = execute_observed_with_alias(
                    &file,
                    "AS p(renamed_id, renamed_label)",
                    Some(limit),
                    || {},
                );
                let ExecutionResult::Query(result) = result.unwrap() else {
                    panic!("expected a normal query result");
                };
                assert_eq!(
                    result
                        .columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect::<Vec<_>>(),
                    vec!["renamed_id", "renamed_label"]
                );
                assert_eq!(
                    result.rows,
                    vec![
                        vec![SqlValue::BigInt(1), SqlValue::Text("alpha".into())],
                        vec![SqlValue::Null, SqlValue::Text("repeat".into())],
                        vec![SqlValue::BigInt(3), SqlValue::Null],
                        vec![SqlValue::BigInt(4), SqlValue::Text("repeat".into())],
                        vec![SqlValue::Null, SqlValue::Text("omega".into())],
                    ]
                );
                println!(
                    "normal seam version={version:?} dictionary={dictionary} compression={compression:?} row_groups=2 data_pages={data_pages} maximum_request={maximum}"
                );
                assert!(maximum <= limit);
            }
        }
    }
}

#[test]
fn issue577_page_reader_adapter_preserves_empty_file() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(File::create(file.path()).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let (result, maximum) = execute_observed(&file);
    let ExecutionResult::Query(result) = result.unwrap() else {
        panic!("expected an empty query result");
    };
    assert!(result.rows.is_empty());
    assert_eq!(result.columns[0].name, "id");
    assert!(maximum <= LIMIT);
}
