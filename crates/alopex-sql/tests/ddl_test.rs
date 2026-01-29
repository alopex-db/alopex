use alopex_sql::{
    AlopexDialect, CreateIndex, CreateTable, DataType, DropIndex, DropTable, IndexMethod,
    Tokenizer, parser::Parser,
};

fn parse_with<F, T>(sql: &str, f: F) -> T
where
    F: FnOnce(&mut Parser<'_>) -> T,
{
    let dialect = AlopexDialect;
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();
    let mut parser = Parser::new(&dialect, tokens);
    f(&mut parser)
}

#[test]
fn parse_create_table_with_constraints_and_vector() {
    let table: CreateTable = parse_with(
        "CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding VECTOR(384, COSINE))",
        |p| p.parse_create_table().unwrap(),
    );

    assert!(table.if_not_exists);
    assert_eq!(table.name, "documents");
    assert_eq!(table.columns.len(), 3);
    assert!(table.with_options.is_empty());
    assert!(matches!(
        table.columns[0].constraints.first().unwrap(),
        alopex_sql::ColumnConstraint::WithSpan { .. }
    ));
    match &table.columns[2].data_type {
        DataType::Vector {
            dimension,
            metric: Some(alopex_sql::VectorMetric::Cosine),
        } => {
            assert_eq!(*dimension, 384);
        }
        other => panic!("unexpected vector type {:?}", other),
    }
}

#[test]
fn parse_drop_table_if_exists() {
    let drop: DropTable = parse_with("DROP TABLE IF EXISTS docs", |p| {
        p.parse_drop_table().unwrap()
    });
    assert!(drop.if_exists);
    assert_eq!(drop.name, "docs");
}

#[test]
fn parse_create_index_with_using_and_with_options() {
    let idx: CreateIndex = parse_with(
        "CREATE INDEX idx_doc_embedding ON documents (embedding) USING HNSW WITH (m = 16, ef_construction = 200)",
        |p| p.parse_create_index().unwrap(),
    );
    assert!(!idx.if_not_exists);
    assert_eq!(idx.table, "documents");
    assert_eq!(idx.column, "embedding");
    assert!(matches!(idx.method, Some(IndexMethod::Hnsw)));
    assert_eq!(idx.options.len(), 2);
}

#[test]
fn parse_drop_index_if_exists() {
    let drop: DropIndex = parse_with("DROP INDEX IF EXISTS idx_docs", |p| {
        p.parse_drop_index().unwrap()
    });
    assert!(drop.if_exists);
    assert_eq!(drop.name, "idx_docs");
}

#[test]
fn parse_create_table_with_options() {
    let table: CreateTable = parse_with(
        "CREATE TABLE tbl (id INTEGER) WITH (storage = ' columnar ', compression = 'zstd', row_group_size = 50000)",
        |p| p.parse_create_table().unwrap(),
    );

    assert_eq!(table.with_options.len(), 3);
    assert!(
        table
            .with_options
            .iter()
            .any(|(k, v)| k == "storage" && v == " columnar ")
    );
    assert!(
        table
            .with_options
            .iter()
            .any(|(k, v)| k == "compression" && v == "zstd")
    );
    assert!(
        table
            .with_options
            .iter()
            .any(|(k, v)| k == "row_group_size" && v == "50000")
    );
}
