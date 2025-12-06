//! Tests for the catalog module.

use super::*;
use crate::ast::ddl::IndexMethod;
use crate::planner::types::ResolvedType;

/// Helper function to create a test table metadata.
fn create_test_table(name: &str) -> TableMetadata {
    TableMetadata::new(
        name,
        vec![
            ColumnMetadata::new("id", ResolvedType::Integer)
                .with_primary_key(true)
                .with_not_null(true),
            ColumnMetadata::new("name", ResolvedType::Text).with_not_null(true),
            ColumnMetadata::new("age", ResolvedType::Integer),
        ],
    )
    .with_primary_key(vec!["id".to_string()])
}

/// Helper function to create a test index metadata.
fn create_test_index(name: &str, table: &str, column: &str) -> IndexMetadata {
    IndexMetadata::new(name, table, column).with_method(IndexMethod::BTree)
}

mod memory_catalog_tests {
    use super::*;

    // ==================== Table CRUD Tests ====================

    #[test]
    fn test_create_table_success() {
        let mut catalog = MemoryCatalog::new();
        let table = create_test_table("users");

        let result = catalog.create_table(table);
        assert!(result.is_ok());
        assert_eq!(catalog.table_count(), 1);
        assert!(catalog.table_exists("users"));
    }

    #[test]
    fn test_create_table_duplicate_error() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();

        let result = catalog.create_table(create_test_table("users"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PlannerError::TableAlreadyExists { name } => {
                assert_eq!(name, "users");
            }
            e => panic!("Expected TableAlreadyExists, got {:?}", e),
        }
    }

    #[test]
    fn test_create_multiple_tables() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog.create_table(create_test_table("orders")).unwrap();
        catalog.create_table(create_test_table("products")).unwrap();

        assert_eq!(catalog.table_count(), 3);
        assert!(catalog.table_exists("users"));
        assert!(catalog.table_exists("orders"));
        assert!(catalog.table_exists("products"));
    }

    #[test]
    fn test_get_table_existing() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();

        let table = catalog.get_table("users");
        assert!(table.is_some());
        let table = table.unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.column_count(), 3);
    }

    #[test]
    fn test_get_table_not_found() {
        let catalog = MemoryCatalog::new();
        assert!(catalog.get_table("nonexistent").is_none());
    }

    #[test]
    fn test_drop_table_success() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        assert!(catalog.table_exists("users"));

        let result = catalog.drop_table("users");
        assert!(result.is_ok());
        assert!(!catalog.table_exists("users"));
        assert_eq!(catalog.table_count(), 0);
    }

    #[test]
    fn test_drop_table_not_found() {
        let mut catalog = MemoryCatalog::new();

        let result = catalog.drop_table("nonexistent");
        assert!(result.is_err());
        match result.unwrap_err() {
            PlannerError::TableNotFound { name, .. } => {
                assert_eq!(name, "nonexistent");
            }
            e => panic!("Expected TableNotFound, got {:?}", e),
        }
    }

    #[test]
    fn test_drop_table_also_drops_indexes() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();
        catalog
            .create_index(create_test_index("idx_users_age", "users", "age"))
            .unwrap();

        assert_eq!(catalog.index_count(), 2);

        catalog.drop_table("users").unwrap();

        assert!(!catalog.table_exists("users"));
        assert_eq!(catalog.index_count(), 0);
    }

    #[test]
    fn test_table_exists() {
        let mut catalog = MemoryCatalog::new();
        assert!(!catalog.table_exists("users"));

        catalog.create_table(create_test_table("users")).unwrap();
        assert!(catalog.table_exists("users"));
        assert!(!catalog.table_exists("orders"));
    }

    // ==================== Index CRUD Tests ====================

    #[test]
    fn test_create_index_success() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();

        let index = create_test_index("idx_users_name", "users", "name");
        let result = catalog.create_index(index);
        assert!(result.is_ok());
        assert_eq!(catalog.index_count(), 1);
        assert!(catalog.index_exists("idx_users_name"));
    }

    #[test]
    fn test_create_index_duplicate_error() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();

        let result = catalog.create_index(create_test_index("idx_users_name", "users", "age"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PlannerError::IndexAlreadyExists { name } => {
                assert_eq!(name, "idx_users_name");
            }
            e => panic!("Expected IndexAlreadyExists, got {:?}", e),
        }
    }

    #[test]
    fn test_create_index_table_not_found() {
        let mut catalog = MemoryCatalog::new();
        // Try to create index on non-existent table
        let result = catalog.create_index(create_test_index("idx_users_name", "users", "name"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PlannerError::TableNotFound { name, .. } => {
                assert_eq!(name, "users");
            }
            e => panic!("Expected TableNotFound, got {:?}", e),
        }
    }

    #[test]
    fn test_create_index_column_not_found() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();

        // Try to create index on non-existent column
        let result = catalog.create_index(create_test_index("idx_users_email", "users", "email"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PlannerError::ColumnNotFound { column, table, .. } => {
                assert_eq!(column, "email");
                assert_eq!(table, "users");
            }
            e => panic!("Expected ColumnNotFound, got {:?}", e),
        }
    }

    #[test]
    fn test_create_multiple_indexes() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();

        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();
        catalog
            .create_index(create_test_index("idx_users_age", "users", "age"))
            .unwrap();

        assert_eq!(catalog.index_count(), 2);
    }

    #[test]
    fn test_get_index_existing() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog
            .create_index(
                IndexMetadata::new("idx_users_name", "users", "name")
                    .with_method(IndexMethod::Hnsw)
                    .with_option("m", "16"),
            )
            .unwrap();

        let index = catalog.get_index("idx_users_name");
        assert!(index.is_some());
        let index = index.unwrap();
        assert_eq!(index.name, "idx_users_name");
        assert_eq!(index.table, "users");
        assert_eq!(index.column, "name");
        assert_eq!(index.method, Some(IndexMethod::Hnsw));
        assert_eq!(index.get_option("m"), Some("16"));
    }

    #[test]
    fn test_get_index_not_found() {
        let catalog = MemoryCatalog::new();
        assert!(catalog.get_index("nonexistent").is_none());
    }

    #[test]
    fn test_get_indexes_for_table() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog.create_table(create_test_table("orders")).unwrap();

        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();
        catalog
            .create_index(create_test_index("idx_users_age", "users", "age"))
            .unwrap();
        catalog
            .create_index(create_test_index("idx_orders_id", "orders", "id"))
            .unwrap();

        let user_indexes = catalog.get_indexes_for_table("users");
        assert_eq!(user_indexes.len(), 2);

        let order_indexes = catalog.get_indexes_for_table("orders");
        assert_eq!(order_indexes.len(), 1);

        let empty_indexes = catalog.get_indexes_for_table("nonexistent");
        assert!(empty_indexes.is_empty());
    }

    #[test]
    fn test_drop_index_success() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();
        assert!(catalog.index_exists("idx_users_name"));

        let result = catalog.drop_index("idx_users_name");
        assert!(result.is_ok());
        assert!(!catalog.index_exists("idx_users_name"));
        assert_eq!(catalog.index_count(), 0);
    }

    #[test]
    fn test_drop_index_not_found() {
        let mut catalog = MemoryCatalog::new();

        let result = catalog.drop_index("nonexistent");
        assert!(result.is_err());
        match result.unwrap_err() {
            PlannerError::IndexNotFound { name } => {
                assert_eq!(name, "nonexistent");
            }
            e => panic!("Expected IndexNotFound, got {:?}", e),
        }
    }

    #[test]
    fn test_index_exists() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();

        assert!(!catalog.index_exists("idx_users_name"));

        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();

        assert!(catalog.index_exists("idx_users_name"));
        assert!(!catalog.index_exists("idx_other"));
    }

    // ==================== Utility Method Tests ====================

    #[test]
    fn test_table_names() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog.create_table(create_test_table("orders")).unwrap();

        let names = catalog.table_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
    }

    #[test]
    fn test_index_names() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog
            .create_index(create_test_index("idx_a", "users", "name"))
            .unwrap();
        catalog
            .create_index(create_test_index("idx_b", "users", "age"))
            .unwrap();

        let names = catalog.index_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"idx_a"));
        assert!(names.contains(&"idx_b"));
    }

    #[test]
    fn test_clear() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();
        catalog.create_table(create_test_table("orders")).unwrap();
        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();

        assert_eq!(catalog.table_count(), 2);
        assert_eq!(catalog.index_count(), 1);

        catalog.clear();

        assert_eq!(catalog.table_count(), 0);
        assert_eq!(catalog.index_count(), 0);
    }

    // ==================== Complex Scenario Tests ====================

    #[test]
    fn test_recreate_dropped_table() {
        let mut catalog = MemoryCatalog::new();

        catalog.create_table(create_test_table("users")).unwrap();
        catalog.drop_table("users").unwrap();
        catalog.create_table(create_test_table("users")).unwrap();

        assert!(catalog.table_exists("users"));
    }

    #[test]
    fn test_recreate_dropped_index() {
        let mut catalog = MemoryCatalog::new();
        catalog.create_table(create_test_table("users")).unwrap();

        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();
        catalog.drop_index("idx_users_name").unwrap();
        catalog
            .create_index(create_test_index("idx_users_name", "users", "name"))
            .unwrap();

        assert!(catalog.index_exists("idx_users_name"));
    }

    #[test]
    fn test_table_with_vector_column() {
        use crate::ast::ddl::VectorMetric;

        let mut catalog = MemoryCatalog::new();

        let table = TableMetadata::new(
            "items",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer).with_primary_key(true),
                ColumnMetadata::new("name", ResolvedType::Text),
                ColumnMetadata::new(
                    "embedding",
                    ResolvedType::Vector {
                        dimension: 128,
                        metric: VectorMetric::Cosine,
                    },
                ),
            ],
        );

        catalog.create_table(table).unwrap();

        let table = catalog.get_table("items").unwrap();
        let embedding_col = table.get_column("embedding").unwrap();

        match &embedding_col.data_type {
            ResolvedType::Vector { dimension, metric } => {
                assert_eq!(*dimension, 128);
                assert_eq!(*metric, VectorMetric::Cosine);
            }
            _ => panic!("Expected Vector type"),
        }
    }

    #[test]
    fn test_hnsw_index_with_options() {
        let mut catalog = MemoryCatalog::new();

        let table = TableMetadata::new(
            "items",
            vec![
                ColumnMetadata::new("id", ResolvedType::Integer),
                ColumnMetadata::new(
                    "embedding",
                    ResolvedType::Vector {
                        dimension: 128,
                        metric: crate::ast::ddl::VectorMetric::Cosine,
                    },
                ),
            ],
        );
        catalog.create_table(table).unwrap();

        let index = IndexMetadata::new("idx_items_embedding", "items", "embedding")
            .with_method(IndexMethod::Hnsw)
            .with_option("m", "16")
            .with_option("ef_construction", "200");

        catalog.create_index(index).unwrap();

        let index = catalog.get_index("idx_items_embedding").unwrap();
        assert_eq!(index.method, Some(IndexMethod::Hnsw));
        assert_eq!(index.get_option("m"), Some("16"));
        assert_eq!(index.get_option("ef_construction"), Some("200"));
    }
}
