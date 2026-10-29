//! Bounded catalog snapshots for ALTER and statements in the same request.
//! The coordinator retains its catalog writer until transaction finalization.

use alopex_sql::catalog::persistent::{PersistedIndexMeta, PersistedTableMeta};
use alopex_sql::catalog::Catalog;

use crate::error::{Result, ServerError};
use crate::session::CatalogTableState;

pub(crate) fn capture(catalog: &dyn Catalog, names: &[String]) -> Vec<CatalogTableState> {
    names
        .iter()
        .map(|name| {
            let mut indexes: Vec<_> = catalog
                .get_indexes_for_table(name)
                .into_iter()
                .cloned()
                .collect();
            indexes.sort_by(|left, right| left.name.cmp(&right.name));
            CatalogTableState {
                name: name.clone(),
                table: catalog.get_table(name).cloned(),
                indexes,
            }
        })
        .collect()
}

pub(crate) fn same(left: &CatalogTableState, right: &CatalogTableState) -> Result<bool> {
    let table = |state: &CatalogTableState| {
        state
            .table
            .as_ref()
            .map(PersistedTableMeta::try_from)
            .transpose()
            .map_err(|err| ServerError::Internal(err.to_string()))
    };
    Ok(left.name == right.name
        && table(left)? == table(right)?
        && left
            .indexes
            .iter()
            .map(PersistedIndexMeta::from)
            .eq(right.indexes.iter().map(PersistedIndexMeta::from)))
}

pub(crate) fn restore(
    catalog: &mut dyn Catalog,
    before: &[CatalogTableState],
    after: &[CatalogTableState],
) -> Result<()> {
    let names: Vec<_> = after.iter().map(|state| state.name.clone()).collect();
    let current = capture(catalog, &names);
    // Verify the entire affected set before the first mutation. A changed
    // identity, definition, or attached index must not be overwritten.
    for (current, expected) in current.iter().zip(after) {
        if !same(current, expected)? {
            return Err(ServerError::Conflict(format!(
                "rollback catalog changed: {}",
                current.name
            )));
        }
    }
    for index in before.iter().flat_map(|state| &state.indexes) {
        if let Some(current) = catalog.get_index(&index.name) {
            if !after
                .iter()
                .flat_map(|state| &state.indexes)
                .any(|expected| {
                    PersistedIndexMeta::from(expected) == PersistedIndexMeta::from(current)
                })
            {
                return Err(ServerError::Conflict(format!(
                    "rollback index identity changed: {}",
                    index.name
                )));
            }
        }
    }
    for state in after {
        if state.table.is_some() {
            catalog
                .drop_table(&state.name)
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        }
    }
    for state in before {
        if let Some(table) = &state.table {
            catalog
                .create_table(table.clone())
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        }
    }
    for index in before.iter().flat_map(|state| &state.indexes) {
        catalog
            .create_index(index.clone())
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_sql::catalog::{ColumnMetadata, IndexMetadata, MemoryCatalog, TableMetadata};
    use alopex_sql::planner::ResolvedType;
    use proptest::prelude::*;

    fn catalog() -> MemoryCatalog {
        let mut catalog = MemoryCatalog::new();
        catalog
            .create_table(
                TableMetadata::new(
                    "items",
                    vec![ColumnMetadata::new("value", ResolvedType::Integer)],
                )
                .with_table_id(1),
            )
            .unwrap();
        catalog
            .create_index(
                IndexMetadata::new(2, "value_idx", "items", vec!["value".into()])
                    .with_column_indices(vec![0]),
            )
            .unwrap();
        catalog
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(8))]
        #[test]
        fn independent_metadata_changes_are_conflicts(new_id in 3u32..1024) {
            // One table and one index. Change each component independently;
            // an unchanged table must never hide an index change (or vice versa).
            for component in 0..4 {
                let mut catalog = catalog();
                let names = ["items".into()];
                let before = capture(&catalog, &names);
                let mut table = catalog.get_table("items").unwrap().clone();
                let mut index = catalog.get_index("value_idx").unwrap().clone();
                match component {
                    0 => table.table_id = new_id,
                    1 => table.columns[0].not_null = true,
                    2 => index.index_id = new_id,
                    _ => index.unique = true,
                }
                catalog.drop_table("items").unwrap();
                catalog.create_table(table.clone()).unwrap();
                catalog.create_index(index.clone()).unwrap();
                let observed = capture(&catalog, &names);
                prop_assert!(!same(&before[0], &observed[0]).unwrap());
                let result = restore(&mut catalog, &before, &before);
                prop_assert!(matches!(result, Err(ServerError::Conflict(_))));
                // Rejection is atomic: the foreign metadata remains intact.
                let actual_table = catalog.get_table("items").unwrap();
                let actual_index = catalog.get_index("value_idx").unwrap();
                prop_assert_eq!(actual_table.table_id, table.table_id);
                prop_assert_eq!(actual_table.columns[0].not_null, table.columns[0].not_null);
                prop_assert_eq!(actual_index.index_id, index.index_id);
                prop_assert_eq!(actual_index.unique, index.unique);
            }
        }
    }

    #[test]
    fn restores_renamed_table_and_index_without_touching_unrelated_table() {
        let mut catalog = catalog();
        catalog
            .create_table(TableMetadata::new("other", vec![]).with_table_id(3))
            .unwrap();
        let names = ["items".into(), "renamed".into()];
        let before = capture(&catalog, &names);
        let mut table = catalog.get_table("items").unwrap().clone();
        let mut index = catalog.get_index("value_idx").unwrap().clone();
        catalog.drop_table("items").unwrap();
        table.name = "renamed".into();
        table.columns[0].name = "renamed_value".into();
        index.table = table.name.clone();
        index.columns = vec!["renamed_value".into()];
        catalog.create_table(table).unwrap();
        catalog.create_index(index).unwrap();
        let after = capture(&catalog, &names);
        restore(&mut catalog, &before, &after).unwrap();
        for (actual, original) in capture(&catalog, &names).iter().zip(&before) {
            assert!(same(actual, original).unwrap());
        }
        assert_eq!(catalog.get_table("other").unwrap().table_id, 3);
    }

    #[test]
    fn rejects_same_id_definition_change_before_any_restore() {
        let mut catalog = catalog();
        let names = ["items".into()];
        let before = capture(&catalog, &names);
        let after = before.clone();
        let mut replacement = catalog.get_table("items").unwrap().clone();
        replacement
            .columns
            .push(ColumnMetadata::new("foreign", ResolvedType::Text));
        catalog.drop_table("items").unwrap();
        catalog.create_table(replacement).unwrap();
        let observed = capture(&catalog, &names);
        assert!(matches!(
            restore(&mut catalog, &before, &after),
            Err(ServerError::Conflict(_))
        ));
        assert!(same(&capture(&catalog, &names)[0], &observed[0]).unwrap());
    }

    #[test]
    fn rejects_foreign_index_collision_before_restoring_table() {
        let mut catalog = catalog();
        let names = ["items".into()];
        let before = capture(&catalog, &names);
        catalog.drop_index("value_idx").unwrap();
        let after = capture(&catalog, &names);
        catalog
            .create_table(
                TableMetadata::new(
                    "foreign",
                    vec![ColumnMetadata::new("value", ResolvedType::Integer)],
                )
                .with_table_id(3),
            )
            .unwrap();
        catalog
            .create_index(
                IndexMetadata::new(4, "value_idx", "foreign", vec!["value".into()])
                    .with_column_indices(vec![0]),
            )
            .unwrap();
        assert!(matches!(
            restore(&mut catalog, &before, &after),
            Err(ServerError::Conflict(_))
        ));
        assert!(same(&capture(&catalog, &names)[0], &after[0]).unwrap());
        assert_eq!(catalog.get_index("value_idx").unwrap().table, "foreign");
    }
}
