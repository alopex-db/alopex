use std::collections::BTreeSet;

use alopex_core::kv::KVStore;

use crate::catalog::IndexMetadata;
use crate::executor::{ExecutorError, Result};
use crate::fts;
use crate::storage::{SqlTxn, SqlValue};

pub(crate) struct FtsBridge;

impl FtsBridge {
    pub(crate) fn prepare(index: &mut IndexMetadata) -> Result<()> {
        match index.get_option("fts_format_version") {
            Some(crate::fts::INDEX_FORMAT_VERSION) => Ok(()),
            Some(version) => Err(ExecutorError::InvalidOperation {
                operation: "CREATE INDEX USING FTS".into(),
                reason: format!("unsupported FTS index format version '{version}'"),
            }),
            None => {
                index.options.push((
                    "fts_format_version".into(),
                    crate::fts::INDEX_FORMAT_VERSION.into(),
                ));
                Ok(())
            }
        }
    }

    pub(crate) fn validate(
        index: &IndexMetadata,
        data_type: &crate::planner::ResolvedType,
    ) -> Result<()> {
        if index.unique
            || index.column_indices.len() != 1
            || *data_type != crate::planner::ResolvedType::Text
        {
            return Err(ExecutorError::InvalidOperation {
                operation: "CREATE INDEX USING FTS".into(),
                reason: "FTS indexes require one non-unique TEXT column".into(),
            });
        }
        fts::tokenize(config(index), "").map_err(invalid)?;
        Ok(())
    }

    pub(crate) fn on_insert<'txn, S: KVStore + 'txn>(
        txn: &mut impl SqlTxn<'txn, S>,
        index: &IndexMetadata,
        row_id: u64,
        row: &[SqlValue],
    ) -> Result<()> {
        let column = index.column_indices[0];
        let mut storage = txn.index_storage(index.index_id, false, vec![column]);
        for term in row_terms(index, row)? {
            let mut key = row.to_vec();
            key[column] = SqlValue::Text(term);
            storage.insert(&key, row_id)?;
        }
        Ok(())
    }

    pub(crate) fn on_delete<'txn, S: KVStore + 'txn>(
        txn: &mut impl SqlTxn<'txn, S>,
        index: &IndexMetadata,
        row_id: u64,
        row: &[SqlValue],
    ) -> Result<()> {
        let column = index.column_indices[0];
        let mut storage = txn.index_storage(index.index_id, false, vec![column]);
        for term in row_terms(index, row)? {
            let mut key = row.to_vec();
            key[column] = SqlValue::Text(term);
            storage.delete(&key, row_id)?;
        }
        Ok(())
    }

    pub(crate) fn on_update<'txn, S: KVStore + 'txn>(
        txn: &mut impl SqlTxn<'txn, S>,
        index: &IndexMetadata,
        row_id: u64,
        old_row: &[SqlValue],
        new_row: &[SqlValue],
    ) -> Result<()> {
        if old_row[index.column_indices[0]] == new_row[index.column_indices[0]] {
            return Ok(());
        }
        Self::on_delete(txn, index, row_id, old_row)?;
        Self::on_insert(txn, index, row_id, new_row)
    }
}

pub(crate) fn config(index: &IndexMetadata) -> &str {
    index
        .options
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("config"))
        .map_or("simple", |(_, value)| value)
}

fn row_terms(index: &IndexMetadata, row: &[SqlValue]) -> Result<BTreeSet<String>> {
    if index.get_option("fts_format_version") != Some(fts::INDEX_FORMAT_VERSION) {
        return Err(invalid(
            "unsupported or missing FTS index format version".into(),
        ));
    }
    match &row[index.column_indices[0]] {
        SqlValue::Null => Ok(BTreeSet::new()),
        SqlValue::Text(document) => Ok(fts::tokenize(config(index), document)
            .map_err(invalid)?
            .into_iter()
            .map(|token| token.text)
            .collect()),
        other => Err(ExecutorError::InvalidOperation {
            operation: "FTS index maintenance".into(),
            reason: format!("expected TEXT, found {}", other.type_name()),
        }),
    }
}

fn invalid(reason: String) -> ExecutorError {
    ExecutorError::InvalidOperation {
        operation: "FTS index maintenance".into(),
        reason,
    }
}
