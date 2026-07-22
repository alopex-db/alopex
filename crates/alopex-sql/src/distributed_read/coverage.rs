//! Rendering of the public v0.8 distributed-read SQL coverage artifacts.
//!
//! The row definitions remain in `catalog_v0_8`; this module only renders
//! them.  Keeping that boundary prevents documentation from expanding the
//! remotely executable SQL surface.

use serde::Serialize;

use super::catalog_v0_8::{REMOTE_READ_CATALOG_VERSION, RemoteReadCatalogV0_8};

#[derive(Serialize)]
struct JsonCoverageMatrix<'a> {
    catalog_version: &'a str,
    entries: &'a [super::catalog_v0_8::RemoteReadCoverageEntry],
}

/// Renders the versioned JSON coverage matrix from the closed catalog.
pub fn render_json() -> String {
    let entries = RemoteReadCatalogV0_8.coverage_entries();
    let matrix = JsonCoverageMatrix {
        catalog_version: REMOTE_READ_CATALOG_VERSION,
        entries: &entries,
    };
    serde_json::to_string_pretty(&matrix)
        .expect("coverage rows contain only serializable static data")
        + "\n"
}

/// Renders the human-readable coverage matrix from the closed catalog.
pub fn render_markdown() -> String {
    let entries = RemoteReadCatalogV0_8.coverage_entries();
    let mut output = format!(
        "# Alopex v{REMOTE_READ_CATALOG_VERSION} Distributed Read SQL Coverage\n\n\
This generated matrix is the public contract for cluster-profile reads. `local_only` never means an implicit local fallback.\n\n\
| ID | Public SQL surface | Remote status | Prerequisite | Normal outcome | Rejection/failure |\n\
| --- | --- | --- | --- | --- | --- |\n"
    );
    for entry in entries {
        output.push_str(&format!(
            "| `{}` | {} | `{}` | {} | {} | {} |\n",
            entry.id,
            entry.public_surface,
            serde_json::to_string(&entry.remote_status)
                .expect("coverage status is serializable")
                .trim_matches('"'),
            entry.prerequisite,
            entry.normal_outcome,
            entry.failure_outcome,
        ));
    }
    output.push_str(
        "\nFunction identities are an explicit versioned list in `RemoteReadCatalogV0_8`; adding a local scalar signature does not add remote support.\n",
    );
    output
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::distributed_read::{
        REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS, REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS,
    };
    use crate::scalar;

    #[test]
    fn every_registered_scalar_has_one_explicit_catalog_category() {
        let catalogued: BTreeSet<_> = REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS
            .iter()
            .chain(REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS)
            .copied()
            .collect();
        let registered: BTreeSet<_> = scalar::signatures()
            .iter()
            .map(|signature| signature.name)
            .collect();
        assert_eq!(
            catalogued, registered,
            "a scalar cannot inherit remote support"
        );
    }

    #[test]
    fn rendered_docs_cover_each_stable_row_once() {
        let entries = RemoteReadCatalogV0_8.coverage_entries();
        let ids: BTreeSet<_> = entries.iter().map(|entry| entry.id).collect();
        assert_eq!(ids.len(), entries.len());

        let json = include_str!("../../../../docs/distributed-read-sql-matrix.json");
        let markdown = include_str!("../../../../docs/distributed-read.md");
        for id in ids {
            assert!(json.contains(id), "JSON is missing {id}");
            assert!(markdown.contains(id), "Markdown is missing {id}");
        }
        assert!(render_json().contains("catalog_version"));
        assert!(render_markdown().contains("Remote status"));
    }
}
