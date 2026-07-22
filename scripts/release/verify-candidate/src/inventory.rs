use crate::artifact_verify::{validate_artifact, ArtifactKind, CandidateArtifact};
use crate::workspace::{inspect_workspace, WorkspaceCrate};
use crate::{io_error, Result, VerificationError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InventoryDeclaration {
    pub schema_version: u32,
    pub classifications: Vec<CrateClassification>,
    pub artifacts: Vec<CandidateArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CrateClassification {
    pub crate_name: String,
    pub scope: CrateScope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CrateScope {
    Product,
    Development,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactInventory {
    pub crates: Vec<InventoryCrate>,
    pub artifacts: Vec<CandidateArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InventoryCrate {
    pub crate_name: String,
    pub version: String,
    pub scope: CrateScope,
}

impl InventoryDeclaration {
    pub fn read(path: &Path) -> Result<Self> {
        let file =
            File::open(path).map_err(|error| io_error("open inventory declaration", error))?;
        serde_json::from_reader(file).map_err(|error| {
            VerificationError::new("invalid_inventory_declaration", error.to_string())
        })
    }

    pub fn verify(
        &self,
        source_root: &Path,
        artifacts_root: &Path,
    ) -> (ArtifactInventory, Vec<VerificationError>) {
        let metadata = match inspect_workspace(source_root) {
            Ok(metadata) => metadata,
            Err(error) => {
                return (
                    ArtifactInventory {
                        crates: Vec::new(),
                        artifacts: self.artifacts.clone(),
                    },
                    vec![error],
                );
            }
        };
        let mut errors = Vec::new();
        if self.schema_version != 1 {
            errors.push(VerificationError::new(
                "inventory_schema_invalid",
                self.schema_version.to_string(),
            ));
        }
        let declared = classification_map(&self.classifications, &mut errors);
        let mut crates = Vec::new();
        record_crates(&metadata.members, &declared, &mut crates, &mut errors);
        record_crates(
            &metadata.development_tools,
            &declared,
            &mut crates,
            &mut errors,
        );
        let expected: BTreeSet<_> = metadata
            .members
            .iter()
            .chain(metadata.development_tools.iter())
            .map(|crate_info| crate_info.name.clone())
            .collect();
        for crate_name in declared.keys() {
            if !expected.contains(crate_name) {
                errors.push(VerificationError::new(
                    "artifact_scope_ambiguous",
                    format!("unknown classified crate {crate_name}"),
                ));
            }
        }
        let artifact_ids = artifact_ids(&self.artifacts, &mut errors);
        for crate_info in &crates {
            if crate_info.scope == CrateScope::Product {
                let matching: Vec<_> = self
                    .artifacts
                    .iter()
                    .filter(|artifact| artifact.crate_name == crate_info.crate_name)
                    .collect();
                if !matching
                    .iter()
                    .any(|artifact| artifact.kind == ArtifactKind::CrateArchive)
                {
                    errors.push(VerificationError::new(
                        "product_crate_artifact_missing",
                        crate_info.crate_name.clone(),
                    ));
                }
                if crate_info.crate_name == "alopex-cli"
                    && !matching
                        .iter()
                        .any(|artifact| artifact.kind == ArtifactKind::CliBinary)
                {
                    errors.push(VerificationError::new(
                        "cli_binary_artifact_missing",
                        "alopex-cli",
                    ));
                }
                if crate_info.crate_name == "alopex-py"
                    && !matching
                        .iter()
                        .any(|artifact| artifact.kind == ArtifactKind::PythonWheel)
                {
                    errors.push(VerificationError::new(
                        "python_wheel_artifact_missing",
                        "alopex-py",
                    ));
                }
                for artifact in matching {
                    errors.extend(validate_artifact(
                        artifact,
                        artifacts_root,
                        &crate_info.version,
                    ));
                }
            }
        }
        for artifact in &self.artifacts {
            if !artifact_ids.contains(&artifact.id) {
                errors.push(VerificationError::new(
                    "artifact_identity_invalid",
                    artifact.id.clone(),
                ));
            }
            if !crates
                .iter()
                .any(|crate_info| crate_info.crate_name == artifact.crate_name)
            {
                errors.push(VerificationError::new(
                    "artifact_scope_ambiguous",
                    format!(
                        "{} references unclassified crate {}",
                        artifact.id, artifact.crate_name
                    ),
                ));
            }
        }
        (
            ArtifactInventory {
                crates,
                artifacts: self.artifacts.clone(),
            },
            errors,
        )
    }
}

fn classification_map(
    classifications: &[CrateClassification],
    errors: &mut Vec<VerificationError>,
) -> BTreeMap<String, CrateScope> {
    let mut declared = BTreeMap::new();
    for classification in classifications {
        if classification.crate_name.trim().is_empty()
            || declared
                .insert(
                    classification.crate_name.clone(),
                    classification.scope.clone(),
                )
                .is_some()
        {
            errors.push(VerificationError::new(
                "artifact_scope_ambiguous",
                classification.crate_name.clone(),
            ));
        }
    }
    declared
}

fn record_crates(
    source: &[WorkspaceCrate],
    declared: &BTreeMap<String, CrateScope>,
    output: &mut Vec<InventoryCrate>,
    errors: &mut Vec<VerificationError>,
) {
    for crate_info in source {
        match declared.get(&crate_info.name) {
            Some(scope) => output.push(InventoryCrate {
                crate_name: crate_info.name.clone(),
                version: crate_info.version.clone(),
                scope: scope.clone(),
            }),
            None => errors.push(VerificationError::new(
                "artifact_scope_ambiguous",
                format!(
                    "{} has no product/development classification",
                    crate_info.name
                ),
            )),
        }
    }
}

fn artifact_ids(
    artifacts: &[CandidateArtifact],
    errors: &mut Vec<VerificationError>,
) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for artifact in artifacts {
        if artifact.id.trim().is_empty() || !ids.insert(artifact.id.clone()) {
            errors.push(VerificationError::new(
                "artifact_identity_invalid",
                artifact.id.clone(),
            ));
        }
    }
    ids
}
