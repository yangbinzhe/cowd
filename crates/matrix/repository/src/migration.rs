//! Verified, maintenance-window Matrix migration primitives.
//!
//! Runtime requests never call this module.  A cutover copies one quiesced
//! SQLite snapshot, proves the source did not move, proves the PostgreSQL
//! target has identical logical payloads and revisions, then writes a small
//! redacted manifest outside the database.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob, MatrixConnectorRun,
    MatrixDataPlaneWatermark, MatrixEntity, MatrixEntityConflictDecision,
    MatrixEntityMatchCandidate, MatrixEvidencePacket, MatrixFact, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricSnapshot, MatrixMetricState, MatrixOntologyPack,
    MatrixQualityGateDecision, MatrixRelation, MatrixScenarioResult, MatrixScenarioRun,
    MatrixScenarioSpec, MatrixSourcePack, MatrixSourceSnapshot,
};

use crate::{
    MatrixSqliteRepository, MatrixStoreError, MatrixStoreResult, PostgresMatrixRepository,
};

pub(crate) const MATRIX_MIGRATION_TABLES: &[&str] = &[
    "matrix_entity",
    "matrix_relation",
    "matrix_fact",
    "matrix_attention_item",
    "matrix_evidence_packet",
    "matrix_quality_gate",
    "matrix_metric_definition",
    "matrix_metric_dependency",
    "matrix_metric_state",
    "matrix_metric_snapshot",
    "matrix_compute_job",
    "matrix_change_event",
    "matrix_source_pack",
    "matrix_connector_run",
    "matrix_source_snapshot",
    "matrix_ontology_pack",
    "matrix_entity_match_candidate",
    "matrix_entity_conflict_decision",
    "matrix_scenario_spec",
    "matrix_scenario_run",
    "matrix_scenario_result",
    "matrix_data_plane_watermark",
];

/// PostgreSQL JSONB returns numbers through a decimal/f64 representation,
/// while several Matrix DTO fields are `f32`.  Canonicalize through the typed
/// DTO at both ends so a semantically identical value has one digest instead
/// of failing a cutover on a last-bit formatting difference.
pub(crate) fn canonicalize_payload(table: &str, payload: Value) -> MatrixStoreResult<Value> {
    macro_rules! canonicalize {
        ($type:ty) => {
            canonicalize_as::<$type>(payload)
        };
    }
    match table {
        "matrix_entity" => canonicalize!(MatrixEntity),
        "matrix_relation" => canonicalize!(MatrixRelation),
        "matrix_fact" => canonicalize!(MatrixFact),
        "matrix_attention_item" => canonicalize!(MatrixAttentionItem),
        "matrix_evidence_packet" => canonicalize!(MatrixEvidencePacket),
        "matrix_quality_gate" => canonicalize!(MatrixQualityGateDecision),
        "matrix_metric_definition" => canonicalize!(MatrixMetricDefinition),
        "matrix_metric_dependency" => canonicalize!(MatrixMetricDependency),
        "matrix_metric_state" => canonicalize!(MatrixMetricState),
        "matrix_metric_snapshot" => canonicalize!(MatrixMetricSnapshot),
        "matrix_compute_job" => canonicalize!(MatrixComputeJob),
        "matrix_change_event" => canonicalize!(MatrixChangeEvent),
        "matrix_source_pack" => canonicalize!(MatrixSourcePack),
        "matrix_connector_run" => canonicalize!(MatrixConnectorRun),
        "matrix_source_snapshot" => canonicalize!(MatrixSourceSnapshot),
        "matrix_ontology_pack" => canonicalize!(MatrixOntologyPack),
        "matrix_entity_match_candidate" => canonicalize!(MatrixEntityMatchCandidate),
        "matrix_entity_conflict_decision" => canonicalize!(MatrixEntityConflictDecision),
        "matrix_scenario_spec" => canonicalize!(MatrixScenarioSpec),
        "matrix_scenario_run" => canonicalize!(MatrixScenarioRun),
        "matrix_scenario_result" => canonicalize!(MatrixScenarioResult),
        "matrix_data_plane_watermark" => canonicalize!(MatrixDataPlaneWatermark),
        unsupported => Err(MatrixStoreError::Backend(format!(
            "matrix migration cannot canonicalize unsupported table `{unsupported}`"
        ))),
    }
}

fn canonicalize_as<T>(payload: Value) -> MatrixStoreResult<Value>
where
    T: DeserializeOwned + Serialize,
{
    let typed = serde_json::from_value::<T>(payload)
        .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
    serde_json::to_value(typed).map_err(|error| MatrixStoreError::Backend(error.to_string()))
}

/// Backend-neutral logical Matrix data.  The map keys are table names and
/// stable aggregate ids; values are the exact typed-DTO JSON payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MatrixMigrationSnapshot {
    pub schema_version: i64,
    pub tables: BTreeMap<String, BTreeMap<String, Value>>,
    /// `resource_kind + NUL + resource_id` -> optimistic revision.
    pub revisions: BTreeMap<String, u64>,
}

impl MatrixMigrationSnapshot {
    pub(crate) fn new(
        schema_version: i64,
        tables: BTreeMap<String, BTreeMap<String, Value>>,
        revisions: BTreeMap<String, u64>,
    ) -> MatrixStoreResult<Self> {
        let snapshot = Self {
            schema_version,
            tables,
            revisions,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> MatrixStoreResult<()> {
        let allowed = MATRIX_MIGRATION_TABLES
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for (table, records) in &self.tables {
            if !allowed.contains(table.as_str()) {
                return Err(MatrixStoreError::Backend(format!(
                    "matrix migration snapshot contains unsupported table `{table}`"
                )));
            }
            for (id, payload) in records {
                if id.trim().is_empty() || !payload.is_object() {
                    return Err(MatrixStoreError::Backend(format!(
                        "matrix migration record `{table}/{id}` is invalid"
                    )));
                }
            }
        }
        for key in self.revisions.keys() {
            if key.split_once('\0').is_none() {
                return Err(MatrixStoreError::Backend(
                    "matrix migration revision key is invalid".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> MatrixStoreResult<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
        Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.tables.values().map(BTreeMap::len).sum()
    }

    fn difference_summary(&self, other: &Self) -> String {
        if self.schema_version != other.schema_version {
            return format!(
                "schema_version source={} target={}",
                self.schema_version, other.schema_version
            );
        }
        let table_names = self
            .tables
            .keys()
            .chain(other.tables.keys())
            .collect::<BTreeSet<_>>();
        for table in table_names {
            let left = self.tables.get(table).map_or(0, BTreeMap::len);
            let right = other.tables.get(table).map_or(0, BTreeMap::len);
            if left != right {
                return format!("table `{table}` count source={left} target={right}");
            }
            if self.tables.get(table) != other.tables.get(table) {
                let (Some(left_records), Some(right_records)) =
                    (self.tables.get(table), other.tables.get(table))
                else {
                    return format!("table `{table}` payload differs");
                };
                let record = left_records
                    .keys()
                    .chain(right_records.keys())
                    .find(|id| left_records.get(*id) != right_records.get(*id))
                    .map_or("unknown", String::as_str);
                return format!("table `{table}` payload differs at `{record}`");
            }
        }
        if self.revisions != other.revisions {
            return format!(
                "revision map differs source={} target={}",
                self.revisions.len(),
                other.revisions.len()
            );
        }
        "canonical serialization differs".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixMigrationManifest {
    pub domain: String,
    pub source_digest: String,
    pub target_digest: String,
    pub schema_version: i64,
    pub record_count: usize,
    pub revision_count: usize,
}

/// Copy a Matrix store during a maintenance barrier.  This deliberately has
/// concrete adapter types: an arbitrary `MatrixStore` cannot prove it can
/// export/import *all* aggregates and revisions.
pub fn copy_quiesced_matrix_store(
    source: &MatrixSqliteRepository,
    target: &PostgresMatrixRepository,
    manifest_path: impl AsRef<Path>,
) -> MatrixStoreResult<MatrixMigrationManifest> {
    let snapshot = source.export_migration_snapshot()?;
    let source_digest = snapshot.canonical_digest()?;
    target.import_migration_snapshot(&snapshot)?;
    let source_after_digest = source.export_migration_snapshot()?.canonical_digest()?;
    if source_after_digest != source_digest {
        return Err(MatrixStoreError::Backend(
            "matrix source changed while migration maintenance barrier was active".to_string(),
        ));
    }
    let target_digest = target.export_migration_snapshot()?.canonical_digest()?;
    if target_digest != source_digest {
        return Err(MatrixStoreError::Backend(format!(
            "matrix target digest differs from source after copy: {}",
            snapshot.difference_summary(&target.export_migration_snapshot()?)
        )));
    }
    let manifest = MatrixMigrationManifest {
        domain: "matrix".to_string(),
        source_digest,
        target_digest,
        schema_version: snapshot.schema_version,
        record_count: snapshot.record_count(),
        revision_count: snapshot.revisions.len(),
    };
    write_manifest(manifest_path.as_ref(), &manifest)?;
    Ok(manifest)
}

fn write_manifest(path: &Path, manifest: &MatrixMigrationManifest) -> MatrixStoreResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
    }
    let temporary = PathBuf::from(format!("{}.{}.tmp", path.display(), uuid::Uuid::new_v4()));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(manifest)
            .map_err(|error| MatrixStoreError::Backend(error.to_string()))?,
    )
    .map_err(|error| MatrixStoreError::Backend(error.to_string()))?;
    fs::rename(temporary, path).map_err(|error| MatrixStoreError::Backend(error.to_string()))
}
