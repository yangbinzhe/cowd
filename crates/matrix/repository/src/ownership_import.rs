use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use matrix_core::{CoreMatrixImportPlan, OwnershipImportObject};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::MatrixSqliteRepositoryError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixOwnershipImportReceipt {
    pub receipt_id: String,
    pub whole_snapshot_digest: String,
    pub core_section_digest: String,
    pub contract_digest: String,
    pub source_version: String,
    pub source_schema_version: u64,
    pub maintenance_fence_id: String,
    pub object_count: u64,
    pub applied_at: DateTime<Utc>,
    pub receipt_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatrixOwnershipImportOutcome {
    Applied(MatrixOwnershipImportReceipt),
    AlreadyApplied(MatrixOwnershipImportReceipt),
}

struct TableSpec {
    table: &'static str,
    columns: &'static [&'static str],
    primary_key: &'static [&'static str],
    rank: u8,
}

macro_rules! spec {
    ($table:literal, [$($column:literal),+], [$($key:literal),+], $rank:literal) => {
        TableSpec { table: $table, columns: &[$($column),+], primary_key: &[$($key),+], rank: $rank }
    };
}

const TABLES: &[TableSpec] = &[
    spec!(
        "matrix_entity",
        [
            "entity_id",
            "entity_type",
            "canonical_key",
            "display_name",
            "source_keys_json",
            "attributes_json",
            "confidence",
            "entity_json",
            "created_at",
            "updated_at"
        ],
        ["entity_id"],
        0
    ),
    spec!(
        "matrix_metric_definition",
        ["metric_id", "definition_json", "created_at", "updated_at"],
        ["metric_id"],
        0
    ),
    spec!(
        "matrix_source_pack",
        [
            "source_pack_id",
            "source_name",
            "access_mode",
            "refresh_mode",
            "source_pack_json",
            "created_at",
            "updated_at"
        ],
        ["source_pack_id"],
        0
    ),
    spec!(
        "matrix_attention_item",
        [
            "attention_id",
            "priority_score",
            "status",
            "attention_json",
            "created_at",
            "updated_at"
        ],
        ["attention_id"],
        0
    ),
    spec!(
        "matrix_entity_source_key",
        [
            "source_system",
            "source_key",
            "entity_id",
            "source_ref",
            "created_at"
        ],
        ["source_system", "source_key"],
        1
    ),
    spec!(
        "matrix_relation",
        [
            "relation_id",
            "relation_type",
            "from_entity_id",
            "to_entity_id",
            "attributes_json",
            "confidence",
            "relation_json",
            "created_at",
            "updated_at"
        ],
        ["relation_id"],
        1
    ),
    spec!(
        "matrix_fact",
        [
            "fact_id",
            "snapshot_id",
            "fact_type",
            "entity_refs_json",
            "metric_key",
            "dimensions_json",
            "measures_json",
            "event_time",
            "valid_from",
            "valid_to",
            "source_ref",
            "confidence",
            "raw_hash",
            "created_at"
        ],
        ["fact_id"],
        1
    ),
    spec!(
        "matrix_evidence_packet",
        ["packet_id", "attention_id", "packet_json", "created_at"],
        ["packet_id"],
        1
    ),
    spec!(
        "matrix_quality_gate",
        [
            "gate_id",
            "target_ref",
            "gate_type",
            "decision",
            "score",
            "gate_json",
            "created_at"
        ],
        ["gate_id"],
        1
    ),
    spec!(
        "matrix_metric_state",
        [
            "state_id",
            "metric_id",
            "entity_scope",
            "period",
            "value",
            "previous_value",
            "delta",
            "status",
            "state_json",
            "computed_at"
        ],
        ["state_id"],
        1
    ),
    spec!(
        "matrix_metric_dependency",
        [
            "dependency_id",
            "upstream_metric_id",
            "downstream_metric_id",
            "dependency_type",
            "confidence",
            "dependency_json",
            "created_at",
            "updated_at"
        ],
        ["dependency_id"],
        1
    ),
    spec!(
        "matrix_compute_job",
        [
            "job_id",
            "trigger_fact_type",
            "status",
            "priority",
            "job_json",
            "created_at",
            "updated_at"
        ],
        ["job_id"],
        1
    ),
    spec!(
        "matrix_change_event",
        [
            "change_id",
            "metric_id",
            "entity_ref",
            "period",
            "delta",
            "severity_hint",
            "change_json",
            "detected_at"
        ],
        ["change_id"],
        1
    ),
    spec!(
        "matrix_data_plane_watermark",
        [
            "source_ref",
            "fact_type",
            "partition_ref",
            "high_watermark",
            "last_batch_id",
            "watermark_json",
            "updated_at"
        ],
        ["source_ref", "fact_type", "partition_ref"],
        1
    ),
    spec!(
        "matrix_connector_run",
        [
            "run_id",
            "source_pack_id",
            "connector_kind",
            "status",
            "run_json",
            "created_at",
            "updated_at"
        ],
        ["run_id"],
        1
    ),
    spec!(
        "matrix_ontology_pack",
        [
            "ontology_id",
            "domain",
            "version",
            "pack_json",
            "updated_at"
        ],
        ["ontology_id"],
        1
    ),
    spec!(
        "matrix_entity_match_candidate",
        [
            "candidate_id",
            "left_entity_id",
            "right_entity_id",
            "confidence",
            "status",
            "candidate_json",
            "created_at"
        ],
        ["candidate_id"],
        2
    ),
    spec!(
        "matrix_entity_conflict_decision",
        [
            "decision_id",
            "candidate_id",
            "survivor_entity_id",
            "retired_entity_id",
            "decision_json",
            "decided_at"
        ],
        ["decision_id"],
        3
    ),
    spec!(
        "matrix_metric_snapshot",
        [
            "snapshot_id",
            "scope_ref",
            "metric_ids_json",
            "snapshot_json",
            "created_at"
        ],
        ["snapshot_id"],
        2
    ),
];

pub(crate) fn apply_sqlite(
    connection: &mut Connection,
    plan: &CoreMatrixImportPlan,
) -> Result<MatrixOwnershipImportOutcome, MatrixSqliteRepositoryError> {
    let unique_objects = plan
        .objects()
        .iter()
        .map(stable_ref)
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if unique_objects.len() != plan.records().len() {
        return Err(migration("typed record/object cardinality mismatch"));
    }
    for record in plan.records() {
        if !plan
            .objects()
            .iter()
            .any(|object| object.source_table == record.table())
        {
            return Err(migration(format!(
                "typed record `{}` has no snapshot object",
                record.table()
            )));
        }
    }
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    initialize_import_schema(&transaction)?;
    if let Some(receipt) = find_receipt(&transaction, plan.whole_snapshot_digest())? {
        transaction.commit()?;
        return Ok(MatrixOwnershipImportOutcome::AlreadyApplied(receipt));
    }
    let fence_conflict: Option<String> = transaction.query_row(
        "SELECT whole_snapshot_digest FROM matrix_ownership_import_receipt WHERE maintenance_fence_id=?1",
        params![plan.source().maintenance_fence_id], |row| row.get(0),
    ).optional()?;
    if let Some(digest) = fence_conflict {
        return Err(migration(format!(
            "maintenance fence already imported with divergent digest `{digest}`"
        )));
    }
    let latest_schema: Option<i64> = transaction.query_row(
        "SELECT MAX(source_schema_version) FROM matrix_ownership_import_receipt",
        [],
        |row| row.get(0),
    )?;
    if latest_schema.is_some_and(|latest| plan.source().schema_version < latest as u64) {
        return Err(migration(format!(
            "source schema revision rollback: latest={}, incoming={}",
            latest_schema.unwrap_or_default(),
            plan.source().schema_version
        )));
    }

    let mut objects = plan.objects().iter().collect::<Vec<_>>();
    objects
        .sort_by_key(|object| table_spec(&object.source_table).map_or(u8::MAX, |spec| spec.rank));
    for object in objects {
        apply_object(&transaction, object, plan.whole_snapshot_digest())?;
    }
    let applied_at = Utc::now();
    let receipt_id = format!("matrix-ownership-{}", &plan.whole_snapshot_digest()[7..27]);
    let receipt_digest = receipt_digest(plan, &receipt_id, applied_at)?;
    transaction.execute(
        "INSERT INTO matrix_ownership_import_receipt (receipt_id,whole_snapshot_digest,core_section_digest,contract_digest,source_version,source_schema_version,maintenance_fence_id,object_count,applied_at,receipt_digest) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![receipt_id, plan.whole_snapshot_digest(), plan.section_digest(), plan.source().ownership_contract_digest,
            plan.source().source_version, plan.source().schema_version as i64, plan.source().maintenance_fence_id,
            plan.objects().len() as i64, applied_at.to_rfc3339(), receipt_digest],
    )?;
    let receipt = MatrixOwnershipImportReceipt {
        receipt_id,
        whole_snapshot_digest: plan.whole_snapshot_digest().to_string(),
        core_section_digest: plan.section_digest().to_string(),
        contract_digest: plan.source().ownership_contract_digest.clone(),
        source_version: plan.source().source_version.clone(),
        source_schema_version: plan.source().schema_version,
        maintenance_fence_id: plan.source().maintenance_fence_id.clone(),
        object_count: plan.objects().len() as u64,
        applied_at,
        receipt_digest,
    };
    transaction.commit()?;
    Ok(MatrixOwnershipImportOutcome::Applied(receipt))
}

fn apply_object(
    transaction: &rusqlite::Transaction<'_>,
    object: &OwnershipImportObject,
    whole_snapshot_digest: &str,
) -> Result<(), MatrixSqliteRepositoryError> {
    let spec = table_spec(&object.source_table)
        .ok_or_else(|| migration(format!("unsupported table `{}`", object.source_table)))?;
    let stable_ref = stable_ref(object)?;
    if let Some((revision_json, payload_digest)) = transaction.query_row(
        "SELECT revision_json,payload_digest FROM matrix_ownership_import_checkpoint WHERE stable_ref=?1",
        params![stable_ref], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    ).optional()? {
        let previous_revision: Value = serde_json::from_str(&revision_json)?;
        if payload_digest == object.payload_digest && previous_revision == object.revision.value { return Ok(()); }
        compare_revision(&previous_revision, &object.revision.value, &stable_ref)?;
    } else if let Some(existing) = read_existing(transaction, spec, object)? {
        let existing_digest = digest_payload(&existing)?;
        if existing_digest != object.payload_digest {
            return Err(migration(format!("target collision for `{stable_ref}` has divergent payload")));
        }
    }

    let columns = spec.columns.join(",");
    let placeholders = (1..=spec.columns.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let updates = spec
        .columns
        .iter()
        .filter(|column| !spec.primary_key.contains(column))
        .map(|column| format!("{column}=excluded.{column}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("INSERT INTO {} ({columns}) VALUES ({placeholders}) ON CONFLICT({}) DO UPDATE SET {updates}", spec.table, spec.primary_key.join(","));
    let values = spec
        .columns
        .iter()
        .map(|column| {
            object
                .payload
                .get(*column)
                .ok_or_else(|| migration(format!("validated payload lost column `{column}`")))
                .and_then(sql_value)
        })
        .collect::<Result<Vec<_>, _>>()?;
    transaction.execute(&sql, params_from_iter(values))?;
    transaction.execute(
        "INSERT INTO matrix_ownership_import_checkpoint (stable_ref,revision_json,payload_digest,source_references_json,evidence_references_json,whole_snapshot_digest,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(stable_ref) DO UPDATE SET revision_json=excluded.revision_json,payload_digest=excluded.payload_digest,source_references_json=excluded.source_references_json,evidence_references_json=excluded.evidence_references_json,whole_snapshot_digest=excluded.whole_snapshot_digest,updated_at=excluded.updated_at",
        params![stable_ref, serde_json::to_string(&object.revision.value)?, object.payload_digest,
            serde_json::to_string(&object.source_references)?, serde_json::to_string(&object.evidence_references)?,
            whole_snapshot_digest, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn read_existing(
    transaction: &rusqlite::Transaction<'_>,
    spec: &TableSpec,
    object: &OwnershipImportObject,
) -> Result<Option<BTreeMap<String, Value>>, MatrixSqliteRepositoryError> {
    let predicates = spec
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, column)| format!("{column}=?{}", i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT {} FROM {} WHERE {predicates}",
        spec.columns.join(","),
        spec.table
    );
    let keys = spec
        .primary_key
        .iter()
        .map(|column| {
            object
                .payload
                .get(*column)
                .ok_or_else(|| migration(format!("validated payload lost stable field `{column}`")))
                .and_then(sql_value)
        })
        .collect::<Result<Vec<_>, _>>()?;
    transaction
        .query_row(&sql, params_from_iter(keys), |row| {
            let mut payload = BTreeMap::new();
            for (index, column) in spec.columns.iter().enumerate() {
                let value = row.get_ref(index)?;
                payload.insert(
                    (*column).to_string(),
                    match value {
                        rusqlite::types::ValueRef::Null => Value::Null,
                        rusqlite::types::ValueRef::Integer(value) => Value::from(value),
                        rusqlite::types::ValueRef::Real(value) => Value::from(value),
                        rusqlite::types::ValueRef::Text(value) => {
                            Value::String(String::from_utf8_lossy(value).into_owned())
                        }
                        rusqlite::types::ValueRef::Blob(_) => {
                            return Err(rusqlite::Error::InvalidColumnType(
                                index,
                                (*column).to_string(),
                                rusqlite::types::Type::Blob,
                            ))
                        }
                    },
                );
            }
            Ok(payload)
        })
        .optional()
        .map_err(Into::into)
}

fn compare_revision(
    previous: &Value,
    incoming: &Value,
    stable_ref: &str,
) -> Result<(), MatrixSqliteRepositoryError> {
    let ordering = match (previous, incoming) {
        (Value::Number(left), Value::Number(right)) => match (left.as_f64(), right.as_f64()) {
            (Some(left), Some(right)) => left.partial_cmp(&right),
            _ => None,
        },
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    };
    match ordering {
        Some(std::cmp::Ordering::Less) => Ok(()),
        Some(std::cmp::Ordering::Equal) => Err(migration(format!(
            "same revision divergence for `{stable_ref}`"
        ))),
        Some(std::cmp::Ordering::Greater) => {
            Err(migration(format!("revision rollback for `{stable_ref}`")))
        }
        None => Err(migration(format!(
            "incomparable revision for `{stable_ref}`"
        ))),
    }
}

fn initialize_import_schema(connection: &Connection) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS matrix_ownership_import_receipt (receipt_id TEXT PRIMARY KEY, whole_snapshot_digest TEXT NOT NULL UNIQUE, core_section_digest TEXT NOT NULL, contract_digest TEXT NOT NULL, source_version TEXT NOT NULL, source_schema_version INTEGER NOT NULL, maintenance_fence_id TEXT NOT NULL UNIQUE, object_count INTEGER NOT NULL, applied_at TEXT NOT NULL, receipt_digest TEXT NOT NULL UNIQUE);
         CREATE TABLE IF NOT EXISTS matrix_ownership_import_checkpoint (stable_ref TEXT PRIMARY KEY, revision_json TEXT NOT NULL, payload_digest TEXT NOT NULL, source_references_json TEXT NOT NULL, evidence_references_json TEXT NOT NULL, whole_snapshot_digest TEXT NOT NULL, updated_at TEXT NOT NULL);"
    )?;
    Ok(())
}

fn find_receipt(
    connection: &Connection,
    digest: &str,
) -> Result<Option<MatrixOwnershipImportReceipt>, MatrixSqliteRepositoryError> {
    connection.query_row(
        "SELECT receipt_id,whole_snapshot_digest,core_section_digest,contract_digest,source_version,source_schema_version,maintenance_fence_id,object_count,applied_at,receipt_digest FROM matrix_ownership_import_receipt WHERE whole_snapshot_digest=?1",
        params![digest], |row| {
            let applied_at: String = row.get(8)?;
            Ok(MatrixOwnershipImportReceipt { receipt_id: row.get(0)?, whole_snapshot_digest: row.get(1)?, core_section_digest: row.get(2)?, contract_digest: row.get(3)?, source_version: row.get(4)?, source_schema_version: row.get::<_, i64>(5)? as u64, maintenance_fence_id: row.get(6)?, object_count: row.get::<_, i64>(7)? as u64, applied_at: applied_at.parse().map_err(|e| rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e)))?, receipt_digest: row.get(9)? })
        },
    ).optional().map_err(Into::into)
}

fn receipt_digest(
    plan: &CoreMatrixImportPlan,
    receipt_id: &str,
    applied_at: DateTime<Utc>,
) -> Result<String, MatrixSqliteRepositoryError> {
    let bytes = serde_json::to_vec(
        &serde_json::json!({"receipt_id": receipt_id, "whole_snapshot_digest": plan.whole_snapshot_digest(), "section_digest": plan.section_digest(), "contract_digest": plan.source().ownership_contract_digest, "source_version": plan.source().source_version, "schema_version": plan.source().schema_version, "fence": plan.source().maintenance_fence_id, "object_count": plan.objects().len(), "applied_at": applied_at}),
    )?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn digest_payload(
    payload: &BTreeMap<String, Value>,
) -> Result<String, MatrixSqliteRepositoryError> {
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(payload)?)
    ))
}
fn stable_ref(object: &OwnershipImportObject) -> Result<String, MatrixSqliteRepositoryError> {
    let parts = object
        .stable_id
        .iter()
        .map(|(key, value)| serde_json::to_string(value).map(|value| format!("{key}={value}")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("{}:{}", object.source_table, parts.join("+")))
}
fn sql_value(value: &Value) -> Result<SqlValue, MatrixSqliteRepositoryError> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(SqlValue::Integer)
            .or_else(|| value.as_f64().map(SqlValue::Real))
            .ok_or_else(|| migration("number is not representable by SQLite"))?,
        Value::String(value) => SqlValue::Text(value.clone()),
        Value::Array(_) | Value::Object(_) => {
            return Err(migration(
                "physical payload values must be scalar; JSON columns must be encoded strings",
            ))
        }
    })
}
fn table_spec(table: &str) -> Option<&'static TableSpec> {
    TABLES.iter().find(|spec| spec.table == table)
}
fn migration(message: impl Into<String>) -> MatrixSqliteRepositoryError {
    MatrixSqliteRepositoryError::Migration(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use matrix_core::{
        ownership_import_digest, ImportedMatrixSourceKey, MatrixEntity, MatrixFact,
        MatrixOntologyPack, MatrixSourceKey, MfgOwnershipSplitSnapshotV1, OwnershipImportContext,
        OwnershipImportRevision, OWNERSHIP_CONTRACT_DIGEST_V1,
    };

    fn object(
        table: &str,
        stable_id: BTreeMap<String, Value>,
        payload: BTreeMap<String, Value>,
    ) -> OwnershipImportObject {
        let payload_digest = digest_payload(&payload).unwrap();
        OwnershipImportObject {
            source_table: table.to_string(),
            stable_id,
            revision: OwnershipImportRevision {
                mapping: "none".to_string(),
                value: Value::Null,
            },
            source_references: Vec::new(),
            evidence_references: Vec::new(),
            payload,
            payload_digest,
        }
    }

    fn fixture() -> CoreMatrixImportPlan {
        fixture_variant(2, "2024-12-30T23:59:58Z", "fence", 1)
    }

    fn fixture_variant(
        source_revision: u64,
        source_created_at: &str,
        fence: &str,
        digest_salt: u64,
    ) -> CoreMatrixImportPlan {
        let entity = MatrixEntity {
            entity_id: "entity-1".to_string(),
            entity_type: "machine".to_string(),
            canonical_key: "m-1".to_string(),
            display_name: "M1".to_string(),
            source_keys: vec![MatrixSourceKey {
                source_system: "erp".to_string(),
                source_key: "m-1".to_string(),
                source_ref: Some("erp://m-1".to_string()),
            }],
            attributes: serde_json::json!({}),
            confidence: 1.0,
            created_at: "2025-01-01T01:02:03Z".parse().unwrap(),
            updated_at: "2025-01-02T01:02:03Z".parse().unwrap(),
        };
        let fact = MatrixFact {
            fact_id: "fact-1".to_string(),
            snapshot_id: "snap-1".to_string(),
            fact_type: "metric".to_string(),
            entity_refs: vec!["entity-1".to_string()],
            metric_key: None,
            dimensions: serde_json::json!({}),
            measures: serde_json::json!({"value": 1}),
            event_time: "2025-01-03T01:02:03Z".parse().unwrap(),
            valid_from: None,
            valid_to: None,
            source_ref: Some("erp://fact-1".to_string()),
            confidence: 0.9,
            raw_hash: "sha256:raw".to_string(),
        };
        let fact_created: DateTime<Utc> = "2024-12-31T23:59:58Z".parse().unwrap();
        let source_created: DateTime<Utc> = source_created_at.parse().unwrap();
        let ontology = MatrixOntologyPack {
            ontology_id: "ontology-1".to_string(),
            domain: "mfg".to_string(),
            version: "1".to_string(),
            concepts: Vec::new(),
            relations: Vec::new(),
            metric_bindings: Vec::new(),
        };
        let ontology_updated: DateTime<Utc> = "2024-12-29T23:59:58Z".parse().unwrap();

        let mut entity_payload = BTreeMap::new();
        entity_payload.insert("entity_id".into(), Value::String(entity.entity_id.clone()));
        entity_payload.insert(
            "entity_type".into(),
            Value::String(entity.entity_type.clone()),
        );
        entity_payload.insert(
            "canonical_key".into(),
            Value::String(entity.canonical_key.clone()),
        );
        entity_payload.insert(
            "display_name".into(),
            Value::String(entity.display_name.clone()),
        );
        entity_payload.insert(
            "source_keys_json".into(),
            Value::String(serde_json::to_string(&entity.source_keys).unwrap()),
        );
        entity_payload.insert(
            "attributes_json".into(),
            Value::String(serde_json::to_string(&entity.attributes).unwrap()),
        );
        entity_payload.insert("confidence".into(), Value::from(entity.confidence));
        entity_payload.insert(
            "entity_json".into(),
            Value::String(serde_json::to_string(&entity).unwrap()),
        );
        entity_payload.insert(
            "created_at".into(),
            Value::String(entity.created_at.to_rfc3339()),
        );
        entity_payload.insert(
            "updated_at".into(),
            Value::String(entity.updated_at.to_rfc3339()),
        );
        let entity_object = object(
            "matrix_entity",
            BTreeMap::from([("entity_id".into(), Value::String(entity.entity_id.clone()))]),
            entity_payload,
        );

        let source_key = ImportedMatrixSourceKey {
            source_system: "erp".into(),
            source_key: "m-1".into(),
            entity_id: "entity-1".into(),
            source_ref: Some("erp://m-1".into()),
            created_at: source_created,
        };
        let source_payload = BTreeMap::from([
            (
                "source_system".into(),
                Value::String(source_key.source_system.clone()),
            ),
            (
                "source_key".into(),
                Value::String(source_key.source_key.clone()),
            ),
            (
                "entity_id".into(),
                Value::String(source_key.entity_id.clone()),
            ),
            (
                "source_ref".into(),
                Value::String(source_key.source_ref.clone().unwrap()),
            ),
            (
                "created_at".into(),
                Value::String(source_created.to_rfc3339()),
            ),
        ]);
        let mut source_object = object(
            "matrix_entity_source_key",
            BTreeMap::from([
                ("source_system".into(), Value::String("erp".into())),
                ("source_key".into(), Value::String("m-1".into())),
            ]),
            source_payload,
        );
        source_object.revision = OwnershipImportRevision {
            mapping: "embedded".into(),
            value: Value::from(source_revision),
        };

        let fact_payload = BTreeMap::from([
            ("fact_id".into(), Value::String(fact.fact_id.clone())),
            (
                "snapshot_id".into(),
                Value::String(fact.snapshot_id.clone()),
            ),
            ("fact_type".into(), Value::String(fact.fact_type.clone())),
            (
                "entity_refs_json".into(),
                Value::String(serde_json::to_string(&fact.entity_refs).unwrap()),
            ),
            ("metric_key".into(), Value::Null),
            (
                "dimensions_json".into(),
                Value::String(serde_json::to_string(&fact.dimensions).unwrap()),
            ),
            (
                "measures_json".into(),
                Value::String(serde_json::to_string(&fact.measures).unwrap()),
            ),
            (
                "event_time".into(),
                Value::String(fact.event_time.to_rfc3339()),
            ),
            ("valid_from".into(), Value::Null),
            ("valid_to".into(), Value::Null),
            (
                "source_ref".into(),
                Value::String(fact.source_ref.clone().unwrap()),
            ),
            ("confidence".into(), Value::from(fact.confidence)),
            ("raw_hash".into(), Value::String(fact.raw_hash.clone())),
            (
                "created_at".into(),
                Value::String(fact_created.to_rfc3339()),
            ),
        ]);
        let fact_object = object(
            "matrix_fact",
            BTreeMap::from([("fact_id".into(), Value::String(fact.fact_id.clone()))]),
            fact_payload,
        );

        let ontology_payload = BTreeMap::from([
            (
                "ontology_id".into(),
                Value::String(ontology.ontology_id.clone()),
            ),
            ("domain".into(), Value::String(ontology.domain.clone())),
            ("version".into(), Value::String(ontology.version.clone())),
            (
                "pack_json".into(),
                Value::String(serde_json::to_string(&ontology).unwrap()),
            ),
            (
                "updated_at".into(),
                Value::String(ontology_updated.to_rfc3339()),
            ),
        ]);
        let ontology_object = object(
            "matrix_ontology_pack",
            BTreeMap::from([(
                "ontology_id".into(),
                Value::String(ontology.ontology_id.clone()),
            )]),
            ontology_payload,
        );

        let objects = vec![entity_object, source_object, fact_object, ontology_object];
        let mfg_base = serde_json::json!({"owner": "mfg", "object_count": 0, "objects": []});
        let core_base =
            serde_json::json!({"owner": "core", "object_count": objects.len(), "objects": objects});
        let reconciliation_base = serde_json::json!({"pending_outbox": [], "command_receipts": [], "mutation_receipts": []});
        let source = serde_json::json!({
            "app_id": "mfg", "source_version": format!("1.{digest_salt}"), "schema_version": 1,
            "exported_at": "2026-08-15T00:00:00Z", "maintenance_fence_id": fence,
            "ownership_contract_digest": OWNERSHIP_CONTRACT_DIGEST_V1,
        });
        let mfg_domain = serde_json::json!({"owner": "mfg", "object_count": 0, "section_digest": ownership_import_digest(&mfg_base).unwrap(), "objects": []});
        let core_domain = serde_json::json!({"owner": "core", "object_count": objects.len(), "section_digest": ownership_import_digest(&core_base).unwrap(), "objects": objects});
        let reconciliation = serde_json::json!({"pending_outbox": [], "command_receipts": [], "mutation_receipts": [], "set_digest": ownership_import_digest(&reconciliation_base).unwrap()});
        let excluded = serde_json::json!([
            {"source_table":"mfg_projection_event","reason":"projection","regeneration":"rebuild"},
            {"source_table":"mfg_live_epoch","reason":"runtime","regeneration":"new epoch"},
            {"source_table":"mfg_live_secret","reason":"secret","regeneration":"rotate"}
        ]);
        let base = serde_json::json!({"contract_version":"cowd.ownership-split/v1","source":source,"mfg_domain":mfg_domain,"core_matrix_domain":core_domain,"reconciliation":reconciliation,"excluded":excluded});
        let mut snapshot = base.as_object().unwrap().clone();
        snapshot.insert(
            "whole_snapshot_digest".into(),
            Value::String(ownership_import_digest(&base).unwrap()),
        );
        MfgOwnershipSplitSnapshotV1::decode_strict(&serde_json::to_vec(&snapshot).unwrap())
            .unwrap()
            .dry_run(&OwnershipImportContext::default())
            .unwrap()
    }

    #[test]
    fn sqlite_import_is_atomic_idempotent_and_preserves_authoritative_timestamps() {
        assert_eq!(TABLES.len(), 19);
        assert_eq!(
            TABLES
                .iter()
                .map(|spec| spec.table)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            19
        );
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE matrix_entity(entity_id TEXT PRIMARY KEY,entity_type TEXT NOT NULL,canonical_key TEXT NOT NULL,display_name TEXT NOT NULL,source_keys_json TEXT NOT NULL,attributes_json TEXT NOT NULL,confidence REAL NOT NULL,entity_json TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(entity_type,canonical_key));
             CREATE TABLE matrix_entity_source_key(source_system TEXT NOT NULL,source_key TEXT NOT NULL,entity_id TEXT NOT NULL,source_ref TEXT,created_at TEXT NOT NULL,PRIMARY KEY(source_system,source_key),FOREIGN KEY(entity_id) REFERENCES matrix_entity(entity_id));
             CREATE TABLE matrix_fact(fact_id TEXT PRIMARY KEY,snapshot_id TEXT NOT NULL,fact_type TEXT NOT NULL,entity_refs_json TEXT NOT NULL,metric_key TEXT,dimensions_json TEXT NOT NULL,measures_json TEXT NOT NULL,event_time TEXT NOT NULL,valid_from TEXT,valid_to TEXT,source_ref TEXT,confidence REAL NOT NULL,raw_hash TEXT NOT NULL,created_at TEXT NOT NULL);
             CREATE TABLE matrix_ontology_pack(ontology_id TEXT PRIMARY KEY,domain TEXT NOT NULL,version TEXT NOT NULL,pack_json TEXT NOT NULL,updated_at TEXT NOT NULL);"
        ).unwrap();
        let plan = fixture();
        assert!(matches!(
            apply_sqlite(&mut connection, &plan).unwrap(),
            MatrixOwnershipImportOutcome::Applied(_)
        ));
        assert!(matches!(
            apply_sqlite(&mut connection, &plan).unwrap(),
            MatrixOwnershipImportOutcome::AlreadyApplied(_)
        ));
        let source_created: String = connection
            .query_row(
                "SELECT created_at FROM matrix_entity_source_key",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let fact_created: String = connection
            .query_row("SELECT created_at FROM matrix_fact", [], |row| row.get(0))
            .unwrap();
        let ontology_updated: String = connection
            .query_row("SELECT updated_at FROM matrix_ontology_pack", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(source_created, "2024-12-30T23:59:58+00:00");
        assert_eq!(fact_created, "2024-12-31T23:59:58+00:00");
        assert_eq!(ontology_updated, "2024-12-29T23:59:58+00:00");
    }

    #[test]
    fn revision_rollback_and_failed_transaction_leave_no_partial_import() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE matrix_entity(entity_id TEXT PRIMARY KEY,entity_type TEXT NOT NULL,canonical_key TEXT NOT NULL,display_name TEXT NOT NULL,source_keys_json TEXT NOT NULL,attributes_json TEXT NOT NULL,confidence REAL NOT NULL,entity_json TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(entity_type,canonical_key));
             CREATE TABLE matrix_entity_source_key(source_system TEXT NOT NULL,source_key TEXT NOT NULL,entity_id TEXT NOT NULL,source_ref TEXT,created_at TEXT NOT NULL,PRIMARY KEY(source_system,source_key),FOREIGN KEY(entity_id) REFERENCES matrix_entity(entity_id));
             CREATE TABLE matrix_fact(fact_id TEXT PRIMARY KEY,snapshot_id TEXT NOT NULL,fact_type TEXT NOT NULL,entity_refs_json TEXT NOT NULL,metric_key TEXT,dimensions_json TEXT NOT NULL,measures_json TEXT NOT NULL,event_time TEXT NOT NULL,valid_from TEXT,valid_to TEXT,source_ref TEXT,confidence REAL NOT NULL,raw_hash TEXT NOT NULL,created_at TEXT NOT NULL);
             CREATE TABLE matrix_ontology_pack(ontology_id TEXT PRIMARY KEY,domain TEXT NOT NULL,version TEXT NOT NULL,pack_json TEXT NOT NULL,updated_at TEXT NOT NULL);"
        ).unwrap();
        let first = fixture();
        apply_sqlite(&mut connection, &first).unwrap();

        let rollback = fixture_variant(1, "2020-01-01T00:00:00Z", "fence-2", 2);
        let error = apply_sqlite(&mut connection, &rollback)
            .unwrap_err()
            .to_string();
        assert!(error.contains("revision rollback"));
        let receipts: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM matrix_ownership_import_receipt",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(receipts, 1);

        let mut fresh = Connection::open_in_memory().unwrap();
        fresh.execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE matrix_entity(entity_id TEXT PRIMARY KEY,entity_type TEXT NOT NULL,canonical_key TEXT NOT NULL,display_name TEXT NOT NULL,source_keys_json TEXT NOT NULL,attributes_json TEXT NOT NULL,confidence REAL NOT NULL,entity_json TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(entity_type,canonical_key));
             CREATE TABLE matrix_entity_source_key(source_system TEXT NOT NULL,source_key TEXT NOT NULL,entity_id TEXT NOT NULL,source_ref TEXT,created_at TEXT NOT NULL CHECK(created_at != '2024-12-30T23:59:58+00:00'),PRIMARY KEY(source_system,source_key),FOREIGN KEY(entity_id) REFERENCES matrix_entity(entity_id));
             CREATE TABLE matrix_fact(fact_id TEXT PRIMARY KEY,snapshot_id TEXT NOT NULL,fact_type TEXT NOT NULL,entity_refs_json TEXT NOT NULL,metric_key TEXT,dimensions_json TEXT NOT NULL,measures_json TEXT NOT NULL,event_time TEXT NOT NULL,valid_from TEXT,valid_to TEXT,source_ref TEXT,confidence REAL NOT NULL,raw_hash TEXT NOT NULL,created_at TEXT NOT NULL);
             CREATE TABLE matrix_ontology_pack(ontology_id TEXT PRIMARY KEY,domain TEXT NOT NULL,version TEXT NOT NULL,pack_json TEXT NOT NULL,updated_at TEXT NOT NULL);"
        ).unwrap();
        let bad = fixture();
        assert!(apply_sqlite(&mut fresh, &bad).is_err());
        let entities: i64 = fresh
            .query_row("SELECT COUNT(*) FROM matrix_entity", [], |row| row.get(0))
            .unwrap();
        assert_eq!(entities, 0);
    }
}
