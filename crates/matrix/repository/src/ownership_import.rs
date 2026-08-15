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
        let incoming_revision = serde_json::to_value(&object.revision)?;
        if payload_digest == object.payload_digest && previous_revision == incoming_revision { return Ok(()); }
        compare_revision(&previous_revision, &incoming_revision, &stable_ref)?;
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
        params![stable_ref, serde_json::to_string(&object.revision)?, object.payload_digest,
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
    if previous["projection_key"] != incoming["projection_key"]
        || previous["context_digest"] != incoming["context_digest"]
    {
        return Err(migration(format!(
            "revision context mismatch for `{stable_ref}`"
        )));
    }
    let previous_axis = previous["axis"]
        .as_array()
        .ok_or_else(|| migration("previous revision axis missing"))?;
    let incoming_axis = incoming["axis"]
        .as_array()
        .ok_or_else(|| migration("incoming revision axis missing"))?;
    if previous_axis.len() != incoming_axis.len() {
        return Err(migration(format!(
            "incomparable revision for `{stable_ref}`"
        )));
    }
    let mut ordering = Some(std::cmp::Ordering::Equal);
    for (left, right) in previous_axis.iter().zip(incoming_axis) {
        let item = match (left, right) {
            (Value::Number(left), Value::Number(right)) => left
                .as_i64()
                .zip(right.as_i64())
                .map(|(left, right)| left.cmp(&right)),
            (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
            _ => None,
        };
        if item != Some(std::cmp::Ordering::Equal) {
            ordering = item;
            break;
        }
    }
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
    if object
        .stable_id
        .starts_with(&format!("{}:", object.source_table))
    {
        Ok(object.stable_id.clone())
    } else {
        Err(migration("stable_id aggregate mismatch"))
    }
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
    use matrix_core::{MfgOwnershipSplitSnapshotV1, OwnershipImportContext};

    fn comprehensive_plan() -> CoreMatrixImportPlan {
        MfgOwnershipSplitSnapshotV1::decode_strict(include_bytes!(
            "../../../../contracts/ownership/v1/golden/comprehensive-snapshot.json"
        ))
        .unwrap()
        .dry_run(&OwnershipImportContext {
            external_reference_catalog: include_bytes!(
                "../../../../contracts/ownership/v1/golden/external-reference-catalog.json"
            )
            .to_vec(),
            revision_baseline: include_bytes!(
                "../../../../contracts/ownership/v1/golden/revision-baseline-comprehensive.json"
            )
            .to_vec(),
            execution_profile: include_bytes!(
                "../../../../contracts/ownership/v1/execution-profile.json"
            )
            .to_vec(),
        })
        .unwrap()
    }

    #[test]
    fn comprehensive_sqlite_import_is_atomic_idempotent_and_lossless() {
        let mut connection = Connection::open_in_memory().unwrap();
        crate::sqlite_repository::initialize_schema(&connection).unwrap();
        let plan = comprehensive_plan();
        assert_eq!(plan.objects().len(), 19);
        assert!(matches!(
            apply_sqlite(&mut connection, &plan).unwrap(),
            MatrixOwnershipImportOutcome::Applied(_)
        ));
        assert!(matches!(
            apply_sqlite(&mut connection, &plan).unwrap(),
            MatrixOwnershipImportOutcome::AlreadyApplied(_)
        ));
        for table in TABLES {
            let count: i64 = connection
                .query_row(
                    &format!("SELECT COUNT(*) FROM {}", table.table),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "{}", table.table);
        }
        let created_at: String = connection
            .query_row("SELECT created_at FROM matrix_entity", [], |row| row.get(0))
            .unwrap();
        let updated_at: String = connection
            .query_row("SELECT updated_at FROM matrix_ontology_pack", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(created_at, "2026-08-15T00:00:00Z");
        assert_eq!(updated_at, "2026-08-15T00:00:00Z");
        let receipts: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM matrix_ownership_import_receipt",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let checkpoints: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM matrix_ownership_import_checkpoint",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!((receipts, checkpoints), (1, 19));
    }

    #[test]
    fn failed_write_rolls_back_all_rows_and_receipt() {
        let mut connection = Connection::open_in_memory().unwrap();
        crate::sqlite_repository::initialize_schema(&connection).unwrap();
        connection.execute("CREATE TRIGGER reject_fact BEFORE INSERT ON matrix_fact BEGIN SELECT RAISE(ABORT, 'injected'); END", []).unwrap();
        assert!(apply_sqlite(&mut connection, &comprehensive_plan()).is_err());
        let entities: i64 = connection
            .query_row("SELECT COUNT(*) FROM matrix_entity", [], |row| row.get(0))
            .unwrap();
        let receipt_tables: i64 = connection.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='matrix_ownership_import_receipt'", [], |row| row.get(0)).unwrap();
        assert_eq!((entities, receipt_tables), (0, 0));
    }
}
