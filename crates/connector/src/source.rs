use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{types::ValueRef, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
pub use surface::{
    SourceBatchCursor, SourceConnectorState, SourceEventBatch, SourceFieldSchema,
    SourceIncrementalRunRequest, SourceIncrementalRunResult, SourceIngestionReceipt,
    SourceReadPlan, SourceRecordBatch, SourceTableSchema, SourceWatermark,
};

const DEFAULT_BATCH_LIMIT: usize = 100;
const MAX_BATCH_LIMIT: usize = 1_000;

fn bounded_limit(plan: &SourceReadPlan) -> usize {
    plan.limit
        .unwrap_or(DEFAULT_BATCH_LIMIT)
        .clamp(1, MAX_BATCH_LIMIT)
}

fn source_offset(plan: &SourceReadPlan) -> usize {
    plan.offset.unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceAdapterManifest {
    pub adapter_id: String,
    pub display_name: String,
    pub family: String,
    pub access_mode: String,
    pub refresh_modes: Vec<String>,
    pub supports_schema_discovery: bool,
    pub supports_snapshot: bool,
    pub supports_incremental: bool,
    pub supports_event_subscription: bool,
    pub requires_sidecar: bool,
    pub config_schema_ref: Option<String>,
    pub notes: Vec<String>,
}

impl SourceAdapterManifest {
    fn local(
        adapter_id: &str,
        display_name: &str,
        access_mode: &str,
        refresh_modes: &[&str],
        schema_ref: &str,
    ) -> Self {
        Self {
            adapter_id: adapter_id.to_string(),
            display_name: display_name.to_string(),
            family: "source.local".to_string(),
            access_mode: access_mode.to_string(),
            refresh_modes: refresh_modes
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            supports_schema_discovery: true,
            supports_snapshot: true,
            supports_incremental: false,
            supports_event_subscription: false,
            requires_sidecar: false,
            config_schema_ref: Some(schema_ref.to_string()),
            notes: Vec::new(),
        }
    }

    fn sidecar(
        adapter_id: &str,
        display_name: &str,
        family: &str,
        refresh_modes: &[&str],
        schema_ref: &str,
        notes: &[&str],
    ) -> Self {
        Self::sidecar_with_access_mode(
            adapter_id,
            display_name,
            family,
            "api",
            refresh_modes,
            schema_ref,
            notes,
        )
    }

    fn sidecar_with_access_mode(
        adapter_id: &str,
        display_name: &str,
        family: &str,
        access_mode: &str,
        refresh_modes: &[&str],
        schema_ref: &str,
        notes: &[&str],
    ) -> Self {
        Self {
            adapter_id: adapter_id.to_string(),
            display_name: display_name.to_string(),
            family: family.to_string(),
            access_mode: access_mode.to_string(),
            refresh_modes: refresh_modes
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            supports_schema_discovery: true,
            supports_snapshot: true,
            supports_incremental: true,
            supports_event_subscription: true,
            requires_sidecar: true,
            config_schema_ref: Some(schema_ref.to_string()),
            notes: notes.iter().map(|value| (*value).to_string()).collect(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SourceAdapterError {
    #[error("unsupported source adapter: {0}")]
    UnsupportedAdapter(String),
    #[error("source resource path is required")]
    MissingResourcePath,
    #[error("sqlite table is required")]
    MissingSqliteTable,
    #[error("invalid sqlite identifier: {0}")]
    InvalidSqliteIdentifier(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[must_use]
pub fn builtin_source_adapter_manifests() -> Vec<SourceAdapterManifest> {
    vec![
        SourceAdapterManifest::local(
            "csv",
            "CSV File",
            "file",
            &["snapshot", "manual_upload"],
            "schema://connector/source/csv/read-plan",
        ),
        SourceAdapterManifest::local(
            "jsonl",
            "JSONL File",
            "file",
            &["snapshot", "manual_upload"],
            "schema://connector/source/jsonl/read-plan",
        ),
        SourceAdapterManifest::local(
            "sqlite",
            "SQLite Database",
            "database_file",
            &["snapshot", "scheduled_snapshot"],
            "schema://connector/source/sqlite/read-plan",
        ),
        SourceAdapterManifest::sidecar_with_access_mode(
            "postgres",
            "PostgreSQL",
            "source.sql",
            "database_service",
            &["snapshot", "incremental", "event"],
            "schema://connector/source/postgres/read-plan",
            &[
                "database network drivers run in a source sidecar, not in the core gateway/runtime",
                "rows are delivered to Matrix through the SourceRecordBatch contract",
                "event payloads are normalized through Edge source event actions",
            ],
        ),
        SourceAdapterManifest::sidecar_with_access_mode(
            "mysql",
            "MySQL",
            "source.sql",
            "database_service",
            &["snapshot", "incremental", "event"],
            "schema://connector/source/mysql/read-plan",
            &[
                "database network drivers run in a source sidecar, not in the core gateway/runtime",
                "rows are delivered to Matrix through the SourceRecordBatch contract",
                "event payloads are normalized through Edge source event actions",
            ],
        ),
        SourceAdapterManifest::sidecar_with_access_mode(
            "mariadb",
            "MariaDB",
            "source.sql",
            "database_service",
            &["snapshot", "incremental", "event"],
            "schema://connector/source/mariadb/read-plan",
            &[
                "database network drivers run in a source sidecar, not in the core gateway/runtime",
                "rows are delivered to Matrix through the SourceRecordBatch contract",
                "event payloads are normalized through Edge source event actions",
            ],
        ),
        SourceAdapterManifest::local(
            "local_file_batch",
            "Local File Batch",
            "file_batch",
            &["snapshot", "manual_upload"],
            "schema://connector/source/local-file-batch/read-plan",
        ),
        SourceAdapterManifest::sidecar(
            "feishu_bitable",
            "Feishu Bitable",
            "source.feishu",
            &["snapshot", "incremental", "event"],
            "schema://connector/source/feishu-bitable/read-plan",
            &[
                "remote API execution is delegated to the Edge source connector sidecar",
                "records are delivered to Matrix through the same SourceRecordBatch contract",
            ],
        ),
        SourceAdapterManifest::sidecar(
            "lark_bitable",
            "Lark Base",
            "source.lark",
            &["snapshot", "incremental", "event"],
            "schema://connector/source/lark-bitable/read-plan",
            &[
                "remote API execution is delegated to the Edge source connector sidecar",
                "records are delivered to Matrix through the same SourceRecordBatch contract",
            ],
        ),
    ]
}

#[must_use]
pub fn source_adapter_manifest(adapter_id: &str) -> Option<SourceAdapterManifest> {
    builtin_source_adapter_manifests()
        .into_iter()
        .find(|manifest| manifest.adapter_id == adapter_id)
}

pub fn read_local_source_batch(
    plan: &SourceReadPlan,
) -> Result<SourceRecordBatch, SourceAdapterError> {
    match plan.adapter_id.as_str() {
        "csv" => read_csv_batch(plan),
        "jsonl" => read_jsonl_batch(plan),
        "sqlite" => read_sqlite_batch(plan),
        "local_file_batch" => read_file_batch_manifest(plan),
        other => Err(SourceAdapterError::UnsupportedAdapter(other.to_string())),
    }
}

fn read_csv_batch(plan: &SourceReadPlan) -> Result<SourceRecordBatch, SourceAdapterError> {
    let path = local_path_from_ref(&plan.resource_ref)?;
    let content = fs::read_to_string(&path)?;
    let mut lines = content.lines();
    let headers = lines
        .next()
        .map(parse_csv_line)
        .unwrap_or_default()
        .into_iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    let limit = bounded_limit(plan);
    let offset = source_offset(plan);
    let mut rows = Vec::new();
    let mut total = 0usize;
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        if total >= offset && rows.len() < limit {
            let values = parse_csv_line(line);
            let mut object = Map::new();
            for (index, header) in headers.iter().enumerate() {
                object.insert(
                    header.clone(),
                    values
                        .get(index)
                        .map(|value| Value::String(value.clone()))
                        .unwrap_or(Value::Null),
                );
            }
            rows.push(Value::Object(object));
        }
        total += 1;
    }
    Ok(batch_from_rows(
        plan,
        table_name_from_path(&path),
        schema_from_rows(table_name_from_path(&path), &headers, &rows),
        rows,
        total,
    ))
}

fn read_jsonl_batch(plan: &SourceReadPlan) -> Result<SourceRecordBatch, SourceAdapterError> {
    let path = local_path_from_ref(&plan.resource_ref)?;
    let content = fs::read_to_string(&path)?;
    let limit = bounded_limit(plan);
    let offset = source_offset(plan);
    let mut rows = Vec::new();
    let mut total = 0usize;
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        if total >= offset && rows.len() < limit {
            rows.push(serde_json::from_str::<Value>(line)?);
        }
        total += 1;
    }
    let headers = rows
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|object| object.keys().cloned())
        .fold(Vec::<String>::new(), |mut acc, key| {
            if !acc.iter().any(|item| item == &key) {
                acc.push(key);
            }
            acc
        });
    Ok(batch_from_rows(
        plan,
        table_name_from_path(&path),
        schema_from_rows(table_name_from_path(&path), &headers, &rows),
        rows,
        total,
    ))
}

fn read_file_batch_manifest(
    plan: &SourceReadPlan,
) -> Result<SourceRecordBatch, SourceAdapterError> {
    let path = local_path_from_ref(&plan.resource_ref)?;
    if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
        let mut next = plan.clone();
        next.adapter_id = "jsonl".to_string();
        return read_jsonl_batch(&next);
    }
    let mut next = plan.clone();
    next.adapter_id = "csv".to_string();
    read_csv_batch(&next)
}

fn read_sqlite_batch(plan: &SourceReadPlan) -> Result<SourceRecordBatch, SourceAdapterError> {
    let path = local_path_from_ref(&plan.resource_ref)?;
    let table = plan
        .table
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(SourceAdapterError::MissingSqliteTable)?;
    validate_identifier(table)?;
    for field in &plan.fields {
        validate_identifier(field)?;
    }
    let connection = Connection::open(path)?;
    let schema = sqlite_schema(&connection, table)?;
    let fields = if plan.fields.is_empty() {
        schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>()
    } else {
        plan.fields.clone()
    };
    let quoted_fields = fields
        .iter()
        .map(|field| format!("\"{}\"", field.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {quoted_fields} FROM \"{}\" LIMIT ?1 OFFSET ?2",
        table.replace('"', "\"\"")
    );
    let mut statement = connection.prepare(&sql)?;
    let mut rows_iter = statement.query(rusqlite::params![
        bounded_limit(plan) as i64,
        source_offset(plan) as i64
    ])?;
    let mut rows = Vec::new();
    while let Some(row) = rows_iter.next()? {
        let mut object = Map::new();
        for (index, field) in fields.iter().enumerate() {
            object.insert(field.clone(), sqlite_value_to_json(row.get_ref(index)?));
        }
        rows.push(Value::Object(object));
    }
    let total = connection.query_row(
        &format!("SELECT COUNT(*) FROM \"{}\"", table.replace('"', "\"\"")),
        [],
        |row| row.get::<_, i64>(0),
    )? as usize;
    Ok(batch_from_rows(
        plan,
        table.to_string(),
        schema,
        rows,
        total,
    ))
}

fn batch_from_rows(
    plan: &SourceReadPlan,
    table: String,
    schema: SourceTableSchema,
    rows: Vec<Value>,
    total: usize,
) -> SourceRecordBatch {
    let offset = source_offset(plan);
    let limit = bounded_limit(plan);
    let next_offset = if offset + rows.len() < total {
        Some(offset + rows.len())
    } else {
        None
    };
    SourceRecordBatch {
        adapter_id: plan.adapter_id.clone(),
        resource_ref: plan.resource_ref.clone(),
        table: Some(table),
        schema,
        checksum: stable_checksum(&rows),
        row_count: total,
        truncated: rows.len() < total.saturating_sub(offset),
        cursor: SourceBatchCursor {
            offset,
            limit,
            next_offset,
        },
        rows,
    }
}

fn local_path_from_ref(resource_ref: &str) -> Result<PathBuf, SourceAdapterError> {
    let trimmed = resource_ref.trim();
    if trimmed.is_empty() {
        return Err(SourceAdapterError::MissingResourcePath);
    }
    let path = trimmed
        .strip_prefix("file://")
        .or_else(|| trimmed.strip_prefix("local://"))
        .unwrap_or(trimmed);
    Ok(PathBuf::from(path))
}

fn table_name_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("source")
        .to_string()
}

fn schema_from_rows(table_name: String, headers: &[String], rows: &[Value]) -> SourceTableSchema {
    let fields = headers
        .iter()
        .map(|name| {
            let data_type = rows
                .iter()
                .filter_map(Value::as_object)
                .filter_map(|object| object.get(name))
                .find(|value| !value.is_null())
                .map(infer_json_type)
                .unwrap_or_else(|| "text".to_string());
            SourceFieldSchema {
                name: name.clone(),
                data_type,
                nullable: rows
                    .iter()
                    .filter_map(Value::as_object)
                    .any(|object| object.get(name).is_none_or(Value::is_null)),
            }
        })
        .collect();
    SourceTableSchema {
        table_name,
        fields,
        primary_key: Vec::new(),
    }
}

fn sqlite_schema(
    connection: &Connection,
    table: &str,
) -> Result<SourceTableSchema, SourceAdapterError> {
    let mut statement = connection.prepare(&format!(
        "PRAGMA table_info(\"{}\")",
        table.replace('"', "\"\"")
    ))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    let mut fields = Vec::new();
    let mut primary_key = Vec::new();
    for row in rows {
        let (name, data_type, not_null, pk) = row?;
        if pk > 0 {
            primary_key.push(name.clone());
        }
        fields.push(SourceFieldSchema {
            name,
            data_type: if data_type.trim().is_empty() {
                "unknown".to_string()
            } else {
                data_type.to_ascii_lowercase()
            },
            nullable: not_null == 0,
        });
    }
    Ok(SourceTableSchema {
        table_name: table.to_string(),
        fields,
        primary_key,
    })
}

fn sqlite_value_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::from(value),
        ValueRef::Real(value) => Value::from(value),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).to_string()),
        ValueRef::Blob(value) => Value::String(format!("blob:{}bytes", value.len())),
    }
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                values.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    values.push(current.trim().to_string());
    values
}

fn infer_json_type(value: &Value) -> String {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "text",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
    .to_string()
}

fn stable_checksum(rows: &[Value]) -> String {
    let mut hasher = Sha256::new();
    for row in rows {
        let bytes = serde_json::to_vec(row).unwrap_or_default();
        hasher.update(bytes);
        hasher.update(b"\n");
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn validate_identifier(value: &str) -> Result<(), SourceAdapterError> {
    let valid = !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    if valid {
        Ok(())
    } else {
        Err(SourceAdapterError::InvalidSqliteIdentifier(
            value.to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_manifests_include_local_and_bitable_sources() {
        let ids = builtin_source_adapter_manifests()
            .into_iter()
            .map(|manifest| manifest.adapter_id)
            .collect::<Vec<_>>();
        assert!(ids.contains(&"csv".to_string()));
        assert!(ids.contains(&"jsonl".to_string()));
        assert!(ids.contains(&"sqlite".to_string()));
        assert!(ids.contains(&"postgres".to_string()));
        assert!(ids.contains(&"mysql".to_string()));
        assert!(ids.contains(&"mariadb".to_string()));
        assert!(ids.contains(&"feishu_bitable".to_string()));
        assert!(ids.contains(&"lark_bitable".to_string()));
    }

    #[test]
    fn database_source_manifests_are_sidecar_event_capable() {
        for adapter_id in ["postgres", "mysql", "mariadb"] {
            let manifest = source_adapter_manifest(adapter_id).unwrap();
            assert!(manifest.requires_sidecar);
            assert!(manifest.supports_schema_discovery);
            assert!(manifest.supports_snapshot);
            assert!(manifest.supports_incremental);
            assert!(manifest.supports_event_subscription);
            assert!(manifest.refresh_modes.iter().any(|mode| mode == "event"));
            assert_eq!(manifest.access_mode, "database_service");
        }
    }

    #[test]
    fn source_contract_serializes_watermark_and_run_result() {
        let watermark = SourceWatermark {
            adapter_id: "postgres".to_string(),
            resource_ref: "postgres://***/orders".to_string(),
            table: Some("orders".to_string()),
            strategy: "updated_at_field".to_string(),
            cursor: None,
            offset: None,
            high_watermark: Some("2026-07-07T00:00:00Z".to_string()),
            checksum: Some("sha256:test".to_string()),
            revision: 7,
            updated_at_ms: 1_783_440_000_000,
        };
        let result = SourceIncrementalRunResult {
            status: "ingested".to_string(),
            chunk_index: 0,
            final_chunk: true,
            batch: None,
            watermark_before: Some(watermark.clone()),
            watermark_after: Some(watermark.clone()),
            degraded_reason: None,
            receipt: Some(SourceIngestionReceipt {
                receipt_id: "receipt-postgres-orders".to_string(),
                adapter_id: "postgres".to_string(),
                resource_ref: watermark.resource_ref.clone(),
                row_count: 2,
                checksum: "sha256:test".to_string(),
                watermark_before: None,
                watermark_after: Some(watermark),
                matrix_refs: vec!["matrix:source_pack:postgres-orders".to_string()],
                created_at_ms: 1_783_440_000_000,
            }),
        };
        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["status"], "ingested");
        assert_eq!(value["receipt"]["row_count"], 2);
        assert_eq!(value["watermark_after"]["strategy"], "updated_at_field");
    }

    #[test]
    fn builtin_source_manifests_explain_event_degraded_modes() {
        for adapter_id in ["postgres", "mysql", "mariadb"] {
            let manifest = source_adapter_manifest(adapter_id).unwrap();
            let notes = manifest.notes.join(" ");
            assert!(notes.contains("event payloads are normalized"));
            assert!(notes.contains("sidecar"));
        }
    }

    #[test]
    fn csv_batch_reads_rows_and_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orders.csv");
        fs::write(&path, "id,qty\nA,2\nB,3\n").unwrap();
        let batch = read_local_source_batch(&SourceReadPlan {
            adapter_id: "csv".to_string(),
            resource_ref: path.display().to_string(),
            table: None,
            fields: Vec::new(),
            limit: Some(10),
            offset: Some(0),
            cursor: None,
            metadata: Value::Null,
        })
        .unwrap();
        assert_eq!(batch.row_count, 2);
        assert_eq!(batch.schema.table_name, "orders");
        assert_eq!(batch.rows[0]["id"], "A");
        assert!(batch.checksum.starts_with("sha256:"));
    }

    #[test]
    fn jsonl_batch_reads_object_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        fs::write(&path, "{\"id\":\"E1\",\"ok\":true}\n{\"id\":\"E2\"}\n").unwrap();
        let batch = read_local_source_batch(&SourceReadPlan {
            adapter_id: "jsonl".to_string(),
            resource_ref: format!("file://{}", path.display()),
            table: None,
            fields: Vec::new(),
            limit: Some(1),
            offset: Some(1),
            cursor: None,
            metadata: Value::Null,
        })
        .unwrap();
        assert_eq!(batch.row_count, 2);
        assert_eq!(batch.cursor.next_offset, None);
        assert_eq!(batch.rows[0]["id"], "E2");
    }

    #[test]
    fn sqlite_batch_reads_table_without_arbitrary_sql() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("demo.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("CREATE TABLE orders (id TEXT PRIMARY KEY, qty INTEGER)", [])
            .unwrap();
        connection
            .execute("INSERT INTO orders (id, qty) VALUES ('A', 2), ('B', 3)", [])
            .unwrap();
        let batch = read_local_source_batch(&SourceReadPlan {
            adapter_id: "sqlite".to_string(),
            resource_ref: path.display().to_string(),
            table: Some("orders".to_string()),
            fields: Vec::new(),
            limit: Some(10),
            offset: Some(0),
            cursor: None,
            metadata: Value::Null,
        })
        .unwrap();
        assert_eq!(batch.row_count, 2);
        assert_eq!(batch.schema.primary_key, vec!["id".to_string()]);
        assert_eq!(batch.rows[0]["qty"], 2);
    }
}
