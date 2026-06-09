use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{IaccAttentionItem, IaccEvidencePacket, IaccEvidenceSourceRef, IaccFact};

pub const IACC_SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Error)]
pub enum IaccStoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("iacc record not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccHealth {
    pub schema_version: i64,
    pub fact_count: u64,
    pub attention_count: u64,
    pub evidence_count: u64,
}

#[derive(Debug)]
pub struct IaccStore {
    connection: Mutex<Connection>,
}

impl IaccStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IaccStoreError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn in_memory() -> Result<Self, IaccStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, IaccStoreError> {
        connection.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
        connection.query_row("PRAGMA busy_timeout=5000", [], |_| Ok(()))?;
        connection.execute_batch("PRAGMA foreign_keys=ON;")?;
        initialize_schema(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn health(&self) -> Result<IaccHealth, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(IaccHealth {
            schema_version: schema_version(&connection)?,
            fact_count: count_table(&connection, "iacc_fact")?,
            attention_count: count_table(&connection, "iacc_attention_item")?,
            evidence_count: count_table(&connection, "iacc_evidence_packet")?,
        })
    }

    pub fn ingest_fact(&self, fact: &IaccFact) -> Result<IaccAttentionItem, IaccStoreError> {
        let attention = IaccAttentionItem::from_fact(
            &fact.fact_id,
            &fact.fact_type,
            fact.entity_refs.first().cloned(),
            fact.confidence,
        );
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection.execute(
            r"INSERT OR REPLACE INTO iacc_fact (
                fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
                dimensions_json, measures_json, event_time, valid_from, valid_to,
                source_ref, confidence, raw_hash, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                fact.fact_id,
                fact.snapshot_id,
                fact.fact_type,
                serde_json::to_string(&fact.entity_refs)?,
                fact.metric_key,
                serde_json::to_string(&fact.dimensions)?,
                serde_json::to_string(&fact.measures)?,
                fact.event_time.to_rfc3339(),
                fact.valid_from.map(|value| value.to_rfc3339()),
                fact.valid_to.map(|value| value.to_rfc3339()),
                fact.source_ref,
                fact.confidence,
                fact.raw_hash,
                Utc::now().to_rfc3339(),
            ],
        )?;
        upsert_attention(&connection, &attention)?;
        Ok(attention)
    }

    pub fn list_attention(&self, limit: usize) -> Result<Vec<IaccAttentionItem>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT attention_json
              FROM iacc_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn build_evidence_packet(
        &self,
        attention_id: Option<&str>,
        problem_statement: Option<&str>,
    ) -> Result<IaccEvidencePacket, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let attention = match attention_id {
            Some(id) => Some(
                find_attention(&connection, id)?
                    .ok_or_else(|| IaccStoreError::NotFound(id.to_string()))?,
            ),
            None => latest_attention(&connection)?,
        };
        let mut packet = IaccEvidencePacket::new(problem_statement.unwrap_or_else(|| {
            attention
                .as_ref()
                .map(|item| item.title.as_str())
                .unwrap_or("IACC operational evidence packet")
        }));
        packet.attention_id = attention.as_ref().map(|item| item.attention_id.clone());
        if let Some(item) = attention {
            packet.confidence = item.confidence.min(0.75);
            packet.business_context = serde_json::json!({
                "business_domain": item.business_domain,
                "entity_ref": item.entity_ref,
                "period": item.period,
                "priority_score": item.priority_score,
                "reason_codes": item.reason_codes,
                "owner_roles": item.owner_roles,
            });
            for reference in item.linked_changes {
                packet.source_refs.push(IaccEvidenceSourceRef {
                    kind: "change_or_fact".to_string(),
                    reference,
                    summary: "V0.9.77 foundation attention source".to_string(),
                });
            }
        }
        insert_evidence_packet(&connection, &packet)?;
        Ok(packet)
    }

    pub fn get_evidence_packet(
        &self,
        packet_id: &str,
    ) -> Result<Option<IaccEvidencePacket>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        connection
            .query_row(
                "SELECT packet_json FROM iacc_evidence_packet WHERE packet_id = ?1",
                params![packet_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
            .transpose()
    }
}

fn initialize_schema(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        r"CREATE TABLE IF NOT EXISTS iacc_schema (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            schema_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
        INSERT INTO iacc_schema (id, schema_version, updated_at)
        VALUES (1, 1, datetime('now'))
        ON CONFLICT(id) DO UPDATE SET schema_version = excluded.schema_version;

        CREATE TABLE IF NOT EXISTS iacc_fact (
            fact_id TEXT PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            fact_type TEXT NOT NULL,
            entity_refs_json TEXT NOT NULL,
            metric_key TEXT,
            dimensions_json TEXT NOT NULL,
            measures_json TEXT NOT NULL,
            event_time TEXT NOT NULL,
            valid_from TEXT,
            valid_to TEXT,
            source_ref TEXT,
            confidence REAL NOT NULL,
            raw_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_fact_type ON iacc_fact(fact_type);
        CREATE INDEX IF NOT EXISTS idx_iacc_fact_snapshot ON iacc_fact(snapshot_id);

        CREATE TABLE IF NOT EXISTS iacc_attention_item (
            attention_id TEXT PRIMARY KEY,
            priority_score REAL NOT NULL,
            status TEXT NOT NULL,
            attention_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_attention_priority
            ON iacc_attention_item(priority_score DESC, updated_at DESC);

        CREATE TABLE IF NOT EXISTS iacc_evidence_packet (
            packet_id TEXT PRIMARY KEY,
            attention_id TEXT,
            packet_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );",
    )
}

fn schema_version(connection: &Connection) -> rusqlite::Result<i64> {
    connection.query_row(
        "SELECT schema_version FROM iacc_schema WHERE id = 1",
        [],
        |row| row.get(0),
    )
}

fn count_table(connection: &Connection, table: &str) -> rusqlite::Result<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    connection
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map(|value| value as u64)
}

fn upsert_attention(
    connection: &Connection,
    item: &IaccAttentionItem,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT OR REPLACE INTO iacc_attention_item (
            attention_id, priority_score, status, attention_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            item.attention_id,
            item.priority_score,
            item.status,
            serde_json::to_string(item)?,
            item.created_at.to_rfc3339(),
            item.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_attention(
    connection: &Connection,
    attention_id: &str,
) -> Result<Option<IaccAttentionItem>, IaccStoreError> {
    connection
        .query_row(
            "SELECT attention_json FROM iacc_attention_item WHERE attention_id = ?1",
            params![attention_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn latest_attention(connection: &Connection) -> Result<Option<IaccAttentionItem>, IaccStoreError> {
    connection
        .query_row(
            r"SELECT attention_json
              FROM iacc_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn insert_evidence_packet(
    connection: &Connection,
    packet: &IaccEvidencePacket,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT OR REPLACE INTO iacc_evidence_packet (
            packet_id, attention_id, packet_json, created_at
        ) VALUES (?1, ?2, ?3, ?4)",
        params![
            packet.packet_id,
            packet.attention_id,
            serde_json::to_string(packet)?,
            packet.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iacc::IaccFactInput;

    #[test]
    fn iacc_store_ingests_fact_and_builds_evidence_packet() {
        let store = IaccStore::in_memory().expect("store opens");
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-1".to_string()),
            snapshot_id: Some("snapshot-1".to_string()),
            fact_type: "supply.material_shortage".to_string(),
            entity_refs: vec!["component:gpu-a".to_string()],
            metric_key: Some("material_shortage_risk".to_string()),
            dimensions: serde_json::json!({"week": "2026-W24"}),
            measures: serde_json::json!({"short_qty": 42}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: Some("connector:mock.docs:shortage".to_string()),
            confidence: Some(0.9),
            raw_hash: None,
        });

        let attention = store.ingest_fact(&fact).expect("fact ingests");
        assert_eq!(attention.business_domain, "supply");

        let hot = store.list_attention(10).expect("attention lists");
        assert_eq!(hot.len(), 1);

        let packet = store
            .build_evidence_packet(Some(&attention.attention_id), None)
            .expect("packet builds");
        assert_eq!(
            packet.attention_id.as_deref(),
            Some(attention.attention_id.as_str())
        );
        assert!(!packet.source_refs.is_empty());

        let health = store.health().expect("health loads");
        assert_eq!(health.schema_version, IACC_SCHEMA_VERSION);
        assert_eq!(health.fact_count, 1);
        assert_eq!(health.attention_count, 1);
        assert_eq!(health.evidence_count, 1);
    }
}
