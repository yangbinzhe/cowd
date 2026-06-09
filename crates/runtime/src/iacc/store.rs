use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::{
    IaccActionExecution, IaccActionExecutionRequest, IaccActionFeedback, IaccAttentionItem,
    IaccChangeEvent, IaccEvidencePacket, IaccEvidenceSourceRef, IaccFact, IaccIncident,
    IaccMetricDefinition, IaccMetricState, IaccOperationalAnalysis, IaccSeverity,
};

pub const IACC_SCHEMA_VERSION: i64 = 5;

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
    pub metric_definition_count: u64,
    pub metric_state_count: u64,
    pub change_count: u64,
    pub attention_count: u64,
    pub evidence_count: u64,
    pub incident_count: u64,
    pub analysis_count: u64,
    pub execution_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccMetricRecomputeResult {
    pub metric_state_count: usize,
    pub change_count: usize,
    pub attention_count: usize,
    pub metric_states: Vec<IaccMetricState>,
    pub changes: Vec<IaccChangeEvent>,
    pub attention: Vec<IaccAttentionItem>,
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
            metric_definition_count: count_table(&connection, "iacc_metric_definition")?,
            metric_state_count: count_table(&connection, "iacc_metric_state")?,
            change_count: count_table(&connection, "iacc_change_event")?,
            attention_count: count_table(&connection, "iacc_attention_item")?,
            evidence_count: count_table(&connection, "iacc_evidence_packet")?,
            incident_count: count_table(&connection, "iacc_incident")?,
            analysis_count: count_table(&connection, "iacc_operational_analysis")?,
            execution_count: count_table(&connection, "iacc_action_execution")?,
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

    pub fn recompute_metrics(&self) -> Result<IaccMetricRecomputeResult, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let facts = metric_facts(&connection)?;
        let mut groups = BTreeMap::<MetricGroupKey, MetricAccumulator>::new();
        for fact in facts {
            groups.entry(fact.key()).or_default().push(fact);
        }

        let mut states = Vec::new();
        let mut changes = Vec::new();
        let mut attention = Vec::new();
        for (key, accumulator) in groups {
            let definition =
                IaccMetricDefinition::inferred(key.metric_id.clone(), &accumulator.fact_type);
            upsert_metric_definition(&connection, &definition)?;
            let previous =
                latest_metric_state(&connection, &key.metric_id, &key.entity_scope, &key.period)?;
            let previous_value = previous.as_ref().map(|state| state.value);
            let value = accumulator.value;
            let delta = previous_value.map_or(value, |previous| value - previous);
            let delta_ratio = previous_value.and_then(|previous| {
                if previous.abs() > f64::EPSILON {
                    Some(delta / previous)
                } else {
                    None
                }
            });
            let state = IaccMetricState {
                state_id: format!("metric-state-{}", uuid::Uuid::new_v4()),
                metric_id: key.metric_id.clone(),
                entity_scope: key.entity_scope.clone(),
                period: key.period.clone(),
                value,
                previous_value,
                delta,
                delta_ratio,
                status: IaccMetricState::status_for_delta(delta),
                computed_at: Utc::now(),
                input_fact_refs: accumulator.fact_ids.clone(),
                confidence: accumulator.confidence(),
            };
            insert_metric_state(&connection, &state)?;
            states.push(state.clone());

            if delta.abs() > f64::EPSILON {
                let change = IaccChangeEvent {
                    change_id: format!("change-{}", uuid::Uuid::new_v4()),
                    change_type: "metric_delta".to_string(),
                    entity_ref: key.entity_scope.clone(),
                    metric_id: Some(key.metric_id.clone()),
                    from_value: previous_value.map(Value::from),
                    to_value: Some(Value::from(value)),
                    delta,
                    period: key.period.clone(),
                    detected_at: Utc::now(),
                    source_fact_refs: accumulator.fact_ids.clone(),
                    severity_hint: IaccChangeEvent::severity_for_delta(delta),
                };
                insert_change_event(&connection, &change)?;
                let item = attention_from_change(&change, &state);
                upsert_attention(&connection, &item)?;
                changes.push(change);
                attention.push(item);
            }
        }
        Ok(IaccMetricRecomputeResult {
            metric_state_count: states.len(),
            change_count: changes.len(),
            attention_count: attention.len(),
            metric_states: states,
            changes,
            attention,
        })
    }

    pub fn list_metric_definitions(&self) -> Result<Vec<IaccMetricDefinition>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT definition_json
              FROM iacc_metric_definition
              ORDER BY metric_id ASC",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn metric_states(&self, metric_id: &str) -> Result<Vec<IaccMetricState>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT state_json
              FROM iacc_metric_state
              WHERE metric_id = ?1
              ORDER BY computed_at DESC",
        )?;
        let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn list_changes(&self, limit: usize) -> Result<Vec<IaccChangeEvent>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT change_json
              FROM iacc_change_event
              ORDER BY detected_at DESC
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
                if let Some(change_id) = reference.strip_prefix("iacc:change:") {
                    if let Some(change) = find_change(&connection, change_id)? {
                        packet.change_evidence.push(serde_json::to_value(&change)?);
                        if let Some(metric_id) = change.metric_id.as_deref() {
                            if let Some(state) =
                                latest_metric_state_for_metric(&connection, metric_id)?
                            {
                                packet.metric_evidence.push(serde_json::to_value(&state)?);
                            }
                        }
                    }
                }
                packet.source_refs.push(IaccEvidenceSourceRef {
                    kind: "change_or_fact".to_string(),
                    reference,
                    summary: "IACC attention evidence source".to_string(),
                });
            }
            if !packet.metric_evidence.is_empty() {
                packet
                    .missing_evidence
                    .retain(|item| !item.contains("metric_network"));
                packet.confidence = packet.confidence.max(0.65);
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
        find_evidence_packet(&connection, packet_id)
    }

    pub fn create_incident(&self, incident: &IaccIncident) -> Result<IaccIncident, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_incident(&connection, incident)?;
        Ok(incident.clone())
    }

    pub fn get_incident(&self, incident_id: &str) -> Result<Option<IaccIncident>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_incident(&connection, incident_id)
    }

    pub fn analyze_incident(
        &self,
        incident_id: &str,
    ) -> Result<IaccOperationalAnalysis, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut incident = find_incident(&connection, incident_id)?
            .ok_or_else(|| IaccStoreError::NotFound(incident_id.to_string()))?;
        let packet_id = incident
            .evidence_packet_id
            .clone()
            .ok_or_else(|| IaccStoreError::NotFound("incident evidence packet".to_string()))?;
        let mut packet = find_evidence_packet(&connection, &packet_id)?
            .ok_or_else(|| IaccStoreError::NotFound(packet_id.clone()))?;
        let analysis = IaccOperationalAnalysis::from_evidence(incident_id, &packet);

        packet.attribution_candidates = analysis
            .attribution_candidates
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        packet.impact_paths = analysis
            .impact_paths
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        packet.missing_evidence.retain(|item| {
            !item.contains("attribution_not_computed")
                && !item.contains("impact_paths_not_computed")
        });
        packet.confidence = packet.confidence.max(analysis.confidence);
        insert_evidence_packet(&connection, &packet)?;
        insert_analysis(&connection, &analysis)?;

        incident.status = "analyzed".to_string();
        incident.updated_at = Utc::now();
        upsert_incident(&connection, &incident)?;
        Ok(analysis)
    }

    pub fn get_analysis(
        &self,
        analysis_id: &str,
    ) -> Result<Option<IaccOperationalAnalysis>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_analysis(&connection, analysis_id)
    }

    pub fn execute_recommended_action(
        &self,
        analysis_id: &str,
        action_id: &str,
        request: &IaccActionExecutionRequest,
    ) -> Result<IaccActionExecution, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let analysis = find_analysis(&connection, analysis_id)?
            .ok_or_else(|| IaccStoreError::NotFound(analysis_id.to_string()))?;
        let action = analysis
            .recommended_actions
            .iter()
            .find(|action| action.action_id == action_id)
            .cloned()
            .ok_or_else(|| IaccStoreError::NotFound(action_id.to_string()))?;
        let execution = IaccActionExecution::from_action(&analysis, &action, request);
        insert_execution(&connection, &execution)?;
        Ok(execution)
    }

    pub fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<IaccActionExecution>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_execution(&connection, execution_id)
    }

    pub fn record_execution_feedback(
        &self,
        execution_id: &str,
        feedback: IaccActionFeedback,
    ) -> Result<IaccActionExecution, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut execution = find_execution(&connection, execution_id)?
            .ok_or_else(|| IaccStoreError::NotFound(execution_id.to_string()))?;
        execution.apply_feedback(feedback);
        insert_execution(&connection, &execution)?;
        if execution.status == "feedback_resolved" {
            if let Some(mut incident) = find_incident(&connection, &execution.incident_id)? {
                incident.status = "closed".to_string();
                incident.updated_at = Utc::now();
                upsert_incident(&connection, &incident)?;
            }
        }
        Ok(execution)
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
        VALUES (1, 5, datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
            schema_version = CASE
                WHEN iacc_schema.schema_version < excluded.schema_version
                THEN excluded.schema_version
                ELSE iacc_schema.schema_version
            END,
            updated_at = excluded.updated_at;

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
        );

        CREATE TABLE IF NOT EXISTS iacc_metric_definition (
            metric_id TEXT PRIMARY KEY,
            definition_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS iacc_metric_state (
            state_id TEXT PRIMARY KEY,
            metric_id TEXT NOT NULL,
            entity_scope TEXT NOT NULL,
            period TEXT NOT NULL,
            value REAL NOT NULL,
            previous_value REAL,
            delta REAL NOT NULL,
            status TEXT NOT NULL,
            state_json TEXT NOT NULL,
            computed_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_metric_state_lookup
            ON iacc_metric_state(metric_id, entity_scope, period, computed_at DESC);

        CREATE TABLE IF NOT EXISTS iacc_change_event (
            change_id TEXT PRIMARY KEY,
            metric_id TEXT,
            entity_ref TEXT NOT NULL,
            period TEXT NOT NULL,
            delta REAL NOT NULL,
            severity_hint TEXT NOT NULL,
            change_json TEXT NOT NULL,
            detected_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_change_detected
            ON iacc_change_event(detected_at DESC);

        CREATE TABLE IF NOT EXISTS iacc_incident (
            incident_id TEXT PRIMARY KEY,
            attention_id TEXT,
            evidence_packet_id TEXT,
            task_id TEXT,
            agent_graph_id TEXT,
            status TEXT NOT NULL,
            incident_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_incident_updated
            ON iacc_incident(updated_at DESC);

        CREATE TABLE IF NOT EXISTS iacc_operational_analysis (
            analysis_id TEXT PRIMARY KEY,
            incident_id TEXT NOT NULL,
            evidence_packet_id TEXT NOT NULL,
            status TEXT NOT NULL,
            confidence REAL NOT NULL,
            analysis_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_analysis_incident
            ON iacc_operational_analysis(incident_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS iacc_action_execution (
            execution_id TEXT PRIMARY KEY,
            analysis_id TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            action_id TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL,
            execution_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_action_execution_analysis
            ON iacc_action_execution(analysis_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_iacc_action_execution_incident
            ON iacc_action_execution(incident_id, updated_at DESC);",
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

fn find_evidence_packet(
    connection: &Connection,
    packet_id: &str,
) -> Result<Option<IaccEvidencePacket>, IaccStoreError> {
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricGroupKey {
    metric_id: String,
    entity_scope: String,
    period: String,
}

#[derive(Debug, Clone)]
struct MetricFactRow {
    fact_id: String,
    fact_type: String,
    metric_id: String,
    entity_scope: String,
    period: String,
    value: f64,
    confidence: f32,
}

impl MetricFactRow {
    fn key(&self) -> MetricGroupKey {
        MetricGroupKey {
            metric_id: self.metric_id.clone(),
            entity_scope: self.entity_scope.clone(),
            period: self.period.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct MetricAccumulator {
    fact_type: String,
    value: f64,
    fact_ids: Vec<String>,
    confidence_sum: f32,
}

impl MetricAccumulator {
    fn push(&mut self, fact: MetricFactRow) {
        if self.fact_type.is_empty() {
            self.fact_type = fact.fact_type;
        }
        self.value += fact.value;
        self.fact_ids.push(format!("iacc:fact:{}", fact.fact_id));
        self.confidence_sum += fact.confidence;
    }

    fn confidence(&self) -> f32 {
        if self.fact_ids.is_empty() {
            0.0
        } else {
            self.confidence_sum / self.fact_ids.len() as f32
        }
    }
}

fn metric_facts(connection: &Connection) -> Result<Vec<MetricFactRow>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT fact_id, fact_type, entity_refs_json, metric_key, dimensions_json,
            measures_json, confidence
          FROM iacc_fact
          WHERE metric_key IS NOT NULL
          ORDER BY event_time ASC, fact_id ASC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, f32>(6)?,
        ))
    })?;
    let mut facts = Vec::new();
    for row in rows {
        let (
            fact_id,
            fact_type,
            entity_refs_json,
            metric_id,
            dimensions_json,
            measures_json,
            confidence,
        ) = row?;
        let entity_refs: Vec<String> = serde_json::from_str(&entity_refs_json)?;
        let dimensions: Value = serde_json::from_str(&dimensions_json)?;
        let measures: Value = serde_json::from_str(&measures_json)?;
        let entity_scope = entity_refs
            .first()
            .cloned()
            .unwrap_or_else(|| "enterprise".to_string());
        let period = dimensions
            .get("period")
            .or_else(|| dimensions.get("week"))
            .and_then(Value::as_str)
            .unwrap_or("current")
            .to_string();
        let value = numeric_measure_sum(&measures);
        facts.push(MetricFactRow {
            fact_id,
            fact_type,
            metric_id,
            entity_scope,
            period,
            value,
            confidence,
        });
    }
    Ok(facts)
}

fn numeric_measure_sum(value: &Value) -> f64 {
    match value {
        Value::Number(number) => number.as_f64().unwrap_or(0.0),
        Value::Object(map) => map.values().map(numeric_measure_sum).sum(),
        Value::Array(items) => items.iter().map(numeric_measure_sum).sum(),
        _ => 0.0,
    }
}

fn upsert_metric_definition(
    connection: &Connection,
    definition: &IaccMetricDefinition,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT INTO iacc_metric_definition (
            metric_id, definition_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(metric_id) DO UPDATE SET
            definition_json = excluded.definition_json,
            updated_at = excluded.updated_at",
        params![
            definition.metric_id,
            serde_json::to_string(definition)?,
            definition.created_at.to_rfc3339(),
            definition.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn latest_metric_state(
    connection: &Connection,
    metric_id: &str,
    entity_scope: &str,
    period: &str,
) -> Result<Option<IaccMetricState>, IaccStoreError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM iacc_metric_state
              WHERE metric_id = ?1 AND entity_scope = ?2 AND period = ?3
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id, entity_scope, period],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn insert_metric_state(
    connection: &Connection,
    state: &IaccMetricState,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT INTO iacc_metric_state (
            state_id, metric_id, entity_scope, period, value, previous_value,
            delta, status, state_json, computed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            state.state_id,
            state.metric_id,
            state.entity_scope,
            state.period,
            state.value,
            state.previous_value,
            state.delta,
            format!("{:?}", state.status).to_ascii_lowercase(),
            serde_json::to_string(state)?,
            state.computed_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn insert_change_event(
    connection: &Connection,
    change: &IaccChangeEvent,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT INTO iacc_change_event (
            change_id, metric_id, entity_ref, period, delta, severity_hint,
            change_json, detected_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            change.change_id,
            change.metric_id,
            change.entity_ref,
            change.period,
            change.delta,
            change.severity_hint,
            serde_json::to_string(change)?,
            change.detected_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_change(
    connection: &Connection,
    change_id: &str,
) -> Result<Option<IaccChangeEvent>, IaccStoreError> {
    connection
        .query_row(
            "SELECT change_json FROM iacc_change_event WHERE change_id = ?1",
            params![change_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn latest_metric_state_for_metric(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<IaccMetricState>, IaccStoreError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM iacc_metric_state
              WHERE metric_id = ?1
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn upsert_incident(connection: &Connection, incident: &IaccIncident) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT OR REPLACE INTO iacc_incident (
            incident_id, attention_id, evidence_packet_id, task_id, agent_graph_id,
            status, incident_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            incident.incident_id,
            incident.attention_id,
            incident.evidence_packet_id,
            incident.task_id,
            incident.agent_graph_id,
            incident.status,
            serde_json::to_string(incident)?,
            incident.created_at.to_rfc3339(),
            incident.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_incident(
    connection: &Connection,
    incident_id: &str,
) -> Result<Option<IaccIncident>, IaccStoreError> {
    connection
        .query_row(
            "SELECT incident_json FROM iacc_incident WHERE incident_id = ?1",
            params![incident_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn insert_analysis(
    connection: &Connection,
    analysis: &IaccOperationalAnalysis,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT OR REPLACE INTO iacc_operational_analysis (
            analysis_id, incident_id, evidence_packet_id, status, confidence,
            analysis_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            analysis.analysis_id,
            analysis.incident_id,
            analysis.evidence_packet_id,
            analysis.status,
            analysis.confidence,
            serde_json::to_string(analysis)?,
            analysis.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_analysis(
    connection: &Connection,
    analysis_id: &str,
) -> Result<Option<IaccOperationalAnalysis>, IaccStoreError> {
    connection
        .query_row(
            "SELECT analysis_json FROM iacc_operational_analysis WHERE analysis_id = ?1",
            params![analysis_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn insert_execution(
    connection: &Connection,
    execution: &IaccActionExecution,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT OR REPLACE INTO iacc_action_execution (
            execution_id, analysis_id, incident_id, action_id, status, mode,
            execution_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            execution.execution_id,
            execution.analysis_id,
            execution.incident_id,
            execution.action_id,
            execution.status,
            execution.mode,
            serde_json::to_string(execution)?,
            execution.created_at.to_rfc3339(),
            execution.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_execution(
    connection: &Connection,
    execution_id: &str,
) -> Result<Option<IaccActionExecution>, IaccStoreError> {
    connection
        .query_row(
            "SELECT execution_json FROM iacc_action_execution WHERE execution_id = ?1",
            params![execution_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn attention_from_change(change: &IaccChangeEvent, state: &IaccMetricState) -> IaccAttentionItem {
    let now = Utc::now();
    let severity = match change.severity_hint.as_str() {
        "critical" => IaccSeverity::Critical,
        "warning" => IaccSeverity::Warning,
        "normal" => IaccSeverity::Normal,
        _ => IaccSeverity::Unknown,
    };
    let severity_score = match severity {
        IaccSeverity::Critical => 1.0,
        IaccSeverity::Warning => 0.65,
        IaccSeverity::Normal => 0.2,
        IaccSeverity::Unknown => 0.35,
    };
    let urgency = if change.delta.abs() > 0.0 { 0.7 } else { 0.2 };
    let impact_scope = (change.delta.abs() / 100.0).min(1.0) as f32;
    let strategic_weight = 0.5_f32;
    let confidence = state.confidence;
    let priority_score = severity_score * 0.30
        + urgency * 0.20
        + impact_scope * 0.20
        + strategic_weight * 0.15
        + confidence * 0.10
        + 0.05;
    IaccAttentionItem {
        attention_id: format!("attention-{}", uuid::Uuid::new_v4()),
        title: format!(
            "Metric {} changed by {} for {}",
            state.metric_id, change.delta, state.entity_scope
        ),
        business_domain: state
            .metric_id
            .split('_')
            .next()
            .unwrap_or("operations")
            .to_string(),
        entity_ref: Some(state.entity_scope.clone()),
        period: Some(state.period.clone()),
        priority_score,
        severity,
        urgency,
        strategic_weight,
        confidence,
        reason_codes: vec![
            "metric_recomputed".to_string(),
            "metric_delta_detected".to_string(),
        ],
        linked_changes: vec![format!("iacc:change:{}", change.change_id)],
        linked_anomalies: Vec::new(),
        linked_impacts: Vec::new(),
        owner_roles: vec!["operations_analyst".to_string()],
        status: "open".to_string(),
        created_at: now,
        updated_at: now,
    }
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

    #[test]
    fn iacc_store_recomputes_metrics_and_emits_changes() {
        let store = IaccStore::in_memory().expect("store opens");
        let first = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-plan-1".to_string()),
            snapshot_id: Some("snapshot-plan-a".to_string()),
            fact_type: "plan.weekly_demand".to_string(),
            entity_refs: vec!["product:server-a".to_string()],
            metric_key: Some("plan_bom_delta".to_string()),
            dimensions: serde_json::json!({"week": "2026-W24"}),
            measures: serde_json::json!({"demand_qty": 100}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.8),
            raw_hash: None,
        });
        store.ingest_fact(&first).expect("first fact ingests");

        let initial = store.recompute_metrics().expect("initial recompute");
        assert_eq!(initial.metric_state_count, 1);
        assert_eq!(initial.change_count, 1);
        assert_eq!(initial.metric_states[0].value, 100.0);
        assert_eq!(initial.metric_states[0].previous_value, None);

        let second = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-plan-2".to_string()),
            snapshot_id: Some("snapshot-plan-b".to_string()),
            fact_type: "plan.weekly_demand".to_string(),
            entity_refs: vec!["product:server-a".to_string()],
            metric_key: Some("plan_bom_delta".to_string()),
            dimensions: serde_json::json!({"week": "2026-W24"}),
            measures: serde_json::json!({"demand_qty": 130}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.9),
            raw_hash: None,
        });
        store.ingest_fact(&second).expect("second fact ingests");

        let next = store.recompute_metrics().expect("second recompute");
        assert_eq!(next.metric_state_count, 1);
        assert_eq!(next.change_count, 1);
        assert_eq!(next.metric_states[0].value, 230.0);
        assert_eq!(next.metric_states[0].previous_value, Some(100.0));
        assert_eq!(next.metric_states[0].delta, 130.0);
        assert_eq!(next.changes[0].severity_hint, "critical");
        assert!(!next.attention.is_empty());

        let metrics = store.list_metric_definitions().expect("metrics list");
        assert_eq!(metrics[0].metric_id, "plan_bom_delta");
        let states = store.metric_states("plan_bom_delta").expect("states list");
        assert_eq!(states.len(), 2);
        let changes = store.list_changes(10).expect("changes list");
        assert_eq!(changes.len(), 2);
    }

    #[test]
    fn evidence_packet_includes_metric_change_and_context_item() {
        let store = IaccStore::in_memory().expect("store opens");
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-plan-context".to_string()),
            snapshot_id: Some("snapshot-plan-context".to_string()),
            fact_type: "plan.weekly_demand".to_string(),
            entity_refs: vec!["product:server-context".to_string()],
            metric_key: Some("plan_bom_delta".to_string()),
            dimensions: serde_json::json!({"week": "2026-W25"}),
            measures: serde_json::json!({"demand_qty": 160}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.9),
            raw_hash: None,
        });
        store.ingest_fact(&fact).expect("fact ingests");
        let recompute = store.recompute_metrics().expect("recompute");
        let attention_id = recompute.attention[0].attention_id.clone();

        let packet = store
            .build_evidence_packet(Some(&attention_id), Some("plan changed"))
            .expect("packet builds");

        assert!(!packet.metric_evidence.is_empty());
        assert!(!packet.change_evidence.is_empty());
        let context_item = packet.to_context_item();
        assert_eq!(
            context_item.id,
            format!("iacc:evidence:{}", packet.packet_id)
        );
        assert!(!context_item.evidence.is_empty());
    }

    #[test]
    fn store_persists_incident() {
        let store = IaccStore::in_memory().expect("store opens");
        let mut incident = IaccIncident::new("material risk");
        incident.attention_id = Some("attention-1".to_string());
        incident.evidence_packet_id = Some("packet-1".to_string());
        incident.task_id = Some("task-1".to_string());
        incident.agent_graph_id = Some("agent-graph-task-1".to_string());
        store.create_incident(&incident).expect("incident saves");

        let loaded = store
            .get_incident(&incident.incident_id)
            .expect("incident loads")
            .expect("incident exists");
        assert_eq!(loaded.title, "material risk");
        assert_eq!(store.health().unwrap().incident_count, 1);
    }

    #[test]
    fn analyze_incident_projects_attribution_impact_and_actions() {
        let store = IaccStore::in_memory().expect("store opens");
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-analysis-shortage".to_string()),
            snapshot_id: Some("snapshot-analysis-shortage".to_string()),
            fact_type: "supply.material_shortage".to_string(),
            entity_refs: vec!["component:gpu-analysis".to_string()],
            metric_key: Some("material_shortage_risk".to_string()),
            dimensions: serde_json::json!({"week": "2026-W27"}),
            measures: serde_json::json!({"short_qty": 240}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.91),
            raw_hash: None,
        });
        store.ingest_fact(&fact).expect("fact ingests");
        let recompute = store.recompute_metrics().expect("recompute");
        let packet = store
            .build_evidence_packet(
                Some(&recompute.attention[0].attention_id),
                Some("GPU shortage threatens build plan"),
            )
            .expect("packet builds");
        let mut incident = IaccIncident::new("GPU shortage");
        incident.attention_id = packet.attention_id.clone();
        incident.evidence_packet_id = Some(packet.packet_id.clone());
        store.create_incident(&incident).expect("incident saves");

        let analysis = store
            .analyze_incident(&incident.incident_id)
            .expect("incident analyzes");

        assert_eq!(analysis.incident_id, incident.incident_id);
        assert_eq!(analysis.evidence_packet_id, packet.packet_id);
        assert_eq!(
            analysis.attribution_candidates[0].cause_type,
            "supply_constraint"
        );
        assert_eq!(
            analysis.impact_paths[0].impact_type,
            "material_availability_risk"
        );
        assert_eq!(
            analysis.recommended_actions[0].action_type,
            "supplier_recovery"
        );
        let updated_packet = store
            .get_evidence_packet(&packet.packet_id)
            .expect("packet loads")
            .expect("packet exists");
        assert!(!updated_packet.attribution_candidates.is_empty());
        assert!(!updated_packet.impact_paths.is_empty());
        assert!(updated_packet.missing_evidence.is_empty());
        let updated_incident = store
            .get_incident(&incident.incident_id)
            .expect("incident loads")
            .expect("incident exists");
        assert_eq!(updated_incident.status, "analyzed");
        assert_eq!(store.health().unwrap().analysis_count, 1);
    }

    #[test]
    fn execute_action_and_feedback_closes_incident() {
        let store = IaccStore::in_memory().expect("store opens");
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-execution-shortage".to_string()),
            snapshot_id: Some("snapshot-execution-shortage".to_string()),
            fact_type: "supply.material_shortage".to_string(),
            entity_refs: vec!["component:gpu-execution".to_string()],
            metric_key: Some("material_shortage_risk".to_string()),
            dimensions: serde_json::json!({"week": "2026-W29"}),
            measures: serde_json::json!({"short_qty": 260}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.93),
            raw_hash: None,
        });
        store.ingest_fact(&fact).expect("fact ingests");
        let recompute = store.recompute_metrics().expect("recompute");
        let packet = store
            .build_evidence_packet(
                Some(&recompute.attention[0].attention_id),
                Some("GPU shortage execution incident"),
            )
            .expect("packet builds");
        let mut incident = IaccIncident::new("GPU shortage execution");
        incident.attention_id = packet.attention_id.clone();
        incident.evidence_packet_id = Some(packet.packet_id.clone());
        store.create_incident(&incident).expect("incident saves");
        let analysis = store
            .analyze_incident(&incident.incident_id)
            .expect("analysis");
        let action_id = analysis.recommended_actions[0].action_id.clone();

        let execution = store
            .execute_recommended_action(
                &analysis.analysis_id,
                &action_id,
                &IaccActionExecutionRequest {
                    mode: "commit".to_string(),
                    operator_id: Some("user:planner".to_string()),
                    note: Some("review and queue recovery".to_string()),
                },
            )
            .expect("execution saves");

        assert_eq!(execution.mode, "commit");
        assert_eq!(execution.status, "queued_for_human_review");
        assert_eq!(execution.action_type, "supplier_recovery");
        assert_eq!(store.health().unwrap().execution_count, 1);

        let execution = store
            .record_execution_feedback(
                &execution.execution_id,
                IaccActionFeedback::new("resolved", "supplier commit secured", Some(-260.0)),
            )
            .expect("feedback saves");
        assert_eq!(execution.status, "feedback_resolved");
        assert_eq!(execution.feedback.as_ref().unwrap().outcome, "resolved");
        assert_eq!(
            store
                .get_execution(&execution.execution_id)
                .unwrap()
                .unwrap()
                .receipt["feedback"]["note"],
            "supplier commit secured"
        );
        let incident = store
            .get_incident(&incident.incident_id)
            .unwrap()
            .expect("incident exists");
        assert_eq!(incident.status, "closed");
    }
}
