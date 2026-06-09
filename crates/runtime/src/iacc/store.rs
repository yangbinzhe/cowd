use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::{
    IaccActionExecution, IaccActionExecutionRequest, IaccActionFeedback, IaccAttentionItem,
    IaccChangeEvent, IaccCockpitProfile, IaccCockpitProjection, IaccCockpitReportDeliveryReceipt,
    IaccCockpitReportRequest, IaccCockpitReportSnapshot, IaccCockpitWidget, IaccComputeJob,
    IaccComputeJobInput, IaccComputePlan, IaccCrossPlaneBridgeReceipt, IaccDomainSeedResult,
    IaccEntity, IaccEvidencePacket, IaccEvidenceSourceRef, IaccFact, IaccImpactHop,
    IaccImpactTrace, IaccIncident, IaccMetricDefinition, IaccMetricDependency, IaccMetricLineage,
    IaccMetricState, IaccOperationalAnalysis, IaccQualityGateDecision, IaccRelation, IaccSeverity,
};

pub const IACC_SCHEMA_VERSION: i64 = 11;

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
    pub entity_count: u64,
    pub relation_count: u64,
    pub metric_dependency_count: u64,
    pub compute_job_count: u64,
    pub quality_gate_count: u64,
    pub cockpit_profile_count: u64,
    pub cockpit_report_count: u64,
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
            entity_count: count_table(&connection, "iacc_entity")?,
            relation_count: count_table(&connection, "iacc_relation")?,
            metric_dependency_count: count_table(&connection, "iacc_metric_dependency")?,
            compute_job_count: count_table(&connection, "iacc_compute_job")?,
            quality_gate_count: count_table(&connection, "iacc_quality_gate")?,
            cockpit_profile_count: count_table(&connection, "iacc_cockpit_profile")?,
            cockpit_report_count: count_table(&connection, "iacc_cockpit_report")?,
        })
    }

    pub fn upsert_cockpit_profile(
        &self,
        profile: &IaccCockpitProfile,
    ) -> Result<IaccCockpitProfile, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_cockpit_profile(&connection, profile)
    }

    pub fn get_cockpit_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<IaccCockpitProfile>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_cockpit_profile(&connection, profile_id)
    }

    pub fn list_cockpit_profiles(
        &self,
        cadence: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IaccCockpitProfile>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_cockpit_profiles(&connection, cadence, limit)
    }

    pub fn cockpit_projection(
        &self,
        profile_id: &str,
    ) -> Result<IaccCockpitProjection, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| IaccStoreError::NotFound(profile_id.to_string()))?;
        build_cockpit_projection(&connection, profile)
    }

    pub fn generate_cockpit_report(
        &self,
        profile_id: &str,
        request: IaccCockpitReportRequest,
    ) -> Result<IaccCockpitReportSnapshot, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| IaccStoreError::NotFound(profile_id.to_string()))?;
        let projection = build_cockpit_projection(&connection, profile)?;
        let report = IaccCockpitReportSnapshot::from_projection(projection, request);
        insert_cockpit_report(&connection, &report)?;
        Ok(report)
    }

    pub fn get_cockpit_report(
        &self,
        report_id: &str,
    ) -> Result<Option<IaccCockpitReportSnapshot>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_cockpit_report(&connection, report_id)
    }

    pub fn attach_cockpit_report_delivery(
        &self,
        report_id: &str,
        receipt: IaccCockpitReportDeliveryReceipt,
    ) -> Result<IaccCockpitReportSnapshot, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut report = find_cockpit_report(&connection, report_id)?
            .ok_or_else(|| IaccStoreError::NotFound(report_id.to_string()))?;
        report.attach_delivery_receipt(receipt);
        insert_cockpit_report(&connection, &report)?;
        Ok(report)
    }

    pub fn upsert_entity(&self, entity: &IaccEntity) -> Result<IaccEntity, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_entity(&connection, entity)
    }

    pub fn get_entity(&self, entity_id: &str) -> Result<Option<IaccEntity>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_entity(&connection, entity_id)
    }

    pub fn resolve_entity_by_source_key(
        &self,
        source_system: &str,
        source_key: &str,
    ) -> Result<Option<IaccEntity>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_entity_by_source_key(&connection, source_system, source_key)
    }

    pub fn list_entities(&self, limit: usize) -> Result<Vec<IaccEntity>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_entities(&connection, limit)
    }

    pub fn upsert_relation(&self, relation: &IaccRelation) -> Result<IaccRelation, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_relation(&connection, relation)
    }

    pub fn list_entity_relations(
        &self,
        entity_id: &str,
        limit: usize,
    ) -> Result<Vec<IaccRelation>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(IaccStoreError::NotFound(entity_id.to_string()));
        }
        list_entity_relations(&connection, entity_id, limit)
    }

    pub fn impact_trace(
        &self,
        entity_id: &str,
        max_depth: usize,
    ) -> Result<IaccImpactTrace, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(IaccStoreError::NotFound(entity_id.to_string()));
        }
        build_impact_trace(&connection, entity_id, max_depth)
    }

    pub fn register_metric_definition(
        &self,
        definition: &IaccMetricDefinition,
    ) -> Result<(), IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_metric_definition(&connection, definition)
    }

    pub fn upsert_metric_dependency(
        &self,
        dependency: &IaccMetricDependency,
    ) -> Result<IaccMetricDependency, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_metric_dependency(&connection, dependency)
    }

    pub fn metric_lineage(
        &self,
        metric_id: &str,
        max_depth: usize,
    ) -> Result<IaccMetricLineage, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        build_metric_lineage(&connection, metric_id, max_depth)
    }

    pub fn metrics_affected_by_fact_type(
        &self,
        fact_type: &str,
    ) -> Result<Vec<String>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics_affected_by_fact_type(&connection, fact_type)
    }

    pub fn plan_compute_job_for_fact_type(
        &self,
        input: IaccComputeJobInput,
    ) -> Result<IaccComputePlan, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut affected_metric_ids = if input.metric_ids.is_empty() {
            metrics_affected_by_fact_type(&connection, &input.trigger_fact_type)?
        } else {
            input.metric_ids.clone()
        };
        if affected_metric_ids.is_empty() {
            affected_metric_ids = metric_ids_for_fact_type(&connection, &input.trigger_fact_type)?;
        }
        affected_metric_ids.sort();
        affected_metric_ids.dedup();
        let mut job = IaccComputeJob::from_input(IaccComputeJobInput {
            metric_ids: affected_metric_ids.clone(),
            ..input
        });
        job.priority = priority_for_compute_job(&job);
        upsert_compute_job(&connection, &job)?;
        Ok(IaccComputePlan {
            job,
            affected_metric_ids,
            planned_at: Utc::now(),
        })
    }

    pub fn get_compute_job(&self, job_id: &str) -> Result<Option<IaccComputeJob>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_compute_job(&connection, job_id)
    }

    pub fn run_compute_job(&self, job_id: &str) -> Result<IaccComputeJob, IaccStoreError> {
        let mut job = {
            let connection = self
                .connection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut job = find_compute_job(&connection, job_id)?
                .ok_or_else(|| IaccStoreError::NotFound(job_id.to_string()))?;
            job.status = "running".to_string();
            job.attempts += 1;
            job.updated_at = Utc::now();
            upsert_compute_job(&connection, &job)?;
            job
        };

        let recompute = self.recompute_metrics_for_metric_ids(&job.metric_ids)?;
        job.status = "completed".to_string();
        job.result_summary = serde_json::json!({
            "metric_ids": job.metric_ids.clone(),
            "metric_state_count": recompute.metric_state_count,
            "change_count": recompute.change_count,
            "attention_count": recompute.attention_count,
        });
        job.updated_at = Utc::now();
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_compute_job(&connection, &job)
    }

    pub fn seed_server_manufacturing_domain(&self) -> Result<IaccDomainSeedResult, IaccStoreError> {
        let plan = super::server_manufacturing_seed_plan();
        for entity in &plan.entities {
            self.upsert_entity(entity)?;
        }
        for relation in &plan.relations {
            self.upsert_relation(relation)?;
        }
        for definition in &plan.metric_definitions {
            self.register_metric_definition(definition)?;
        }
        for dependency in &plan.metric_dependencies {
            self.upsert_metric_dependency(dependency)?;
        }
        for fact in &plan.facts {
            self.ingest_fact(fact)?;
        }
        Ok(IaccDomainSeedResult {
            domain_id: plan.pack.domain_id,
            version: plan.pack.version,
            entity_count: plan.entities.len(),
            relation_count: plan.relations.len(),
            metric_definition_count: plan.metric_definitions.len(),
            metric_dependency_count: plan.metric_dependencies.len(),
            fact_count: plan.facts.len(),
            scenario_count: plan.pack.scenarios.len(),
            seeded_at: Utc::now(),
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
        self.recompute_metrics_with_filter(None)
    }

    pub fn recompute_metrics_for_metric_ids(
        &self,
        metric_ids: &[String],
    ) -> Result<IaccMetricRecomputeResult, IaccStoreError> {
        let filter = metric_ids.iter().cloned().collect::<BTreeSet<_>>();
        self.recompute_metrics_with_filter(Some(&filter))
    }

    fn recompute_metrics_with_filter(
        &self,
        metric_filter: Option<&BTreeSet<String>>,
    ) -> Result<IaccMetricRecomputeResult, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let facts = metric_facts(&connection)?;
        let mut groups = BTreeMap::<MetricGroupKey, MetricAccumulator>::new();
        for fact in facts {
            if metric_filter.is_some_and(|filter| !filter.contains(&fact.metric_id)) {
                continue;
            }
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

    pub fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> Result<IaccQualityGateDecision, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let packet = find_evidence_packet(&connection, packet_id)?
            .ok_or_else(|| IaccStoreError::NotFound(packet_id.to_string()))?;
        let decision = IaccQualityGateDecision::for_evidence_packet(&packet);
        insert_quality_gate(&connection, &decision)?;
        Ok(decision)
    }

    pub fn get_quality_gate(
        &self,
        gate_id: &str,
    ) -> Result<Option<IaccQualityGateDecision>, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_quality_gate(&connection, gate_id)
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

    pub fn attach_cross_plane_receipt(
        &self,
        execution_id: &str,
        receipt: IaccCrossPlaneBridgeReceipt,
    ) -> Result<IaccActionExecution, IaccStoreError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut execution = find_execution(&connection, execution_id)?
            .ok_or_else(|| IaccStoreError::NotFound(execution_id.to_string()))?;
        execution.attach_cross_plane_receipt(receipt);
        insert_execution(&connection, &execution)?;
        Ok(execution)
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
        VALUES (1, 11, datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
            schema_version = CASE
                WHEN iacc_schema.schema_version < excluded.schema_version
                THEN excluded.schema_version
                ELSE iacc_schema.schema_version
            END,
            updated_at = excluded.updated_at;

        CREATE TABLE IF NOT EXISTS iacc_cockpit_profile (
            profile_id TEXT PRIMARY KEY,
            owner_ref TEXT NOT NULL,
            profile_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_cockpit_profile_owner
            ON iacc_cockpit_profile(owner_ref, updated_at DESC);

        CREATE TABLE IF NOT EXISTS iacc_cockpit_report (
            report_id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            owner_ref TEXT NOT NULL,
            status TEXT NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_cockpit_report_profile
            ON iacc_cockpit_report(profile_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS iacc_entity (
            entity_id TEXT PRIMARY KEY,
            entity_type TEXT NOT NULL,
            canonical_key TEXT NOT NULL,
            display_name TEXT NOT NULL,
            source_keys_json TEXT NOT NULL,
            attributes_json TEXT NOT NULL,
            confidence REAL NOT NULL,
            entity_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(entity_type, canonical_key)
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_entity_type
            ON iacc_entity(entity_type, canonical_key);

        CREATE TABLE IF NOT EXISTS iacc_entity_source_key (
            source_system TEXT NOT NULL,
            source_key TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            source_ref TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY(source_system, source_key),
            FOREIGN KEY(entity_id) REFERENCES iacc_entity(entity_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_entity_source_entity
            ON iacc_entity_source_key(entity_id);

        CREATE TABLE IF NOT EXISTS iacc_relation (
            relation_id TEXT PRIMARY KEY,
            relation_type TEXT NOT NULL,
            from_entity_id TEXT NOT NULL,
            to_entity_id TEXT NOT NULL,
            attributes_json TEXT NOT NULL,
            confidence REAL NOT NULL,
            relation_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(relation_type, from_entity_id, to_entity_id),
            FOREIGN KEY(from_entity_id) REFERENCES iacc_entity(entity_id) ON DELETE CASCADE,
            FOREIGN KEY(to_entity_id) REFERENCES iacc_entity(entity_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_relation_from
            ON iacc_relation(from_entity_id, relation_type);
        CREATE INDEX IF NOT EXISTS idx_iacc_relation_to
            ON iacc_relation(to_entity_id, relation_type);

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

        CREATE TABLE IF NOT EXISTS iacc_quality_gate (
            gate_id TEXT PRIMARY KEY,
            target_ref TEXT NOT NULL,
            gate_type TEXT NOT NULL,
            decision TEXT NOT NULL,
            score REAL NOT NULL,
            gate_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_quality_gate_target
            ON iacc_quality_gate(target_ref, created_at DESC);

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

        CREATE TABLE IF NOT EXISTS iacc_metric_dependency (
            dependency_id TEXT PRIMARY KEY,
            upstream_metric_id TEXT NOT NULL,
            downstream_metric_id TEXT NOT NULL,
            dependency_type TEXT NOT NULL,
            confidence REAL NOT NULL,
            dependency_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(upstream_metric_id, downstream_metric_id, dependency_type)
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_metric_dependency_upstream
            ON iacc_metric_dependency(upstream_metric_id, downstream_metric_id);
        CREATE INDEX IF NOT EXISTS idx_iacc_metric_dependency_downstream
            ON iacc_metric_dependency(downstream_metric_id, upstream_metric_id);

        CREATE TABLE IF NOT EXISTS iacc_compute_job (
            job_id TEXT PRIMARY KEY,
            trigger_fact_type TEXT NOT NULL,
            status TEXT NOT NULL,
            priority REAL NOT NULL,
            job_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_iacc_compute_job_status
            ON iacc_compute_job(status, priority DESC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_iacc_compute_job_fact_type
            ON iacc_compute_job(trigger_fact_type, updated_at DESC);

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

fn upsert_cockpit_profile(
    connection: &Connection,
    profile: &IaccCockpitProfile,
) -> Result<IaccCockpitProfile, IaccStoreError> {
    let mut profile = profile.clone();
    if let Some(existing) = find_cockpit_profile(connection, &profile.profile_id)? {
        profile.created_at = existing.created_at;
    }
    profile.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO iacc_cockpit_profile (
            profile_id, owner_ref, profile_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(profile_id) DO UPDATE SET
            owner_ref = excluded.owner_ref,
            profile_json = excluded.profile_json,
            updated_at = excluded.updated_at",
        params![
            profile.profile_id,
            profile.owner_ref,
            serde_json::to_string(&profile)?,
            profile.created_at.to_rfc3339(),
            profile.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(profile)
}

fn find_cockpit_profile(
    connection: &Connection,
    profile_id: &str,
) -> Result<Option<IaccCockpitProfile>, IaccStoreError> {
    connection
        .query_row(
            "SELECT profile_json FROM iacc_cockpit_profile WHERE profile_id = ?1",
            params![profile_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn list_cockpit_profiles(
    connection: &Connection,
    cadence: Option<&str>,
    limit: usize,
) -> Result<Vec<IaccCockpitProfile>, IaccStoreError> {
    let cadence = cadence
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let mut statement = connection.prepare(
        "SELECT profile_json FROM iacc_cockpit_profile ORDER BY updated_at DESC, profile_id ASC",
    )?;
    let profiles = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|row| {
            let json = row?;
            serde_json::from_str::<IaccCockpitProfile>(&json).map_err(IaccStoreError::from)
        })
        .filter_map(|result| match result {
            Ok(profile)
                if cadence
                    .as_ref()
                    .is_none_or(|cadence| profile.cadence == *cadence) =>
            {
                Some(Ok(profile))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .take(limit.max(1))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(profiles)
}

fn insert_cockpit_report(
    connection: &Connection,
    report: &IaccCockpitReportSnapshot,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT OR REPLACE INTO iacc_cockpit_report (
            report_id, profile_id, owner_ref, status, report_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            report.report_id,
            report.profile_id,
            report.owner_ref,
            report.status,
            serde_json::to_string(report)?,
            report.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_cockpit_report(
    connection: &Connection,
    report_id: &str,
) -> Result<Option<IaccCockpitReportSnapshot>, IaccStoreError> {
    connection
        .query_row(
            "SELECT report_json FROM iacc_cockpit_report WHERE report_id = ?1",
            params![report_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn build_cockpit_projection(
    connection: &Connection,
    profile: IaccCockpitProfile,
) -> Result<IaccCockpitProjection, IaccStoreError> {
    let attention = list_attention(connection, 50)?
        .into_iter()
        .filter(|item| attention_matches_profile(item, &profile))
        .take(8)
        .collect::<Vec<_>>();
    let quality_gates = list_recent_quality_gates(connection, 20)?;
    let executions = list_recent_executions(connection, 20)?;
    let mut widgets = Vec::new();

    let attention_status = if attention
        .iter()
        .any(|item| matches!(item.severity, IaccSeverity::Critical))
    {
        "critical"
    } else if attention.is_empty() {
        "clear"
    } else {
        "watch"
    };
    let attention_sources = attention
        .iter()
        .map(|item| format!("iacc:attention:{}", item.attention_id))
        .collect::<Vec<_>>();
    widgets.push(IaccCockpitWidget::new(
        "attention_queue",
        "Focused operational attention",
        attention_status,
        attention
            .iter()
            .map(|item| item.priority_score)
            .fold(0.0_f32, f32::max),
        serde_json::json!({
            "count": attention.len(),
            "items": attention,
        }),
        attention_sources,
    ));

    let pass_count = quality_gates
        .iter()
        .filter(|gate| gate.decision == "pass")
        .count();
    let review_count = quality_gates
        .iter()
        .filter(|gate| gate.decision == "review")
        .count();
    let fail_count = quality_gates
        .iter()
        .filter(|gate| gate.decision == "fail")
        .count();
    let gate_status = if fail_count > 0 {
        "fail"
    } else if review_count > 0 {
        "review"
    } else if pass_count > 0 {
        "pass"
    } else {
        "empty"
    };
    widgets.push(IaccCockpitWidget::new(
        "quality_gate_status",
        "Evidence and insight quality",
        gate_status,
        (fail_count as f32 * 1.0 + review_count as f32 * 0.65 + pass_count as f32 * 0.25).min(1.0),
        serde_json::json!({
            "pass_count": pass_count,
            "review_count": review_count,
            "fail_count": fail_count,
            "recent": quality_gates,
        }),
        Vec::new(),
    ));

    let active_executions = executions
        .iter()
        .filter(|execution| {
            !matches!(
                execution.status.as_str(),
                "feedback_resolved" | "feedback_rejected"
            )
        })
        .count();
    widgets.push(IaccCockpitWidget::new(
        "action_execution_status",
        "Governed action execution",
        if active_executions > 0 {
            "active"
        } else {
            "clear"
        },
        (active_executions as f32 / 5.0).min(1.0),
        serde_json::json!({
            "active_count": active_executions,
            "recent": executions,
        }),
        Vec::new(),
    ));

    widgets.push(IaccCockpitWidget::new(
        "focus_thresholds",
        "Personal focus and thresholds",
        if profile.thresholds.is_null() {
            "empty"
        } else {
            "configured"
        },
        0.2,
        serde_json::json!({
            "focus_refs": profile.focus_refs,
            "focus_metric_ids": profile.focus_metric_ids,
            "thresholds": profile.thresholds,
            "cadence": profile.cadence,
        }),
        Vec::new(),
    ));

    let summary = format!(
        "profile={} attention={} quality_gates={} active_executions={}",
        profile.profile_id,
        widgets
            .iter()
            .find(|widget| widget.widget_type == "attention_queue")
            .and_then(|widget| widget.data.get("count"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        pass_count + review_count + fail_count,
        active_executions
    );
    Ok(IaccCockpitProjection {
        projection_id: format!("cockpit-projection-{}", uuid::Uuid::new_v4()),
        profile,
        widgets,
        summary,
        generated_at: Utc::now(),
    })
}

fn attention_matches_profile(item: &IaccAttentionItem, profile: &IaccCockpitProfile) -> bool {
    if profile.focus_refs.is_empty() && profile.focus_metric_ids.is_empty() {
        return true;
    }
    if item.entity_ref.as_ref().is_some_and(|entity_ref| {
        profile
            .focus_refs
            .iter()
            .any(|focus_ref| focus_ref == entity_ref)
    }) {
        return true;
    }
    profile
        .focus_metric_ids
        .iter()
        .any(|metric_id| item.title.contains(metric_id))
}

fn upsert_entity(
    connection: &Connection,
    entity: &IaccEntity,
) -> Result<IaccEntity, IaccStoreError> {
    let mut entity = entity.clone();
    if let Some(existing) =
        find_entity_by_canonical(connection, &entity.entity_type, &entity.canonical_key)?
    {
        entity.entity_id = existing.entity_id;
        entity.created_at = existing.created_at;
        entity.source_keys = merged_source_keys(&existing.source_keys, &entity.source_keys);
    }
    entity.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO iacc_entity (
            entity_id, entity_type, canonical_key, display_name, source_keys_json,
            attributes_json, confidence, entity_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(entity_id) DO UPDATE SET
            entity_type = excluded.entity_type,
            canonical_key = excluded.canonical_key,
            display_name = excluded.display_name,
            source_keys_json = excluded.source_keys_json,
            attributes_json = excluded.attributes_json,
            confidence = excluded.confidence,
            entity_json = excluded.entity_json,
            updated_at = excluded.updated_at",
        params![
            entity.entity_id,
            entity.entity_type,
            entity.canonical_key,
            entity.display_name,
            serde_json::to_string(&entity.source_keys)?,
            serde_json::to_string(&entity.attributes)?,
            entity.confidence,
            serde_json::to_string(&entity)?,
            entity.created_at.to_rfc3339(),
            entity.updated_at.to_rfc3339(),
        ],
    )?;
    connection.execute(
        "DELETE FROM iacc_entity_source_key WHERE entity_id = ?1",
        params![entity.entity_id],
    )?;
    for source_key in &entity.source_keys {
        connection.execute(
            r"INSERT INTO iacc_entity_source_key (
                source_system, source_key, entity_id, source_ref, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(source_system, source_key) DO UPDATE SET
                entity_id = excluded.entity_id,
                source_ref = excluded.source_ref",
            params![
                source_key.normalized_system(),
                source_key.normalized_key(),
                entity.entity_id,
                source_key.source_ref,
                Utc::now().to_rfc3339(),
            ],
        )?;
    }
    Ok(entity)
}

fn merged_source_keys(
    existing: &[super::IaccSourceKey],
    incoming: &[super::IaccSourceKey],
) -> Vec<super::IaccSourceKey> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::new();
    for source_key in existing.iter().chain(incoming.iter()) {
        let key = (source_key.normalized_system(), source_key.normalized_key());
        if seen.insert(key) {
            keys.push(source_key.clone());
        }
    }
    keys
}

fn find_entity(
    connection: &Connection,
    entity_id: &str,
) -> Result<Option<IaccEntity>, IaccStoreError> {
    connection
        .query_row(
            "SELECT entity_json FROM iacc_entity WHERE entity_id = ?1",
            params![entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn find_entity_by_canonical(
    connection: &Connection,
    entity_type: &str,
    canonical_key: &str,
) -> Result<Option<IaccEntity>, IaccStoreError> {
    connection
        .query_row(
            r"SELECT entity_json
              FROM iacc_entity
              WHERE entity_type = ?1 AND canonical_key = ?2",
            params![entity_type, canonical_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn find_entity_by_source_key(
    connection: &Connection,
    source_system: &str,
    source_key: &str,
) -> Result<Option<IaccEntity>, IaccStoreError> {
    connection
        .query_row(
            r"SELECT e.entity_json
              FROM iacc_entity_source_key s
              JOIN iacc_entity e ON e.entity_id = s.entity_id
              WHERE s.source_system = ?1 AND s.source_key = ?2",
            params![
                super::entity::normalize_key(source_system),
                super::entity::normalize_key(source_key),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn list_entities(connection: &Connection, limit: usize) -> Result<Vec<IaccEntity>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT entity_json
          FROM iacc_entity
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn upsert_relation(
    connection: &Connection,
    relation: &IaccRelation,
) -> Result<IaccRelation, IaccStoreError> {
    if find_entity(connection, &relation.from_entity_id)?.is_none() {
        return Err(IaccStoreError::NotFound(relation.from_entity_id.clone()));
    }
    if find_entity(connection, &relation.to_entity_id)?.is_none() {
        return Err(IaccStoreError::NotFound(relation.to_entity_id.clone()));
    }

    let mut relation = relation.clone();
    if let Some(existing) = find_relation_by_key(
        connection,
        &relation.relation_type,
        &relation.from_entity_id,
        &relation.to_entity_id,
    )? {
        relation.relation_id = existing.relation_id;
        relation.created_at = existing.created_at;
    }
    relation.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO iacc_relation (
            relation_id, relation_type, from_entity_id, to_entity_id, attributes_json,
            confidence, relation_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(relation_id) DO UPDATE SET
            relation_type = excluded.relation_type,
            from_entity_id = excluded.from_entity_id,
            to_entity_id = excluded.to_entity_id,
            attributes_json = excluded.attributes_json,
            confidence = excluded.confidence,
            relation_json = excluded.relation_json,
            updated_at = excluded.updated_at",
        params![
            relation.relation_id,
            relation.relation_type,
            relation.from_entity_id,
            relation.to_entity_id,
            serde_json::to_string(&relation.attributes)?,
            relation.confidence,
            serde_json::to_string(&relation)?,
            relation.created_at.to_rfc3339(),
            relation.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(relation)
}

fn find_relation_by_key(
    connection: &Connection,
    relation_type: &str,
    from_entity_id: &str,
    to_entity_id: &str,
) -> Result<Option<IaccRelation>, IaccStoreError> {
    connection
        .query_row(
            r"SELECT relation_json
              FROM iacc_relation
              WHERE relation_type = ?1 AND from_entity_id = ?2 AND to_entity_id = ?3",
            params![relation_type, from_entity_id, to_entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn list_entity_relations(
    connection: &Connection,
    entity_id: &str,
    limit: usize,
) -> Result<Vec<IaccRelation>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT relation_json
          FROM iacc_relation
          WHERE from_entity_id = ?1 OR to_entity_id = ?1
          ORDER BY updated_at DESC
          LIMIT ?2",
    )?;
    let rows = statement.query_map(params![entity_id, limit as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn build_impact_trace(
    connection: &Connection,
    root_entity_id: &str,
    max_depth: usize,
) -> Result<IaccImpactTrace, IaccStoreError> {
    let max_depth = max_depth.clamp(1, 5);
    let mut queue = VecDeque::from([(root_entity_id.to_string(), 0usize)]);
    let mut seen_entities = BTreeSet::from([root_entity_id.to_string()]);
    let mut seen_relations = BTreeSet::new();
    let mut hops = Vec::new();

    while let Some((entity_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for relation in list_entity_relations(connection, &entity_id, 500)? {
            if !seen_relations.insert(relation.relation_id.clone()) {
                continue;
            }
            let next_entity_id = if relation.from_entity_id == entity_id {
                relation.to_entity_id.clone()
            } else {
                relation.from_entity_id.clone()
            };
            let traversal_direction = if relation.from_entity_id == entity_id {
                "outbound"
            } else {
                "inbound"
            }
            .to_string();
            let from_entity = find_entity(connection, &relation.from_entity_id)?;
            let to_entity = find_entity(connection, &relation.to_entity_id)?;
            hops.push(IaccImpactHop {
                depth: depth + 1,
                traversal_direction,
                relation,
                from_entity,
                to_entity,
            });
            if seen_entities.insert(next_entity_id.clone()) {
                queue.push_back((next_entity_id, depth + 1));
            }
        }
    }

    let mut entities = Vec::new();
    for entity_id in &seen_entities {
        if let Some(entity) = find_entity(connection, entity_id)? {
            entities.push(entity);
        }
    }
    Ok(IaccImpactTrace {
        root_entity_id: root_entity_id.to_string(),
        max_depth,
        entities,
        hops,
        generated_at: Utc::now(),
    })
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

fn list_attention(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<IaccAttentionItem>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT attention_json
          FROM iacc_attention_item
          ORDER BY priority_score DESC, updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
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

fn insert_quality_gate(
    connection: &Connection,
    gate: &IaccQualityGateDecision,
) -> Result<(), IaccStoreError> {
    connection.execute(
        r"INSERT OR REPLACE INTO iacc_quality_gate (
            gate_id, target_ref, gate_type, decision, score, gate_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            gate.gate_id,
            gate.target_ref,
            gate.gate_type,
            gate.decision,
            gate.score,
            serde_json::to_string(gate)?,
            gate.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_quality_gate(
    connection: &Connection,
    gate_id: &str,
) -> Result<Option<IaccQualityGateDecision>, IaccStoreError> {
    connection
        .query_row(
            "SELECT gate_json FROM iacc_quality_gate WHERE gate_id = ?1",
            params![gate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn list_recent_quality_gates(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<IaccQualityGateDecision>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT gate_json
          FROM iacc_quality_gate
          ORDER BY created_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
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

fn upsert_metric_dependency(
    connection: &Connection,
    dependency: &IaccMetricDependency,
) -> Result<IaccMetricDependency, IaccStoreError> {
    let mut dependency = dependency.clone();
    if let Some(existing) = find_metric_dependency_by_key(
        connection,
        &dependency.upstream_metric_id,
        &dependency.downstream_metric_id,
        &dependency.dependency_type,
    )? {
        dependency.dependency_id = existing.dependency_id;
        dependency.created_at = existing.created_at;
    }
    dependency.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO iacc_metric_dependency (
            dependency_id, upstream_metric_id, downstream_metric_id, dependency_type,
            confidence, dependency_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(dependency_id) DO UPDATE SET
            upstream_metric_id = excluded.upstream_metric_id,
            downstream_metric_id = excluded.downstream_metric_id,
            dependency_type = excluded.dependency_type,
            confidence = excluded.confidence,
            dependency_json = excluded.dependency_json,
            updated_at = excluded.updated_at",
        params![
            dependency.dependency_id,
            dependency.upstream_metric_id,
            dependency.downstream_metric_id,
            dependency.dependency_type,
            dependency.confidence,
            serde_json::to_string(&dependency)?,
            dependency.created_at.to_rfc3339(),
            dependency.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(dependency)
}

fn find_metric_dependency_by_key(
    connection: &Connection,
    upstream_metric_id: &str,
    downstream_metric_id: &str,
    dependency_type: &str,
) -> Result<Option<IaccMetricDependency>, IaccStoreError> {
    connection
        .query_row(
            r"SELECT dependency_json
              FROM iacc_metric_dependency
              WHERE upstream_metric_id = ?1
                AND downstream_metric_id = ?2
                AND dependency_type = ?3",
            params![upstream_metric_id, downstream_metric_id, dependency_type],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
}

fn list_upstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<IaccMetricDependency>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM iacc_metric_dependency
          WHERE downstream_metric_id = ?1
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn list_downstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<IaccMetricDependency>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM iacc_metric_dependency
          WHERE upstream_metric_id = ?1
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn build_metric_lineage(
    connection: &Connection,
    metric_id: &str,
    max_depth: usize,
) -> Result<IaccMetricLineage, IaccStoreError> {
    let max_depth = max_depth.clamp(1, 6);
    let upstream_dependencies = list_upstream_metric_dependencies(connection, metric_id)?;
    let downstream_dependencies = list_downstream_metric_dependencies(connection, metric_id)?;
    let mut impacted = BTreeSet::new();
    let mut queue = VecDeque::from([(metric_id.to_string(), 0usize)]);
    while let Some((current_metric_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for dependency in list_downstream_metric_dependencies(connection, &current_metric_id)? {
            if impacted.insert(dependency.downstream_metric_id.clone()) {
                queue.push_back((dependency.downstream_metric_id, depth + 1));
            }
        }
    }
    Ok(IaccMetricLineage {
        metric_id: metric_id.to_string(),
        upstream_dependencies,
        downstream_dependencies,
        impacted_metric_ids: impacted.into_iter().collect(),
        generated_at: Utc::now(),
    })
}

fn metrics_affected_by_fact_type(
    connection: &Connection,
    fact_type: &str,
) -> Result<Vec<String>, IaccStoreError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM iacc_metric_dependency
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let dependency: IaccMetricDependency = serde_json::from_str(&row?)?;
        if dependency
            .required_fact_types
            .iter()
            .any(|candidate| candidate == fact_type)
        {
            impacted.insert(dependency.upstream_metric_id.clone());
            impacted.insert(dependency.downstream_metric_id.clone());
            for metric_id in build_metric_lineage(connection, &dependency.downstream_metric_id, 6)?
                .impacted_metric_ids
            {
                impacted.insert(metric_id);
            }
        }
    }
    Ok(impacted.into_iter().collect())
}

fn metric_ids_for_fact_type(
    connection: &Connection,
    fact_type: &str,
) -> Result<Vec<String>, IaccStoreError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT definition_json
          FROM iacc_metric_definition
          ORDER BY metric_id ASC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let definition: IaccMetricDefinition = serde_json::from_str(&row?)?;
        if definition.inputs.iter().any(|input| input == fact_type) {
            impacted.insert(definition.metric_id);
        }
    }
    Ok(impacted.into_iter().collect())
}

fn priority_for_compute_job(job: &IaccComputeJob) -> f32 {
    let metric_score = (job.metric_ids.len() as f32 / 8.0).min(1.0);
    let trigger_score = if job.trigger_fact_type.contains("shortage")
        || job.trigger_fact_type.contains("delivery")
        || job.trigger_fact_type.contains("quality")
    {
        0.9
    } else {
        0.55
    };
    (metric_score * 0.45 + trigger_score * 0.55).min(1.0)
}

fn upsert_compute_job(
    connection: &Connection,
    job: &IaccComputeJob,
) -> Result<IaccComputeJob, IaccStoreError> {
    connection.execute(
        r"INSERT INTO iacc_compute_job (
            job_id, trigger_fact_type, status, priority, job_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(job_id) DO UPDATE SET
            trigger_fact_type = excluded.trigger_fact_type,
            status = excluded.status,
            priority = excluded.priority,
            job_json = excluded.job_json,
            updated_at = excluded.updated_at",
        params![
            job.job_id,
            job.trigger_fact_type,
            job.status,
            job.priority,
            serde_json::to_string(job)?,
            job.created_at.to_rfc3339(),
            job.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(job.clone())
}

fn find_compute_job(
    connection: &Connection,
    job_id: &str,
) -> Result<Option<IaccComputeJob>, IaccStoreError> {
    connection
        .query_row(
            "SELECT job_json FROM iacc_compute_job WHERE job_id = ?1",
            params![job_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(IaccStoreError::from))
        .transpose()
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

fn list_recent_executions(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<IaccActionExecution>, IaccStoreError> {
    let mut statement = connection.prepare(
        r"SELECT execution_json
          FROM iacc_action_execution
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
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
    use crate::iacc::{
        IaccCockpitProfileInput, IaccCockpitReportDeliveryPayload,
        IaccCockpitReportDeliveryPayloadRequest, IaccCockpitReportDeliveryReceipt,
        IaccCockpitReportDeliveryState, IaccCockpitReportRequest, IaccComputeJobInput,
        IaccEntityInput, IaccFactInput, IaccRelationInput, IaccSourceKey,
    };

    #[test]
    fn entity_source_keys_resolve_to_one_canonical_entity() {
        let store = IaccStore::in_memory().expect("store opens");
        let first = IaccEntity::from_input(IaccEntityInput {
            entity_id: None,
            entity_type: "Component".to_string(),
            canonical_key: "GPU-H100".to_string(),
            display_name: Some("GPU H100".to_string()),
            source_keys: vec![IaccSourceKey {
                source_system: "ERP".to_string(),
                source_key: "MAT-GPU-H100".to_string(),
                source_ref: Some("connector:erp:material".to_string()),
            }],
            attributes: serde_json::json!({"family": "gpu"}),
            confidence: Some(0.96),
        });
        let first = store.upsert_entity(&first).expect("entity saves");

        let second = IaccEntity::from_input(IaccEntityInput {
            entity_id: None,
            entity_type: "component".to_string(),
            canonical_key: "gpu-h100".to_string(),
            display_name: Some("H100 accelerator".to_string()),
            source_keys: vec![IaccSourceKey {
                source_system: "PLM".to_string(),
                source_key: "GPU_H100_80GB".to_string(),
                source_ref: Some("connector:plm:item".to_string()),
            }],
            attributes: serde_json::json!({"thermal_design": "high"}),
            confidence: Some(0.91),
        });
        let second = store.upsert_entity(&second).expect("entity merges");

        assert_eq!(first.entity_id, second.entity_id);
        assert_eq!(second.source_keys.len(), 2);
        let resolved = store
            .resolve_entity_by_source_key("plm", "GPU_H100_80GB")
            .expect("source key resolves")
            .expect("entity exists");
        assert_eq!(resolved.entity_id, first.entity_id);
        assert_eq!(store.health().unwrap().entity_count, 1);
    }

    #[test]
    fn relation_network_traces_component_impact_to_orders() {
        let store = IaccStore::in_memory().expect("store opens");
        let component = store
            .upsert_entity(&IaccEntity::from_input(IaccEntityInput {
                entity_id: Some("entity-component-gpu".to_string()),
                entity_type: "component".to_string(),
                canonical_key: "gpu-h100".to_string(),
                display_name: Some("GPU H100".to_string()),
                source_keys: Vec::new(),
                attributes: serde_json::json!({}),
                confidence: Some(0.98),
            }))
            .expect("component saves");
        let product = store
            .upsert_entity(&IaccEntity::from_input(IaccEntityInput {
                entity_id: Some("entity-product-server".to_string()),
                entity_type: "product".to_string(),
                canonical_key: "server-ai-8gpu".to_string(),
                display_name: Some("AI Server 8GPU".to_string()),
                source_keys: Vec::new(),
                attributes: serde_json::json!({}),
                confidence: Some(0.95),
            }))
            .expect("product saves");
        let order = store
            .upsert_entity(&IaccEntity::from_input(IaccEntityInput {
                entity_id: Some("entity-order-customer-a".to_string()),
                entity_type: "customer_order".to_string(),
                canonical_key: "co-2026-0001".to_string(),
                display_name: Some("Customer order CO-2026-0001".to_string()),
                source_keys: Vec::new(),
                attributes: serde_json::json!({"priority": "strategic"}),
                confidence: Some(0.92),
            }))
            .expect("order saves");

        let requires = store
            .upsert_relation(&IaccRelation::from_input(IaccRelationInput {
                relation_id: None,
                relation_type: "requires".to_string(),
                from_entity_id: product.entity_id.clone(),
                to_entity_id: component.entity_id.clone(),
                attributes: serde_json::json!({"qty_per": 8}),
                confidence: Some(0.97),
            }))
            .expect("requires relation saves");
        store
            .upsert_relation(&IaccRelation::from_input(IaccRelationInput {
                relation_id: None,
                relation_type: "reserved_for".to_string(),
                from_entity_id: order.entity_id.clone(),
                to_entity_id: product.entity_id.clone(),
                attributes: serde_json::json!({"week": "2026-W30"}),
                confidence: Some(0.9),
            }))
            .expect("order relation saves");

        let component_relations = store
            .list_entity_relations(&component.entity_id, 10)
            .expect("relations list");
        assert_eq!(component_relations.len(), 1);
        assert_eq!(component_relations[0].relation_id, requires.relation_id);

        let trace = store
            .impact_trace(&component.entity_id, 3)
            .expect("impact path builds");
        assert_eq!(trace.root_entity_id, component.entity_id);
        assert_eq!(trace.hops.len(), 2);
        assert!(trace
            .entities
            .iter()
            .any(|entity| entity.entity_id == order.entity_id));
        assert_eq!(store.health().unwrap().relation_count, 2);
    }

    #[test]
    fn server_manufacturing_seed_creates_domain_network_and_metric_facts() {
        let store = IaccStore::in_memory().expect("store opens");
        let result = store
            .seed_server_manufacturing_domain()
            .expect("domain seed runs");

        assert_eq!(result.domain_id, "server_manufacturing");
        assert_eq!(result.scenario_count, 3);
        assert!(result.entity_count >= 10);
        assert!(result.relation_count >= 10);
        assert!(result.fact_count >= 5);

        let health = store.health().expect("health loads");
        assert_eq!(health.entity_count, result.entity_count as u64);
        assert_eq!(health.relation_count, result.relation_count as u64);
        assert_eq!(
            health.metric_definition_count,
            result.metric_definition_count as u64
        );
        assert_eq!(health.fact_count, result.fact_count as u64);

        let resolved = store
            .resolve_entity_by_source_key("plm", "GPU_H100_80GB")
            .expect("source resolves")
            .expect("entity exists");
        assert_eq!(resolved.entity_id, "entity-component-gpu-h100");

        let trace = store
            .impact_trace("entity-component-gpu-h100", 3)
            .expect("impact trace builds");
        assert!(trace
            .entities
            .iter()
            .any(|entity| entity.entity_id == "entity-order-co-2026-0001"));

        let recompute = store.recompute_metrics().expect("metrics recompute");
        assert!(recompute
            .metric_states
            .iter()
            .any(|state| state.metric_id == "material_shortage_risk"));
        assert!(!recompute.attention.is_empty());
    }

    #[test]
    fn metric_dependency_graph_projects_lineage_and_fact_impact() {
        let store = IaccStore::in_memory().expect("store opens");
        let result = store
            .seed_server_manufacturing_domain()
            .expect("domain seed runs");
        assert_eq!(result.metric_dependency_count, 5);

        let lineage = store
            .metric_lineage("supplier_commit_variance", 6)
            .expect("lineage builds");
        assert!(lineage
            .downstream_dependencies
            .iter()
            .any(|dependency| dependency.downstream_metric_id == "material_shortage_risk"));
        assert!(lineage
            .impacted_metric_ids
            .iter()
            .any(|metric_id| metric_id == "order_delivery_risk"));

        let affected = store
            .metrics_affected_by_fact_type("supply.commit_variance")
            .expect("affected metrics resolve");
        assert!(affected
            .iter()
            .any(|metric_id| metric_id == "supplier_commit_variance"));
        assert!(affected
            .iter()
            .any(|metric_id| metric_id == "order_delivery_risk"));
        assert_eq!(store.health().unwrap().metric_dependency_count, 5);
    }

    #[test]
    fn compute_job_plans_and_runs_scoped_metric_recompute() {
        let store = IaccStore::in_memory().expect("store opens");
        store
            .seed_server_manufacturing_domain()
            .expect("domain seed runs");

        let plan = store
            .plan_compute_job_for_fact_type(IaccComputeJobInput {
                job_id: Some("compute-job-supply-commit".to_string()),
                trigger_fact_type: "supply.commit_variance".to_string(),
                trigger_fact_refs: vec!["iacc:fact:fact-smfg-commit-gpu-alpha-w30".to_string()],
                entity_scope: Some("supplier:supplier-gpu-alpha".to_string()),
                period: Some("2026-W30".to_string()),
                metric_ids: Vec::new(),
                priority: None,
            })
            .expect("job plans");

        assert_eq!(plan.job.status, "planned");
        assert!(plan
            .affected_metric_ids
            .iter()
            .any(|metric_id| metric_id == "supplier_commit_variance"));
        assert!(plan
            .affected_metric_ids
            .iter()
            .any(|metric_id| metric_id == "order_delivery_risk"));
        assert_eq!(store.health().unwrap().compute_job_count, 1);

        let job = store.run_compute_job(&plan.job.job_id).expect("job runs");
        assert_eq!(job.status, "completed");
        assert_eq!(job.attempts, 1);
        assert_eq!(job.result_summary["metric_state_count"], 3);
        assert!(
            store
                .metric_states("supplier_commit_variance")
                .expect("states load")
                .len()
                == 1
        );
        assert!(store
            .metric_states("work_center_load")
            .expect("states load")
            .is_empty());
    }

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
    fn quality_gate_reviews_evidence_then_passes_after_analysis() {
        let store = IaccStore::in_memory().expect("store opens");
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-quality-shortage".to_string()),
            snapshot_id: Some("snapshot-quality-shortage".to_string()),
            fact_type: "supply.material_shortage".to_string(),
            entity_refs: vec!["component:gpu-quality".to_string()],
            metric_key: Some("material_shortage_risk".to_string()),
            dimensions: serde_json::json!({"week": "2026-W28"}),
            measures: serde_json::json!({"short_qty": 220}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: Some("connector:erp:shortage".to_string()),
            confidence: Some(0.92),
            raw_hash: None,
        });
        store.ingest_fact(&fact).expect("fact ingests");
        let recompute = store.recompute_metrics().expect("recompute");
        let packet = store
            .build_evidence_packet(
                Some(&recompute.attention[0].attention_id),
                Some("GPU shortage quality gated incident"),
            )
            .expect("packet builds");

        let review_gate = store
            .evaluate_evidence_quality(&packet.packet_id)
            .expect("quality gate evaluates");
        assert_eq!(review_gate.decision, "review");
        assert!(review_gate
            .required_actions
            .iter()
            .any(|action| action == "run_incident_analysis"));
        assert_eq!(
            store
                .get_quality_gate(&review_gate.gate_id)
                .expect("gate loads")
                .expect("gate exists")
                .target_ref,
            format!("iacc:evidence:{}", packet.packet_id)
        );

        let mut incident = IaccIncident::new("GPU shortage quality gate");
        incident.attention_id = packet.attention_id.clone();
        incident.evidence_packet_id = Some(packet.packet_id.clone());
        store.create_incident(&incident).expect("incident saves");
        store
            .analyze_incident(&incident.incident_id)
            .expect("incident analyzes");

        let pass_gate = store
            .evaluate_evidence_quality(&packet.packet_id)
            .expect("quality gate re-evaluates");
        assert_eq!(pass_gate.decision, "pass");
        assert!(pass_gate.score >= 0.75);
        assert_eq!(store.health().unwrap().quality_gate_count, 2);
    }

    #[test]
    fn cockpit_projection_aggregates_focus_quality_and_actions() {
        let store = IaccStore::in_memory().expect("store opens");
        let fact = IaccFact::from_input(IaccFactInput {
            fact_id: Some("fact-cockpit-shortage".to_string()),
            snapshot_id: Some("snapshot-cockpit-shortage".to_string()),
            fact_type: "supply.material_shortage".to_string(),
            entity_refs: vec!["component:gpu-cockpit".to_string()],
            metric_key: Some("material_shortage_risk".to_string()),
            dimensions: serde_json::json!({"week": "2026-W32"}),
            measures: serde_json::json!({"short_qty": 260}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: Some("connector:erp:shortage".to_string()),
            confidence: Some(0.94),
            raw_hash: None,
        });
        store.ingest_fact(&fact).expect("fact ingests");
        let recompute = store.recompute_metrics().expect("recompute");
        let packet = store
            .build_evidence_packet(
                Some(&recompute.attention[0].attention_id),
                Some("GPU shortage cockpit incident"),
            )
            .expect("packet builds");
        store
            .evaluate_evidence_quality(&packet.packet_id)
            .expect("review gate evaluates");
        let mut incident = IaccIncident::new("GPU shortage cockpit");
        incident.attention_id = packet.attention_id.clone();
        incident.evidence_packet_id = Some(packet.packet_id.clone());
        store.create_incident(&incident).expect("incident saves");
        let analysis = store
            .analyze_incident(&incident.incident_id)
            .expect("analysis");
        store
            .evaluate_evidence_quality(&packet.packet_id)
            .expect("pass gate evaluates");
        store
            .execute_recommended_action(
                &analysis.analysis_id,
                &analysis.recommended_actions[0].action_id,
                &IaccActionExecutionRequest {
                    mode: "commit".to_string(),
                    operator_id: Some("user:ops-planner".to_string()),
                    note: Some("cockpit action".to_string()),
                },
            )
            .expect("execution saves");

        let profile = IaccCockpitProfile::from_input(IaccCockpitProfileInput {
            profile_id: Some("cockpit-profile-ops".to_string()),
            owner_ref: "user:ops-planner".to_string(),
            display_name: Some("Ops planner".to_string()),
            focus_refs: vec!["component:gpu-cockpit".to_string()],
            focus_metric_ids: vec!["material_shortage_risk".to_string()],
            thresholds: serde_json::json!({"material_shortage_risk": {"critical": 100}}),
            template_id: Some("ops.default".to_string()),
            cadence: Some("daily".to_string()),
        });
        let profile = store
            .upsert_cockpit_profile(&profile)
            .expect("profile saves");
        assert_eq!(store.health().unwrap().cockpit_profile_count, 1);
        let daily_profiles = store
            .list_cockpit_profiles(Some("daily"), 10)
            .expect("profiles list");
        assert_eq!(daily_profiles.len(), 1);
        assert_eq!(daily_profiles[0].profile_id, profile.profile_id);

        let projection = store
            .cockpit_projection(&profile.profile_id)
            .expect("projection builds");
        assert_eq!(projection.profile.owner_ref, "user:ops-planner");
        let attention_widget = projection
            .widgets
            .iter()
            .find(|widget| widget.widget_type == "attention_queue")
            .expect("attention widget exists");
        assert!(attention_widget.data["count"].as_u64().unwrap_or(0) >= 1);
        assert!(!attention_widget.source_refs.is_empty());
        let quality_widget = projection
            .widgets
            .iter()
            .find(|widget| widget.widget_type == "quality_gate_status")
            .expect("quality widget exists");
        assert_eq!(quality_widget.data["pass_count"], 1);
        let action_widget = projection
            .widgets
            .iter()
            .find(|widget| widget.widget_type == "action_execution_status")
            .expect("action widget exists");
        assert_eq!(action_widget.data["active_count"], 1);
        let threshold_widget = projection
            .widgets
            .iter()
            .find(|widget| widget.widget_type == "focus_thresholds")
            .expect("threshold widget exists");
        assert_eq!(threshold_widget.status, "configured");

        let report = store
            .generate_cockpit_report(
                &profile.profile_id,
                IaccCockpitReportRequest {
                    report_id: Some("cockpit-report-ops-daily".to_string()),
                    cadence: Some("daily".to_string()),
                    delivery_ref: Some("channel://feishu/user/ops-planner".to_string()),
                    note: Some("daily cockpit report".to_string()),
                },
            )
            .expect("report generates");
        assert_eq!(report.status, "generated");
        assert_eq!(report.profile_id, profile.profile_id);
        assert_eq!(report.projection.widgets.len(), 4);
        let loaded_report = store
            .get_cockpit_report(&report.report_id)
            .expect("report loads")
            .expect("report exists");
        assert_eq!(loaded_report.delivery_ref, report.delivery_ref);
        assert_eq!(store.health().unwrap().cockpit_report_count, 1);

        let payload = IaccCockpitReportDeliveryPayload::from_report(
            &report,
            IaccCockpitReportDeliveryPayloadRequest {
                channel: Some("feishu".to_string()),
                template_id: Some("ops.alert.compact".to_string()),
                target_ref: report.delivery_ref.clone(),
                requested_capability: None,
            },
        );
        assert_eq!(payload.channel, "feishu");
        assert_eq!(payload.template_id, "ops.alert.compact");
        assert_eq!(payload.requested_capability, "channel.feishu.send_text");
        assert!(payload.resource_ref.starts_with("text://"));
        assert!(payload
            .constraints
            .contains(&"payload_kind:text".to_string()));
        assert!(payload
            .constraints
            .contains(&"target_ref_present".to_string()));

        let delivered = store
            .attach_cockpit_report_delivery(
                &report.report_id,
                IaccCockpitReportDeliveryReceipt::new(
                    report.report_id.clone(),
                    "cpx-report-test",
                    "planned",
                    "dry_run",
                    Some("cpa-report-test".to_string()),
                ),
            )
            .expect("report delivery attaches");
        assert_eq!(delivered.status, "delivery_planned");
        assert_eq!(delivered.delivery_receipts.len(), 1);
        let delivery_state = IaccCockpitReportDeliveryState::from_report(&delivered);
        assert_eq!(delivery_state.classification, "dry_run_planned");
        assert!(!delivery_state.retryable);
        assert_eq!(delivery_state.attempt_count, 1);
        let delivered = store
            .attach_cockpit_report_delivery(
                &report.report_id,
                IaccCockpitReportDeliveryReceipt::new(
                    report.report_id.clone(),
                    "cpx-report-test",
                    "planned",
                    "dry_run",
                    Some("cpa-report-test".to_string()),
                ),
            )
            .expect("report delivery deduplicates");
        assert_eq!(delivered.delivery_receipts.len(), 1);
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
            .attach_cross_plane_receipt(
                &execution.execution_id,
                IaccCrossPlaneBridgeReceipt::new(
                    execution.execution_id.clone(),
                    "cpx-iacc-test",
                    "planned",
                    "dry_run",
                    Some("cpa-iacc-test".to_string()),
                ),
            )
            .expect("bridge receipt attaches");
        assert_eq!(execution.status, "cross_plane_planned");
        assert_eq!(execution.cross_plane_receipts.len(), 1);
        assert_eq!(
            execution.receipt["cross_plane_receipts"][0]["cross_plane_receipt_id"],
            "cpx-iacc-test"
        );
        let execution = store
            .attach_cross_plane_receipt(
                &execution.execution_id,
                IaccCrossPlaneBridgeReceipt::new(
                    execution.execution_id.clone(),
                    "cpx-iacc-test",
                    "planned",
                    "dry_run",
                    Some("cpa-iacc-test".to_string()),
                ),
            )
            .expect("bridge receipt deduplicates");
        assert_eq!(execution.cross_plane_receipts.len(), 1);

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
