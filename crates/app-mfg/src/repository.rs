#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::{SqliteConnectionFactory, StorageHandle};
use thiserror::Error;

use crate::{
    mfg_ontology_pack, mfg_seed_plan, mfg_widget_catalog, MfgActionExecution,
    MfgActionExecutionRequest, MfgActionFeedback, MfgAlertCommand, MfgAlertCommandInput,
    MfgAlertOccurrence, MfgAlertRule, MfgAssignment, MfgAssignmentCommand,
    MfgAssignmentCommandInput, MfgCasePromotion, MfgCockpitProfile, MfgCockpitProjection,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportRequest, MfgCockpitReportSnapshot,
    MfgCockpitWidget, MfgCommandReceipt, MfgCrossPlaneBridgeReceipt, MfgDomainSeedResult,
    MfgForecastProjection, MfgForecastSignal, MfgIncident, MfgLiveProjection,
    MfgLiveProjectionEvent, MfgMemoryCase, MfgOperationalAnalysis, MfgPlaybook, MfgSkillRun,
    MfgWidgetDefinition, MfgWidgetInstance, MfgWorkflowGraph, MfgWorkflowGraphError,
};

use matrix_core::{
    build_metric_compute_jobs, MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob,
    MatrixComputeJobInput, MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput,
    MatrixDataPlane, MatrixDataPlaneHealth, MatrixDataPlaneIngestPlan,
    MatrixDataPlaneIngestPlanInput, MatrixDataPlaneWatermark, MatrixEntity, MatrixEvidencePacket,
    MatrixEvidenceSourceRef, MatrixFact, MatrixImpactHop, MatrixImpactTrace,
    MatrixMetricAttentionPlan, MatrixMetricAttentionScore, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricSnapshotItem,
    MatrixMetricState, MatrixOntologyPack, MatrixQualityGateDecision, MatrixRelation,
    MatrixSeverity, MatrixSourceDeltaPlan, MatrixSourcePack, MatrixSourcePackValidation,
};
use matrix_repository::MatrixSqliteDataPlane;

#[derive(Debug, Error)]
pub enum MfgRepositoryError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("storage error: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("mfg record not found: {0}")]
    NotFound(String),
    #[error(
        "MFG workflow {workflow_id} revision conflict: expected {expected:?}, actual {actual:?}"
    )]
    WorkflowRevisionConflict {
        workflow_id: String,
        expected: Option<u64>,
        actual: Option<u64>,
    },
    #[error(
        "MFG {domain} {subject_id} revision conflict: expected {expected:?}, actual {actual:?}"
    )]
    RevisionConflict {
        domain: String,
        subject_id: String,
        expected: Option<u64>,
        actual: Option<u64>,
    },
    #[error("MFG command rejected: {0}")]
    CommandRejected(String),
    #[error("MFG workflow graph error: {0}")]
    Workflow(#[from] MfgWorkflowGraphError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgHealth {
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
    pub memory_case_count: u64,
    pub playbook_count: u64,
    pub source_pack_count: u64,
    pub data_plane_watermark_count: u64,
    pub connector_run_count: u64,
    pub ontology_pack_count: u64,
    pub entity_match_candidate_count: u64,
    pub entity_conflict_decision_count: u64,
    pub metric_snapshot_count: u64,
    pub skill_execution_count: u64,
    pub workflow_graph_count: u64,
    pub alert_rule_count: u64,
    pub alert_occurrence_count: u64,
    pub assignment_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgMetricRecomputeResult {
    pub metric_state_count: usize,
    pub change_count: usize,
    pub attention_count: usize,
    pub metric_states: Vec<MatrixMetricState>,
    pub changes: Vec<MatrixChangeEvent>,
    pub attention: Vec<MatrixAttentionItem>,
}

#[derive(Debug)]
pub struct MfgRepository {
    connection: Mutex<Connection>,
}

impl MfgRepository {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MfgRepositoryError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    pub fn open_storage_handle(handle: &StorageHandle) -> Result<Self, MfgRepositoryError> {
        let connection = SqliteConnectionFactory::default().open_handle(handle)?;
        Self::from_connection(connection)
    }

    pub fn in_memory() -> Result<Self, MfgRepositoryError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, MfgRepositoryError> {
        connection.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
        connection.query_row("PRAGMA busy_timeout=5000", [], |_| Ok(()))?;
        connection.execute_batch("PRAGMA foreign_keys=ON;")?;
        initialize_schema(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn health(&self) -> Result<MfgHealth, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(MfgHealth {
            schema_version: schema_version(&connection)?,
            fact_count: count_table(&connection, "matrix_fact")?,
            metric_definition_count: count_table(&connection, "matrix_metric_definition")?,
            metric_state_count: count_table(&connection, "matrix_metric_state")?,
            change_count: count_table(&connection, "matrix_change_event")?,
            attention_count: count_table(&connection, "matrix_attention_item")?,
            evidence_count: count_table(&connection, "matrix_evidence_packet")?,
            incident_count: count_table(&connection, "mfg_incident")?,
            analysis_count: count_table(&connection, "mfg_operational_analysis")?,
            execution_count: count_table(&connection, "mfg_action_execution")?,
            entity_count: count_table(&connection, "matrix_entity")?,
            relation_count: count_table(&connection, "matrix_relation")?,
            metric_dependency_count: count_table(&connection, "matrix_metric_dependency")?,
            compute_job_count: count_table(&connection, "matrix_compute_job")?,
            quality_gate_count: count_table(&connection, "matrix_quality_gate")?,
            cockpit_profile_count: count_table(&connection, "mfg_cockpit_profile")?,
            cockpit_report_count: count_table(&connection, "mfg_cockpit_report")?,
            memory_case_count: count_table(&connection, "mfg_memory_case")?,
            playbook_count: count_table(&connection, "mfg_playbook")?,
            source_pack_count: count_table(&connection, "matrix_source_pack")?,
            data_plane_watermark_count: count_table(&connection, "matrix_data_plane_watermark")?,
            connector_run_count: count_table(&connection, "matrix_connector_run")?,
            ontology_pack_count: count_table(&connection, "matrix_ontology_pack")?,
            entity_match_candidate_count: count_table(
                &connection,
                "matrix_entity_match_candidate",
            )?,
            entity_conflict_decision_count: count_table(
                &connection,
                "matrix_entity_conflict_decision",
            )?,
            metric_snapshot_count: count_table(&connection, "matrix_metric_snapshot")?,
            skill_execution_count: count_table(&connection, "mfg_skill_execution")?,
            workflow_graph_count: count_table(&connection, "mfg_workflow_graph")?,
            alert_rule_count: count_table(&connection, "mfg_alert_rule")?,
            alert_occurrence_count: count_table(&connection, "mfg_alert_occurrence")?,
            assignment_count: count_table(&connection, "mfg_assignment")?,
        })
    }

    pub fn data_plane_health(&self) -> Result<MatrixDataPlaneHealth, MfgRepositoryError> {
        let health = self.health()?;
        Ok(MatrixSqliteDataPlane::new(health.data_plane_watermark_count).health())
    }

    pub fn plan_data_plane_ingest(
        &self,
        input: MatrixDataPlaneIngestPlanInput,
    ) -> Result<MatrixDataPlaneIngestPlan, MfgRepositoryError> {
        let mut plan = MatrixSqliteDataPlane::new(self.health()?.data_plane_watermark_count)
            .plan_ingest(input);
        if plan.affected_metric_ids.is_empty() {
            let connection = self
                .connection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut affected = metrics_affected_by_fact_type(&connection, &plan.fact_type)?;
            affected.extend(metric_ids_for_fact_type(&connection, &plan.fact_type)?);
            affected.sort();
            affected.dedup();
            plan.compute_jobs = affected
                .iter()
                .map(|metric_id| MatrixComputeJobInput {
                    job_id: Some(format!("compute-job-{}-{}", plan.batch_id, metric_id)),
                    trigger_fact_type: plan.fact_type.clone(),
                    trigger_fact_refs: vec![format!("matrix:data-plane-batch:{}", plan.batch_id)],
                    entity_scope: None,
                    period: Some(plan.partition_ref.clone()),
                    metric_ids: vec![metric_id.clone()],
                    priority: Some(0.72),
                })
                .collect();
            plan.affected_metric_ids = affected;
        }
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_data_plane_watermark(&connection, &plan.watermark)?;
        Ok(plan)
    }

    pub fn upsert_cockpit_profile(
        &self,
        profile: &MfgCockpitProfile,
        expected_revision: Option<u64>,
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_cockpit_profile(&connection, profile, expected_revision)
    }

    pub fn upsert_cockpit_profile_receipted(
        &self,
        profile: &MfgCockpitProfile,
        expected_revision: Option<u64>,
        command: &str,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgCockpitProfile, MfgCommandReceipt), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_cockpit_profile_receipted(
            &connection,
            profile,
            expected_revision,
            command,
            actor_ref,
            idempotency_key,
        )
    }

    pub fn get_cockpit_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<MfgCockpitProfile>, MfgRepositoryError> {
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
    ) -> Result<Vec<MfgCockpitProfile>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_cockpit_profiles(&connection, cadence, limit)
    }

    pub fn cockpit_projection(
        &self,
        profile_id: &str,
    ) -> Result<MfgCockpitProjection, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
        render_cockpit_projection(&connection, profile)
    }

    pub fn generate_cockpit_report(
        &self,
        profile_id: &str,
        request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
        let projection = render_cockpit_projection(&connection, profile)?;
        let report = MfgCockpitReportSnapshot::from_projection(projection, request);
        insert_cockpit_report(&connection, &report)?;
        Ok(report)
    }

    pub fn get_cockpit_report(
        &self,
        report_id: &str,
    ) -> Result<Option<MfgCockpitReportSnapshot>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_cockpit_report(&connection, report_id)
    }

    pub fn attach_cockpit_report_delivery(
        &self,
        report_id: &str,
        receipt: MfgCockpitReportDeliveryReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut report = find_cockpit_report(&connection, report_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(report_id.to_string()))?;
        report.attach_delivery_receipt(receipt);
        insert_cockpit_report(&connection, &report)?;
        Ok(report)
    }

    pub fn delete_cockpit_profile(
        &self,
        profile_id: &str,
        expected_revision: u64,
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
        ensure_revision(
            "cockpit_profile",
            profile_id,
            expected_revision,
            profile.revision,
        )?;
        connection.execute(
            "DELETE FROM mfg_cockpit_profile WHERE profile_id = ?1",
            params![profile_id],
        )?;
        append_projection_event(
            &connection,
            "cockpit",
            &format!("mfg:cockpit-profile:{profile_id}"),
            "profile.deleted",
            serde_json::to_value(&profile)?,
        )?;
        Ok(profile)
    }

    pub fn delete_cockpit_profile_receipted(
        &self,
        profile_id: &str,
        expected_revision: u64,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(Option<MfgCockpitProfile>, MfgCommandReceipt), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        delete_cockpit_profile_receipted(
            &connection,
            profile_id,
            expected_revision,
            actor_ref,
            idempotency_key,
        )
    }

    pub fn upsert_alert_rule(
        &self,
        rule: &MfgAlertRule,
        expected_revision: Option<u64>,
    ) -> Result<MfgAlertRule, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_alert_rule(&connection, rule, expected_revision)
    }

    pub fn upsert_alert_rule_receipted(
        &self,
        rule: &MfgAlertRule,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAlertRule, MfgCommandReceipt), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_alert_rule_receipted(
            &connection,
            rule,
            expected_revision,
            actor_ref,
            idempotency_key,
        )
    }

    pub fn list_alert_rules(
        &self,
        owner_ref: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAlertRule>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_alert_rules(&connection, owner_ref, limit)
    }

    pub fn list_alert_occurrences(
        &self,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAlertOccurrence>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_alert_occurrences(&connection, status, limit)
    }

    pub fn upsert_alert_subscription(
        &self,
        subscription: &MfgAlertSubscription,
        expected_revision: Option<u64>,
    ) -> Result<MfgAlertSubscription, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_alert_subscription(&connection, subscription, expected_revision)
    }

    pub fn upsert_alert_subscription_receipted(
        &self,
        subscription: &MfgAlertSubscription,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAlertSubscription, MfgCommandReceipt), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_alert_subscription_receipted(
            &connection,
            subscription,
            expected_revision,
            actor_ref,
            idempotency_key,
        )
    }

    pub fn list_alert_subscriptions(
        &self,
        subscriber_ref: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAlertSubscription>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_alert_subscriptions(&connection, subscriber_ref, limit)
    }

    pub fn command_alert(
        &self,
        occurrence_id: &str,
        command: MfgAlertCommandInput,
    ) -> Result<(MfgAlertOccurrence, MfgCommandReceipt), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        command_alert(&connection, occurrence_id, command)
    }

    pub fn forecasts(
        &self,
        metric_refs: &[String],
        horizon: &str,
        limit: usize,
    ) -> Result<Vec<MfgForecastProjection>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        build_forecasts(&connection, metric_refs, horizon, limit)
    }

    pub fn upsert_assignment(
        &self,
        assignment: &MfgAssignment,
        expected_revision: Option<u64>,
    ) -> Result<MfgAssignment, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_assignment(&connection, assignment, expected_revision)
    }

    pub fn upsert_assignment_receipted(
        &self,
        assignment: &MfgAssignment,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_assignment_receipted(
            &connection,
            assignment,
            expected_revision,
            actor_ref,
            idempotency_key,
        )
    }

    pub fn get_assignment(
        &self,
        assignment_id: &str,
    ) -> Result<Option<MfgAssignment>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_assignment(&connection, assignment_id)
    }

    pub fn list_assignments(
        &self,
        assignee_ref: Option<&str>,
        incident_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAssignment>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_assignments(&connection, assignee_ref, incident_id, limit)
    }

    pub fn command_assignment(
        &self,
        assignment_id: &str,
        command: MfgAssignmentCommandInput,
    ) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        command_assignment(&connection, assignment_id, command)
    }

    pub fn live_projection(
        &self,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<MfgLiveProjection, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        build_live_projection(&connection, cursor, limit)
    }

    pub fn record_command_notifications(
        &self,
        idempotency_key: &str,
        notification_refs: Vec<String>,
    ) -> Result<MfgCommandReceipt, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        record_command_notifications(&connection, idempotency_key, notification_refs)
    }

    pub fn upsert_entity(&self, entity: &MatrixEntity) -> Result<MatrixEntity, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_entity(&connection, entity)
    }

    pub fn get_entity(&self, entity_id: &str) -> Result<Option<MatrixEntity>, MfgRepositoryError> {
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
    ) -> Result<Option<MatrixEntity>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_entity_by_source_key(&connection, source_system, source_key)
    }

    pub fn list_entities(&self, limit: usize) -> Result<Vec<MatrixEntity>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_entities(&connection, limit)
    }

    pub fn seed_mfg_ontology(&self) -> Result<MatrixOntologyPack, MfgRepositoryError> {
        let pack = mfg_ontology_pack();
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        insert_ontology_pack(&connection, &pack)?;
        Ok(pack)
    }

    pub fn get_ontology_pack(
        &self,
        ontology_id: &str,
    ) -> Result<Option<MatrixOntologyPack>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_ontology_pack(&connection, ontology_id)
    }

    pub fn propose_entity_match(
        &self,
        left_entity_id: &str,
        right_entity_id: &str,
    ) -> Result<matrix_core::MatrixEntityMatchCandidate, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let left = find_entity(&connection, left_entity_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(left_entity_id.to_string()))?;
        let right = find_entity(&connection, right_entity_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(right_entity_id.to_string()))?;
        let candidate = matrix_core::match_candidate(&left, &right).ok_or_else(|| {
            MfgRepositoryError::NotFound(
                "entity match candidate below confidence threshold".to_string(),
            )
        })?;
        insert_entity_match_candidate(&connection, &candidate)?;
        Ok(candidate)
    }

    pub fn decide_entity_conflict(
        &self,
        candidate_id: &str,
        survivor_entity_id: &str,
        retired_entity_id: &str,
        survivorship_rule: &str,
        notes: Option<String>,
    ) -> Result<matrix_core::MatrixEntityConflictDecision, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_entity_match_candidate(&connection, candidate_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(candidate_id.to_string()))?;
        let survivor = find_entity(&connection, survivor_entity_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(survivor_entity_id.to_string()))?;
        let retired = find_entity(&connection, retired_entity_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(retired_entity_id.to_string()))?;
        let decision = matrix_core::MatrixEntityConflictDecision {
            decision_id: format!("entity-conflict-decision-{}", uuid::Uuid::new_v4()),
            candidate_id: candidate_id.to_string(),
            decision: "merge".to_string(),
            survivor_entity_id: survivor.entity_id,
            retired_entity_id: retired.entity_id,
            survivorship_rule: survivorship_rule.to_string(),
            notes,
            decision_metadata: serde_json::json!({
                "source": "matrix.entity_governance",
                "policy": survivorship_rule,
            }),
            decided_at: Utc::now(),
        };
        insert_entity_conflict_decision(&connection, &decision)?;
        Ok(decision)
    }

    pub fn plan_metric_attention(
        &self,
        trigger_fact_type: &str,
        entity_scope: Option<String>,
        period: Option<String>,
        limit: usize,
    ) -> Result<MatrixMetricAttentionPlan, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut metric_ids = metrics_affected_by_fact_type(&connection, trigger_fact_type)?;
        metric_ids.extend(metric_ids_for_fact_type(&connection, trigger_fact_type)?);
        metric_ids.sort();
        metric_ids.dedup();
        let plan = build_metric_attention_plan(
            &connection,
            trigger_fact_type,
            entity_scope,
            period,
            metric_ids,
            limit,
        )?;
        Ok(plan)
    }

    pub fn materialize_metric_snapshot(
        &self,
        metric_ids: Vec<String>,
        scope_ref: Option<String>,
    ) -> Result<MatrixMetricSnapshot, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = build_metric_snapshot(&connection, metric_ids, scope_ref)?;
        insert_metric_snapshot(&connection, &snapshot)?;
        Ok(snapshot)
    }

    pub fn upsert_relation(
        &self,
        relation: &MatrixRelation,
    ) -> Result<MatrixRelation, MfgRepositoryError> {
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
    ) -> Result<Vec<MatrixRelation>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(MfgRepositoryError::NotFound(entity_id.to_string()));
        }
        list_entity_relations(&connection, entity_id, limit)
    }

    pub fn impact_trace(
        &self,
        entity_id: &str,
        max_depth: usize,
    ) -> Result<MatrixImpactTrace, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if find_entity(&connection, entity_id)?.is_none() {
            return Err(MfgRepositoryError::NotFound(entity_id.to_string()));
        }
        build_impact_trace(&connection, entity_id, max_depth)
    }

    pub fn register_metric_definition(
        &self,
        definition: &MatrixMetricDefinition,
    ) -> Result<(), MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_metric_definition(&connection, definition)
    }

    pub fn upsert_metric_dependency(
        &self,
        dependency: &MatrixMetricDependency,
    ) -> Result<MatrixMetricDependency, MfgRepositoryError> {
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
    ) -> Result<MatrixMetricLineage, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        build_metric_lineage(&connection, metric_id, max_depth)
    }

    pub fn metrics_affected_by_fact_type(
        &self,
        fact_type: &str,
    ) -> Result<Vec<String>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics_affected_by_fact_type(&connection, fact_type)
    }

    pub fn plan_compute_job_for_fact_type(
        &self,
        input: MatrixComputeJobInput,
    ) -> Result<MatrixComputePlan, MfgRepositoryError> {
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
        let mut job = MatrixComputeJob::from_input(MatrixComputeJobInput {
            metric_ids: affected_metric_ids.clone(),
            ..input
        });
        job.priority = priority_for_compute_job(&job);
        upsert_compute_job(&connection, &job)?;
        Ok(MatrixComputePlan {
            job,
            affected_metric_ids,
            planned_at: Utc::now(),
        })
    }

    pub fn get_compute_job(
        &self,
        job_id: &str,
    ) -> Result<Option<MatrixComputeJob>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_compute_job(&connection, job_id)
    }

    pub fn run_compute_job(&self, job_id: &str) -> Result<MatrixComputeJob, MfgRepositoryError> {
        let mut job = {
            let connection = self
                .connection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut job = find_compute_job(&connection, job_id)?
                .ok_or_else(|| MfgRepositoryError::NotFound(job_id.to_string()))?;
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

    pub fn seed_mfg_domain(&self) -> Result<MfgDomainSeedResult, MfgRepositoryError> {
        let plan = mfg_seed_plan();
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
        Ok(MfgDomainSeedResult {
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

    pub fn ingest_fact(
        &self,
        fact: &MatrixFact,
    ) -> Result<MatrixAttentionItem, MfgRepositoryError> {
        let attention = MatrixAttentionItem::from_fact(
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
            r"INSERT OR REPLACE INTO matrix_fact (
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

    pub fn upsert_source_pack(
        &self,
        source_pack: MatrixSourcePack,
    ) -> Result<MatrixSourcePack, MfgRepositoryError> {
        let source_pack = source_pack.normalized();
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        insert_source_pack(&connection, &source_pack)?;
        Ok(source_pack)
    }

    pub fn get_source_pack(
        &self,
        source_pack_id: &str,
    ) -> Result<Option<MatrixSourcePack>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_source_pack(&connection, source_pack_id)
    }

    pub fn list_source_packs(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_source_packs(&connection, limit)
    }

    pub fn validate_source_pack(
        &self,
        source_pack_id: &str,
    ) -> Result<MatrixSourcePackValidation, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(source_pack_id.to_string()))?;
        Ok(source_pack.validate())
    }

    pub fn source_pack_delta_plan(
        &self,
        source_pack_id: &str,
    ) -> Result<MatrixSourceDeltaPlan, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(source_pack_id.to_string()))?;
        source_pack_delta_plan_for(&connection, &source_pack)
    }

    pub fn plan_connector_run(
        &self,
        source_pack_id: &str,
        input: MatrixConnectorRunInput,
    ) -> Result<MatrixConnectorRun, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_pack = find_source_pack(&connection, source_pack_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(source_pack_id.to_string()))?;
        let delta_plan = source_pack_delta_plan_for(&connection, &source_pack)?;
        let run = MatrixConnectorRun::from_source_pack(&source_pack, &delta_plan, input);
        insert_connector_run(&connection, &run)?;
        Ok(run)
    }

    pub fn get_connector_run(
        &self,
        run_id: &str,
    ) -> Result<Option<MatrixConnectorRun>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_connector_run(&connection, run_id)
    }

    pub fn list_attention(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT attention_json
              FROM matrix_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn list_facts(&self, limit: usize) -> Result<Vec<MatrixFact>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_facts(&connection, limit)
    }

    pub fn recompute_metrics(&self) -> Result<MfgMetricRecomputeResult, MfgRepositoryError> {
        self.recompute_metrics_with_filter(None)
    }

    pub fn recompute_metrics_for_metric_ids(
        &self,
        metric_ids: &[String],
    ) -> Result<MfgMetricRecomputeResult, MfgRepositoryError> {
        let filter = metric_ids.iter().cloned().collect::<BTreeSet<_>>();
        self.recompute_metrics_with_filter(Some(&filter))
    }

    fn recompute_metrics_with_filter(
        &self,
        metric_filter: Option<&BTreeSet<String>>,
    ) -> Result<MfgMetricRecomputeResult, MfgRepositoryError> {
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
                MatrixMetricDefinition::inferred(key.metric_id.clone(), &accumulator.fact_type);
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
            let state = MatrixMetricState {
                state_id: format!("metric-state-{}", uuid::Uuid::new_v4()),
                metric_id: key.metric_id.clone(),
                entity_scope: key.entity_scope.clone(),
                period: key.period.clone(),
                value,
                previous_value,
                delta,
                delta_ratio,
                status: MatrixMetricState::status_for_delta(delta),
                computed_at: Utc::now(),
                input_fact_refs: accumulator.fact_ids.clone(),
                confidence: accumulator.confidence(),
            };
            insert_metric_state(&connection, &state)?;
            states.push(state.clone());

            if delta.abs() > f64::EPSILON {
                let change = MatrixChangeEvent {
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
                    severity_hint: MatrixChangeEvent::severity_for_delta(delta),
                };
                insert_change_event(&connection, &change)?;
                let item = attention_from_change(&change, &state);
                upsert_attention(&connection, &item)?;
                changes.push(change);
                attention.push(item);
            }
        }
        Ok(MfgMetricRecomputeResult {
            metric_state_count: states.len(),
            change_count: changes.len(),
            attention_count: attention.len(),
            metric_states: states,
            changes,
            attention,
        })
    }

    pub fn list_metric_definitions(
        &self,
    ) -> Result<Vec<MatrixMetricDefinition>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT definition_json
              FROM matrix_metric_definition
              ORDER BY metric_id ASC",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn metric_states(
        &self,
        metric_id: &str,
    ) -> Result<Vec<MatrixMetricState>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1
              ORDER BY computed_at DESC",
        )?;
        let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    pub fn list_changes(&self, limit: usize) -> Result<Vec<MatrixChangeEvent>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut statement = connection.prepare(
            r"SELECT change_json
              FROM matrix_change_event
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
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let attention = match attention_id {
            Some(id) => Some(
                find_attention(&connection, id)?
                    .ok_or_else(|| MfgRepositoryError::NotFound(id.to_string()))?,
            ),
            None => latest_attention(&connection)?,
        };
        let mut packet = MatrixEvidencePacket::new(problem_statement.unwrap_or_else(|| {
            attention
                .as_ref()
                .map(|item| item.title.as_str())
                .unwrap_or("MATRIX operational evidence packet")
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
                if let Some(change_id) = reference.strip_prefix("matrix:change:") {
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
                packet.source_refs.push(MatrixEvidenceSourceRef {
                    kind: "change_or_fact".to_string(),
                    reference,
                    summary: "MATRIX attention evidence source".to_string(),
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
    ) -> Result<Option<MatrixEvidencePacket>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_evidence_packet(&connection, packet_id)
    }

    pub fn upsert_evidence_packet(
        &self,
        packet: &MatrixEvidencePacket,
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        insert_evidence_packet(&connection, packet)?;
        Ok(packet.clone())
    }

    pub fn list_evidence_packets(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_evidence_packets(&connection, limit)
    }

    pub fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let packet = find_evidence_packet(&connection, packet_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(packet_id.to_string()))?;
        let decision = MatrixQualityGateDecision::for_evidence_packet(&packet);
        insert_quality_gate(&connection, &decision)?;
        Ok(decision)
    }

    pub fn get_quality_gate(
        &self,
        gate_id: &str,
    ) -> Result<Option<MatrixQualityGateDecision>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_quality_gate(&connection, gate_id)
    }

    pub fn list_data_plane_watermarks(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixDataPlaneWatermark>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_data_plane_watermarks(&connection, limit)
    }

    pub fn create_incident(
        &self,
        incident: &MfgIncident,
    ) -> Result<MfgIncident, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        upsert_incident(&connection, incident)?;
        Ok(incident.clone())
    }

    pub fn get_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgIncident>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_incident(&connection, incident_id)
    }

    pub fn list_incidents(&self, limit: usize) -> Result<Vec<MfgIncident>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_incidents(&connection, limit)
    }

    pub fn save_workflow_graph(
        &self,
        graph: &MfgWorkflowGraph,
        expected_revision: Option<u64>,
    ) -> Result<MfgWorkflowGraph, MfgRepositoryError> {
        graph.validate()?;
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction()?;
        persist_workflow_graph(&transaction, graph, expected_revision)?;
        transaction.commit()?;
        Ok(graph.clone())
    }

    pub fn create_incident_workflow(
        &self,
        incident: &MfgIncident,
        packet: &MatrixEvidencePacket,
    ) -> Result<(MfgIncident, MfgWorkflowGraph), MfgRepositoryError> {
        let mut incident = incident.clone();
        let mut graph = MfgWorkflowGraph::for_incident(&incident)?;
        graph.attach_evidence_packet(packet)?;
        // Creating an incident from a structured evidence packet is the
        // completion of its planning step. Evidence research and selected
        // skills can now proceed concurrently; governance review remains a
        // later, explicit consumer of those outputs.
        graph.set_node_terminal_result(
            "planner",
            "incident workflow initialized from structured evidence packet",
        )?;
        incident.workflow_graph_id = Some(graph.workflow_id.clone());
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction()?;
        upsert_incident(&transaction, &incident)?;
        persist_workflow_graph(&transaction, &graph, None)?;
        transaction.commit()?;
        Ok((incident, graph))
    }

    pub fn plan_incident_workflow_skills(
        &self,
        incident_id: &str,
        plan: &crate::MfgSkillPlan,
    ) -> Result<MfgWorkflowGraph, MfgRepositoryError> {
        self.mutate_incident_workflow(incident_id, |graph| graph.plan_skills(plan))
    }

    pub fn complete_incident_workflow_skill(
        &self,
        incident_id: &str,
        run: &MfgSkillRun,
    ) -> Result<MfgWorkflowGraph, MfgRepositoryError> {
        self.mutate_incident_workflow(incident_id, |graph| graph.complete_skill(run))
    }

    pub fn record_skill_run_and_complete_workflow(
        &self,
        run: &MfgSkillRun,
    ) -> Result<(MfgSkillRun, MfgWorkflowGraph), MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction()?;
        let run = insert_skill_execution(&transaction, run)?;
        let mut graph = find_workflow_graph(&transaction, "incident_id", &run.incident_id)?
            .ok_or_else(|| {
                MfgRepositoryError::NotFound(format!("workflow for {}", run.incident_id))
            })?;
        let expected_revision = graph.revision;
        graph.complete_skill(&run)?;
        persist_workflow_graph(&transaction, &graph, Some(expected_revision))?;
        transaction.commit()?;
        Ok((run, graph))
    }

    fn mutate_incident_workflow(
        &self,
        incident_id: &str,
        mutate: impl FnOnce(&mut MfgWorkflowGraph) -> Result<(), MfgWorkflowGraphError>,
    ) -> Result<MfgWorkflowGraph, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction()?;
        let mut graph = find_workflow_graph(&transaction, "incident_id", incident_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(format!("workflow for {incident_id}")))?;
        let expected_revision = graph.revision;
        mutate(&mut graph)?;
        persist_workflow_graph(&transaction, &graph, Some(expected_revision))?;
        transaction.commit()?;
        Ok(graph)
    }

    pub fn get_workflow_graph(
        &self,
        workflow_id: &str,
    ) -> Result<Option<MfgWorkflowGraph>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_workflow_graph(&connection, "workflow_id", workflow_id)
    }

    pub fn workflow_graph_for_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgWorkflowGraph>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_workflow_graph(&connection, "incident_id", incident_id)
    }

    pub fn workflow_graph_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<MfgWorkflowGraph>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_workflow_graph(&connection, "task_id", task_id)
    }

    pub fn list_workflow_graphs(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgWorkflowGraph>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_workflow_graphs(&connection, limit)
    }

    pub fn analyze_incident(
        &self,
        incident_id: &str,
    ) -> Result<MfgOperationalAnalysis, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut incident = find_incident(&connection, incident_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(incident_id.to_string()))?;
        let packet_id = incident
            .evidence_packet_id
            .clone()
            .ok_or_else(|| MfgRepositoryError::NotFound("incident evidence packet".to_string()))?;
        let mut packet = find_evidence_packet(&connection, &packet_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(packet_id.clone()))?;
        let analysis = MfgOperationalAnalysis::from_evidence(incident_id, &packet);

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
    ) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_analysis(&connection, analysis_id)
    }

    pub fn latest_analysis_for_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        latest_analysis_for_incident(&connection, incident_id)
    }

    pub fn execute_recommended_action(
        &self,
        analysis_id: &str,
        action_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let analysis = find_analysis(&connection, analysis_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(analysis_id.to_string()))?;
        let action = analysis
            .recommended_actions
            .iter()
            .find(|action| action.action_id == action_id)
            .cloned()
            .ok_or_else(|| MfgRepositoryError::NotFound(action_id.to_string()))?;
        let execution = MfgActionExecution::from_action(&analysis, &action, request);
        insert_execution(&connection, &execution)?;
        Ok(execution)
    }

    pub fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgActionExecution>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_execution(&connection, execution_id)
    }

    pub fn record_skill_run(&self, run: &MfgSkillRun) -> Result<MfgSkillRun, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        insert_skill_execution(&connection, run)
    }

    pub fn get_skill_run(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgSkillRun>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_skill_execution(&connection, execution_id)
    }

    pub fn list_skill_runs_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_skill_executions_for_incident(&connection, incident_id, limit)
    }

    pub fn list_recent_skill_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_recent_skill_executions(&connection, limit)
    }

    pub fn list_executions_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_executions_for_incident(&connection, incident_id, limit)
    }

    pub fn list_recent_action_executions(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_recent_executions(&connection, limit)
    }

    pub fn attach_cross_plane_receipt(
        &self,
        execution_id: &str,
        receipt: MfgCrossPlaneBridgeReceipt,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut execution = find_execution(&connection, execution_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(execution_id.to_string()))?;
        execution.attach_cross_plane_receipt(receipt);
        insert_execution(&connection, &execution)?;
        Ok(execution)
    }

    pub fn record_execution_feedback(
        &self,
        execution_id: &str,
        feedback: MfgActionFeedback,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut execution = find_execution(&connection, execution_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(execution_id.to_string()))?;
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

    pub fn promote_incident_to_memory_case(
        &self,
        incident_id: &str,
    ) -> Result<MfgCasePromotion, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let incident = find_incident(&connection, incident_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(incident_id.to_string()))?;
        let analysis = latest_analysis_for_incident(&connection, incident_id)?;
        let packet = incident
            .evidence_packet_id
            .as_deref()
            .map(|packet_id| find_evidence_packet(&connection, packet_id))
            .transpose()?
            .flatten();
        let executions = list_executions_for_incident(&connection, incident_id, 20)?;
        let mut memory_case = MfgMemoryCase::from_closed_loop(
            &incident,
            analysis.as_ref(),
            packet.as_ref(),
            &executions,
        );
        let playbook = MfgPlaybook::from_memory_case(&memory_case, analysis.as_ref());
        memory_case.playbook_id = Some(playbook.playbook_id.clone());
        insert_memory_case(&connection, &memory_case)?;
        insert_playbook(&connection, &playbook)?;
        Ok(MfgCasePromotion {
            memory_case,
            playbook,
        })
    }

    pub fn get_memory_case(
        &self,
        case_id: &str,
    ) -> Result<Option<MfgMemoryCase>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_memory_case(&connection, case_id)
    }

    pub fn search_memory_cases(
        &self,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgMemoryCase>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        search_memory_cases(&connection, query, limit)
    }

    pub fn upsert_playbook(
        &self,
        playbook: &MfgPlaybook,
    ) -> Result<MfgPlaybook, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        insert_playbook(&connection, playbook)?;
        Ok(playbook.clone())
    }

    pub fn get_playbook(
        &self,
        playbook_id: &str,
    ) -> Result<Option<MfgPlaybook>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_playbook(&connection, playbook_id)
    }

    pub fn recommend_playbooks_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgPlaybook>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let incident = find_incident(&connection, incident_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(incident_id.to_string()))?;
        let analysis = latest_analysis_for_incident(&connection, incident_id)?;
        let packet = incident
            .evidence_packet_id
            .as_deref()
            .map(|packet_id| find_evidence_packet(&connection, packet_id))
            .transpose()?
            .flatten();
        let probe =
            MfgMemoryCase::from_closed_loop(&incident, analysis.as_ref(), packet.as_ref(), &[]);
        recommend_playbooks(&connection, &probe.metric_keys, &probe.entity_refs, limit)
    }
}

fn initialize_schema(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute(
        "CREATE TABLE IF NOT EXISTS matrix_schema (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            schema_version INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )",
        [],
    )?;
    connection.execute(
        r"INSERT INTO matrix_schema (id, schema_version, updated_at)
        VALUES (1, ?1, datetime('now'))
        ON CONFLICT(id) DO UPDATE SET
            schema_version = CASE
                WHEN matrix_schema.schema_version < excluded.schema_version
                THEN excluded.schema_version
                ELSE matrix_schema.schema_version
            END,
            updated_at = excluded.updated_at",
        [matrix_core::MATRIX_SCHEMA_VERSION],
    )?;
    connection.execute_batch(
        r"

        CREATE TABLE IF NOT EXISTS mfg_cockpit_profile (
            profile_id TEXT PRIMARY KEY,
            owner_ref TEXT NOT NULL,
            profile_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_cockpit_profile_owner
            ON mfg_cockpit_profile(owner_ref, updated_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_cockpit_report (
            report_id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            owner_ref TEXT NOT NULL,
            status TEXT NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_cockpit_report_profile
            ON mfg_cockpit_report(profile_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_alert_rule (
            rule_id TEXT PRIMARY KEY,
            owner_ref TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            revision INTEGER NOT NULL,
            rule_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_alert_rule_owner
            ON mfg_alert_rule(owner_ref, enabled, updated_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_alert_occurrence (
            occurrence_id TEXT PRIMARY KEY,
            rule_id TEXT NOT NULL,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            occurrence_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_alert_occurrence_status
            ON mfg_alert_occurrence(status, updated_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_alert_subscription (
            subscription_id TEXT PRIMARY KEY,
            rule_id TEXT NOT NULL,
            subscriber_ref TEXT NOT NULL,
            revision INTEGER NOT NULL,
            subscription_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_alert_subscription_rule
            ON mfg_alert_subscription(rule_id, subscriber_ref);

        CREATE TABLE IF NOT EXISTS mfg_assignment (
            assignment_id TEXT PRIMARY KEY,
            task_ref TEXT NOT NULL,
            workflow_id TEXT,
            incident_id TEXT,
            assignee_ref TEXT NOT NULL,
            status TEXT NOT NULL,
            visibility TEXT NOT NULL,
            revision INTEGER NOT NULL,
            assignment_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_assignment_assignee
            ON mfg_assignment(assignee_ref, status, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_mfg_assignment_incident
            ON mfg_assignment(incident_id, updated_at DESC) WHERE incident_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_mfg_assignment_task_assignee
            ON mfg_assignment(task_ref, assignee_ref);

        CREATE TABLE IF NOT EXISTS mfg_command_receipt (
            idempotency_key TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            subject_ref TEXT NOT NULL,
            receipt_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mfg_projection_event (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            subject_ref TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_projection_event_cursor
            ON mfg_projection_event(event_id);

        CREATE TABLE IF NOT EXISTS matrix_entity (
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
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_type
            ON matrix_entity(entity_type, canonical_key);

        CREATE TABLE IF NOT EXISTS matrix_entity_source_key (
            source_system TEXT NOT NULL,
            source_key TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            source_ref TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY(source_system, source_key),
            FOREIGN KEY(entity_id) REFERENCES matrix_entity(entity_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_source_entity
            ON matrix_entity_source_key(entity_id);

        CREATE TABLE IF NOT EXISTS matrix_relation (
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
            FOREIGN KEY(from_entity_id) REFERENCES matrix_entity(entity_id) ON DELETE CASCADE,
            FOREIGN KEY(to_entity_id) REFERENCES matrix_entity(entity_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_relation_from
            ON matrix_relation(from_entity_id, relation_type);
        CREATE INDEX IF NOT EXISTS idx_matrix_relation_to
            ON matrix_relation(to_entity_id, relation_type);

        CREATE TABLE IF NOT EXISTS matrix_fact (
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
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_type ON matrix_fact(fact_type);
        CREATE INDEX IF NOT EXISTS idx_matrix_fact_snapshot ON matrix_fact(snapshot_id);

        CREATE TABLE IF NOT EXISTS matrix_attention_item (
            attention_id TEXT PRIMARY KEY,
            priority_score REAL NOT NULL,
            status TEXT NOT NULL,
            attention_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_attention_priority
            ON matrix_attention_item(priority_score DESC, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_evidence_packet (
            packet_id TEXT PRIMARY KEY,
            attention_id TEXT,
            packet_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matrix_quality_gate (
            gate_id TEXT PRIMARY KEY,
            target_ref TEXT NOT NULL,
            gate_type TEXT NOT NULL,
            decision TEXT NOT NULL,
            score REAL NOT NULL,
            gate_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_quality_gate_target
            ON matrix_quality_gate(target_ref, created_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_metric_definition (
            metric_id TEXT PRIMARY KEY,
            definition_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matrix_metric_state (
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
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_state_lookup
            ON matrix_metric_state(metric_id, entity_scope, period, computed_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_metric_dependency (
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
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_dependency_upstream
            ON matrix_metric_dependency(upstream_metric_id, downstream_metric_id);
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_dependency_downstream
            ON matrix_metric_dependency(downstream_metric_id, upstream_metric_id);

        CREATE TABLE IF NOT EXISTS matrix_compute_job (
            job_id TEXT PRIMARY KEY,
            trigger_fact_type TEXT NOT NULL,
            status TEXT NOT NULL,
            priority REAL NOT NULL,
            job_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_compute_job_status
            ON matrix_compute_job(status, priority DESC, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_matrix_compute_job_fact_type
            ON matrix_compute_job(trigger_fact_type, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_change_event (
            change_id TEXT PRIMARY KEY,
            metric_id TEXT,
            entity_ref TEXT NOT NULL,
            period TEXT NOT NULL,
            delta REAL NOT NULL,
            severity_hint TEXT NOT NULL,
            change_json TEXT NOT NULL,
            detected_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_change_detected
            ON matrix_change_event(detected_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_incident (
            incident_id TEXT PRIMARY KEY,
            attention_id TEXT,
            evidence_packet_id TEXT,
            task_id TEXT,
            workflow_graph_id TEXT,
            status TEXT NOT NULL,
            incident_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_incident_updated
            ON mfg_incident(updated_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_operational_analysis (
            analysis_id TEXT PRIMARY KEY,
            incident_id TEXT NOT NULL,
            evidence_packet_id TEXT NOT NULL,
            status TEXT NOT NULL,
            confidence REAL NOT NULL,
            analysis_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_analysis_incident
            ON mfg_operational_analysis(incident_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_action_execution (
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
        CREATE INDEX IF NOT EXISTS idx_mfg_action_execution_analysis
            ON mfg_action_execution(analysis_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_mfg_action_execution_incident
            ON mfg_action_execution(incident_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_memory_case (
            case_id TEXT PRIMARY KEY,
            incident_id TEXT NOT NULL,
            problem_signature TEXT NOT NULL,
            outcome TEXT NOT NULL,
            memory_case_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_memory_case_incident
            ON mfg_memory_case(incident_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_mfg_memory_case_signature
            ON mfg_memory_case(problem_signature);

        CREATE TABLE IF NOT EXISTS mfg_playbook (
            playbook_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            scenario TEXT NOT NULL,
            playbook_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_playbook_domain
            ON mfg_playbook(domain, scenario);

        CREATE TABLE IF NOT EXISTS matrix_source_pack (
            source_pack_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            access_mode TEXT NOT NULL,
            refresh_mode TEXT NOT NULL,
            source_pack_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_source_pack_source
            ON matrix_source_pack(source_name, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_data_plane_watermark (
            source_ref TEXT NOT NULL,
            fact_type TEXT NOT NULL,
            partition_ref TEXT NOT NULL,
            high_watermark TEXT NOT NULL,
            last_batch_id TEXT NOT NULL,
            watermark_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(source_ref, fact_type, partition_ref)
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_data_plane_watermark_updated
            ON matrix_data_plane_watermark(updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_connector_run (
            run_id TEXT PRIMARY KEY,
            source_pack_id TEXT NOT NULL,
            connector_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            run_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_connector_run_source
            ON matrix_connector_run(source_pack_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_matrix_connector_run_status
            ON matrix_connector_run(status, updated_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_ontology_pack (
            ontology_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            version TEXT NOT NULL,
            pack_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matrix_entity_match_candidate (
            candidate_id TEXT PRIMARY KEY,
            left_entity_id TEXT NOT NULL,
            right_entity_id TEXT NOT NULL,
            confidence REAL NOT NULL,
            status TEXT NOT NULL,
            candidate_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_match_candidate_entities
            ON matrix_entity_match_candidate(left_entity_id, right_entity_id);

        CREATE TABLE IF NOT EXISTS matrix_entity_conflict_decision (
            decision_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            survivor_entity_id TEXT NOT NULL,
            retired_entity_id TEXT NOT NULL,
            decision_json TEXT NOT NULL,
            decided_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_entity_conflict_candidate
            ON matrix_entity_conflict_decision(candidate_id, decided_at DESC);

        CREATE TABLE IF NOT EXISTS matrix_metric_snapshot (
            snapshot_id TEXT PRIMARY KEY,
            scope_ref TEXT NOT NULL,
            metric_ids_json TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_matrix_metric_snapshot_scope
            ON matrix_metric_snapshot(scope_ref, created_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_skill_execution (
            execution_id TEXT PRIMARY KEY,
            incident_id TEXT NOT NULL,
            skill_id TEXT NOT NULL,
            status TEXT NOT NULL,
            execution_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_skill_execution_incident
            ON mfg_skill_execution(incident_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_mfg_skill_execution_skill
            ON mfg_skill_execution(skill_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS mfg_workflow_graph (
            workflow_id TEXT PRIMARY KEY,
            incident_id TEXT NOT NULL UNIQUE,
            task_id TEXT,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            graph_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mfg_workflow_graph_incident
            ON mfg_workflow_graph(incident_id);
        CREATE INDEX IF NOT EXISTS idx_mfg_workflow_graph_task
            ON mfg_workflow_graph(task_id) WHERE task_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_mfg_workflow_graph_status
            ON mfg_workflow_graph(status, updated_at DESC);",
    )?;
    migrate_mfg_incident_workflow_column(connection)
}

fn migrate_mfg_incident_workflow_column(connection: &Connection) -> rusqlite::Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(mfg_incident)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if columns.iter().any(|column| column == "agent_graph_id")
        && !columns.iter().any(|column| column == "workflow_graph_id")
    {
        connection.execute_batch(
            "ALTER TABLE mfg_incident RENAME COLUMN agent_graph_id TO workflow_graph_id;",
        )?;
    }
    Ok(())
}

fn schema_version(connection: &Connection) -> rusqlite::Result<i64> {
    connection.query_row(
        "SELECT schema_version FROM matrix_schema WHERE id = 1",
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

fn workflow_graph_revision(
    connection: &Connection,
    workflow_id: &str,
) -> rusqlite::Result<Option<u64>> {
    connection
        .query_row(
            "SELECT revision FROM mfg_workflow_graph WHERE workflow_id = ?1",
            params![workflow_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map(|revision| revision.map(|value| value as u64))
}

fn persist_workflow_graph(
    connection: &Connection,
    graph: &MfgWorkflowGraph,
    expected_revision: Option<u64>,
) -> Result<(), MfgRepositoryError> {
    graph.validate()?;
    let graph_json = serde_json::to_string(graph)?;
    let changed = if let Some(expected_revision) = expected_revision {
        connection.execute(
            r"UPDATE mfg_workflow_graph SET
                incident_id = ?2,
                task_id = ?3,
                status = ?4,
                revision = ?5,
                graph_json = ?6,
                updated_at = ?7
              WHERE workflow_id = ?1 AND revision = ?8",
            params![
                graph.workflow_id,
                graph.incident_id,
                graph.task_id,
                graph.status.as_str(),
                graph.revision as i64,
                graph_json,
                graph.updated_at.to_rfc3339(),
                expected_revision as i64,
            ],
        )?
    } else {
        connection.execute(
            r"INSERT OR IGNORE INTO mfg_workflow_graph (
            workflow_id, incident_id, task_id, status, revision,
            graph_json, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                graph.workflow_id,
                graph.incident_id,
                graph.task_id,
                graph.status.as_str(),
                graph.revision as i64,
                graph_json,
                graph.created_at.to_rfc3339(),
                graph.updated_at.to_rfc3339(),
            ],
        )?
    };
    if changed == 1 {
        return Ok(());
    }
    Err(MfgRepositoryError::WorkflowRevisionConflict {
        workflow_id: graph.workflow_id.clone(),
        expected: expected_revision,
        actual: workflow_graph_revision(connection, &graph.workflow_id)?,
    })
}

fn find_workflow_graph(
    connection: &Connection,
    column: &str,
    value: &str,
) -> Result<Option<MfgWorkflowGraph>, MfgRepositoryError> {
    debug_assert!(matches!(column, "workflow_id" | "incident_id" | "task_id"));
    let sql = format!("SELECT graph_json FROM mfg_workflow_graph WHERE {column} = ?1 LIMIT 1");
    connection
        .query_row(&sql, params![value], |row| row.get::<_, String>(0))
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_workflow_graphs(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MfgWorkflowGraph>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT graph_json
          FROM mfg_workflow_graph
          ORDER BY updated_at DESC, workflow_id ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit.clamp(1, 500) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| {
        serde_json::from_str::<MfgWorkflowGraph>(&row?).map_err(MfgRepositoryError::from)
    })
    .collect()
}

fn upsert_cockpit_profile(
    connection: &Connection,
    profile: &MfgCockpitProfile,
    expected_revision: Option<u64>,
) -> Result<MfgCockpitProfile, MfgRepositoryError> {
    let mut profile = profile.clone();
    if let Some(existing) = find_cockpit_profile(connection, &profile.profile_id)? {
        if expected_revision != Some(existing.revision) {
            return Err(MfgRepositoryError::RevisionConflict {
                domain: "cockpit_profile".to_string(),
                subject_id: profile.profile_id.clone(),
                expected: expected_revision,
                actual: Some(existing.revision),
            });
        }
        profile.created_at = existing.created_at;
        profile.revision = existing.revision.saturating_add(1);
    } else if expected_revision.is_some_and(|revision| revision != 0) {
        return Err(MfgRepositoryError::RevisionConflict {
            domain: "cockpit_profile".to_string(),
            subject_id: profile.profile_id.clone(),
            expected: expected_revision,
            actual: None,
        });
    }
    profile.normalize_legacy();
    validate_cockpit_profile(&profile)?;
    profile.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO mfg_cockpit_profile (
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
    append_projection_event(
        connection,
        "cockpit",
        &format!("mfg:cockpit-profile:{}", profile.profile_id),
        "profile.upserted",
        serde_json::to_value(&profile)?,
    )?;
    Ok(profile)
}

fn find_cockpit_profile(
    connection: &Connection,
    profile_id: &str,
) -> Result<Option<MfgCockpitProfile>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT profile_json FROM mfg_cockpit_profile WHERE profile_id = ?1",
            params![profile_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| {
            let mut profile: MfgCockpitProfile = serde_json::from_str(&json)?;
            profile.normalize_legacy();
            Ok(profile)
        })
        .transpose()
}

fn validate_cockpit_profile(profile: &MfgCockpitProfile) -> Result<(), MfgRepositoryError> {
    if !(1..=24).contains(&profile.layout.columns)
        || profile.layout.row_height == 0
        || profile.layout.gap > 96
    {
        return Err(MfgRepositoryError::CommandRejected(
            "dashboard layout is outside supported bounds".to_string(),
        ));
    }
    let catalog = mfg_widget_catalog()
        .into_iter()
        .map(|item| (item.definition_id.clone(), item))
        .collect::<BTreeMap<_, _>>();
    let mut instance_ids = BTreeSet::new();
    for instance in &profile.widget_instances {
        if !instance_ids.insert(&instance.instance_id) || instance.instance_id.trim().is_empty() {
            return Err(MfgRepositoryError::CommandRejected(
                "widget instance ids must be unique and non-empty".to_string(),
            ));
        }
        let definition = catalog.get(&instance.definition_id).ok_or_else(|| {
            MfgRepositoryError::CommandRejected(format!(
                "widget definition `{}` is not registered",
                instance.definition_id
            ))
        })?;
        if !(instance.config.is_null() || instance.config.is_object())
            || !(instance.query.is_null() || instance.query.is_object())
        {
            return Err(MfgRepositoryError::CommandRejected(
                "widget config and query must be JSON objects".to_string(),
            ));
        }
        let placement = &instance.placement;
        if placement.width < definition.min_width
            || placement.width > definition.max_width
            || placement.height < definition.min_height
            || placement.height > definition.max_height
            || placement.x.saturating_add(placement.width) > profile.layout.columns
        {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "widget `{}` placement is outside its definition or dashboard bounds",
                instance.instance_id
            )));
        }
    }
    Ok(())
}

fn list_cockpit_profiles(
    connection: &Connection,
    cadence: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgCockpitProfile>, MfgRepositoryError> {
    let cadence = cadence
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let mut statement = connection.prepare(
        "SELECT profile_json FROM mfg_cockpit_profile ORDER BY updated_at DESC, profile_id ASC",
    )?;
    let profiles = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|row| {
            let json = row?;
            let mut profile = serde_json::from_str::<MfgCockpitProfile>(&json)?;
            profile.normalize_legacy();
            Ok(profile)
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
    report: &MfgCockpitReportSnapshot,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO mfg_cockpit_report (
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
) -> Result<Option<MfgCockpitReportSnapshot>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT report_json FROM mfg_cockpit_report WHERE report_id = ?1",
            params![report_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn render_cockpit_projection(
    connection: &Connection,
    mut profile: MfgCockpitProfile,
) -> Result<MfgCockpitProjection, MfgRepositoryError> {
    profile.normalize_legacy();
    let catalog = mfg_widget_catalog()
        .into_iter()
        .map(|definition| (definition.definition_id.clone(), definition))
        .collect::<BTreeMap<_, _>>();
    let widgets = profile
        .widget_instances
        .iter()
        .filter(|instance| instance.visible)
        .map(|instance| match catalog.get(&instance.definition_id) {
            Some(definition) => render_cockpit_widget(connection, &profile, instance, definition)
                .unwrap_or_else(|error| {
                    MfgCockpitWidget::unavailable(instance, Some(definition), error.to_string())
                }),
            None => {
                MfgCockpitWidget::unavailable(instance, None, "widget definition is not registered")
            }
        })
        .collect::<Vec<_>>();
    let unavailable = widgets
        .iter()
        .filter(|widget| widget.status == "unavailable")
        .count();
    let urgent = widgets
        .iter()
        .filter(|widget| matches!(widget.status.as_str(), "critical" | "fail" | "escalated"))
        .count();
    let summary = format!(
        "profile={} widgets={} urgent={} unavailable={}",
        profile.profile_id,
        widgets.len(),
        urgent,
        unavailable
    );
    Ok(MfgCockpitProjection {
        projection_id: format!("cockpit-projection-{}", uuid::Uuid::new_v4()),
        profile,
        widgets,
        summary,
        generated_at: Utc::now(),
    })
}

fn render_cockpit_widget(
    connection: &Connection,
    profile: &MfgCockpitProfile,
    instance: &MfgWidgetInstance,
    definition: &MfgWidgetDefinition,
) -> Result<MfgCockpitWidget, MfgRepositoryError> {
    let limit = instance
        .query
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(20)
        .clamp(1, 100) as usize;
    let (status, priority, data, source_refs) = match definition.definition_id.as_str() {
        "attention.queue" | "risk.matrix" => {
            let items = list_attention(connection, limit * 2)?
                .into_iter()
                .filter(|item| attention_matches_profile(item, profile))
                .take(limit)
                .collect::<Vec<_>>();
            let status = if items
                .iter()
                .any(|item| matches!(item.severity, MatrixSeverity::Critical))
            {
                "critical"
            } else if items.is_empty() {
                "clear"
            } else {
                "watch"
            };
            let priority = items
                .iter()
                .map(|item| item.priority_score)
                .fold(0.0_f32, f32::max);
            let refs = items
                .iter()
                .map(|item| format!("matrix:attention:{}", item.attention_id))
                .collect();
            (
                status,
                priority,
                serde_json::json!({ "count": items.len(), "items": items }),
                refs,
            )
        }
        "quality.gates" => {
            let gates = list_recent_quality_gates(connection, limit)?;
            let pass = gates.iter().filter(|gate| gate.decision == "pass").count();
            let review = gates
                .iter()
                .filter(|gate| gate.decision == "review")
                .count();
            let fail = gates.iter().filter(|gate| gate.decision == "fail").count();
            (
                if fail > 0 {
                    "fail"
                } else if review > 0 {
                    "review"
                } else if pass > 0 {
                    "pass"
                } else {
                    "empty"
                },
                (fail as f32 + review as f32 * 0.65).min(1.0),
                serde_json::json!({ "pass_count": pass, "review_count": review, "fail_count": fail, "recent": gates }),
                Vec::new(),
            )
        }
        "action.executions" => {
            let executions = list_recent_executions(connection, limit)?;
            let active = executions
                .iter()
                .filter(|execution| {
                    !matches!(
                        execution.status.as_str(),
                        "feedback_resolved" | "feedback_rejected"
                    )
                })
                .count();
            (
                if active > 0 { "active" } else { "clear" },
                (active as f32 / 5.0).min(1.0),
                serde_json::json!({ "active_count": active, "recent": executions }),
                Vec::new(),
            )
        }
        "focus.summary" => (
            if profile.thresholds.is_null() {
                "empty"
            } else {
                "configured"
            },
            0.2,
            serde_json::json!({ "focus_refs": profile.focus_refs, "focus_metric_ids": profile.focus_metric_ids, "thresholds": profile.thresholds, "filters": profile.global_filters, "cadence": profile.cadence }),
            Vec::new(),
        ),
        "incident.queue" => {
            let incidents = list_incidents(connection, limit)?;
            let refs = incidents
                .iter()
                .map(|item| format!("mfg:incident:{}", item.incident_id))
                .collect();
            (
                if incidents.is_empty() {
                    "clear"
                } else {
                    "active"
                },
                (incidents.len() as f32 / 10.0).min(1.0),
                serde_json::json!({ "count": incidents.len(), "items": incidents }),
                refs,
            )
        }
        "workflow.progress" => {
            let graphs = list_workflow_graphs(connection, limit)?;
            let refs = graphs
                .iter()
                .map(|item| format!("mfg:workflow:{}", item.workflow_id))
                .collect();
            (
                if graphs.is_empty() { "empty" } else { "active" },
                0.4,
                serde_json::json!({ "count": graphs.len(), "items": graphs }),
                refs,
            )
        }
        "metric.lineage" => {
            let metric_id = profile.focus_metric_ids.first().ok_or_else(|| {
                MfgRepositoryError::CommandRejected(
                    "metric.lineage requires a focused metric".to_string(),
                )
            })?;
            let lineage = build_metric_lineage(connection, metric_id, 4)?;
            (
                "ready",
                0.3,
                serde_json::to_value(&lineage)?,
                vec![format!("matrix:metric:{metric_id}")],
            )
        }
        "entity.impact" => {
            let entity_ref = profile.focus_refs.first().ok_or_else(|| {
                MfgRepositoryError::CommandRejected(
                    "entity.impact requires a focused entity".to_string(),
                )
            })?;
            let trace = build_impact_trace(connection, entity_ref, 4)?;
            (
                "ready",
                0.3,
                serde_json::to_value(&trace)?,
                vec![format!("matrix:entity:{entity_ref}")],
            )
        }
        "data.freshness" => {
            let watermarks = list_data_plane_watermarks(connection, limit)?;
            (
                if watermarks.is_empty() {
                    "unavailable"
                } else {
                    "ready"
                },
                0.25,
                serde_json::json!({ "count": watermarks.len(), "items": watermarks }),
                Vec::new(),
            )
        }
        "kpi.summary" | "trend.metrics" => {
            let states = list_recent_metric_states(connection, profile, limit)?;
            let refs = states
                .iter()
                .map(|state| format!("matrix:metric-state:{}", state.state_id))
                .collect();
            (
                if states.is_empty() { "empty" } else { "ready" },
                0.35,
                serde_json::json!({ "count": states.len(), "items": states }),
                refs,
            )
        }
        "report.delivery" => (
            "ready",
            0.1,
            serde_json::json!({ "profile_id": profile.profile_id, "cadence": profile.cadence }),
            vec![format!("mfg:cockpit-profile:{}", profile.profile_id)],
        ),
        other => {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "renderer is not implemented for {other}"
            )))
        }
    };
    Ok(MfgCockpitWidget {
        widget_id: instance.instance_id.clone(),
        widget_type: definition.renderer.clone(),
        title: definition.title.clone(),
        status: status.to_string(),
        priority_score: priority,
        data,
        source_refs,
        instance_id: instance.instance_id.clone(),
        definition_id: definition.definition_id.clone(),
        renderer_version: definition.renderer_version,
        freshness: serde_json::json!({ "status": "current", "generated_at": Utc::now() }),
        error: None,
    })
}

fn list_recent_metric_states(
    connection: &Connection,
    profile: &MfgCockpitProfile,
    limit: usize,
) -> Result<Vec<MatrixMetricState>, MfgRepositoryError> {
    let mut statement = connection
        .prepare("SELECT state_json FROM matrix_metric_state ORDER BY computed_at DESC LIMIT ?1")?;
    let rows = statement.query_map(params![(limit * 4).clamp(1, 400) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixMetricState>(&row?)?))
        .filter(|result| {
            result.as_ref().map_or(true, |state| {
                profile.focus_metric_ids.is_empty()
                    || profile.focus_metric_ids.contains(&state.metric_id)
            })
        })
        .take(limit)
        .collect()
}

fn attention_matches_profile(item: &MatrixAttentionItem, profile: &MfgCockpitProfile) -> bool {
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
    item.metric_refs.iter().any(|metric_ref| {
        profile
            .focus_metric_ids
            .iter()
            .any(|metric_id| metric_ref == metric_id)
    })
}

fn ensure_revision(
    domain: &str,
    subject_id: &str,
    expected: u64,
    actual: u64,
) -> Result<(), MfgRepositoryError> {
    if expected == actual {
        return Ok(());
    }
    Err(MfgRepositoryError::RevisionConflict {
        domain: domain.to_string(),
        subject_id: subject_id.to_string(),
        expected: Some(expected),
        actual: Some(actual),
    })
}

fn append_projection_event(
    connection: &Connection,
    domain: &str,
    subject_ref: &str,
    event_type: &str,
    payload: Value,
) -> Result<u64, MfgRepositoryError> {
    let event = serde_json::json!({ "domain": domain, "event_type": event_type, "subject_ref": subject_ref, "payload": payload, "created_at": Utc::now() });
    connection.execute(
        "INSERT INTO mfg_projection_event (domain, subject_ref, event_type, event_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![domain, subject_ref, event_type, serde_json::to_string(&event)?, Utc::now().to_rfc3339()],
    )?;
    Ok(connection.last_insert_rowid() as u64)
}

fn find_command_receipt(
    connection: &Connection,
    key: &str,
    domain: &str,
    subject_ref: &str,
) -> Result<Option<MfgCommandReceipt>, MfgRepositoryError> {
    let value = connection.query_row(
        "SELECT domain, subject_ref, receipt_json FROM mfg_command_receipt WHERE idempotency_key = ?1",
        params![key], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
    ).optional()?;
    let Some((stored_domain, stored_subject, json)) = value else {
        return Ok(None);
    };
    if stored_domain != domain || stored_subject != subject_ref {
        return Err(MfgRepositoryError::CommandRejected(
            "idempotency key is already bound to another command subject".to_string(),
        ));
    }
    let mut receipt: MfgCommandReceipt = serde_json::from_str(&json)?;
    receipt.idempotent_replay = true;
    Ok(Some(receipt))
}

fn insert_command_receipt(
    connection: &Connection,
    receipt: &MfgCommandReceipt,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        "INSERT INTO mfg_command_receipt (idempotency_key, domain, subject_ref, receipt_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![receipt.idempotency_key, receipt.domain, receipt.subject_ref, serde_json::to_string(receipt)?, receipt.created_at.to_rfc3339()],
    )?;
    Ok(())
}

fn mutation_receipt(
    domain: &str,
    subject_ref: String,
    command: &str,
    actor_ref: &str,
    idempotency_key: &str,
    previous_revision: u64,
    current_revision: u64,
) -> Result<MfgCommandReceipt, MfgRepositoryError> {
    if actor_ref.trim().is_empty() || idempotency_key.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "actor and idempotency key are required".to_string(),
        ));
    }
    Ok(MfgCommandReceipt {
        receipt_id: format!("receipt-{}", uuid::Uuid::new_v4()),
        domain: domain.to_string(),
        subject_ref: subject_ref.clone(),
        command: command.to_string(),
        actor_ref: actor_ref.to_string(),
        idempotency_key: idempotency_key.to_string(),
        idempotent_replay: false,
        previous_revision,
        current_revision,
        audit_ref: format!("audit://mfg/{domain}/{}/{}", subject_ref.rsplit(':').next().unwrap_or("unknown"), current_revision),
        notification_refs: Vec::new(),
        created_at: Utc::now(),
    })
}

fn upsert_cockpit_profile_receipted(
    connection: &Connection,
    profile: &MfgCockpitProfile,
    expected_revision: Option<u64>,
    command: &str,
    actor_ref: &str,
    idempotency_key: &str,
) -> Result<(MfgCockpitProfile, MfgCommandReceipt), MfgRepositoryError> {
    let subject_ref = format!("mfg:cockpit-profile:{}", profile.profile_id);
    if let Some(receipt) = find_command_receipt(connection, idempotency_key, "cockpit", &subject_ref)? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let profile = find_cockpit_profile(connection, &profile.profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile.profile_id.clone()))?;
        return Ok((profile, receipt));
    }
    let previous_revision = find_cockpit_profile(connection, &profile.profile_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let profile = upsert_cockpit_profile(connection, profile, expected_revision)?;
    let receipt = mutation_receipt(
        "cockpit",
        subject_ref.clone(),
        command,
        actor_ref,
        idempotency_key,
        previous_revision,
        profile.revision,
    )?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(connection, "cockpit", &subject_ref, "profile.receipted", serde_json::json!({ "profile": profile, "receipt": receipt }))?;
    Ok((profile, receipt))
}

fn delete_cockpit_profile_receipted(
    connection: &Connection,
    profile_id: &str,
    expected_revision: u64,
    actor_ref: &str,
    idempotency_key: &str,
) -> Result<(Option<MfgCockpitProfile>, MfgCommandReceipt), MfgRepositoryError> {
    let subject_ref = format!("mfg:cockpit-profile:{profile_id}");
    if let Some(receipt) = find_command_receipt(connection, idempotency_key, "cockpit", &subject_ref)? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        return Ok((None, receipt));
    }
    let profile = find_cockpit_profile(connection, profile_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
    ensure_revision("cockpit_profile", profile_id, expected_revision, profile.revision)?;
    connection.execute("DELETE FROM mfg_cockpit_profile WHERE profile_id = ?1", params![profile_id])?;
    let receipt = mutation_receipt(
        "cockpit",
        subject_ref.clone(),
        "profile.delete",
        actor_ref,
        idempotency_key,
        profile.revision,
        profile.revision,
    )?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(connection, "cockpit", &subject_ref, "profile.deleted", serde_json::json!({ "profile": profile, "receipt": receipt }))?;
    Ok((Some(profile), receipt))
}

fn upsert_alert_rule_receipted(
    connection: &Connection,
    rule: &MfgAlertRule,
    expected_revision: Option<u64>,
    actor_ref: &str,
    idempotency_key: &str,
) -> Result<(MfgAlertRule, MfgCommandReceipt), MfgRepositoryError> {
    let subject_ref = format!("mfg:alert-rule:{}", rule.rule_id);
    if let Some(receipt) = find_command_receipt(connection, idempotency_key, "alert", &subject_ref)? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let rule = find_alert_rule(connection, &rule.rule_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(rule.rule_id.clone()))?;
        return Ok((rule, receipt));
    }
    let previous_revision = find_alert_rule(connection, &rule.rule_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let rule = upsert_alert_rule(connection, rule, expected_revision)?;
    let receipt = mutation_receipt(
        "alert",
        subject_ref.clone(),
        "rule.upsert",
        actor_ref,
        idempotency_key,
        previous_revision,
        rule.revision,
    )?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(connection, "alert", &subject_ref, "alert_rule.receipted", serde_json::json!({ "rule": rule, "receipt": receipt }))?;
    Ok((rule, receipt))
}

fn upsert_alert_subscription_receipted(
    connection: &Connection,
    subscription: &MfgAlertSubscription,
    expected_revision: Option<u64>,
    actor_ref: &str,
    idempotency_key: &str,
) -> Result<(MfgAlertSubscription, MfgCommandReceipt), MfgRepositoryError> {
    let subject_ref = format!("mfg:alert-subscription:{}", subscription.subscription_id);
    if let Some(receipt) = find_command_receipt(connection, idempotency_key, "alert", &subject_ref)? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let subscription = find_alert_subscription(connection, &subscription.subscription_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(subscription.subscription_id.clone()))?;
        return Ok((subscription, receipt));
    }
    let previous_revision = find_alert_subscription(connection, &subscription.subscription_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let subscription = upsert_alert_subscription(connection, subscription, expected_revision)?;
    let receipt = mutation_receipt(
        "alert",
        subject_ref.clone(),
        "subscription.upsert",
        actor_ref,
        idempotency_key,
        previous_revision,
        subscription.revision,
    )?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(connection, "alert", &subject_ref, "alert_subscription.receipted", serde_json::json!({ "subscription": subscription, "receipt": receipt }))?;
    Ok((subscription, receipt))
}

fn upsert_assignment_receipted(
    connection: &Connection,
    assignment: &MfgAssignment,
    expected_revision: Option<u64>,
    actor_ref: &str,
    idempotency_key: &str,
) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
    let subject_ref = format!("mfg:assignment:{}", assignment.assignment_id);
    if let Some(receipt) = find_command_receipt(connection, idempotency_key, "assignment", &subject_ref)? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let assignment = find_assignment(connection, &assignment.assignment_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(assignment.assignment_id.clone()))?;
        return Ok((assignment, receipt));
    }
    let previous_revision = find_assignment(connection, &assignment.assignment_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let assignment = upsert_assignment(connection, assignment, expected_revision)?;
    let receipt = mutation_receipt(
        "assignment",
        subject_ref.clone(),
        "assignment.upsert",
        actor_ref,
        idempotency_key,
        previous_revision,
        assignment.revision,
    )?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(connection, "assignment", &subject_ref, "assignment.receipted", serde_json::json!({ "assignment": assignment, "receipt": receipt }))?;
    Ok((assignment, receipt))
}

fn record_command_notifications(
    connection: &Connection,
    idempotency_key: &str,
    notification_refs: Vec<String>,
) -> Result<MfgCommandReceipt, MfgRepositoryError> {
    let value = connection
        .query_row(
            "SELECT receipt_json FROM mfg_command_receipt WHERE idempotency_key = ?1",
            params![idempotency_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| MfgRepositoryError::NotFound(idempotency_key.to_string()))?;
    let mut receipt: MfgCommandReceipt = serde_json::from_str(&value)?;
    receipt.notification_refs = notification_refs;
    connection.execute(
        "UPDATE mfg_command_receipt SET receipt_json = ?2 WHERE idempotency_key = ?1",
        params![idempotency_key, serde_json::to_string(&receipt)?],
    )?;
    append_projection_event(
        connection,
        &receipt.domain,
        &receipt.subject_ref,
        "notification.delivery_observed",
        serde_json::to_value(&receipt)?,
    )?;
    Ok(receipt)
}

fn upsert_alert_rule(
    connection: &Connection,
    rule: &MfgAlertRule,
    expected_revision: Option<u64>,
) -> Result<MfgAlertRule, MfgRepositoryError> {
    let mut rule = rule.clone();
    let existing = find_alert_rule(connection, &rule.rule_id)?;
    match existing {
        Some(existing) => {
            if expected_revision != Some(existing.revision) {
                return Err(MfgRepositoryError::RevisionConflict {
                    domain: "alert_rule".to_string(),
                    subject_id: rule.rule_id.clone(),
                    expected: expected_revision,
                    actual: Some(existing.revision),
                });
            }
            rule.created_at = existing.created_at;
            rule.revision = existing.revision.saturating_add(1);
        }
        None if expected_revision.is_some_and(|revision| revision != 0) => {
            return Err(MfgRepositoryError::RevisionConflict {
                domain: "alert_rule".to_string(),
                subject_id: rule.rule_id.clone(),
                expected: expected_revision,
                actual: None,
            });
        }
        None => {}
    }
    rule.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO mfg_alert_rule (rule_id, owner_ref, enabled, revision, rule_json, created_at, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
           ON CONFLICT(rule_id) DO UPDATE SET owner_ref=excluded.owner_ref, enabled=excluded.enabled, revision=excluded.revision, rule_json=excluded.rule_json, updated_at=excluded.updated_at",
        params![rule.rule_id, rule.owner_ref, rule.enabled, rule.revision as i64, serde_json::to_string(&rule)?, rule.created_at.to_rfc3339(), rule.updated_at.to_rfc3339()],
    )?;
    materialize_alert_occurrences(connection, &rule)?;
    append_projection_event(
        connection,
        "alert",
        &format!("mfg:alert-rule:{}", rule.rule_id),
        "alert_rule.upserted",
        serde_json::to_value(&rule)?,
    )?;
    Ok(rule)
}

fn find_alert_rule(
    connection: &Connection,
    rule_id: &str,
) -> Result<Option<MfgAlertRule>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT rule_json FROM mfg_alert_rule WHERE rule_id = ?1",
            params![rule_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_alert_rules(
    connection: &Connection,
    owner_ref: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgAlertRule>, MfgRepositoryError> {
    let mut statement = connection
        .prepare("SELECT rule_json FROM mfg_alert_rule ORDER BY updated_at DESC LIMIT ?1")?;
    let rows = statement.query_map(params![limit.clamp(1, 500) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MfgAlertRule>(&row?)?))
        .filter(|item| {
            item.as_ref().map_or(true, |rule| {
                owner_ref.is_none_or(|owner| rule.owner_ref == owner)
            })
        })
        .collect()
}

fn upsert_alert_subscription(
    connection: &Connection,
    subscription: &MfgAlertSubscription,
    expected_revision: Option<u64>,
) -> Result<MfgAlertSubscription, MfgRepositoryError> {
    if find_alert_rule(connection, &subscription.rule_id)?.is_none() {
        return Err(MfgRepositoryError::NotFound(subscription.rule_id.clone()));
    }
    let mut subscription = subscription.clone();
    let existing = find_alert_subscription(connection, &subscription.subscription_id)?;
    match existing {
        Some(existing) => {
            if expected_revision != Some(existing.revision) {
                return Err(MfgRepositoryError::RevisionConflict {
                    domain: "alert_subscription".to_string(),
                    subject_id: subscription.subscription_id.clone(),
                    expected: expected_revision,
                    actual: Some(existing.revision),
                });
            }
            subscription.created_at = existing.created_at;
            subscription.revision = existing.revision.saturating_add(1);
        }
        None if expected_revision.is_some_and(|revision| revision != 0) => {
            return Err(MfgRepositoryError::RevisionConflict {
                domain: "alert_subscription".to_string(),
                subject_id: subscription.subscription_id.clone(),
                expected: expected_revision,
                actual: None,
            })
        }
        None => {}
    }
    subscription.updated_at = Utc::now();
    connection.execute(r"INSERT INTO mfg_alert_subscription (subscription_id, rule_id, subscriber_ref, revision, subscription_json, created_at, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(subscription_id) DO UPDATE SET rule_id=excluded.rule_id, subscriber_ref=excluded.subscriber_ref, revision=excluded.revision, subscription_json=excluded.subscription_json, updated_at=excluded.updated_at",
        params![subscription.subscription_id, subscription.rule_id, subscription.subscriber_ref, subscription.revision as i64, serde_json::to_string(&subscription)?, subscription.created_at.to_rfc3339(), subscription.updated_at.to_rfc3339()])?;
    append_projection_event(
        connection,
        "alert",
        &format!("mfg:alert-subscription:{}", subscription.subscription_id),
        "alert_subscription.upserted",
        serde_json::to_value(&subscription)?,
    )?;
    Ok(subscription)
}

fn find_alert_subscription(
    connection: &Connection,
    subscription_id: &str,
) -> Result<Option<MfgAlertSubscription>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT subscription_json FROM mfg_alert_subscription WHERE subscription_id = ?1",
            params![subscription_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_alert_subscriptions(
    connection: &Connection,
    subscriber_ref: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgAlertSubscription>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT subscription_json FROM mfg_alert_subscription ORDER BY updated_at DESC LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit.clamp(1, 500) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MfgAlertSubscription>(&row?)?))
        .filter(|item| {
            item.as_ref().map_or(true, |subscription| {
                subscriber_ref.is_none_or(|filter| subscription.subscriber_ref == filter)
            })
        })
        .collect()
}

fn materialize_alert_occurrences(
    connection: &Connection,
    rule: &MfgAlertRule,
) -> Result<(), MfgRepositoryError> {
    if !rule.enabled {
        return Ok(());
    }
    for attention in list_attention(connection, 200)?.into_iter().filter(|item| {
        (rule.metric_refs.is_empty()
            || item
                .metric_refs
                .iter()
                .any(|value| rule.metric_refs.contains(value)))
            && (rule.entity_refs.is_empty()
                || item
                    .entity_ref
                    .as_ref()
                    .is_some_and(|value| rule.entity_refs.contains(value)))
    }) {
        let occurrence_id = format!(
            "alert-occurrence-{}-{}",
            rule.rule_id, attention.attention_id
        );
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM mfg_alert_occurrence WHERE occurrence_id = ?1)",
            params![occurrence_id],
            |row| row.get(0),
        )?;
        if exists {
            continue;
        }
        let occurrence = MfgAlertOccurrence {
            occurrence_id,
            rule_id: rule.rule_id.clone(),
            attention_ref: Some(format!("matrix:attention:{}", attention.attention_id)),
            incident_ref: None,
            status: "open".to_string(),
            severity: rule.severity.clone(),
            summary: attention.title,
            evidence_refs: attention.linked_changes,
            revision: 1,
            snoozed_until: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        save_alert_occurrence(connection, &occurrence)?;
        append_projection_event(
            connection,
            "alert",
            &format!("mfg:alert-occurrence:{}", occurrence.occurrence_id),
            "alert.opened",
            serde_json::to_value(&occurrence)?,
        )?;
    }
    Ok(())
}

fn save_alert_occurrence(
    connection: &Connection,
    occurrence: &MfgAlertOccurrence,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT INTO mfg_alert_occurrence (occurrence_id, rule_id, status, revision, occurrence_json, created_at, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
           ON CONFLICT(occurrence_id) DO UPDATE SET status=excluded.status, revision=excluded.revision, occurrence_json=excluded.occurrence_json, updated_at=excluded.updated_at",
        params![occurrence.occurrence_id, occurrence.rule_id, occurrence.status, occurrence.revision as i64, serde_json::to_string(occurrence)?, occurrence.created_at.to_rfc3339(), occurrence.updated_at.to_rfc3339()],
    )?;
    Ok(())
}

fn find_alert_occurrence(
    connection: &Connection,
    occurrence_id: &str,
) -> Result<Option<MfgAlertOccurrence>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT occurrence_json FROM mfg_alert_occurrence WHERE occurrence_id = ?1",
            params![occurrence_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_alert_occurrences(
    connection: &Connection,
    status: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgAlertOccurrence>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT occurrence_json FROM mfg_alert_occurrence ORDER BY updated_at DESC LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit.clamp(1, 500) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MfgAlertOccurrence>(&row?)?))
        .filter(|item| {
            item.as_ref().map_or(true, |occurrence| {
                status.is_none_or(|filter| occurrence.status == filter)
            })
        })
        .collect()
}

fn command_alert(
    connection: &Connection,
    occurrence_id: &str,
    input: MfgAlertCommandInput,
) -> Result<(MfgAlertOccurrence, MfgCommandReceipt), MfgRepositoryError> {
    let subject_ref = format!("mfg:alert-occurrence:{occurrence_id}");
    if let Some(receipt) =
        find_command_receipt(connection, &input.idempotency_key, "alert", &subject_ref)?
    {
        if receipt.actor_ref != input.actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let occurrence = find_alert_occurrence(connection, occurrence_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(occurrence_id.to_string()))?;
        return Ok((occurrence, receipt));
    }
    if input.actor_ref.trim().is_empty() || input.idempotency_key.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "actor and idempotency key are required".to_string(),
        ));
    }
    let mut occurrence = find_alert_occurrence(connection, occurrence_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(occurrence_id.to_string()))?;
    ensure_revision(
        "alert_occurrence",
        occurrence_id,
        input.expected_revision,
        occurrence.revision,
    )?;
    let previous_revision = occurrence.revision;
    occurrence.status = match input.command {
        MfgAlertCommand::Acknowledge => "acknowledged",
        MfgAlertCommand::Snooze => "snoozed",
        MfgAlertCommand::Resolve => "resolved",
        MfgAlertCommand::Escalate => "escalated",
    }
    .to_string();
    occurrence.snoozed_until = if matches!(input.command, MfgAlertCommand::Snooze) {
        input.until
    } else {
        None
    };
    occurrence.revision = occurrence.revision.saturating_add(1);
    occurrence.updated_at = Utc::now();
    save_alert_occurrence(connection, &occurrence)?;
    let command = format!("{:?}", input.command).to_lowercase();
    let receipt = MfgCommandReceipt {
        receipt_id: format!("receipt-{}", uuid::Uuid::new_v4()),
        domain: "alert".to_string(),
        subject_ref: subject_ref.clone(),
        command: command.clone(),
        actor_ref: input.actor_ref,
        idempotency_key: input.idempotency_key,
        idempotent_replay: false,
        previous_revision,
        current_revision: occurrence.revision,
        audit_ref: format!("audit://mfg/alert/{occurrence_id}/{}", occurrence.revision),
        notification_refs: Vec::new(),
        created_at: Utc::now(),
    };
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "alert",
        &subject_ref,
        &format!("alert.{command}"),
        serde_json::json!({ "occurrence": occurrence, "receipt": receipt, "reason": input.reason }),
    )?;
    Ok((occurrence, receipt))
}

fn build_forecasts(
    connection: &Connection,
    metric_refs: &[String],
    horizon: &str,
    limit: usize,
) -> Result<Vec<MfgForecastProjection>, MfgRepositoryError> {
    let profile = MfgCockpitProfile {
        profile_id: "forecast".to_string(),
        owner_ref: "system".to_string(),
        display_name: "forecast".to_string(),
        focus_refs: Vec::new(),
        focus_metric_ids: metric_refs.to_vec(),
        thresholds: Value::Null,
        template_id: "forecast".to_string(),
        cadence: "on_demand".to_string(),
        revision: 1,
        scope: Default::default(),
        layout: Default::default(),
        global_filters: Value::Null,
        widget_instances: Vec::new(),
        sharing_policy: Default::default(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let states = list_recent_metric_states(connection, &profile, limit)?;
    let mut forecasts = states.into_iter().map(|state| {
        let next = state.value + state.delta;
        MfgForecastProjection { forecast_id: format!("forecast-{}-{}", state.metric_id, state.state_id), metric_ref: state.metric_id.clone(), entity_ref: Some(state.entity_scope.clone()), status: "available".to_string(),
            horizon: horizon.to_string(), interval: "next_period".to_string(), confidence: Some((state.confidence * 0.85).clamp(0.0, 1.0)), method: "bounded_linear_delta".to_string(), generated_at: Utc::now(), expires_at: Utc::now() + chrono::Duration::hours(6),
            leading_signals: vec![MfgForecastSignal { signal_ref: format!("matrix:metric-state:{}", state.state_id), label: "latest metric delta".to_string(), direction: if state.delta >= 0.0 { "up" } else { "down" }.to_string(), weight: 1.0 }],
            evidence_refs: state.input_fact_refs.clone(), points: vec![serde_json::json!({ "period": state.period, "value": state.value, "kind": "observed" }), serde_json::json!({ "period": "next", "value": next, "kind": "forecast" })], unavailable_reason: None }
    }).collect::<Vec<_>>();
    for metric_ref in metric_refs
        .iter()
        .filter(|metric_ref| !forecasts.iter().any(|item| &item.metric_ref == *metric_ref))
    {
        forecasts.push(MfgForecastProjection {
            forecast_id: format!("forecast-unavailable-{metric_ref}"),
            metric_ref: metric_ref.clone(),
            entity_ref: None,
            status: "unavailable".to_string(),
            horizon: horizon.to_string(),
            interval: "unknown".to_string(),
            confidence: None,
            method: "unavailable".to_string(),
            generated_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(15),
            leading_signals: Vec::new(),
            evidence_refs: Vec::new(),
            points: Vec::new(),
            unavailable_reason: Some(
                "no metric state is available for the requested scope".to_string(),
            ),
        });
    }
    Ok(forecasts)
}

fn upsert_assignment(
    connection: &Connection,
    assignment: &MfgAssignment,
    expected_revision: Option<u64>,
) -> Result<MfgAssignment, MfgRepositoryError> {
    if assignment.task_ref.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment must reference an existing task".to_string(),
        ));
    }
    if let Some(workflow_id) = &assignment.workflow_id {
        let graph = find_workflow_graph(connection, "workflow_id", workflow_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(workflow_id.clone()))?;
        if let Some(task_id) = &graph.task_id {
            let canonical = assignment
                .task_ref
                .trim_start_matches("task:")
                .trim_start_matches("task://");
            if canonical != task_id {
                return Err(MfgRepositoryError::CommandRejected(
                    "assignment task_ref does not match the workflow task".to_string(),
                ));
            }
        }
    }
    let mut assignment = assignment.clone();
    match find_assignment(connection, &assignment.assignment_id)? {
        Some(existing) => {
            if expected_revision != Some(existing.revision) {
                return Err(MfgRepositoryError::RevisionConflict {
                    domain: "assignment".to_string(),
                    subject_id: assignment.assignment_id.clone(),
                    expected: expected_revision,
                    actual: Some(existing.revision),
                });
            }
            assignment.created_at = existing.created_at;
            assignment.created_by = existing.created_by;
            assignment.status = existing.status;
            assignment.revision = existing.revision.saturating_add(1);
        }
        None if expected_revision.is_some_and(|revision| revision != 0) => {
            return Err(MfgRepositoryError::RevisionConflict {
                domain: "assignment".to_string(),
                subject_id: assignment.assignment_id.clone(),
                expected: expected_revision,
                actual: None,
            })
        }
        None => {}
    }
    assignment.updated_at = Utc::now();
    save_assignment(connection, &assignment)?;
    append_projection_event(
        connection,
        "assignment",
        &format!("mfg:assignment:{}", assignment.assignment_id),
        "assignment.upserted",
        serde_json::to_value(&assignment)?,
    )?;
    Ok(assignment)
}

fn save_assignment(
    connection: &Connection,
    assignment: &MfgAssignment,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT INTO mfg_assignment (assignment_id, task_ref, workflow_id, incident_id, assignee_ref, status, visibility, revision, assignment_json, created_at, updated_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
           ON CONFLICT(assignment_id) DO UPDATE SET task_ref=excluded.task_ref, workflow_id=excluded.workflow_id, incident_id=excluded.incident_id, assignee_ref=excluded.assignee_ref, status=excluded.status, visibility=excluded.visibility, revision=excluded.revision, assignment_json=excluded.assignment_json, updated_at=excluded.updated_at",
        params![assignment.assignment_id, assignment.task_ref, assignment.workflow_id, assignment.incident_id, assignment.assignee_ref, assignment.status, assignment.visibility, assignment.revision as i64, serde_json::to_string(assignment)?, assignment.created_at.to_rfc3339(), assignment.updated_at.to_rfc3339()],
    )?;
    Ok(())
}

fn find_assignment(
    connection: &Connection,
    assignment_id: &str,
) -> Result<Option<MfgAssignment>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT assignment_json FROM mfg_assignment WHERE assignment_id = ?1",
            params![assignment_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_assignments(
    connection: &Connection,
    assignee_ref: Option<&str>,
    incident_id: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgAssignment>, MfgRepositoryError> {
    let mut statement = connection
        .prepare("SELECT assignment_json FROM mfg_assignment ORDER BY updated_at DESC LIMIT ?1")?;
    let rows = statement.query_map(params![limit.clamp(1, 500) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MfgAssignment>(&row?)?))
        .filter(|item| {
            item.as_ref().map_or(true, |assignment| {
                assignee_ref.is_none_or(|filter| assignment.assignee_ref == filter)
                    && incident_id
                        .is_none_or(|filter| assignment.incident_id.as_deref() == Some(filter))
            })
        })
        .collect()
}

fn command_assignment(
    connection: &Connection,
    assignment_id: &str,
    input: MfgAssignmentCommandInput,
) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
    let subject_ref = format!("mfg:assignment:{assignment_id}");
    if let Some(receipt) = find_command_receipt(
        connection,
        &input.idempotency_key,
        "assignment",
        &subject_ref,
    )? {
        if receipt.actor_ref != input.actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let assignment = find_assignment(connection, assignment_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(assignment_id.to_string()))?;
        return Ok((assignment, receipt));
    }
    let mut assignment = find_assignment(connection, assignment_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(assignment_id.to_string()))?;
    if input.actor_ref.trim().is_empty() || input.idempotency_key.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "actor and idempotency key are required".to_string(),
        ));
    }
    if assignment.visibility == "private"
        && input.actor_ref != assignment.created_by
        && input.actor_ref != assignment.assignee_ref
        && !assignment.watcher_refs.contains(&input.actor_ref)
    {
        return Err(MfgRepositoryError::CommandRejected(
            "private assignment is not visible to this actor".to_string(),
        ));
    }
    if matches!(
        input.command,
        MfgAssignmentCommand::Assign
            | MfgAssignmentCommand::Transfer
            | MfgAssignmentCommand::Unassign
            | MfgAssignmentCommand::Escalate
    ) && input.actor_ref != assignment.created_by
        && input.actor_ref != assignment.assignee_ref
    {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment command requires the owner or current assignee".to_string(),
        ));
    }
    ensure_revision(
        "assignment",
        assignment_id,
        input.expected_revision,
        assignment.revision,
    )?;
    let previous_revision = assignment.revision;
    match input.command {
        MfgAssignmentCommand::Assign | MfgAssignmentCommand::Transfer => {
            assignment.assignee_ref = input
                .target_ref
                .clone()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    MfgRepositoryError::CommandRejected("target_ref is required".to_string())
                })?;
            assignment.status = "assigned".to_string();
        }
        MfgAssignmentCommand::Claim => {
            assignment.assignee_ref = input.actor_ref.clone();
            assignment.status = "claimed".to_string();
        }
        MfgAssignmentCommand::Unassign => assignment.status = "unassigned".to_string(),
        MfgAssignmentCommand::Watch => {
            if !assignment.watcher_refs.contains(&input.actor_ref) {
                assignment.watcher_refs.push(input.actor_ref.clone());
            }
        }
        MfgAssignmentCommand::RequestUpdate => assignment.status = "update_requested".to_string(),
        MfgAssignmentCommand::Escalate => {
            assignment.status = "escalated".to_string();
            assignment.priority = "urgent".to_string();
        }
    }
    assignment.revision = assignment.revision.saturating_add(1);
    assignment.updated_at = Utc::now();
    save_assignment(connection, &assignment)?;
    let command = format!("{:?}", input.command).to_lowercase();
    let receipt = MfgCommandReceipt {
        receipt_id: format!("receipt-{}", uuid::Uuid::new_v4()),
        domain: "assignment".to_string(),
        subject_ref: subject_ref.clone(),
        command: command.clone(),
        actor_ref: input.actor_ref,
        idempotency_key: input.idempotency_key,
        idempotent_replay: false,
        previous_revision,
        current_revision: assignment.revision,
        audit_ref: format!(
            "audit://mfg/assignment/{assignment_id}/{}",
            assignment.revision
        ),
        notification_refs: Vec::new(),
        created_at: Utc::now(),
    };
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "assignment",
        &subject_ref,
        &format!("assignment.{command}"),
        serde_json::json!({ "assignment": assignment, "receipt": receipt, "reason": input.reason }),
    )?;
    Ok((assignment, receipt))
}

fn build_live_projection(
    connection: &Connection,
    cursor: Option<u64>,
    limit: usize,
) -> Result<MfgLiveProjection, MfgRepositoryError> {
    let latest = connection.query_row(
        "SELECT COALESCE(MAX(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let oldest = connection.query_row(
        "SELECT COALESCE(MIN(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    if cursor.is_none() {
        return Ok(MfgLiveProjection {
            kind: "snapshot".to_string(),
            cursor: latest,
            recoverable: true,
            snapshot: serde_json::json!({
                "cockpit_profiles": list_cockpit_profiles(connection, None, 100)?,
                "alert_rules": list_alert_rules(connection, None, 100)?,
                "alerts": list_alert_occurrences(connection, None, 100)?,
                "assignments": list_assignments(connection, None, None, 100)?,
                "incidents": list_incidents(connection, 100)?,
                "workflows": list_workflow_graphs(connection, 100)?,
            }),
            events: Vec::new(),
            resync_reason: None,
        });
    }
    let cursor = cursor.unwrap_or(0);
    if cursor > latest || (oldest > 0 && cursor.saturating_add(1) < oldest) {
        return Ok(MfgLiveProjection {
            kind: "resync".to_string(),
            cursor: latest,
            recoverable: true,
            snapshot: Value::Null,
            events: Vec::new(),
            resync_reason: Some("cursor is outside the retained event window".to_string()),
        });
    }
    let mut statement = connection.prepare("SELECT event_id, event_type, subject_ref, event_json, created_at FROM mfg_projection_event WHERE event_id > ?1 ORDER BY event_id ASC LIMIT ?2")?;
    let rows = statement.query_map(params![cursor as i64, limit.clamp(1, 500) as i64], |row| {
        Ok((
            row.get::<_, u64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    let events = rows
        .map(|row| {
            let (event_cursor, event_type, subject_ref, json, created_at) = row?;
            let value: Value = serde_json::from_str(&json)?;
            Ok(MfgLiveProjectionEvent {
                cursor: event_cursor,
                event_type,
                subject_ref,
                payload: value.get("payload").cloned().unwrap_or(Value::Null),
                created_at: parse_rfc3339_utc(&created_at)?,
            })
        })
        .collect::<Result<Vec<_>, MfgRepositoryError>>()?;
    let next_cursor = events.last().map_or(cursor, |event| event.cursor);
    Ok(MfgLiveProjection {
        kind: "delta".to_string(),
        cursor: next_cursor,
        recoverable: true,
        snapshot: Value::Null,
        events,
        resync_reason: None,
    })
}

fn insert_ontology_pack(
    connection: &Connection,
    pack: &MatrixOntologyPack,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_ontology_pack (
            ontology_id, domain, version, pack_json, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            pack.ontology_id,
            pack.domain,
            pack.version,
            serde_json::to_string(pack)?,
            Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_ontology_pack(
    connection: &Connection,
    ontology_id: &str,
) -> Result<Option<MatrixOntologyPack>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT pack_json FROM matrix_ontology_pack WHERE ontology_id = ?1",
            params![ontology_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn insert_entity_match_candidate(
    connection: &Connection,
    candidate: &matrix_core::MatrixEntityMatchCandidate,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_entity_match_candidate (
            candidate_id, left_entity_id, right_entity_id, confidence, status,
            candidate_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            candidate.candidate_id,
            candidate.left_entity_id,
            candidate.right_entity_id,
            candidate.confidence,
            candidate.status,
            serde_json::to_string(candidate)?,
            candidate.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_entity_match_candidate(
    connection: &Connection,
    candidate_id: &str,
) -> Result<Option<matrix_core::MatrixEntityMatchCandidate>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT candidate_json FROM matrix_entity_match_candidate WHERE candidate_id = ?1",
            params![candidate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn insert_entity_conflict_decision(
    connection: &Connection,
    decision: &matrix_core::MatrixEntityConflictDecision,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_entity_conflict_decision (
            decision_id, candidate_id, survivor_entity_id, retired_entity_id,
            decision_json, decided_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            decision.decision_id,
            decision.candidate_id,
            decision.survivor_entity_id,
            decision.retired_entity_id,
            serde_json::to_string(decision)?,
            decision.decided_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn upsert_entity(
    connection: &Connection,
    entity: &MatrixEntity,
) -> Result<MatrixEntity, MfgRepositoryError> {
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
        r"INSERT INTO matrix_entity (
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
        "DELETE FROM matrix_entity_source_key WHERE entity_id = ?1",
        params![entity.entity_id],
    )?;
    for source_key in &entity.source_keys {
        connection.execute(
            r"INSERT INTO matrix_entity_source_key (
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
    existing: &[matrix_core::MatrixSourceKey],
    incoming: &[matrix_core::MatrixSourceKey],
) -> Vec<matrix_core::MatrixSourceKey> {
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
) -> Result<Option<MatrixEntity>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT entity_json FROM matrix_entity WHERE entity_id = ?1",
            params![entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn find_entity_by_canonical(
    connection: &Connection,
    entity_type: &str,
    canonical_key: &str,
) -> Result<Option<MatrixEntity>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT entity_json
              FROM matrix_entity
              WHERE entity_type = ?1 AND canonical_key = ?2",
            params![entity_type, canonical_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn find_entity_by_source_key(
    connection: &Connection,
    source_system: &str,
    source_key: &str,
) -> Result<Option<MatrixEntity>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT e.entity_json
              FROM matrix_entity_source_key s
              JOIN matrix_entity e ON e.entity_id = s.entity_id
              WHERE s.source_system = ?1 AND s.source_key = ?2",
            params![
                matrix_core::normalize_key(source_system),
                matrix_core::normalize_key(source_key),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_entities(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEntity>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT entity_json
          FROM matrix_entity
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixEntity>(&row?)?))
        .collect()
}

fn upsert_relation(
    connection: &Connection,
    relation: &MatrixRelation,
) -> Result<MatrixRelation, MfgRepositoryError> {
    if find_entity(connection, &relation.from_entity_id)?.is_none() {
        return Err(MfgRepositoryError::NotFound(
            relation.from_entity_id.clone(),
        ));
    }
    if find_entity(connection, &relation.to_entity_id)?.is_none() {
        return Err(MfgRepositoryError::NotFound(relation.to_entity_id.clone()));
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
        r"INSERT INTO matrix_relation (
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
) -> Result<Option<MatrixRelation>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT relation_json
              FROM matrix_relation
              WHERE relation_type = ?1 AND from_entity_id = ?2 AND to_entity_id = ?3",
            params![relation_type, from_entity_id, to_entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_entity_relations(
    connection: &Connection,
    entity_id: &str,
    limit: usize,
) -> Result<Vec<MatrixRelation>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT relation_json
          FROM matrix_relation
          WHERE from_entity_id = ?1 OR to_entity_id = ?1
          ORDER BY updated_at DESC
          LIMIT ?2",
    )?;
    let rows = statement.query_map(params![entity_id, limit as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixRelation>(&row?)?))
        .collect()
}

fn build_impact_trace(
    connection: &Connection,
    root_entity_id: &str,
    max_depth: usize,
) -> Result<MatrixImpactTrace, MfgRepositoryError> {
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
            hops.push(MatrixImpactHop {
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
    Ok(MatrixImpactTrace {
        root_entity_id: root_entity_id.to_string(),
        max_depth,
        entities,
        hops,
        generated_at: Utc::now(),
    })
}

fn upsert_attention(
    connection: &Connection,
    item: &MatrixAttentionItem,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_attention_item (
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
) -> Result<Vec<MatrixAttentionItem>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT attention_json
          FROM matrix_attention_item
          ORDER BY priority_score DESC, updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixAttentionItem>(&row?)?))
        .collect()
}

fn find_attention(
    connection: &Connection,
    attention_id: &str,
) -> Result<Option<MatrixAttentionItem>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT attention_json FROM matrix_attention_item WHERE attention_id = ?1",
            params![attention_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn latest_attention(
    connection: &Connection,
) -> Result<Option<MatrixAttentionItem>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT attention_json
              FROM matrix_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn insert_evidence_packet(
    connection: &Connection,
    packet: &MatrixEvidencePacket,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_evidence_packet (
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
) -> Result<Option<MatrixEvidencePacket>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT packet_json FROM matrix_evidence_packet WHERE packet_id = ?1",
            params![packet_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_evidence_packets(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEvidencePacket>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT packet_json
          FROM matrix_evidence_packet
          ORDER BY created_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixEvidencePacket>(&row?)?))
        .collect()
}

fn insert_quality_gate(
    connection: &Connection,
    gate: &MatrixQualityGateDecision,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_quality_gate (
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
) -> Result<Option<MatrixQualityGateDecision>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT gate_json FROM matrix_quality_gate WHERE gate_id = ?1",
            params![gate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_recent_quality_gates(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixQualityGateDecision>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT gate_json
          FROM matrix_quality_gate
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
        self.fact_ids.push(format!("matrix:fact:{}", fact.fact_id));
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

fn metric_facts(connection: &Connection) -> Result<Vec<MetricFactRow>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT fact_id, fact_type, entity_refs_json, metric_key, dimensions_json,
            measures_json, confidence
          FROM matrix_fact
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

fn list_facts(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixFact>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
            dimensions_json, measures_json, event_time, valid_from, valid_to,
            source_ref, confidence, raw_hash
          FROM matrix_fact
          ORDER BY event_time DESC, fact_id ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, f32>(11)?,
            row.get::<_, String>(12)?,
        ))
    })?;

    let mut facts = Vec::new();
    for row in rows {
        let (
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs_json,
            metric_key,
            dimensions_json,
            measures_json,
            event_time,
            valid_from,
            valid_to,
            source_ref,
            confidence,
            raw_hash,
        ) = row?;
        facts.push(MatrixFact {
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs: serde_json::from_str(&entity_refs_json)?,
            metric_key,
            dimensions: serde_json::from_str(&dimensions_json)?,
            measures: serde_json::from_str(&measures_json)?,
            event_time: parse_rfc3339_utc(&event_time)?,
            valid_from: parse_optional_rfc3339_utc(valid_from)?,
            valid_to: parse_optional_rfc3339_utc(valid_to)?,
            source_ref,
            confidence,
            raw_hash,
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
    definition: &MatrixMetricDefinition,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_metric_definition (
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

fn find_metric_definition(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<MatrixMetricDefinition>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT definition_json FROM matrix_metric_definition WHERE metric_id = ?1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn upsert_metric_dependency(
    connection: &Connection,
    dependency: &MatrixMetricDependency,
) -> Result<MatrixMetricDependency, MfgRepositoryError> {
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
        r"INSERT INTO matrix_metric_dependency (
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
) -> Result<Option<MatrixMetricDependency>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT dependency_json
              FROM matrix_metric_dependency
              WHERE upstream_metric_id = ?1
                AND downstream_metric_id = ?2
                AND dependency_type = ?3",
            params![upstream_metric_id, downstream_metric_id, dependency_type],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_upstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<MatrixMetricDependency>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          WHERE downstream_metric_id = ?1
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn list_downstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<MatrixMetricDependency>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
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
) -> Result<MatrixMetricLineage, MfgRepositoryError> {
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
    Ok(MatrixMetricLineage {
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
) -> Result<Vec<String>, MfgRepositoryError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let dependency: MatrixMetricDependency = serde_json::from_str(&row?)?;
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
) -> Result<Vec<String>, MfgRepositoryError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT definition_json
          FROM matrix_metric_definition
          ORDER BY metric_id ASC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let definition: MatrixMetricDefinition = serde_json::from_str(&row?)?;
        if definition.inputs.iter().any(|input| input == fact_type) {
            impacted.insert(definition.metric_id);
        }
    }
    Ok(impacted.into_iter().collect())
}

fn build_metric_attention_plan(
    connection: &Connection,
    trigger_fact_type: &str,
    entity_scope: Option<String>,
    period: Option<String>,
    metric_ids: Vec<String>,
    limit: usize,
) -> Result<MatrixMetricAttentionPlan, MfgRepositoryError> {
    let limit = limit.clamp(1, 24);
    let mut scores = Vec::new();
    for metric_id in metric_ids {
        let definition = find_metric_definition(connection, &metric_id)?.unwrap_or_else(|| {
            MatrixMetricDefinition::inferred(metric_id.clone(), trigger_fact_type)
        });
        let lineage = build_metric_lineage(connection, &metric_id, 6)?;
        let latest = latest_metric_state_for_metric(connection, &metric_id)?;
        let latest_status = latest
            .as_ref()
            .map(|state| format!("{:?}", state.status).to_ascii_lowercase());
        let latest_delta = latest.as_ref().map(|state| state.delta);
        let score = MatrixMetricAttentionScore::new(
            metric_id.clone(),
            definition.business_priority,
            lineage.impacted_metric_ids.len() + lineage.upstream_dependencies.len(),
            latest_status,
            latest_delta,
        );
        scores.push(score);
    }
    scores.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .business_priority
                    .partial_cmp(&left.business_priority)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    scores.truncate(limit);
    let selected_metric_ids = scores
        .iter()
        .map(|score| score.metric_id.clone())
        .collect::<Vec<_>>();
    let compute_jobs = build_metric_compute_jobs(
        trigger_fact_type,
        &selected_metric_ids,
        entity_scope.clone(),
        period.clone(),
    );
    Ok(MatrixMetricAttentionPlan {
        plan_id: format!("metric-attention-plan-{}", uuid::Uuid::new_v4()),
        trigger_fact_type: trigger_fact_type.to_string(),
        entity_scope,
        period,
        limit,
        scored_metrics: scores,
        selected_metric_ids,
        compute_jobs,
        generated_at: Utc::now(),
    })
}

fn build_metric_snapshot(
    connection: &Connection,
    metric_ids: Vec<String>,
    scope_ref: Option<String>,
) -> Result<MatrixMetricSnapshot, MfgRepositoryError> {
    let mut unique_metric_ids = metric_ids;
    unique_metric_ids.sort();
    unique_metric_ids.dedup();
    let mut items = Vec::new();
    for metric_id in &unique_metric_ids {
        let state = latest_metric_state_for_metric(connection, metric_id)?;
        items.push(MatrixMetricSnapshotItem {
            metric_id: metric_id.clone(),
            state,
        });
    }
    let state_count = items.iter().filter(|item| item.state.is_some()).count();
    Ok(MatrixMetricSnapshot {
        snapshot_id: format!("metric-snapshot-{}", uuid::Uuid::new_v4()),
        scope_ref: scope_ref.unwrap_or_else(|| "global".to_string()),
        metric_ids: unique_metric_ids,
        items,
        created_at: Utc::now(),
        summary: format!("metric states materialized: {state_count}"),
    })
}

fn insert_metric_snapshot(
    connection: &Connection,
    snapshot: &MatrixMetricSnapshot,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_metric_snapshot (
            snapshot_id, scope_ref, metric_ids_json, snapshot_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            snapshot.snapshot_id,
            snapshot.scope_ref,
            serde_json::to_string(&snapshot.metric_ids)?,
            serde_json::to_string(snapshot)?,
            snapshot.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn insert_skill_execution(
    connection: &Connection,
    run: &MfgSkillRun,
) -> Result<MfgSkillRun, MfgRepositoryError> {
    let mut run = run.clone();
    let execution_id = run.execution_id.clone().unwrap_or_else(|| {
        let generated = format!("skill-execution-{}", uuid::Uuid::new_v4());
        run.execution_id = Some(generated.clone());
        generated
    });
    let created_at = run
        .telemetry
        .as_ref()
        .map(|telemetry| telemetry.completed_at)
        .unwrap_or_else(Utc::now);
    let updated_at = run
        .telemetry
        .as_ref()
        .map(|telemetry| telemetry.completed_at)
        .unwrap_or(created_at);
    connection.execute(
        r"INSERT INTO mfg_skill_execution (
            execution_id, incident_id, skill_id, status, execution_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(execution_id) DO UPDATE SET
            incident_id = excluded.incident_id,
            skill_id = excluded.skill_id,
            status = excluded.status,
            execution_json = excluded.execution_json,
            updated_at = excluded.updated_at",
        params![
            execution_id,
            run.incident_id,
            run.skill_id,
            run.status,
            serde_json::to_string(&run)?,
            created_at.to_rfc3339(),
            updated_at.to_rfc3339(),
        ],
    )?;
    Ok(run)
}

fn find_skill_execution(
    connection: &Connection,
    execution_id: &str,
) -> Result<Option<MfgSkillRun>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT execution_json FROM mfg_skill_execution WHERE execution_id = ?1",
            params![execution_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_skill_executions_for_incident(
    connection: &Connection,
    incident_id: &str,
    limit: usize,
) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT execution_json
          FROM mfg_skill_execution
          WHERE incident_id = ?1
          ORDER BY updated_at DESC
          LIMIT ?2",
    )?;
    let rows = statement.query_map(params![incident_id, limit.max(1) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn priority_for_compute_job(job: &MatrixComputeJob) -> f32 {
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
    job: &MatrixComputeJob,
) -> Result<MatrixComputeJob, MfgRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_compute_job (
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
) -> Result<Option<MatrixComputeJob>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT job_json FROM matrix_compute_job WHERE job_id = ?1",
            params![job_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn latest_metric_state(
    connection: &Connection,
    metric_id: &str,
    entity_scope: &str,
    period: &str,
) -> Result<Option<MatrixMetricState>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1 AND entity_scope = ?2 AND period = ?3
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id, entity_scope, period],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn insert_metric_state(
    connection: &Connection,
    state: &MatrixMetricState,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_metric_state (
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
    change: &MatrixChangeEvent,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_change_event (
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
) -> Result<Option<MatrixChangeEvent>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT change_json FROM matrix_change_event WHERE change_id = ?1",
            params![change_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn latest_metric_state_for_metric(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<MatrixMetricState>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn upsert_incident(
    connection: &Connection,
    incident: &MfgIncident,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO mfg_incident (
            incident_id, attention_id, evidence_packet_id, task_id, workflow_graph_id,
            status, incident_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            incident.incident_id,
            incident.attention_id,
            incident.evidence_packet_id,
            incident.task_id,
            incident.workflow_graph_id,
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
) -> Result<Option<MfgIncident>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT incident_json FROM mfg_incident WHERE incident_id = ?1",
            params![incident_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_incidents(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MfgIncident>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT incident_json
          FROM mfg_incident
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit.max(1) as i64], |row| row.get::<_, String>(0))?;
    let incidents = rows
        .map(|row| {
            let json = row.map_err(MfgRepositoryError::from)?;
            serde_json::from_str::<MfgIncident>(&json).map_err(MfgRepositoryError::from)
        })
        .collect::<Result<Vec<MfgIncident>, MfgRepositoryError>>()?;
    Ok(incidents
        .into_iter()
        .filter(|incident| {
            !matches!(
                incident.status.as_str(),
                "closed" | "resolved" | "done" | "archived"
            )
        })
        .collect())
}

fn insert_analysis(
    connection: &Connection,
    analysis: &MfgOperationalAnalysis,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO mfg_operational_analysis (
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
) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT analysis_json FROM mfg_operational_analysis WHERE analysis_id = ?1",
            params![analysis_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn latest_analysis_for_incident(
    connection: &Connection,
    incident_id: &str,
) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
    connection
        .query_row(
            r"SELECT analysis_json
              FROM mfg_operational_analysis
              WHERE incident_id = ?1
              ORDER BY created_at DESC
              LIMIT 1",
            params![incident_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn insert_execution(
    connection: &Connection,
    execution: &MfgActionExecution,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO mfg_action_execution (
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
) -> Result<Option<MfgActionExecution>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT execution_json FROM mfg_action_execution WHERE execution_id = ?1",
            params![execution_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_recent_executions(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT execution_json
          FROM mfg_action_execution
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn list_recent_skill_executions(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT execution_json
          FROM mfg_skill_execution
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit.max(1) as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn list_executions_for_incident(
    connection: &Connection,
    incident_id: &str,
    limit: usize,
) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT execution_json
          FROM mfg_action_execution
          WHERE incident_id = ?1
          ORDER BY updated_at DESC
          LIMIT ?2",
    )?;
    let rows = statement.query_map(params![incident_id, limit as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn insert_memory_case(
    connection: &Connection,
    memory_case: &MfgMemoryCase,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO mfg_memory_case (
            case_id, incident_id, problem_signature, outcome, memory_case_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            memory_case.case_id,
            memory_case.incident_id,
            memory_case.problem_signature,
            memory_case.outcome,
            serde_json::to_string(memory_case)?,
            memory_case.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_memory_case(
    connection: &Connection,
    case_id: &str,
) -> Result<Option<MfgMemoryCase>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT memory_case_json FROM mfg_memory_case WHERE case_id = ?1",
            params![case_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn search_memory_cases(
    connection: &Connection,
    query: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgMemoryCase>, MfgRepositoryError> {
    let query = query.map(str::trim).filter(|value| !value.is_empty());
    if let Some(query) = query {
        let pattern = format!("%{}%", query.to_lowercase());
        let mut statement = connection.prepare(
            r"SELECT memory_case_json
              FROM mfg_memory_case
              WHERE lower(problem_signature) LIKE ?1 OR lower(memory_case_json) LIKE ?1
              ORDER BY created_at DESC
              LIMIT ?2",
        )?;
        let rows = statement.query_map(params![pattern, limit as i64], |row| {
            row.get::<_, String>(0)
        })?;
        rows.map(|row| Ok(serde_json::from_str::<MfgMemoryCase>(&row?)?))
            .collect()
    } else {
        let mut statement = connection.prepare(
            r"SELECT memory_case_json
              FROM mfg_memory_case
              ORDER BY created_at DESC
              LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str::<MfgMemoryCase>(&row?)?))
            .collect()
    }
}

fn insert_playbook(
    connection: &Connection,
    playbook: &MfgPlaybook,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO mfg_playbook (
            playbook_id, domain, scenario, playbook_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            playbook.playbook_id,
            playbook.domain,
            playbook.scenario,
            serde_json::to_string(playbook)?,
            playbook.created_at.to_rfc3339(),
            playbook.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_playbook(
    connection: &Connection,
    playbook_id: &str,
) -> Result<Option<MfgPlaybook>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT playbook_json FROM mfg_playbook WHERE playbook_id = ?1",
            params![playbook_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn recommend_playbooks(
    connection: &Connection,
    metric_keys: &[String],
    entity_refs: &[String],
    limit: usize,
) -> Result<Vec<MfgPlaybook>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT playbook_json
          FROM mfg_playbook
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![(limit.max(20)) as i64], |row| {
        row.get::<_, String>(0)
    })?;
    let mut playbooks = rows
        .map(|row| Ok(serde_json::from_str::<MfgPlaybook>(&row?)?))
        .collect::<Result<Vec<_>, MfgRepositoryError>>()?;
    playbooks.sort_by(|left, right| {
        score_playbook(right, metric_keys, entity_refs).cmp(&score_playbook(
            left,
            metric_keys,
            entity_refs,
        ))
    });
    playbooks.truncate(limit);
    Ok(playbooks)
}

fn score_playbook(playbook: &MfgPlaybook, metric_keys: &[String], entity_refs: &[String]) -> usize {
    let metric_score = playbook
        .metric_keys
        .iter()
        .filter(|metric| metric_keys.contains(metric))
        .count()
        * 10;
    let entity_score = entity_refs
        .iter()
        .filter(|entity| playbook.scenario.contains(entity.as_str()))
        .count();
    metric_score + entity_score
}

fn insert_source_pack(
    connection: &Connection,
    source_pack: &MatrixSourcePack,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_source_pack (
            source_pack_id, source_name, access_mode, refresh_mode,
            source_pack_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            source_pack.source_pack_id,
            source_pack.source_name,
            source_pack.access_mode,
            source_pack.refresh_mode,
            serde_json::to_string(source_pack)?,
            source_pack.created_at.to_rfc3339(),
            source_pack.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_source_pack(
    connection: &Connection,
    source_pack_id: &str,
) -> Result<Option<MatrixSourcePack>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT source_pack_json FROM matrix_source_pack WHERE source_pack_id = ?1",
            params![source_pack_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_source_packs(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixSourcePack>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT source_pack_json
          FROM matrix_source_pack
          ORDER BY updated_at DESC, source_pack_id ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn source_pack_delta_plan_for(
    connection: &Connection,
    source_pack: &MatrixSourcePack,
) -> Result<MatrixSourceDeltaPlan, MfgRepositoryError> {
    let mut fact_types = source_pack
        .fact_mappings
        .iter()
        .map(|mapping| mapping.fact_type.clone())
        .collect::<Vec<_>>();
    fact_types.sort();
    fact_types.dedup();
    let mut affected_metric_ids = Vec::new();
    for fact_type in &fact_types {
        affected_metric_ids.extend(metrics_affected_by_fact_type(connection, fact_type)?);
        affected_metric_ids.extend(metric_ids_for_fact_type(connection, fact_type)?);
    }
    affected_metric_ids.extend(
        source_pack
            .fact_mappings
            .iter()
            .map(|mapping| mapping.metric_key.clone()),
    );
    affected_metric_ids.sort();
    affected_metric_ids.dedup();
    Ok(MatrixSourceDeltaPlan {
        source_pack_id: source_pack.source_pack_id.clone(),
        fact_types,
        affected_metric_ids,
        compute_scope: "partitioned_by_source_period_entity".to_string(),
        planned_at: Utc::now(),
    })
}

fn insert_connector_run(
    connection: &Connection,
    run: &MatrixConnectorRun,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_connector_run (
            run_id, source_pack_id, connector_kind, status, run_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            run.run_id,
            run.source_pack_id,
            run.connector_kind,
            run.status,
            serde_json::to_string(run)?,
            run.created_at.to_rfc3339(),
            run.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn find_connector_run(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<MatrixConnectorRun>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT run_json FROM matrix_connector_run WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn upsert_data_plane_watermark(
    connection: &Connection,
    watermark: &MatrixDataPlaneWatermark,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_data_plane_watermark (
            source_ref, fact_type, partition_ref, high_watermark, last_batch_id,
            watermark_json, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            watermark.source_ref,
            watermark.fact_type,
            watermark.partition_ref,
            watermark.high_watermark,
            watermark.last_batch_id,
            serde_json::to_string(watermark)?,
            watermark.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn list_data_plane_watermarks(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixDataPlaneWatermark>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT watermark_json
          FROM matrix_data_plane_watermark
          ORDER BY updated_at DESC, source_ref ASC, fact_type ASC, partition_ref ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

fn parse_rfc3339_utc(value: &str) -> Result<chrono::DateTime<Utc>, MfgRepositoryError> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|error| {
            MfgRepositoryError::Json(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error,
            )))
        })?
        .with_timezone(&Utc))
}

fn parse_optional_rfc3339_utc(
    value: Option<String>,
) -> Result<Option<chrono::DateTime<Utc>>, MfgRepositoryError> {
    value.as_deref().map(parse_rfc3339_utc).transpose()
}

fn attention_from_change(
    change: &MatrixChangeEvent,
    state: &MatrixMetricState,
) -> MatrixAttentionItem {
    let now = Utc::now();
    let severity = match change.severity_hint.as_str() {
        "critical" => MatrixSeverity::Critical,
        "warning" => MatrixSeverity::Warning,
        "normal" => MatrixSeverity::Normal,
        _ => MatrixSeverity::Unknown,
    };
    let severity_score = match severity {
        MatrixSeverity::Critical => 1.0,
        MatrixSeverity::Warning => 0.65,
        MatrixSeverity::Normal => 0.2,
        MatrixSeverity::Unknown => 0.35,
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
    MatrixAttentionItem {
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
        metric_refs: vec![state.metric_id.clone()],
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
        linked_changes: vec![format!("matrix:change:{}", change.change_id)],
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
    use crate::{
        MfgCockpitProfileInput, MfgCockpitReportDeliveryPayload,
        MfgCockpitReportDeliveryPayloadRequest, MfgCockpitReportDeliveryReceipt,
        MfgCockpitReportDeliveryState, MfgCockpitReportRequest,
    };
    use matrix_core::{
        MatrixComputeJobInput, MatrixEntityInput, MatrixFactInput, MatrixMetricStatus,
        MatrixRelationInput, MatrixSourceKey,
    };
    use std::sync::{Arc, Barrier};

    #[test]
    fn legacy_incident_graph_column_is_renamed_without_dual_truth() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                r"CREATE TABLE mfg_incident (
                    incident_id TEXT PRIMARY KEY,
                    attention_id TEXT,
                    evidence_packet_id TEXT,
                    task_id TEXT,
                    agent_graph_id TEXT,
                    status TEXT NOT NULL,
                    incident_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );",
            )
            .unwrap();
        let repository = MfgRepository::from_connection(connection).unwrap();
        let connection = repository.connection.lock().unwrap();
        let mut statement = connection
            .prepare("PRAGMA table_info(mfg_incident)")
            .unwrap();
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(columns.iter().any(|column| column == "workflow_graph_id"));
        assert!(!columns.iter().any(|column| column == "agent_graph_id"));
    }

    #[test]
    fn workflow_revision_cas_allows_only_one_concurrent_writer() {
        let path = std::env::temp_dir().join(format!("mfg-cas-{}.sqlite", uuid::Uuid::new_v4()));
        let seed = MfgRepository::open(&path).unwrap();
        let incident = MfgIncident::new("concurrent supplier recovery");
        let graph = MfgWorkflowGraph::for_incident(&incident).unwrap();
        seed.save_workflow_graph(&graph, None).unwrap();
        drop(seed);

        let barrier = Arc::new(Barrier::new(2));
        let writers = ["writer-a", "writer-b"].map(|writer| {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            let workflow_id = graph.workflow_id.clone();
            std::thread::spawn(move || {
                let repository = MfgRepository::open(path).unwrap();
                let mut graph = repository
                    .get_workflow_graph(&workflow_id)
                    .unwrap()
                    .unwrap();
                let expected = graph.revision;
                graph
                    .add_evidence("planner", "decision", format!("mfg:{writer}"), "commit")
                    .unwrap();
                barrier.wait();
                repository.save_workflow_graph(&graph, Some(expected))
            })
        });
        let results = writers.map(|writer| writer.join().unwrap());

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(MfgRepositoryError::WorkflowRevisionConflict { .. })
                ))
                .count(),
            1
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn entity_source_keys_resolve_to_one_canonical_entity() {
        let store = MfgRepository::in_memory().expect("store opens");
        let first = MatrixEntity::from_input(MatrixEntityInput {
            entity_id: None,
            entity_type: "Component".to_string(),
            canonical_key: "GPU-H100".to_string(),
            display_name: Some("GPU H100".to_string()),
            source_keys: vec![MatrixSourceKey {
                source_system: "ERP".to_string(),
                source_key: "MAT-GPU-H100".to_string(),
                source_ref: Some("connector:erp:material".to_string()),
            }],
            attributes: serde_json::json!({"family": "gpu"}),
            confidence: Some(0.96),
        });
        let first = store.upsert_entity(&first).expect("entity saves");

        let second = MatrixEntity::from_input(MatrixEntityInput {
            entity_id: None,
            entity_type: "component".to_string(),
            canonical_key: "gpu-h100".to_string(),
            display_name: Some("H100 accelerator".to_string()),
            source_keys: vec![MatrixSourceKey {
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
        let store = MfgRepository::in_memory().expect("store opens");
        let component = store
            .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
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
            .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
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
            .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
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
            .upsert_relation(&MatrixRelation::from_input(MatrixRelationInput {
                relation_id: None,
                relation_type: "requires".to_string(),
                from_entity_id: product.entity_id.clone(),
                to_entity_id: component.entity_id.clone(),
                attributes: serde_json::json!({"qty_per": 8}),
                confidence: Some(0.97),
            }))
            .expect("requires relation saves");
        store
            .upsert_relation(&MatrixRelation::from_input(MatrixRelationInput {
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
    fn mfg_seed_creates_domain_network_and_metric_facts() {
        let store = MfgRepository::in_memory().expect("store opens");
        let result = store.seed_mfg_domain().expect("domain seed runs");

        let expected_domain = ["server", "_manufacturing"].concat();
        assert_eq!(result.domain_id, expected_domain);
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
        let store = MfgRepository::in_memory().expect("store opens");
        let result = store.seed_mfg_domain().expect("domain seed runs");
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
        let store = MfgRepository::in_memory().expect("store opens");
        store.seed_mfg_domain().expect("domain seed runs");

        let plan = store
            .plan_compute_job_for_fact_type(MatrixComputeJobInput {
                job_id: Some("compute-job-supply-commit".to_string()),
                trigger_fact_type: "supply.commit_variance".to_string(),
                trigger_fact_refs: vec!["matrix:fact:fact-smfg-commit-gpu-alpha-w30".to_string()],
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
    fn mfg_scenario_eval_covers_decision_inference_and_quality_gate() {
        struct ScenarioExpectation {
            scenario_id: &'static str,
            trigger_metric: &'static str,
            trigger_fact_type: &'static str,
            root_entity_id: &'static str,
            must_reach_entity_id: &'static str,
            expected_cause_type: &'static str,
            expected_impact_type: &'static str,
            expected_action_type: &'static str,
        }

        let expectations = [
            ScenarioExpectation {
                scenario_id: "server_mfg_gpu_shortage",
                trigger_metric: "material_shortage_risk",
                trigger_fact_type: "supply.material_shortage",
                root_entity_id: "entity-component-gpu-h100",
                must_reach_entity_id: "entity-order-co-2026-0001",
                expected_cause_type: "supply_constraint",
                expected_impact_type: "material_availability_risk",
                expected_action_type: "supplier_recovery",
            },
            ScenarioExpectation {
                scenario_id: "server_mfg_bottleneck_load",
                trigger_metric: "work_center_load",
                trigger_fact_type: "manufacturing.work_center_load",
                root_entity_id: "entity-work-center-final-assembly",
                must_reach_entity_id: "entity-work-order-wo-2026-w30-001",
                expected_cause_type: "capacity_constraint",
                expected_impact_type: "capacity_throughput_risk",
                expected_action_type: "capacity_rebalance",
            },
            ScenarioExpectation {
                scenario_id: "server_mfg_quality_escape",
                trigger_metric: "quality_escape_risk",
                trigger_fact_type: "quality.escape_risk",
                root_entity_id: "entity-component-dimm-64g",
                must_reach_entity_id: "entity-product-storage-server",
                expected_cause_type: "quality_escape",
                expected_impact_type: "delivery_quality_risk",
                expected_action_type: "quality_containment",
            },
        ];

        for expected in expectations {
            let store = MfgRepository::in_memory().expect("store opens");
            let seed = store.seed_mfg_domain().expect("domain seed runs");
            assert_eq!(seed.scenario_count, 3);

            let recompute = store.recompute_metrics().expect("metrics recompute");
            let metric_state = recompute
                .metric_states
                .iter()
                .find(|state| state.metric_id == expected.trigger_metric)
                .unwrap_or_else(|| panic!("{} metric state exists", expected.trigger_metric));
            assert!(metric_state.value > 0.0);
            assert_eq!(metric_state.status, MatrixMetricStatus::Critical);
            assert!(!metric_state.input_fact_refs.is_empty());

            let affected = store
                .metrics_affected_by_fact_type(expected.trigger_fact_type)
                .expect("affected metrics resolve");
            assert!(
                affected
                    .iter()
                    .any(|metric_id| metric_id == expected.trigger_metric),
                "{} should affect {}",
                expected.trigger_fact_type,
                expected.trigger_metric
            );

            let attention = recompute
                .attention
                .iter()
                .find(|item| item.title.contains(expected.trigger_metric))
                .unwrap_or_else(|| panic!("{} attention exists", expected.trigger_metric));
            let packet = store
                .build_evidence_packet(
                    Some(&attention.attention_id),
                    Some(&format!(
                        "manufacturing scenario eval {}",
                        expected.scenario_id
                    )),
                )
                .expect("packet builds");
            assert!(!packet.metric_evidence.is_empty());
            assert!(!packet.change_evidence.is_empty());

            let review_gate = store
                .evaluate_evidence_quality(&packet.packet_id)
                .expect("review gate evaluates");
            assert_eq!(review_gate.decision, "review");

            let mut incident = MfgIncident::new(format!(
                "Manufacturing scenario eval {}",
                expected.scenario_id
            ));
            incident.attention_id = packet.attention_id.clone();
            incident.evidence_packet_id = Some(packet.packet_id.clone());
            store.create_incident(&incident).expect("incident saves");
            let analysis = store
                .analyze_incident(&incident.incident_id)
                .expect("incident analyzes");

            assert_eq!(
                analysis.attribution_candidates[0].cause_type,
                expected.expected_cause_type
            );
            assert_eq!(
                analysis.impact_paths[0].impact_type,
                expected.expected_impact_type
            );
            assert_eq!(
                analysis.recommended_actions[0].action_type,
                expected.expected_action_type
            );
            assert_eq!(analysis.status, "ready_for_review");
            assert!(analysis.confidence >= 0.65);

            let pass_gate = store
                .evaluate_evidence_quality(&packet.packet_id)
                .expect("pass gate evaluates");
            assert_eq!(pass_gate.decision, "pass");
            assert!(pass_gate.score >= 0.75);

            let trace = store
                .impact_trace(expected.root_entity_id, 4)
                .expect("impact trace builds");
            assert!(
                trace
                    .entities
                    .iter()
                    .any(|entity| entity.entity_id == expected.must_reach_entity_id),
                "{} should reach {}",
                expected.root_entity_id,
                expected.must_reach_entity_id
            );

            let plan = store
                .plan_compute_job_for_fact_type(MatrixComputeJobInput {
                    job_id: Some(format!(
                        "eval-compute-{}",
                        expected.trigger_metric.replace('_', "-")
                    )),
                    trigger_fact_type: expected.trigger_fact_type.to_string(),
                    trigger_fact_refs: metric_state
                        .input_fact_refs
                        .iter()
                        .map(|fact_id| format!("matrix:fact:{fact_id}"))
                        .collect(),
                    entity_scope: Some(expected.root_entity_id.to_string()),
                    period: Some(metric_state.period.clone()),
                    metric_ids: Vec::new(),
                    priority: None,
                })
                .expect("compute plan builds");
            assert!(plan
                .affected_metric_ids
                .iter()
                .any(|metric_id| metric_id == expected.trigger_metric));
        }
    }

    #[test]
    fn matrix_store_ingests_fact_and_builds_evidence_packet() {
        let store = MfgRepository::in_memory().expect("store opens");
        let fact = MatrixFact::from_input(MatrixFactInput {
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
        assert_eq!(health.schema_version, matrix_core::MATRIX_SCHEMA_VERSION);
        assert_eq!(health.fact_count, 1);
        assert_eq!(health.attention_count, 1);
        assert_eq!(health.evidence_count, 1);
    }

    #[test]
    fn matrix_store_recomputes_metrics_and_emits_changes() {
        let store = MfgRepository::in_memory().expect("store opens");
        let first = MatrixFact::from_input(MatrixFactInput {
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

        let second = MatrixFact::from_input(MatrixFactInput {
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
        let store = MfgRepository::in_memory().expect("store opens");
        let fact = MatrixFact::from_input(MatrixFactInput {
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
        assert_eq!(packet.problem_statement, "plan changed");
        assert!(!packet.source_refs.is_empty());
    }

    #[test]
    fn store_persists_incident() {
        let store = MfgRepository::in_memory().expect("store opens");
        let mut incident = MfgIncident::new("material risk");
        incident.attention_id = Some("attention-1".to_string());
        incident.evidence_packet_id = Some("packet-1".to_string());
        incident.task_id = Some("task-1".to_string());
        incident.workflow_graph_id = Some("mfg-workflow-task-1".to_string());
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
        let store = MfgRepository::in_memory().expect("store opens");
        let fact = MatrixFact::from_input(MatrixFactInput {
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
            format!("matrix:evidence:{}", packet.packet_id)
        );

        let mut incident = MfgIncident::new("GPU shortage quality gate");
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
        let store = MfgRepository::in_memory().expect("store opens");
        let fact = MatrixFact::from_input(MatrixFactInput {
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
        let mut incident = MfgIncident::new("GPU shortage cockpit");
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
                &MfgActionExecutionRequest {
                    mode: "commit".to_string(),
                    operator_id: Some("user:ops-planner".to_string()),
                    note: Some("cockpit action".to_string()),
                },
            )
            .expect("execution saves");

        let profile = MfgCockpitProfile::from_input(MfgCockpitProfileInput {
            profile_id: Some("cockpit-profile-ops".to_string()),
            owner_ref: "user:ops-planner".to_string(),
            display_name: Some("Ops planner".to_string()),
            focus_refs: vec!["component:gpu-cockpit".to_string()],
            focus_metric_ids: vec!["material_shortage_risk".to_string()],
            thresholds: serde_json::json!({"material_shortage_risk": {"critical": 100}}),
            template_id: Some("ops.default".to_string()),
            cadence: Some("daily".to_string()),
            expected_revision: None,
            scope: None,
            layout: None,
            global_filters: Value::Null,
            widget_instances: Vec::new(),
            sharing_policy: None,
        });
        let profile = store
            .upsert_cockpit_profile(&profile, None)
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
                MfgCockpitReportRequest {
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

        let payload = MfgCockpitReportDeliveryPayload::from_report(
            &report,
            MfgCockpitReportDeliveryPayloadRequest {
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
                MfgCockpitReportDeliveryReceipt::new(
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
        let delivery_state = MfgCockpitReportDeliveryState::from_report(&delivered);
        assert_eq!(delivery_state.classification, "dry_run_planned");
        assert!(!delivery_state.retryable);
        assert_eq!(delivery_state.attempt_count, 1);
        let delivered = store
            .attach_cockpit_report_delivery(
                &report.report_id,
                MfgCockpitReportDeliveryReceipt::new(
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
    fn cockpit_profile_mutations_persist_actor_bound_idempotency_receipts() {
        let store = MfgRepository::in_memory().expect("store opens");
        let profile = MfgCockpitProfile::from_input(MfgCockpitProfileInput {
            profile_id: Some("cockpit-profile-idempotency".to_string()),
            owner_ref: "user:planner".to_string(),
            display_name: Some("Idempotent cockpit".to_string()),
            focus_refs: Vec::new(),
            focus_metric_ids: Vec::new(),
            thresholds: Value::Null,
            template_id: None,
            cadence: Some("daily".to_string()),
            expected_revision: None,
            scope: None,
            layout: None,
            global_filters: Value::Null,
            widget_instances: Vec::new(),
            sharing_policy: None,
        });
        let (saved, receipt) = store
            .upsert_cockpit_profile_receipted(
                &profile,
                None,
                "profile.upsert",
                "user:planner",
                "cockpit-upsert-key",
            )
            .expect("profile saves with a receipt");
        let (replayed, replay_receipt) = store
            .upsert_cockpit_profile_receipted(
                &profile,
                None,
                "profile.upsert",
                "user:planner",
                "cockpit-upsert-key",
            )
            .expect("profile write replays");
        assert_eq!(saved.revision, 1);
        assert_eq!(replayed.revision, saved.revision);
        assert_eq!(replay_receipt.receipt_id, receipt.receipt_id);
        assert!(replay_receipt.idempotent_replay);
        assert!(receipt.audit_ref.contains("cockpit-profile-idempotency"));

        let (deleted, deletion_receipt) = store
            .delete_cockpit_profile_receipted(
                &saved.profile_id,
                saved.revision,
                "user:planner",
                "cockpit-delete-key",
            )
            .expect("profile deletes with a receipt");
        let (deleted_replay, deletion_replay) = store
            .delete_cockpit_profile_receipted(
                &saved.profile_id,
                saved.revision,
                "user:planner",
                "cockpit-delete-key",
            )
            .expect("profile deletion replays");
        assert_eq!(deleted.expect("first deletion returns profile").profile_id, saved.profile_id);
        assert!(deleted_replay.is_none());
        assert_eq!(deletion_replay.receipt_id, deletion_receipt.receipt_id);
        assert!(deletion_replay.idempotent_replay);
    }

    #[test]
    fn analyze_incident_projects_attribution_impact_and_actions() {
        let store = MfgRepository::in_memory().expect("store opens");
        let fact = MatrixFact::from_input(MatrixFactInput {
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
        let mut incident = MfgIncident::new("GPU shortage");
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
        let store = MfgRepository::in_memory().expect("store opens");
        let fact = MatrixFact::from_input(MatrixFactInput {
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
        let mut incident = MfgIncident::new("GPU shortage execution");
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
                &MfgActionExecutionRequest {
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
                MfgCrossPlaneBridgeReceipt::new(
                    execution.execution_id.clone(),
                    "cpx-matrix-test",
                    "planned",
                    "dry_run",
                    Some("cpa-matrix-test".to_string()),
                ),
            )
            .expect("bridge receipt attaches");
        assert_eq!(execution.status, "cross_plane_planned");
        assert_eq!(execution.cross_plane_receipts.len(), 1);
        assert_eq!(
            execution.receipt["cross_plane_receipts"][0]["cross_plane_receipt_id"],
            "cpx-matrix-test"
        );
        let execution = store
            .attach_cross_plane_receipt(
                &execution.execution_id,
                MfgCrossPlaneBridgeReceipt::new(
                    execution.execution_id.clone(),
                    "cpx-matrix-test",
                    "planned",
                    "dry_run",
                    Some("cpa-matrix-test".to_string()),
                ),
            )
            .expect("bridge receipt deduplicates");
        assert_eq!(execution.cross_plane_receipts.len(), 1);

        let execution = store
            .record_execution_feedback(
                &execution.execution_id,
                MfgActionFeedback::new("resolved", "supplier commit secured", Some(-260.0)),
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
