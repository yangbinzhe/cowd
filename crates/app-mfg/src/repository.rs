#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use storage::{SqliteConnectionFactory, StorageHandle};
use thiserror::Error;

use crate::{
    mfg_ontology_pack, mfg_seed_plan, mfg_widget_catalog, MfgActionExecution,
    MfgActionExecutionRequest, MfgActionFeedback, MfgAlertCommand, MfgAlertCommandInput,
    MfgAlertOccurrence, MfgAlertRule, MfgAlertSubscription, MfgAssignment, MfgAssignmentCommand,
    MfgAssignmentCommandInput, MfgCasePromotion, MfgCockpitProfile, MfgCockpitProjection,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportRequest, MfgCockpitReportSnapshot,
    MfgCockpitWidget, MfgCockpitWidgetProjection, MfgCommandReceipt, MfgCrossPlaneBridgeReceipt,
    MfgDomainSeedResult, MfgForecastProjection, MfgForecastSignal, MfgIncident, MfgLiveDeltaRead,
    MfgLiveEpoch, MfgLiveProjectionEvent, MfgLiveSnapshotRead, MfgMemoryCase,
    MfgOperationalAnalysis, MfgPlaybook, MfgSkillRun, MfgWidgetDefinition, MfgWidgetInstance,
    MfgWorkflowGraph, MfgWorkflowGraphError,
};

use app_mfg_contract::{
    MfgLiveSnapshotStateV1, MfgReportDeliveryReview, MfgReportDeliveryReviewDecision,
    MfgReportDeliveryReviewEffect, MfgReportDeliveryReviewRerouteTarget,
    MfgReportDeliveryReviewStatus,
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

#[derive(Debug, Clone)]
pub enum MfgMutationClaim {
    Acquired(app_mfg_contract::MfgReceiptV1),
    Pending(app_mfg_contract::MfgReceiptV1),
    NativeRecovery(MfgCommandReceipt),
    Replayed(app_mfg_contract::MfgReceiptV1, serde_json::Value),
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
        let source_ref = input.source_ref.clone();
        let mut plan = MatrixSqliteDataPlane::new(self.health()?.data_plane_watermark_count)
            .plan_ingest(input);
        if plan.affected_metric_ids.is_empty() {
            let connection = self
                .connection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut affected = metrics_affected_by_fact_type(&connection, &plan.fact_type)?;
            affected.extend(metric_ids_for_fact_type(&connection, &plan.fact_type)?);
            // A source-pack is the canonical declaration of the metrics that
            // its facts materialize.  A newly saved pack need not already
            // have persisted metric dependencies, so the generic fact-type
            // lookup above alone would incorrectly return an empty ingest
            // plan on its first use.
            if let Some(source_pack_id) = source_ref
                .strip_prefix("source-pack://")
                .map(str::trim)
                .filter(|id| !id.is_empty())
            {
                if let Some(source_pack) = find_source_pack(&connection, source_pack_id)? {
                    affected.extend(
                        source_pack
                            .fact_mappings
                            .iter()
                            .filter(|mapping| mapping.fact_type == plan.fact_type)
                            .map(|mapping| mapping.metric_key.clone()),
                    );
                }
            }
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = upsert_cockpit_profile_receipted(
            &transaction,
            profile,
            expected_revision,
            command,
            actor_ref,
            idempotency_key,
        )?;
        transaction.commit()?;
        Ok(result)
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

    pub fn cockpit_projection_with_filters(
        &self,
        profile_id: &str,
        filters: Value,
    ) -> Result<MfgCockpitProjection, MfgRepositoryError> {
        validate_cockpit_filters(&filters, "projection.query", false)?;
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
        profile.global_filters = filters;
        render_cockpit_projection(&connection, profile)
    }

    pub fn cockpit_widget_projection(
        &self,
        profile_id: &str,
        instance_id: &str,
    ) -> Result<MfgCockpitWidgetProjection, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
        profile.normalize_legacy();
        let instance = profile
            .widget_instances
            .iter()
            .find(|instance| instance.instance_id == instance_id && instance.visible)
            .ok_or_else(|| MfgRepositoryError::NotFound(instance_id.to_string()))?;
        let definition = mfg_widget_catalog()
            .into_iter()
            .find(|definition| definition.definition_id == instance.definition_id);
        let scoped = effective_cockpit_profile(&profile, instance);
        let widget = match definition.as_ref() {
            Some(definition) => render_cockpit_widget(&connection, &scoped, instance, definition)
                .unwrap_or_else(|error| {
                    MfgCockpitWidget::unavailable(instance, Some(definition), error.to_string())
                }),
            None => {
                MfgCockpitWidget::unavailable(instance, None, "widget definition is not registered")
            }
        };
        Ok(MfgCockpitWidgetProjection {
            projection_id: format!("cockpit-widget-projection-{}", uuid::Uuid::new_v4()),
            profile_id: profile.profile_id,
            profile_revision: profile.revision,
            widget,
            generated_at: Utc::now(),
        })
    }

    pub fn cockpit_widget_projection_with_filters(
        &self,
        profile_id: &str,
        instance_id: &str,
        filters: Value,
    ) -> Result<MfgCockpitWidgetProjection, MfgRepositoryError> {
        validate_cockpit_filters(&filters, "widget_projection.query", false)?;
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut profile = find_cockpit_profile(&connection, profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
        profile.global_filters = filters;
        profile.normalize_legacy();
        let instance = profile
            .widget_instances
            .iter()
            .find(|instance| instance.instance_id == instance_id && instance.visible)
            .ok_or_else(|| MfgRepositoryError::NotFound(instance_id.to_string()))?;
        let definition = mfg_widget_catalog()
            .into_iter()
            .find(|definition| definition.definition_id == instance.definition_id);
        let scoped = effective_cockpit_profile(&profile, instance);
        let widget = match definition.as_ref() {
            Some(definition) => render_cockpit_widget(&connection, &scoped, instance, definition)
                .unwrap_or_else(|error| {
                    MfgCockpitWidget::unavailable(instance, Some(definition), error.to_string())
                }),
            None => {
                MfgCockpitWidget::unavailable(instance, None, "widget definition is not registered")
            }
        };
        Ok(MfgCockpitWidgetProjection {
            projection_id: format!("cockpit-widget-projection-{}", uuid::Uuid::new_v4()),
            profile_id: profile.profile_id,
            profile_revision: profile.revision,
            widget,
            generated_at: Utc::now(),
        })
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

    pub fn generate_cockpit_report_idempotent(
        &self,
        profile_id: &str,
        report_id: &str,
        mut request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = find_cockpit_report(&transaction, report_id)? {
            if existing.profile_id == profile_id {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(MfgRepositoryError::CommandRejected(
                "report id is bound to another cockpit profile".to_string(),
            ));
        }
        let profile = find_cockpit_profile(&transaction, profile_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(profile_id.to_string()))?;
        request.report_id = Some(report_id.to_string());
        let projection = render_cockpit_projection(&transaction, profile)?;
        let report = MfgCockpitReportSnapshot::from_projection(projection, request);
        insert_cockpit_report(&transaction, &report)?;
        transaction.commit()?;
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

    pub fn list_cockpit_reports(
        &self,
        profile_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgCockpitReportSnapshot>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_cockpit_reports(&connection, profile_id, limit)
    }

    pub fn attach_cockpit_report_delivery(
        &self,
        report_id: &str,
        receipt: MfgCockpitReportDeliveryReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut report = find_cockpit_report(&transaction, report_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(report_id.to_string()))?;
        if let Some(existing) = report
            .delivery_receipts
            .iter()
            .find(|existing| existing.cross_plane_receipt_id == receipt.cross_plane_receipt_id)
        {
            if existing.report_id != receipt.report_id
                || existing.cross_plane_status != receipt.cross_plane_status
                || existing.cross_plane_dispatch_status != receipt.cross_plane_dispatch_status
                || existing.audit_record_id != receipt.audit_record_id
            {
                return Err(MfgRepositoryError::CommandRejected(
                    "cross-plane receipt id is bound to a different delivery result".to_string(),
                ));
            }
            transaction.commit()?;
            return Ok(report);
        }
        report.attach_delivery_receipt(receipt);
        insert_cockpit_report(&transaction, &report)?;
        transaction.commit()?;
        Ok(report)
    }

    pub fn create_report_delivery_review(
        &self,
        report: &MfgCockpitReportSnapshot,
        expected_report_revision: u64,
        requester_principal: &str,
        reason: &str,
        evidence_refs: Vec<String>,
        idempotency_key: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        create_report_delivery_review(
            &connection,
            report,
            expected_report_revision,
            requester_principal,
            reason,
            evidence_refs,
            idempotency_key,
        )
    }

    pub fn bind_report_delivery_review_approval(
        &self,
        review_id: &str,
        expected_revision: u64,
        approval_id: &str,
        actor_principal: &str,
        idempotency_key: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        bind_report_delivery_review_approval(
            &connection,
            review_id,
            expected_revision,
            approval_id,
            actor_principal,
            idempotency_key,
        )
    }

    pub fn get_report_delivery_review(
        &self,
        review_id: &str,
    ) -> Result<Option<MfgReportDeliveryReview>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_report_delivery_review(&connection, review_id)
    }

    pub fn report_delivery_review_by_transition_key(
        &self,
        review_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<MfgReportDeliveryReview>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let exists = connection
            .query_row(
                "SELECT 1 FROM mfg_report_delivery_review_transition
                 WHERE review_id = ?1 AND idempotency_key = ?2",
                params![review_id, idempotency_key],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if exists {
            find_report_delivery_review(&connection, review_id)
        } else {
            Ok(None)
        }
    }

    pub fn list_report_delivery_reviews(
        &self,
        report_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgReportDeliveryReview>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        list_report_delivery_reviews(&connection, report_id, limit)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_report_delivery_review_decision(
        &self,
        review_id: &str,
        expected_revision: u64,
        decision: MfgReportDeliveryReviewDecision,
        reviewer_principal: &str,
        reason: &str,
        evidence_refs: Vec<String>,
        reroute: Option<MfgReportDeliveryReviewRerouteTarget>,
        decision_lease_ref: &str,
        idempotency_key: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prepare_report_delivery_review_decision(
            &connection,
            review_id,
            expected_revision,
            decision,
            reviewer_principal,
            reason,
            evidence_refs,
            reroute,
            decision_lease_ref,
            idempotency_key,
        )
    }

    pub fn activate_report_delivery_review_decision(
        &self,
        review_id: &str,
        expected_revision: u64,
        actor_principal: &str,
        idempotency_key: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        activate_report_delivery_review_decision(
            &connection,
            review_id,
            expected_revision,
            actor_principal,
            idempotency_key,
        )
    }

    pub fn claim_report_delivery_review_effects(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgReportDeliveryReviewEffect>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        claim_report_delivery_review_effects(&connection, limit)
    }

    pub fn complete_report_delivery_review_effect(
        &self,
        effect_key: &str,
        receipt_ref: &str,
        actor_principal: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        complete_report_delivery_review_effect(
            &connection,
            effect_key,
            receipt_ref,
            actor_principal,
        )
    }

    pub fn fail_report_delivery_review_effect(
        &self,
        effect_key: &str,
        error: &str,
        actor_principal: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fail_report_delivery_review_effect(&connection, effect_key, error, actor_principal)
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
            serde_json::json!({"profile": profile}),
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = delete_cockpit_profile_receipted(
            &transaction,
            profile_id,
            expected_revision,
            actor_ref,
            idempotency_key,
        )?;
        transaction.commit()?;
        Ok(result)
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = upsert_alert_rule_receipted(
            &transaction,
            rule,
            expected_revision,
            actor_ref,
            idempotency_key,
        )?;
        transaction.commit()?;
        Ok(result)
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = upsert_alert_subscription_receipted(
            &transaction,
            subscription,
            expected_revision,
            actor_ref,
            idempotency_key,
        )?;
        transaction.commit()?;
        Ok(result)
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = command_alert(&transaction, occurrence_id, command)?;
        transaction.commit()?;
        Ok(result)
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = upsert_assignment_receipted(
            &transaction,
            assignment,
            expected_revision,
            actor_ref,
            idempotency_key,
        )?;
        transaction.commit()?;
        Ok(result)
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = command_assignment(&transaction, assignment_id, command)?;
        transaction.commit()?;
        Ok(result)
    }

    pub fn reserve_assignment_completion(
        &self,
        assignment_id: &str,
        expected_revision: u64,
        actor_ref: &str,
        correlation_id: &str,
    ) -> Result<MfgAssignment, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = reserve_assignment_completion(
            &transaction,
            assignment_id,
            expected_revision,
            actor_ref,
            correlation_id,
        )?;
        transaction.commit()?;
        Ok(result)
    }

    pub fn live_epoch(&self) -> Result<MfgLiveEpoch, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        load_live_epoch(&connection)
    }

    pub fn rotate_live_epoch(&self, reason: &str) -> Result<MfgLiveEpoch, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        rotate_live_epoch(&transaction, reason)?;
        let epoch = load_live_epoch(&transaction)?;
        transaction.commit()?;
        Ok(epoch)
    }

    pub fn live_snapshot_read(&self) -> Result<MfgLiveSnapshotRead, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let snapshot = build_live_snapshot_read(&transaction)?;
        transaction.commit()?;
        Ok(snapshot)
    }

    pub fn live_delta_read(
        &self,
        cursor: u64,
        limit: usize,
    ) -> Result<MfgLiveDeltaRead, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let delta = build_live_delta_read(&transaction, cursor, limit)?;
        transaction.commit()?;
        Ok(delta)
    }

    pub fn record_command_notifications(
        &self,
        idempotency_key: &str,
        notification_refs: Vec<String>,
    ) -> Result<MfgCommandReceipt, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result =
            record_command_notifications(&transaction, idempotency_key, notification_refs)?;
        transaction.commit()?;
        Ok(result)
    }

    pub fn command_notification_refs_for_resource(
        &self,
        resource_ref: &str,
    ) -> Result<Vec<String>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        command_notification_refs_for_resource(&connection, resource_ref)
    }

    pub fn native_command_receipt_by_identity(
        &self,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
    ) -> Result<Option<MfgCommandReceipt>, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_native_command_receipt_by_identity(
            &connection,
            idempotency_key,
            actor_principal,
            action_id,
            resource_ref,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim_mutation_receipt(
        &self,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
        expected_revision: Option<u64>,
        payload_digest: &str,
        correlation_id: &str,
    ) -> Result<MfgMutationClaim, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        claim_mutation_receipt(
            &connection,
            idempotency_key,
            actor_principal,
            action_id,
            resource_ref,
            expected_revision,
            payload_digest,
            correlation_id,
        )
    }

    pub fn release_mutation_claim(
        &self,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        payload_digest: &str,
    ) -> Result<bool, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(connection.execute(
            "DELETE FROM mfg_mutation_receipt
             WHERE idempotency_key = ?1
               AND actor_principal = ?2
               AND action_id = ?3
               AND payload_digest = ?4
               AND status = 'accepted'",
            params![idempotency_key, actor_principal, action_id, payload_digest],
        )? == 1)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn find_mutation_receipt(
        &self,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
        payload_digest: &str,
    ) -> Result<Option<(app_mfg_contract::MfgReceiptV1, serde_json::Value)>, MfgRepositoryError>
    {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        find_mutation_receipt(
            &connection,
            idempotency_key,
            actor_principal,
            action_id,
            resource_ref,
            payload_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_mutation_receipt(
        &self,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
        expected_revision: Option<u64>,
        result_revision: Option<u64>,
        payload_digest: &str,
        response: &serde_json::Value,
    ) -> Result<app_mfg_contract::MfgReceiptV1, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let receipt = record_mutation_receipt(
            &transaction,
            idempotency_key,
            actor_principal,
            action_id,
            resource_ref,
            expected_revision,
            result_revision,
            payload_digest,
            response,
        )?;
        transaction.commit()?;
        Ok(receipt)
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let survivor = find_entity(&transaction, survivor_entity_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(survivor_entity_id.to_string()))?;
        let retired = find_entity(&transaction, retired_entity_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(retired_entity_id.to_string()))?;
        let candidate = match find_entity_match_candidate(&transaction, candidate_id)? {
            Some(candidate) => candidate,
            None => {
                let candidate =
                    matrix_core::match_candidate(&survivor, &retired).ok_or_else(|| {
                        MfgRepositoryError::NotFound(
                            "entity match candidate below confidence threshold".to_string(),
                        )
                    })?;
                if candidate.candidate_id != candidate_id {
                    return Err(MfgRepositoryError::NotFound(candidate_id.to_string()));
                }
                insert_entity_match_candidate(&transaction, &candidate)?;
                candidate
            }
        };
        let candidate_pair_matches = (candidate.left_entity_id == survivor_entity_id
            && candidate.right_entity_id == retired_entity_id)
            || (candidate.left_entity_id == retired_entity_id
                && candidate.right_entity_id == survivor_entity_id);
        if !candidate_pair_matches {
            return Err(MfgRepositoryError::CommandRejected(
                "entity conflict decision does not match the candidate pair".to_string(),
            ));
        }
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
        insert_entity_conflict_decision(&transaction, &decision)?;
        transaction.commit()?;
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
        append_projection_event(
            &connection,
            "data_compute",
            &format!("matrix:fact:{}", fact.fact_id),
            "fact.ingested",
            serde_json::json!({"fact": fact}),
        )?;
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let packet =
            build_evidence_packet_transaction(&transaction, attention_id, problem_statement, None)?;
        transaction.commit()?;
        Ok(packet)
    }

    pub fn build_evidence_packet_idempotent(
        &self,
        packet_id: &str,
        attention_id: Option<&str>,
        problem_statement: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(packet) = find_evidence_packet(&transaction, packet_id)? {
            transaction.commit()?;
            return Ok(packet);
        }
        let packet = build_evidence_packet_transaction(
            &transaction,
            attention_id,
            problem_statement,
            Some(packet_id),
        )?;
        transaction.commit()?;
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
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        upsert_incident(&transaction, &incident)?;
        persist_workflow_graph(&transaction, &graph, None)?;
        transaction.commit()?;
        Ok((incident, graph))
    }

    pub fn create_incident_workflow_idempotent(
        &self,
        incident: &MfgIncident,
        packet: &MatrixEvidencePacket,
        workflow_id: &str,
    ) -> Result<(MfgIncident, MfgWorkflowGraph), MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = find_incident(&transaction, &incident.incident_id)? {
            if existing.evidence_packet_id != Some(packet.packet_id.clone())
                || existing.task_id != incident.task_id
            {
                return Err(MfgRepositoryError::CommandRejected(
                    "incident id is bound to another evidence packet or task".to_string(),
                ));
            }
            let graph = find_workflow_graph(&transaction, "incident_id", &incident.incident_id)?
                .ok_or_else(|| {
                    MfgRepositoryError::NotFound(format!("workflow for {}", incident.incident_id))
                })?;
            transaction.commit()?;
            return Ok((existing, graph));
        }
        let mut incident = incident.clone();
        let mut graph = MfgWorkflowGraph::for_incident(&incident)?;
        graph.workflow_id = workflow_id.to_string();
        graph.attach_evidence_packet(packet)?;
        graph.set_node_terminal_result(
            "planner",
            "incident workflow initialized from structured evidence packet",
        )?;
        incident.workflow_graph_id = Some(graph.workflow_id.clone());
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
        if run.status != "completed"
            || run
                .runtime_execution_ref
                .as_deref()
                .is_none_or(str::is_empty)
            || run.tool_results.len() != run.tool_plan.len()
            || run
                .tool_results
                .iter()
                .any(|result| result.status != "completed")
        {
            return Err(MfgRepositoryError::CommandRejected(
                "MFG skill completion requires a terminal Runtime execution and every tool receipt"
                    .to_string(),
            ));
        }
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(execution_id) = run.execution_id.as_deref() {
            if let Some(existing) = find_skill_execution(&transaction, execution_id)? {
                if existing.incident_id != run.incident_id || existing.skill_id != run.skill_id {
                    return Err(MfgRepositoryError::CommandRejected(
                        "skill execution id is bound to another incident or skill".to_string(),
                    ));
                }
                let graph =
                    find_workflow_graph(&transaction, "incident_id", &existing.incident_id)?
                        .ok_or_else(|| {
                            MfgRepositoryError::NotFound(format!(
                                "workflow for {}",
                                existing.incident_id
                            ))
                        })?;
                transaction.commit()?;
                return Ok((existing, graph));
            }
        }
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
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let analysis = analyze_incident_transaction(&transaction, incident_id, None)?;
        transaction.commit()?;
        Ok(analysis)
    }

    pub fn analyze_incident_idempotent(
        &self,
        incident_id: &str,
        analysis_id: &str,
    ) -> Result<MfgOperationalAnalysis, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = find_analysis(&transaction, analysis_id)? {
            if existing.incident_id == incident_id {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(MfgRepositoryError::CommandRejected(
                "analysis id is bound to another incident".to_string(),
            ));
        }
        let analysis = analyze_incident_transaction(&transaction, incident_id, Some(analysis_id))?;
        transaction.commit()?;
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

    pub fn preview_recommended_action(
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
        let mut execution = MfgActionExecution::from_action(&analysis, &action, request);
        execution.execution_id = format!("preview-{}", execution.execution_id);
        Ok(execution)
    }

    pub fn execute_recommended_action_idempotent(
        &self,
        analysis_id: &str,
        action_id: &str,
        execution_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = find_execution(&transaction, execution_id)? {
            if existing.analysis_id == analysis_id && existing.action_id == action_id {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(MfgRepositoryError::CommandRejected(
                "execution id is bound to another analysis action".to_string(),
            ));
        }
        let analysis = find_analysis(&transaction, analysis_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(analysis_id.to_string()))?;
        let action = analysis
            .recommended_actions
            .iter()
            .find(|action| action.action_id == action_id)
            .cloned()
            .ok_or_else(|| MfgRepositoryError::NotFound(action_id.to_string()))?;
        let mut execution = MfgActionExecution::from_action(&analysis, &action, request);
        execution.execution_id = execution_id.to_string();
        insert_execution(&transaction, &execution)?;
        transaction.commit()?;
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
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut execution = find_execution(&transaction, execution_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(execution_id.to_string()))?;
        if let Some(existing) = execution
            .cross_plane_receipts
            .iter()
            .find(|existing| existing.cross_plane_receipt_id == receipt.cross_plane_receipt_id)
        {
            if existing.execution_id != receipt.execution_id
                || existing.cross_plane_status != receipt.cross_plane_status
                || existing.cross_plane_dispatch_status != receipt.cross_plane_dispatch_status
                || existing.audit_record_id != receipt.audit_record_id
            {
                return Err(MfgRepositoryError::CommandRejected(
                    "cross-plane receipt id is bound to a different execution result".to_string(),
                ));
            }
            transaction.commit()?;
            return Ok(execution);
        }
        execution.attach_cross_plane_receipt(receipt);
        insert_execution(&transaction, &execution)?;
        transaction.commit()?;
        Ok(execution)
    }

    pub fn record_execution_feedback(
        &self,
        execution_id: &str,
        feedback: MfgActionFeedback,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        let mut connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut execution = find_execution(&transaction, execution_id)?
            .ok_or_else(|| MfgRepositoryError::NotFound(execution_id.to_string()))?;
        if let Some(existing) = execution.feedback.as_ref() {
            if existing.outcome == feedback.outcome
                && existing.note == feedback.note
                && existing.actor_ref == feedback.actor_ref
                && existing.metric_delta == feedback.metric_delta
            {
                transaction.commit()?;
                return Ok(execution);
            }
            return Err(MfgRepositoryError::CommandRejected(
                "execution feedback is immutable after the first governed submission".to_string(),
            ));
        }
        execution.apply_feedback(feedback);
        insert_execution(&transaction, &execution)?;
        if execution.status == "feedback_resolved" {
            if let Some(mut incident) = find_incident(&transaction, &execution.incident_id)? {
                incident.status = "closed".to_string();
                incident.revision = incident.revision.saturating_add(1);
                incident.updated_at = Utc::now();
                upsert_incident(&transaction, &incident)?;
            }
        }
        transaction.commit()?;
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
        expected_revision: Option<u64>,
    ) -> Result<MfgPlaybook, MfgRepositoryError> {
        let connection = self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let existing = find_playbook(&connection, &playbook.playbook_id)?;
        let mut playbook = playbook.clone();
        match existing {
            Some(existing) => {
                if expected_revision != Some(existing.revision) {
                    return Err(MfgRepositoryError::RevisionConflict {
                        domain: "playbook".to_string(),
                        subject_id: playbook.playbook_id.clone(),
                        expected: expected_revision,
                        actual: Some(existing.revision),
                    });
                }
                playbook.revision = existing.revision.checked_add(1).ok_or_else(|| {
                    MfgRepositoryError::CommandRejected(
                        "playbook revision cannot be advanced further".to_string(),
                    )
                })?;
                playbook.created_at = existing.created_at;
                playbook.updated_at = Utc::now();
            }
            None => {
                if expected_revision.is_some() {
                    return Err(MfgRepositoryError::RevisionConflict {
                        domain: "playbook".to_string(),
                        subject_id: playbook.playbook_id.clone(),
                        expected: expected_revision,
                        actual: None,
                    });
                }
                playbook.revision = 1;
            }
        }
        insert_playbook(&connection, &playbook)?;
        Ok(playbook)
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

        CREATE TABLE IF NOT EXISTS mfg_report_delivery_review (
            review_id TEXT PRIMARY KEY,
            report_id TEXT NOT NULL,
            report_revision INTEGER NOT NULL,
            delivery_revision INTEGER NOT NULL,
            dead_letter_digest TEXT NOT NULL,
            requester_principal TEXT NOT NULL,
            approval_id TEXT,
            correlation_id TEXT NOT NULL,
            requested_action TEXT,
            decision TEXT,
            reviewer_principal TEXT,
            decision_lease_ref TEXT,
            effect_key TEXT,
            effect_receipt_ref TEXT,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            review_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS review_approval_id_uq
            ON mfg_report_delivery_review(approval_id) WHERE approval_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS review_correlation_id_uq
            ON mfg_report_delivery_review(correlation_id);
        CREATE INDEX IF NOT EXISTS review_status_updated_idx
            ON mfg_report_delivery_review(status, updated_at);

        CREATE TABLE IF NOT EXISTS mfg_report_delivery_review_transition (
            transition_id TEXT PRIMARY KEY,
            review_id TEXT NOT NULL,
            from_status TEXT NOT NULL,
            to_status TEXT NOT NULL,
            action TEXT,
            actor_principal TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            detail_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(review_id, idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS transition_review_created_idx
            ON mfg_report_delivery_review_transition(review_id, created_at);

        CREATE TABLE IF NOT EXISTS mfg_report_delivery_review_effect_outbox (
            effect_id TEXT PRIMARY KEY,
            review_id TEXT NOT NULL,
            action TEXT NOT NULL,
            effect_key TEXT NOT NULL UNIQUE,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            next_attempt_at TEXT,
            last_error TEXT,
            receipt_ref TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS outbox_due_idx
            ON mfg_report_delivery_review_effect_outbox(status, next_attempt_at);

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

        CREATE TABLE IF NOT EXISTS mfg_mutation_receipt (
            receipt_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL UNIQUE,
            actor_principal TEXT NOT NULL,
            action_id TEXT NOT NULL,
            resource_ref TEXT NOT NULL,
            expected_revision INTEGER,
            result_revision INTEGER,
            payload_digest TEXT NOT NULL,
            status TEXT NOT NULL,
            response_json TEXT NOT NULL,
            contract_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS mfg_mutation_receipt_idempotency_uq
            ON mfg_mutation_receipt(idempotency_key);

        CREATE TABLE IF NOT EXISTS mfg_mutation_receipt_alias (
            legacy_idempotency_key TEXT PRIMARY KEY,
            receipt_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(receipt_id) REFERENCES mfg_mutation_receipt(receipt_id)
        );

        CREATE TABLE IF NOT EXISTS mfg_mutation_receipt_repair_report (
            report_id TEXT PRIMARY KEY,
            idempotency_key TEXT NOT NULL,
            existing_receipt_json TEXT NOT NULL,
            incoming_receipt_json TEXT NOT NULL,
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

        CREATE TABLE IF NOT EXISTS mfg_live_epoch (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            epoch_id TEXT NOT NULL,
            contract_version TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            rotation_reason TEXT NOT NULL,
            retention_low_cursor INTEGER NOT NULL,
            retention_high_cursor INTEGER NOT NULL,
            last_sweep_high_cursor INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );

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
    migrate_mfg_incident_workflow_column(connection)?;
    recover_interrupted_report_delivery_review_effects(connection)?;
    migrate_mfg_command_receipts(connection)?;
    migrate_mfg_live_epoch_sweep_cursor(connection)?;
    ensure_live_epoch(connection)?;
    Ok(())
}

fn recover_interrupted_report_delivery_review_effects(
    connection: &Connection,
) -> rusqlite::Result<()> {
    connection.execute(
        "UPDATE mfg_report_delivery_review_effect_outbox
         SET status = 'retry_wait', next_attempt_at = ?1,
             last_error = COALESCE(last_error, 'recovered_after_process_restart'),
             updated_at = ?1
         WHERE status = 'processing'",
        params![Utc::now().to_rfc3339()],
    )?;
    Ok(())
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

fn migrate_mfg_live_epoch_sweep_cursor(connection: &Connection) -> rusqlite::Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(mfg_live_epoch)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns
        .iter()
        .any(|column| column == "last_sweep_high_cursor")
    {
        connection.execute_batch(
            "ALTER TABLE mfg_live_epoch
             ADD COLUMN last_sweep_high_cursor INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    Ok(())
}

fn migrate_mfg_command_receipts(connection: &Connection) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(
        "SELECT idempotency_key, domain, subject_ref, receipt_json, created_at
         FROM mfg_command_receipt ORDER BY idempotency_key",
    )?;
    let legacy = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    if legacy.is_empty() {
        return Ok(());
    }

    connection.execute_batch("BEGIN IMMEDIATE")?;
    let mut repair_report: Option<(String, String, String)> = None;
    let migrated = (|| -> rusqlite::Result<()> {
        for (key, domain, subject_ref, receipt_json, created_at) in legacy {
            let migrated_alias = connection
                .query_row(
                    "SELECT receipt.receipt_id
                     FROM mfg_mutation_receipt_alias AS alias
                     JOIN mfg_mutation_receipt AS receipt
                       ON receipt.receipt_id = alias.receipt_id
                     WHERE alias.legacy_idempotency_key = ?1",
                    params![key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if migrated_alias.is_some() {
                continue;
            }
            let mut receipt =
                serde_json::from_str::<MfgCommandReceipt>(&receipt_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
            if receipt.action_id.is_empty() || receipt.action_id.ends_with(".upsert") {
                receipt.action_id = if receipt.command.ends_with(".upsert") {
                    canonical_upsert_action_id(&domain, &receipt.command, receipt.previous_revision)
                } else {
                    canonical_action_id(&domain, &receipt.command)
                };
            }
            if receipt.payload_digest.is_empty() {
                receipt.payload_digest = stable_payload_digest(&(
                    domain.as_str(),
                    subject_ref.as_str(),
                    &receipt.command,
                ))
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            }
            if receipt.contract_version.is_empty() {
                receipt.contract_version = app_mfg_contract::MFG_CONTRACT_VERSION.to_string();
            }
            let encoded = serde_json::to_string(&receipt)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            let existing = connection
                .query_row(
                    "SELECT receipt_id, actor_principal, action_id, resource_ref,
                            payload_digest, response_json, result_revision
                     FROM mfg_mutation_receipt WHERE idempotency_key = ?1",
                    params![key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Option<i64>>(6)?,
                        ))
                    },
                )
                .optional()?;
            let target_receipt_id = if let Some((
                existing_receipt_id,
                existing_actor,
                existing_action,
                existing_resource,
                existing_digest,
                existing_response,
                existing_result_revision,
            )) = existing
            {
                let existing_response_value = serde_json::from_str::<Value>(&existing_response)
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                let incoming_response_value = serde_json::from_str::<Value>(&encoded)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                let same = existing_actor == receipt.actor_ref
                    && existing_action == receipt.action_id
                    && existing_resource == subject_ref
                    && existing_digest == receipt.payload_digest
                    && existing_result_revision == Some(receipt.current_revision as i64)
                    && existing_response_value == incoming_response_value;
                if !same {
                    repair_report = Some((
                        key.clone(),
                        serde_json::json!({
                            "receipt_id": existing_receipt_id,
                            "actor_principal": existing_actor,
                            "action_id": existing_action,
                            "resource_ref": existing_resource,
                            "payload_digest": existing_digest,
                            "result_revision": existing_result_revision,
                            "response": existing_response_value,
                        })
                        .to_string(),
                        encoded,
                    ));
                    return Err(rusqlite::Error::InvalidQuery);
                }
                existing_receipt_id
            } else {
                let expected_revision = if receipt.action_id.ends_with(".create") {
                    None
                } else {
                    Some(receipt.previous_revision as i64)
                };
                connection.execute(
                    "INSERT INTO mfg_mutation_receipt (
                        receipt_id, idempotency_key, actor_principal, action_id, resource_ref,
                        expected_revision, result_revision, payload_digest, status, response_json,
                        contract_version, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'completed', ?9, ?10, ?11, ?11)",
                    params![
                        receipt.receipt_id,
                        key,
                        receipt.actor_ref,
                        receipt.action_id,
                        subject_ref,
                        expected_revision,
                        receipt.current_revision as i64,
                        receipt.payload_digest,
                        encoded,
                        receipt.contract_version,
                        created_at,
                    ],
                )?;
                receipt.receipt_id.clone()
            };
            connection.execute(
                "INSERT INTO mfg_mutation_receipt_alias (
                    legacy_idempotency_key, receipt_id, created_at
                 ) VALUES (?1, ?2, ?3)
                 ON CONFLICT(legacy_idempotency_key) DO UPDATE SET
                    receipt_id = excluded.receipt_id,
                    created_at = excluded.created_at",
                params![key, target_receipt_id, created_at],
            )?;
        }
        Ok(())
    })();
    match migrated {
        Ok(()) => connection.execute_batch("COMMIT"),
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            if let Some((key, existing, incoming)) = repair_report {
                connection.execute(
                    "INSERT INTO mfg_mutation_receipt_repair_report (
                        report_id, idempotency_key, existing_receipt_json,
                        incoming_receipt_json, created_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        format!("repair-{}", uuid::Uuid::new_v4()),
                        key,
                        existing,
                        incoming,
                        Utc::now().to_rfc3339(),
                    ],
                )?;
            }
            Err(error)
        }
    }
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
        append_projection_event(
            connection,
            "workflow",
            &format!("mfg:workflow:{}", graph.workflow_id),
            "workflow.updated",
            serde_json::json!({"workflow": graph}),
        )?;
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
    } else if expected_revision.is_some() {
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
        serde_json::json!({"profile": profile}),
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
            Ok::<MfgCockpitProfile, MfgRepositoryError>(profile)
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
    validate_cockpit_filters(&profile.global_filters, "profile.global_filters", false)?;
    match profile.scope.kind.as_str() {
        "personal"
            if profile
                .scope
                .scope_ref
                .as_deref()
                .is_none_or(|value| value.trim().is_empty()) => {}
        "personal" => {
            return Err(MfgRepositoryError::CommandRejected(
                "personal dashboard scope must not carry scope_ref".to_string(),
            ));
        }
        "team" | "role" | "organization"
            if profile
                .scope
                .scope_ref
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()) => {}
        "team" | "role" | "organization" => {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "{} dashboard scope requires scope_ref",
                profile.scope.kind
            )));
        }
        _ => {
            return Err(MfgRepositoryError::CommandRejected(
                "dashboard scope must be personal, team, role, or organization".to_string(),
            ));
        }
    }
    if !matches!(
        profile.sharing_policy.visibility.as_str(),
        "private" | "team" | "public"
    ) {
        return Err(MfgRepositoryError::CommandRejected(
            "dashboard sharing visibility must be private, team, or public".to_string(),
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
        validate_cockpit_config(&instance.config, &instance.instance_id)?;
        validate_cockpit_filters(&instance.query, &instance.instance_id, true)?;
        let supported_config_keys = definition
            .config_schema
            .get("properties")
            .and_then(Value::as_object)
            .map(|properties| properties.keys().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        if let Some(config) = instance.config.as_object() {
            if let Some(unsupported) = config
                .keys()
                .find(|key| !supported_config_keys.contains(key.as_str()))
            {
                return Err(MfgRepositoryError::CommandRejected(format!(
                    "widget `{}` config `{unsupported}` is not supported by `{}`",
                    instance.instance_id, instance.definition_id
                )));
            }
        }
        let supported_query_keys = definition
            .query_schema
            .get("properties")
            .and_then(Value::as_object)
            .map(|properties| properties.keys().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        if let Some(query) = instance.query.as_object() {
            if let Some(unsupported) = query
                .keys()
                .find(|key| !supported_query_keys.contains(key.as_str()))
            {
                return Err(MfgRepositoryError::CommandRejected(format!(
                    "widget `{}` query `{unsupported}` is not supported by `{}`",
                    instance.instance_id, instance.definition_id
                )));
            }
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

fn validate_cockpit_config(config: &Value, instance_id: &str) -> Result<(), MfgRepositoryError> {
    let Some(config) = config.as_object() else {
        return Ok(());
    };
    for (key, value) in config {
        let valid = match key.as_str() {
            "title" => value
                .as_str()
                .is_some_and(|title| !title.trim().is_empty() && title.len() <= 120),
            "show_legend" => value.is_boolean(),
            "precision" => value.as_u64().is_some_and(|precision| precision <= 6),
            "refresh_interval_seconds" => value
                .as_u64()
                .is_some_and(|seconds| (10..=3600).contains(&seconds)),
            _ => false,
        };
        if !valid {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "widget `{instance_id}` has invalid config `{key}`"
            )));
        }
    }
    Ok(())
}

fn validate_cockpit_filters(
    filters: &Value,
    source: &str,
    allow_limit: bool,
) -> Result<(), MfgRepositoryError> {
    if filters.is_null() {
        return Ok(());
    }
    let filters = filters.as_object().ok_or_else(|| {
        MfgRepositoryError::CommandRejected(format!("{source} filters must be a JSON object"))
    })?;
    for (key, value) in filters {
        let valid = match key.as_str() {
            "entity_refs" | "metric_ids" | "statuses" => value.as_array().is_some_and(|items| {
                items
                    .iter()
                    .all(|item| item.as_str().is_some_and(|text| !text.trim().is_empty()))
            }),
            "severities" => value.as_array().is_some_and(|items| {
                items.iter().all(|item| {
                    matches!(
                        item.as_str(),
                        Some("normal" | "warning" | "critical" | "unknown")
                    )
                })
            }),
            "from" | "to" => value
                .as_str()
                .is_some_and(|text| chrono::DateTime::parse_from_rfc3339(text).is_ok()),
            "limit" if allow_limit => value
                .as_u64()
                .is_some_and(|limit| (1..=100).contains(&limit)),
            _ => false,
        };
        if !valid {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "{source} has invalid or unsupported filter `{key}`"
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
        .collect::<Result<Vec<_>, MfgRepositoryError>>()?;
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
    append_projection_event(
        connection,
        "report",
        &format!("mfg:cockpit-report:{}", report.report_id),
        "report.updated",
        serde_json::json!({
            "report": report,
            "profile": find_cockpit_profile(connection, &report.profile_id)?,
        }),
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
        .map(|json| {
            let mut report = serde_json::from_str::<MfgCockpitReportSnapshot>(&json)?;
            report.normalize_legacy();
            Ok(report)
        })
        .transpose()
}

fn list_cockpit_reports(
    connection: &Connection,
    profile_id: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgCockpitReportSnapshot>, MfgRepositoryError> {
    let limit = limit.clamp(1, 500) as i64;
    if let Some(profile_id) = profile_id {
        let mut statement = connection.prepare(
            "SELECT report_json FROM mfg_cockpit_report WHERE profile_id = ?1 ORDER BY created_at DESC, report_id ASC LIMIT ?2",
        )?;
        let rows =
            statement.query_map(params![profile_id, limit], |row| row.get::<_, String>(0))?;
        return rows
            .map(|row| {
                let mut report = serde_json::from_str::<MfgCockpitReportSnapshot>(&row?)?;
                report.normalize_legacy();
                Ok(report)
            })
            .collect();
    }
    let mut statement = connection.prepare(
        "SELECT report_json FROM mfg_cockpit_report ORDER BY created_at DESC, report_id ASC LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit], |row| row.get::<_, String>(0))?;
    rows.map(|row| {
        let mut report = serde_json::from_str::<MfgCockpitReportSnapshot>(&row?)?;
        report.normalize_legacy();
        Ok(report)
    })
    .collect()
}

fn create_report_delivery_review(
    connection: &Connection,
    report: &MfgCockpitReportSnapshot,
    expected_report_revision: u64,
    requester_principal: &str,
    reason: &str,
    evidence_refs: Vec<String>,
    idempotency_key: &str,
) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
    if requester_principal.trim().is_empty() || idempotency_key.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "review requester and idempotency key are required".to_string(),
        ));
    }
    ensure_revision(
        "cockpit_report",
        &report.report_id,
        expected_report_revision,
        report.revision,
    )?;
    let delivery_state = crate::MfgCockpitReportDeliveryState::from_report(report);
    if !delivery_state.dead_lettered {
        return Err(MfgRepositoryError::CommandRejected(format!(
            "report delivery is not dead-lettered: {}",
            delivery_state.classification
        )));
    }
    let dead_letter_digest = stable_review_digest(&serde_json::json!({
        "report_id": report.report_id,
        "report_revision": report.revision,
        "delivery_revision": report.delivery_receipts.len(),
        "delivery_state": delivery_state,
    }))?;
    let stable = stable_review_digest(&serde_json::json!({
        "report_id": report.report_id,
        "idempotency_key": idempotency_key,
        "requester": requester_principal,
    }))?;
    let suffix = stable.trim_start_matches("sha256:");
    let review_id = format!("mfg-report-review-{}", &suffix[..24.min(suffix.len())]);
    let correlation_id = format!("mfg-report-review-correlation:{suffix}");
    if let Some(existing) = find_report_delivery_review_by_correlation(connection, &correlation_id)?
    {
        if existing.report_id == report.report_id
            && existing.report_revision == report.revision
            && existing.requester_principal == requester_principal
            && existing.dead_letter_digest == dead_letter_digest
        {
            return Ok(existing);
        }
        return Err(MfgRepositoryError::CommandRejected(
            "review idempotency key is bound to another request".to_string(),
        ));
    }
    let now = Utc::now();
    let review = MfgReportDeliveryReview {
        review_id,
        report_id: report.report_id.clone(),
        report_revision: report.revision,
        delivery_revision: report.delivery_receipts.len() as u64,
        dead_letter_digest,
        requester_principal: requester_principal.to_string(),
        approval_id: None,
        correlation_id,
        requested_action: None,
        decision: None,
        reviewer_principal: None,
        reason: reason.trim().to_string(),
        evidence_refs,
        decision_lease_ref: None,
        effect_key: None,
        effect_payload: Value::Null,
        effect_receipt_ref: None,
        effect_error: None,
        status: MfgReportDeliveryReviewStatus::ApprovalSubmissionPending,
        revision: 1,
        created_at: now,
        updated_at: now,
    };
    let transaction = connection.unchecked_transaction()?;
    insert_report_delivery_review(&transaction, &review)?;
    insert_report_delivery_review_transition(
        &transaction,
        &review.review_id,
        "none",
        review.status,
        None,
        requester_principal,
        idempotency_key,
        serde_json::json!({
            "report_id": review.report_id,
            "report_revision": review.report_revision,
            "dead_letter_digest": review.dead_letter_digest,
        }),
    )?;
    append_projection_event(
        &transaction,
        "review",
        &format!("mfg:report-review:{}", review.review_id),
        "report_review.requested",
        serde_json::json!({
            "review": review,
            "report": report,
            "profile": find_cockpit_profile(&transaction, &report.profile_id)?,
        }),
    )?;
    transaction.commit()?;
    Ok(review)
}

fn bind_report_delivery_review_approval(
    connection: &Connection,
    review_id: &str,
    expected_revision: u64,
    approval_id: &str,
    actor_principal: &str,
    idempotency_key: &str,
) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
    let current = find_report_delivery_review(connection, review_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(review_id.to_string()))?;
    let replayed_transition = connection
        .query_row(
            "SELECT 1 FROM mfg_report_delivery_review_transition
             WHERE review_id = ?1 AND idempotency_key = ?2",
            params![review_id, idempotency_key],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if current.status == MfgReportDeliveryReviewStatus::PendingApproval
        && current.approval_id.as_deref() == Some(approval_id)
        && replayed_transition
    {
        return Ok(current);
    }
    if current.status != MfgReportDeliveryReviewStatus::ApprovalSubmissionPending {
        return Err(review_conflict(&current, expected_revision));
    }
    ensure_revision(
        "report_delivery_review",
        review_id,
        expected_revision,
        current.revision,
    )?;
    if approval_id.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "approval id must not be empty".to_string(),
        ));
    }
    let mut next = current.clone();
    next.approval_id = Some(approval_id.to_string());
    next.status = MfgReportDeliveryReviewStatus::PendingApproval;
    next.revision = next.revision.saturating_add(1);
    next.updated_at = Utc::now();
    persist_report_delivery_review_transition(
        connection,
        &current,
        &next,
        None,
        actor_principal,
        idempotency_key,
        serde_json::json!({"approval_id": approval_id}),
    )
}

fn find_report_delivery_review(
    connection: &Connection,
    review_id: &str,
) -> Result<Option<MfgReportDeliveryReview>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT review_json FROM mfg_report_delivery_review WHERE review_id = ?1",
            params![review_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn find_report_delivery_review_by_correlation(
    connection: &Connection,
    correlation_id: &str,
) -> Result<Option<MfgReportDeliveryReview>, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT review_json FROM mfg_report_delivery_review WHERE correlation_id = ?1",
            params![correlation_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MfgRepositoryError::from))
        .transpose()
}

fn list_report_delivery_reviews(
    connection: &Connection,
    report_id: Option<&str>,
    limit: usize,
) -> Result<Vec<MfgReportDeliveryReview>, MfgRepositoryError> {
    let limit = limit.clamp(1, 500) as i64;
    let mut values = Vec::new();
    if let Some(report_id) = report_id {
        let mut statement = connection.prepare(
            "SELECT review_json FROM mfg_report_delivery_review
             WHERE report_id = ?1 ORDER BY updated_at DESC, review_id ASC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![report_id, limit], |row| row.get::<_, String>(0))?;
        for row in rows {
            values.push(serde_json::from_str(&row?)?);
        }
    } else {
        let mut statement = connection.prepare(
            "SELECT review_json FROM mfg_report_delivery_review
             ORDER BY updated_at DESC, review_id ASC LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit], |row| row.get::<_, String>(0))?;
        for row in rows {
            values.push(serde_json::from_str(&row?)?);
        }
    }
    Ok(values)
}

#[allow(clippy::too_many_arguments)]
fn prepare_report_delivery_review_decision(
    connection: &Connection,
    review_id: &str,
    expected_revision: u64,
    decision: MfgReportDeliveryReviewDecision,
    reviewer_principal: &str,
    reason: &str,
    evidence_refs: Vec<String>,
    reroute: Option<MfgReportDeliveryReviewRerouteTarget>,
    decision_lease_ref: &str,
    idempotency_key: &str,
) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
    if reviewer_principal.trim().is_empty()
        || decision_lease_ref.trim().is_empty()
        || idempotency_key.trim().is_empty()
    {
        return Err(MfgRepositoryError::CommandRejected(
            "reviewer, decision lease and idempotency key are required".to_string(),
        ));
    }
    match decision {
        MfgReportDeliveryReviewDecision::Reroute => {
            let target = reroute.as_ref().ok_or_else(|| {
                MfgRepositoryError::CommandRejected(
                    "reroute decision requires a validated target".to_string(),
                )
            })?;
            if [
                &target.target_ref,
                &target.provider_account,
                &target.channel,
                &target.requested_capability,
            ]
            .iter()
            .any(|value| value.trim().is_empty())
            {
                return Err(MfgRepositoryError::CommandRejected(
                    "reroute target/provider/channel/capability must not be empty".to_string(),
                ));
            }
        }
        MfgReportDeliveryReviewDecision::Resolve
            if reason.trim().is_empty() || evidence_refs.is_empty() =>
        {
            return Err(MfgRepositoryError::CommandRejected(
                "resolve requires an external disposition note and evidence".to_string(),
            ));
        }
        MfgReportDeliveryReviewDecision::Abandon if reason.trim().is_empty() => {
            return Err(MfgRepositoryError::CommandRejected(
                "abandon requires an irreversible decision reason".to_string(),
            ));
        }
        _ => {}
    }
    let current = find_report_delivery_review(connection, review_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(review_id.to_string()))?;
    let effect_key = format!(
        "mfg-review-effect:{}",
        stable_review_digest(&serde_json::json!({
            "review_id": review_id,
            "decision": decision,
            "idempotency_key": idempotency_key,
        }))?
        .trim_start_matches("sha256:")
    );
    if current.status == MfgReportDeliveryReviewStatus::DecisionPendingEffect
        && current.decision == Some(decision)
        && current.effect_key.as_deref() == Some(effect_key.as_str())
    {
        return Ok(current);
    }
    if current.status != MfgReportDeliveryReviewStatus::PendingApproval {
        return Err(review_conflict(&current, expected_revision));
    }
    ensure_revision(
        "report_delivery_review",
        review_id,
        expected_revision,
        current.revision,
    )?;
    if current.approval_id.is_none() {
        return Err(MfgRepositoryError::CommandRejected(
            "review has no correlated approval".to_string(),
        ));
    }
    let mut next = current.clone();
    next.requested_action = Some(decision);
    next.decision = Some(decision);
    next.reviewer_principal = Some(reviewer_principal.to_string());
    next.reason = reason.trim().to_string();
    next.evidence_refs = evidence_refs;
    next.decision_lease_ref = Some(decision_lease_ref.to_string());
    next.effect_key = Some(effect_key);
    next.effect_payload = reroute
        .map(|target| serde_json::to_value(target).unwrap_or(Value::Null))
        .unwrap_or(Value::Null);
    next.effect_error = None;
    next.status = MfgReportDeliveryReviewStatus::DecisionPendingEffect;
    next.revision = next.revision.saturating_add(1);
    next.updated_at = Utc::now();
    persist_report_delivery_review_transition(
        connection,
        &current,
        &next,
        Some(decision),
        reviewer_principal,
        idempotency_key,
        serde_json::json!({
            "decision_lease_ref": decision_lease_ref,
            "effect_key": next.effect_key,
            "reason": next.reason,
            "evidence_refs": next.evidence_refs,
            "effect_payload": next.effect_payload,
        }),
    )
}

fn activate_report_delivery_review_decision(
    connection: &Connection,
    review_id: &str,
    expected_revision: u64,
    actor_principal: &str,
    idempotency_key: &str,
) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
    let current = find_report_delivery_review(connection, review_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(review_id.to_string()))?;
    if current.status.is_terminal()
        || (matches!(
            current.status,
            MfgReportDeliveryReviewStatus::DecisionPendingEffect
                | MfgReportDeliveryReviewStatus::ApprovedPendingEffect
        ) && report_delivery_review_effect_exists(connection, review_id)?)
    {
        return Ok(current);
    }
    if current.status != MfgReportDeliveryReviewStatus::DecisionPendingEffect {
        return Err(review_conflict(&current, expected_revision));
    }
    ensure_revision(
        "report_delivery_review",
        review_id,
        expected_revision,
        current.revision,
    )?;
    let decision = current.decision.ok_or_else(|| {
        MfgRepositoryError::CommandRejected("review decision is missing".to_string())
    })?;
    let transaction = connection.unchecked_transaction()?;
    let result = match decision {
        MfgReportDeliveryReviewDecision::Reject => {
            let mut next = current.clone();
            next.status = MfgReportDeliveryReviewStatus::Rejected;
            next.effect_receipt_ref = Some(format!(
                "mfg://report-review/{}/rejected",
                current.review_id
            ));
            next.revision = next.revision.saturating_add(1);
            next.updated_at = Utc::now();
            persist_report_delivery_review_transition_in_transaction(
                &transaction,
                &current,
                &next,
                Some(decision),
                actor_principal,
                idempotency_key,
                serde_json::json!({"effect": "none", "delivery_unchanged": true}),
            )?;
            next
        }
        MfgReportDeliveryReviewDecision::Abandon | MfgReportDeliveryReviewDecision::Resolve => {
            let mut report = find_cockpit_report(&transaction, &current.report_id)?
                .ok_or_else(|| MfgRepositoryError::NotFound(current.report_id.clone()))?;
            ensure_revision(
                "cockpit_report",
                &report.report_id,
                current.report_revision,
                report.revision,
            )?;
            report.status = if decision == MfgReportDeliveryReviewDecision::Abandon {
                "delivery_abandoned".to_string()
            } else {
                "delivery_resolved_external".to_string()
            };
            report.revision = report.revision.saturating_add(1);
            insert_cockpit_report(&transaction, &report)?;
            let mut next = current.clone();
            next.status = if decision == MfgReportDeliveryReviewDecision::Abandon {
                MfgReportDeliveryReviewStatus::Abandoned
            } else {
                MfgReportDeliveryReviewStatus::ResolvedExternal
            };
            next.effect_receipt_ref = Some(format!(
                "mfg://report/{}/status/{}",
                report.report_id, report.status
            ));
            next.revision = next.revision.saturating_add(1);
            next.updated_at = Utc::now();
            persist_report_delivery_review_transition_in_transaction(
                &transaction,
                &current,
                &next,
                Some(decision),
                actor_principal,
                idempotency_key,
                serde_json::json!({
                    "effect": "local_terminal",
                    "report_status": report.status,
                    "report_revision": report.revision,
                }),
            )?;
            next
        }
        MfgReportDeliveryReviewDecision::ForceRetry | MfgReportDeliveryReviewDecision::Reroute => {
            let effect_key = current.effect_key.clone().ok_or_else(|| {
                MfgRepositoryError::CommandRejected("review effect key is missing".to_string())
            })?;
            let now = Utc::now();
            transaction.execute(
                "INSERT INTO mfg_report_delivery_review_effect_outbox (
                    effect_id, review_id, action, effect_key, payload_json, status,
                    attempt_count, next_attempt_at, last_error, receipt_ref, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, NULL, NULL, NULL, ?6, ?6)
                 ON CONFLICT(effect_key) DO NOTHING",
                params![
                    format!("mfg-review-effect-{}", uuid::Uuid::new_v4()),
                    current.review_id,
                    review_decision_string(decision),
                    effect_key,
                    serde_json::to_string(&current.effect_payload)?,
                    now.to_rfc3339(),
                ],
            )?;
            let report = find_cockpit_report(&transaction, &current.report_id)?;
            let profile = report
                .as_ref()
                .map(|report| find_cockpit_profile(&transaction, &report.profile_id))
                .transpose()?
                .flatten();
            append_projection_event(
                &transaction,
                "review",
                &format!("mfg:report-review:{}", current.review_id),
                "report_review.effect_queued",
                serde_json::json!({
                    "review_id": current.review_id,
                    "review": current,
                    "report": report,
                    "profile": profile,
                    "decision": decision,
                    "effect_key": current.effect_key,
                }),
            )?;
            current.clone()
        }
    };
    transaction.commit()?;
    Ok(result)
}

fn claim_report_delivery_review_effects(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MfgReportDeliveryReviewEffect>, MfgRepositoryError> {
    let now = Utc::now();
    let stale = now - chrono::Duration::minutes(2);
    let transaction = connection.unchecked_transaction()?;
    transaction.execute(
        "UPDATE mfg_report_delivery_review_effect_outbox
         SET status = 'retry_wait', next_attempt_at = ?1,
             last_error = COALESCE(last_error, 'reclaimed_after_interrupted_processing'),
             updated_at = ?1
         WHERE status = 'processing' AND updated_at <= ?2",
        params![now.to_rfc3339(), stale.to_rfc3339()],
    )?;
    let mut statement = transaction.prepare(
        "SELECT effect_id, review_id, action, effect_key, payload_json, status,
                attempt_count, next_attempt_at, last_error, receipt_ref, created_at, updated_at
         FROM mfg_report_delivery_review_effect_outbox
         WHERE status = 'pending'
            OR (status = 'retry_wait' AND (next_attempt_at IS NULL OR next_attempt_at <= ?1))
         ORDER BY created_at ASC, effect_id ASC LIMIT ?2",
    )?;
    let rows = statement
        .query_map(
            params![now.to_rfc3339(), limit.clamp(1, 100) as i64],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    let mut claimed = Vec::new();
    for (
        effect_id,
        review_id,
        action,
        effect_key,
        payload_json,
        _status,
        attempt_count,
        next_attempt_at,
        last_error,
        receipt_ref,
        created_at,
        _updated_at,
    ) in rows
    {
        let changed = transaction.execute(
            "UPDATE mfg_report_delivery_review_effect_outbox
             SET status = 'processing', attempt_count = attempt_count + 1, updated_at = ?1
             WHERE effect_id = ?2 AND status IN ('pending', 'retry_wait')",
            params![now.to_rfc3339(), effect_id],
        )?;
        if changed != 1 {
            continue;
        }
        claimed.push(MfgReportDeliveryReviewEffect {
            effect_id,
            review_id,
            action: parse_review_decision(&action)?,
            effect_key,
            payload: serde_json::from_str(&payload_json)?,
            status: "processing".to_string(),
            attempt_count: attempt_count.max(0) as u64 + 1,
            next_attempt_at: next_attempt_at
                .as_deref()
                .map(parse_rfc3339_utc)
                .transpose()?,
            last_error,
            receipt_ref,
            created_at: parse_rfc3339_utc(&created_at)?,
            updated_at: now,
        });
    }
    transaction.commit()?;
    Ok(claimed)
}

fn complete_report_delivery_review_effect(
    connection: &Connection,
    effect_key: &str,
    receipt_ref: &str,
    actor_principal: &str,
) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
    let effect = find_report_delivery_review_effect(connection, effect_key)?
        .ok_or_else(|| MfgRepositoryError::NotFound(effect_key.to_string()))?;
    let current = find_report_delivery_review(connection, &effect.review_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(effect.review_id.clone()))?;
    if effect.status == "completed" && current.status.is_terminal() {
        return Ok(current);
    }
    if !matches!(
        current.status,
        MfgReportDeliveryReviewStatus::DecisionPendingEffect
            | MfgReportDeliveryReviewStatus::ApprovedPendingEffect
    ) {
        return Err(review_conflict(&current, current.revision));
    }
    let terminal = match effect.action {
        MfgReportDeliveryReviewDecision::ForceRetry => {
            MfgReportDeliveryReviewStatus::EffectAppliedForceRetry
        }
        MfgReportDeliveryReviewDecision::Reroute => {
            MfgReportDeliveryReviewStatus::EffectAppliedReroute
        }
        _ => {
            return Err(MfgRepositoryError::CommandRejected(
                "outbox contains a non-delivery review action".to_string(),
            ));
        }
    };
    let mut next = current.clone();
    next.status = terminal;
    next.effect_receipt_ref = Some(receipt_ref.to_string());
    next.effect_error = None;
    next.revision = next.revision.saturating_add(1);
    next.updated_at = Utc::now();
    let transaction = connection.unchecked_transaction()?;
    persist_report_delivery_review_transition_in_transaction(
        &transaction,
        &current,
        &next,
        Some(effect.action),
        actor_principal,
        &format!("{}:complete", effect.effect_key),
        serde_json::json!({"receipt_ref": receipt_ref, "attempt_count": effect.attempt_count}),
    )?;
    transaction.execute(
        "UPDATE mfg_report_delivery_review_effect_outbox
         SET status = 'completed', receipt_ref = ?1, last_error = NULL,
             next_attempt_at = NULL, updated_at = ?2
         WHERE effect_key = ?3 AND status = 'processing'",
        params![receipt_ref, next.updated_at.to_rfc3339(), effect_key],
    )?;
    transaction.commit()?;
    Ok(next)
}

fn fail_report_delivery_review_effect(
    connection: &Connection,
    effect_key: &str,
    error: &str,
    actor_principal: &str,
) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
    let effect = find_report_delivery_review_effect(connection, effect_key)?
        .ok_or_else(|| MfgRepositoryError::NotFound(effect_key.to_string()))?;
    let current = find_report_delivery_review(connection, &effect.review_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(effect.review_id.clone()))?;
    let mut next = current.clone();
    if next.status != MfgReportDeliveryReviewStatus::ApprovedPendingEffect {
        next.status = MfgReportDeliveryReviewStatus::ApprovedPendingEffect;
        next.revision = next.revision.saturating_add(1);
    }
    next.effect_error = Some(error.to_string());
    next.updated_at = Utc::now();
    let backoff_seconds = 2_i64.pow(effect.attempt_count.min(8) as u32).min(300);
    let next_attempt_at = next.updated_at + chrono::Duration::seconds(backoff_seconds);
    let transaction = connection.unchecked_transaction()?;
    if next.revision != current.revision {
        persist_report_delivery_review_transition_in_transaction(
            &transaction,
            &current,
            &next,
            Some(effect.action),
            actor_principal,
            &format!("{}:failure:{}", effect.effect_key, effect.attempt_count),
            serde_json::json!({
                "error": error,
                "attempt_count": effect.attempt_count,
                "next_attempt_at": next_attempt_at,
            }),
        )?;
    } else {
        update_report_delivery_review_row(&transaction, &next, &current)?;
    }
    transaction.execute(
        "UPDATE mfg_report_delivery_review_effect_outbox
         SET status = 'retry_wait', next_attempt_at = ?1, last_error = ?2, updated_at = ?3
         WHERE effect_key = ?4 AND status = 'processing'",
        params![
            next_attempt_at.to_rfc3339(),
            error,
            next.updated_at.to_rfc3339(),
            effect_key,
        ],
    )?;
    transaction.commit()?;
    Ok(next)
}

fn insert_report_delivery_review(
    connection: &Connection,
    review: &MfgReportDeliveryReview,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        "INSERT INTO mfg_report_delivery_review (
            review_id, report_id, report_revision, delivery_revision, dead_letter_digest,
            requester_principal, approval_id, correlation_id, requested_action, decision,
            reviewer_principal, decision_lease_ref, effect_key, effect_receipt_ref,
            status, revision, review_json, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                   ?15, ?16, ?17, ?18, ?19)",
        params![
            review.review_id,
            review.report_id,
            review.report_revision as i64,
            review.delivery_revision as i64,
            review.dead_letter_digest,
            review.requester_principal,
            review.approval_id,
            review.correlation_id,
            review.requested_action.map(review_decision_string),
            review.decision.map(review_decision_string),
            review.reviewer_principal,
            review.decision_lease_ref,
            review.effect_key,
            review.effect_receipt_ref,
            review_status_string(review.status),
            review.revision as i64,
            serde_json::to_string(review)?,
            review.created_at.to_rfc3339(),
            review.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn persist_report_delivery_review_transition(
    connection: &Connection,
    current: &MfgReportDeliveryReview,
    next: &MfgReportDeliveryReview,
    action: Option<MfgReportDeliveryReviewDecision>,
    actor_principal: &str,
    idempotency_key: &str,
    detail: Value,
) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
    let transaction = connection.unchecked_transaction()?;
    persist_report_delivery_review_transition_in_transaction(
        &transaction,
        current,
        next,
        action,
        actor_principal,
        idempotency_key,
        detail,
    )?;
    transaction.commit()?;
    Ok(next.clone())
}

#[allow(clippy::too_many_arguments)]
fn persist_report_delivery_review_transition_in_transaction(
    connection: &Connection,
    current: &MfgReportDeliveryReview,
    next: &MfgReportDeliveryReview,
    action: Option<MfgReportDeliveryReviewDecision>,
    actor_principal: &str,
    idempotency_key: &str,
    detail: Value,
) -> Result<(), MfgRepositoryError> {
    update_report_delivery_review_row(connection, next, current)?;
    insert_report_delivery_review_transition(
        connection,
        &current.review_id,
        &review_status_string(current.status),
        next.status,
        action,
        actor_principal,
        idempotency_key,
        detail,
    )?;
    let report = find_cockpit_report(connection, &current.report_id)?;
    let profile = report
        .as_ref()
        .map(|report| find_cockpit_profile(connection, &report.profile_id))
        .transpose()?
        .flatten();
    append_projection_event(
        connection,
        "review",
        &format!("mfg:report-review:{}", current.review_id),
        "report_review.transitioned",
        serde_json::json!({
            "review_id": current.review_id,
            "review": next,
            "report": report,
            "profile": profile,
            "from_status": current.status,
            "to_status": next.status,
            "action": action,
            "revision": next.revision,
            "actor_principal": actor_principal,
        }),
    )?;
    Ok(())
}

fn update_report_delivery_review_row(
    connection: &Connection,
    next: &MfgReportDeliveryReview,
    current: &MfgReportDeliveryReview,
) -> Result<(), MfgRepositoryError> {
    let changed = connection.execute(
        "UPDATE mfg_report_delivery_review
         SET approval_id = ?1, requested_action = ?2, decision = ?3,
             reviewer_principal = ?4, decision_lease_ref = ?5, effect_key = ?6,
             effect_receipt_ref = ?7, status = ?8, revision = ?9,
             review_json = ?10, updated_at = ?11
         WHERE review_id = ?12 AND revision = ?13 AND status = ?14",
        params![
            next.approval_id,
            next.requested_action.map(review_decision_string),
            next.decision.map(review_decision_string),
            next.reviewer_principal,
            next.decision_lease_ref,
            next.effect_key,
            next.effect_receipt_ref,
            review_status_string(next.status),
            next.revision as i64,
            serde_json::to_string(next)?,
            next.updated_at.to_rfc3339(),
            current.review_id,
            current.revision as i64,
            review_status_string(current.status),
        ],
    )?;
    if changed != 1 {
        return Err(review_conflict(current, current.revision));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_report_delivery_review_transition(
    connection: &Connection,
    review_id: &str,
    from_status: &str,
    to_status: MfgReportDeliveryReviewStatus,
    action: Option<MfgReportDeliveryReviewDecision>,
    actor_principal: &str,
    idempotency_key: &str,
    detail: Value,
) -> Result<(), MfgRepositoryError> {
    connection.execute(
        "INSERT INTO mfg_report_delivery_review_transition (
            transition_id, review_id, from_status, to_status, action,
            actor_principal, idempotency_key, detail_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            format!(
                "mfg-review-transition:{}",
                stable_review_digest(&serde_json::json!({
                    "review_id": review_id,
                    "idempotency_key": idempotency_key,
                }))?
                .trim_start_matches("sha256:")
            ),
            review_id,
            from_status,
            review_status_string(to_status),
            action.map(review_decision_string),
            actor_principal,
            idempotency_key,
            serde_json::to_string(&detail)?,
            Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn report_delivery_review_effect_exists(
    connection: &Connection,
    review_id: &str,
) -> Result<bool, MfgRepositoryError> {
    Ok(connection
        .query_row(
            "SELECT 1 FROM mfg_report_delivery_review_effect_outbox WHERE review_id = ?1 LIMIT 1",
            params![review_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn find_report_delivery_review_effect(
    connection: &Connection,
    effect_key: &str,
) -> Result<Option<MfgReportDeliveryReviewEffect>, MfgRepositoryError> {
    let row = connection
        .query_row(
            "SELECT effect_id, review_id, action, effect_key, payload_json, status,
                    attempt_count, next_attempt_at, last_error, receipt_ref, created_at, updated_at
             FROM mfg_report_delivery_review_effect_outbox WHERE effect_key = ?1",
            params![effect_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(
            effect_id,
            review_id,
            action,
            effect_key,
            payload_json,
            status,
            attempt_count,
            next_attempt_at,
            last_error,
            receipt_ref,
            created_at,
            updated_at,
        )| {
            Ok(MfgReportDeliveryReviewEffect {
                effect_id,
                review_id,
                action: parse_review_decision(&action)?,
                effect_key,
                payload: serde_json::from_str(&payload_json)?,
                status,
                attempt_count: attempt_count.max(0) as u64,
                next_attempt_at: next_attempt_at
                    .as_deref()
                    .map(parse_rfc3339_utc)
                    .transpose()?,
                last_error,
                receipt_ref,
                created_at: parse_rfc3339_utc(&created_at)?,
                updated_at: parse_rfc3339_utc(&updated_at)?,
            })
        },
    )
    .transpose()
}

fn stable_review_digest(value: &Value) -> Result<String, MfgRepositoryError> {
    let encoded = serde_json::to_vec(value)?;
    Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
}

fn review_status_string(status: MfgReportDeliveryReviewStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn review_decision_string(decision: MfgReportDeliveryReviewDecision) -> String {
    serde_json::to_value(decision)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn parse_review_decision(
    value: &str,
) -> Result<MfgReportDeliveryReviewDecision, MfgRepositoryError> {
    serde_json::from_value(Value::String(value.to_string())).map_err(MfgRepositoryError::from)
}

fn review_conflict(review: &MfgReportDeliveryReview, expected_revision: u64) -> MfgRepositoryError {
    MfgRepositoryError::RevisionConflict {
        domain: "report_delivery_review".to_string(),
        subject_id: review.review_id.clone(),
        expected: Some(expected_revision),
        actual: Some(review.revision),
    }
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
            Some(definition) => render_cockpit_widget(
                connection,
                &effective_cockpit_profile(&profile, instance),
                instance,
                definition,
            )
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

fn effective_cockpit_profile(
    profile: &MfgCockpitProfile,
    instance: &MfgWidgetInstance,
) -> MfgCockpitProfile {
    let mut scoped = profile.clone();
    let mut merged = profile
        .global_filters
        .as_object()
        .cloned()
        .unwrap_or_default();
    if let Some(query) = instance.query.as_object() {
        for (key, value) in query {
            if key != "limit" {
                merged.insert(key.clone(), value.clone());
            }
        }
    }
    if let Some(values) = merged.get("entity_refs").and_then(Value::as_array) {
        scoped.focus_refs = values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
    }
    if let Some(values) = merged.get("metric_ids").and_then(Value::as_array) {
        scoped.focus_metric_ids = values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
    }
    scoped.global_filters = Value::Object(merged);
    scoped
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
            )));
        }
    };
    let title = instance
        .config
        .get("title")
        .and_then(Value::as_str)
        .filter(|title| !title.trim().is_empty())
        .unwrap_or(&definition.title)
        .to_string();
    Ok(MfgCockpitWidget {
        widget_id: instance.instance_id.clone(),
        widget_type: definition.renderer.clone(),
        title,
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
                (profile.focus_metric_ids.is_empty()
                    || profile.focus_metric_ids.contains(&state.metric_id))
                    && cockpit_time_matches(state.computed_at, profile)
            })
        })
        .take(limit)
        .collect()
}

fn attention_matches_profile(item: &MatrixAttentionItem, profile: &MfgCockpitProfile) -> bool {
    if !cockpit_time_matches(item.updated_at, profile) {
        return false;
    }
    let severities = profile
        .global_filters
        .get("severities")
        .and_then(Value::as_array);
    if severities.is_some_and(|values| {
        let actual = match item.severity {
            MatrixSeverity::Normal => "normal",
            MatrixSeverity::Warning => "warning",
            MatrixSeverity::Critical => "critical",
            MatrixSeverity::Unknown => "unknown",
        };
        !values.iter().any(|value| value.as_str() == Some(actual))
    }) {
        return false;
    }
    if profile
        .global_filters
        .get("statuses")
        .and_then(Value::as_array)
        .is_some_and(|values| {
            !values
                .iter()
                .any(|value| value.as_str() == Some(item.status.as_str()))
        })
    {
        return false;
    }
    let no_focus = profile.focus_refs.is_empty() && profile.focus_metric_ids.is_empty();
    let entity_match = item.entity_ref.as_ref().is_some_and(|entity_ref| {
        profile
            .focus_refs
            .iter()
            .any(|focus_ref| focus_ref == entity_ref)
    });
    let metric_match = item.metric_refs.iter().any(|metric_ref| {
        profile
            .focus_metric_ids
            .iter()
            .any(|metric_id| metric_ref == metric_id)
    });
    no_focus || entity_match || metric_match
}

fn cockpit_time_matches(timestamp: chrono::DateTime<Utc>, profile: &MfgCockpitProfile) -> bool {
    let from_matches = profile
        .global_filters
        .get("from")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .is_none_or(|from| timestamp >= from.with_timezone(&Utc));
    let to_matches = profile
        .global_filters
        .get("to")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .is_none_or(|to| timestamp <= to.with_timezone(&Utc));
    from_matches && to_matches
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
    let cursor = connection.last_insert_rowid() as u64;
    connection.execute(
        "UPDATE mfg_live_epoch
         SET retention_high_cursor = ?1
         WHERE singleton_id = 1",
        params![cursor],
    )?;
    compact_live_events_if_due(connection)?;
    Ok(cursor)
}

fn ensure_live_epoch(connection: &Connection) -> rusqlite::Result<()> {
    let high = connection.query_row(
        "SELECT COALESCE(MAX(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let oldest = connection.query_row(
        "SELECT COALESCE(MIN(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let now = Utc::now().to_rfc3339();
    connection.execute(
        "INSERT OR IGNORE INTO mfg_live_epoch (
            singleton_id, epoch_id, contract_version, schema_version,
            created_at, rotation_reason, retention_low_cursor,
            retention_high_cursor, updated_at
         ) VALUES (1, ?1, ?2, 1, ?3, 'initial', ?4, ?5, ?3)",
        params![
            format!("mfg-live-epoch-{}", uuid::Uuid::new_v4()),
            app_mfg_contract::MFG_CONTRACT_VERSION,
            now,
            oldest.saturating_sub(1),
            high,
        ],
    )?;
    let stored = connection.query_row(
        "SELECT contract_version, schema_version, retention_high_cursor
         FROM mfg_live_epoch WHERE singleton_id = 1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, u64>(2)?,
            ))
        },
    )?;
    let rotation_reason = if stored.0 != app_mfg_contract::MFG_CONTRACT_VERSION || stored.1 != 1 {
        Some("incompatible_contract_or_schema")
    } else if stored.2 > high {
        Some("event_log_rewritten")
    } else {
        None
    };
    if let Some(reason) = rotation_reason {
        connection.execute(
            "UPDATE mfg_live_epoch
             SET epoch_id = ?1, contract_version = ?2, schema_version = 1,
                 created_at = ?3, rotation_reason = ?4,
                 retention_low_cursor = ?5, retention_high_cursor = ?6,
                 last_sweep_high_cursor = 0,
                 updated_at = ?3
             WHERE singleton_id = 1",
            params![
                format!("mfg-live-epoch-{}", uuid::Uuid::new_v4()),
                app_mfg_contract::MFG_CONTRACT_VERSION,
                now,
                reason,
                oldest.saturating_sub(1),
                high,
            ],
        )?;
        return Ok(());
    }
    connection.execute(
        "UPDATE mfg_live_epoch
         SET retention_high_cursor = CASE
             WHEN retention_high_cursor < ?1 THEN ?1
             ELSE retention_high_cursor
         END
         WHERE singleton_id = 1",
        params![high],
    )?;
    Ok(())
}

fn rotate_live_epoch(connection: &Connection, reason: &str) -> Result<(), MfgRepositoryError> {
    let high = connection.query_row(
        "SELECT COALESCE(MAX(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let oldest = connection.query_row(
        "SELECT COALESCE(MIN(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let now = Utc::now().to_rfc3339();
    connection.execute(
        "UPDATE mfg_live_epoch
         SET epoch_id = ?1, contract_version = ?2, schema_version = 1,
             created_at = ?3, rotation_reason = ?4,
             retention_low_cursor = ?5, retention_high_cursor = ?6,
             last_sweep_high_cursor = 0,
             updated_at = ?3
         WHERE singleton_id = 1",
        params![
            format!("mfg-live-epoch-{}", uuid::Uuid::new_v4()),
            app_mfg_contract::MFG_CONTRACT_VERSION,
            now,
            reason,
            oldest.saturating_sub(1),
            high,
        ],
    )?;
    Ok(())
}

fn load_live_epoch(connection: &Connection) -> Result<MfgLiveEpoch, MfgRepositoryError> {
    connection
        .query_row(
            "SELECT epoch_id, contract_version, schema_version, created_at,
                    rotation_reason, retention_low_cursor,
                    retention_high_cursor, updated_at
             FROM mfg_live_epoch WHERE singleton_id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u64>(5)?,
                    row.get::<_, u64>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .map_err(MfgRepositoryError::from)
        .and_then(
            |(
                epoch_id,
                contract_version,
                schema_version,
                created_at,
                rotation_reason,
                retention_low_cursor,
                retention_high_cursor,
                updated_at,
            )| {
                Ok(MfgLiveEpoch {
                    epoch_id,
                    contract_version,
                    schema_version,
                    created_at: parse_rfc3339_utc(&created_at)?,
                    rotation_reason,
                    retention_low_cursor,
                    retention_high_cursor,
                    updated_at: parse_rfc3339_utc(&updated_at)?,
                })
            },
        )
}

fn compact_live_events_if_due(connection: &Connection) -> Result<(), MfgRepositoryError> {
    let (events_since_sweep, due) = connection.query_row(
        "SELECT retention_high_cursor - last_sweep_high_cursor,
                julianday('now') - julianday(updated_at) >= (5.0 / 1440.0)
         FROM mfg_live_epoch WHERE singleton_id = 1",
        [],
        |row| Ok((row.get::<_, u64>(0)?, row.get::<_, bool>(1)?)),
    )?;
    if events_since_sweep <= 60_000 && !due {
        return Ok(());
    }
    let keep_from = connection
        .query_row(
            "SELECT event_id FROM mfg_projection_event
             ORDER BY event_id DESC LIMIT 1 OFFSET 49999",
            [],
            |row| row.get::<_, u64>(0),
        )
        .optional()?;
    if let Some(keep_from) = keep_from {
        connection.execute(
            "DELETE FROM mfg_projection_event
             WHERE event_id < ?1
               AND julianday(created_at) < julianday('now', '-7 days')",
            params![keep_from],
        )?;
    }
    let high = connection.query_row(
        "SELECT COALESCE(MAX(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let oldest = connection.query_row(
        "SELECT COALESCE(MIN(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    connection.execute(
        "UPDATE mfg_live_epoch
         SET retention_low_cursor = ?1, retention_high_cursor = ?2,
             last_sweep_high_cursor = ?2,
             updated_at = ?3
         WHERE singleton_id = 1",
        params![oldest.saturating_sub(1), high, Utc::now().to_rfc3339()],
    )?;
    Ok(())
}

fn find_command_receipt(
    connection: &Connection,
    key: &str,
    actor_principal: &str,
    action_id: &str,
    resource_ref: &str,
    payload_digest: &str,
) -> Result<Option<MfgCommandReceipt>, MfgRepositoryError> {
    let value = connection
        .query_row(
            "SELECT actor_principal, action_id, resource_ref, payload_digest, status, response_json
         FROM mfg_mutation_receipt WHERE idempotency_key = ?1",
            params![key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((stored_actor, stored_action, stored_resource, _stored_digest, status, json)) = value
    else {
        return Ok(None);
    };
    if stored_actor != actor_principal
        || stored_action != action_id
        || stored_resource != resource_ref
    {
        return Err(MfgRepositoryError::CommandRejected(
            "idempotency key is already bound to another actor/action/resource".to_string(),
        ));
    }
    // Gateway claims a durable key before a native repository command runs.
    // Its canonical HTTP digest intentionally differs from the native typed
    // command digest; an accepted claim is not yet a replayable business row.
    if status == "accepted" {
        return Ok(None);
    }
    let mut receipt: MfgCommandReceipt = serde_json::from_str(&json)?;
    if receipt.payload_digest != payload_digest {
        return Err(MfgRepositoryError::CommandRejected(
            "idempotency key is already bound to another native command payload".to_string(),
        ));
    }
    receipt.idempotent_replay = true;
    Ok(Some(receipt))
}

fn find_native_command_receipt_by_identity(
    connection: &Connection,
    key: &str,
    actor_principal: &str,
    action_id: &str,
    resource_ref: &str,
) -> Result<Option<MfgCommandReceipt>, MfgRepositoryError> {
    let json = connection
        .query_row(
            "SELECT response_json
             FROM mfg_mutation_receipt
             WHERE idempotency_key = ?1
               AND actor_principal = ?2
               AND action_id = ?3
               AND resource_ref = ?4
               AND status = 'business_completed'",
            params![key, actor_principal, action_id, resource_ref],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(json) = json else {
        return Ok(None);
    };
    let mut receipt = match serde_json::from_str::<MfgCommandReceipt>(&json) {
        Ok(receipt) => receipt,
        Err(_) => return Ok(None),
    };
    receipt.idempotent_replay = true;
    Ok(Some(receipt))
}

fn command_receipt_snapshot<T: serde::de::DeserializeOwned>(
    receipt: &MfgCommandReceipt,
) -> Option<T> {
    (!receipt.response_snapshot.is_null())
        .then(|| serde_json::from_value(receipt.response_snapshot.clone()).ok())
        .flatten()
}

fn insert_command_receipt(
    connection: &Connection,
    receipt: &MfgCommandReceipt,
) -> Result<(), MfgRepositoryError> {
    let changed = connection.execute(
        "INSERT INTO mfg_mutation_receipt (
            receipt_id, idempotency_key, actor_principal, action_id, resource_ref,
            expected_revision, result_revision, payload_digest, status, response_json,
            contract_version, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'business_completed', ?9, ?10, ?11, ?11)
         ON CONFLICT(idempotency_key) DO UPDATE SET
            result_revision = excluded.result_revision,
            status = 'business_completed',
            response_json = excluded.response_json,
            contract_version = excluded.contract_version,
            updated_at = excluded.updated_at
         WHERE mfg_mutation_receipt.status = 'accepted'
           AND mfg_mutation_receipt.actor_principal = excluded.actor_principal
           AND mfg_mutation_receipt.action_id = excluded.action_id
           AND mfg_mutation_receipt.resource_ref = excluded.resource_ref",
        params![
            receipt.receipt_id,
            receipt.idempotency_key,
            receipt.actor_ref,
            receipt.action_id,
            receipt.subject_ref,
            receipt.previous_revision as i64,
            receipt.current_revision as i64,
            receipt.payload_digest,
            serde_json::to_string(receipt)?,
            receipt.contract_version,
            receipt.created_at.to_rfc3339(),
        ],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(MfgRepositoryError::CommandRejected(
            "idempotency key is already bound to another command or completed receipt".to_string(),
        ))
    }
}

fn mutation_receipt(
    domain: &str,
    subject_ref: String,
    command: &str,
    actor_ref: &str,
    idempotency_key: &str,
    payload_digest: String,
    previous_revision: u64,
    current_revision: u64,
) -> Result<MfgCommandReceipt, MfgRepositoryError> {
    if actor_ref.trim().is_empty() || idempotency_key.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "actor and idempotency key are required".to_string(),
        ));
    }
    let mut action_id = canonical_action_id(domain, command);
    if command.ends_with(".upsert") {
        action_id = action_id.trim_end_matches(".upsert").to_string()
            + if previous_revision == 0 {
                ".create"
            } else {
                ".update"
            };
    }
    Ok(MfgCommandReceipt {
        receipt_id: format!("receipt-{}", uuid::Uuid::new_v4()),
        domain: domain.to_string(),
        subject_ref: subject_ref.clone(),
        command: command.to_string(),
        action_id,
        actor_ref: actor_ref.to_string(),
        idempotency_key: idempotency_key.to_string(),
        payload_digest,
        correlation_id: None,
        contract_version: app_mfg_contract::MFG_CONTRACT_VERSION.to_string(),
        idempotent_replay: false,
        previous_revision,
        current_revision,
        audit_ref: format!(
            "audit://mfg/{domain}/{}/{}",
            subject_ref.rsplit(':').next().unwrap_or("unknown"),
            current_revision
        ),
        notification_refs: Vec::new(),
        response_snapshot: Value::Null,
        created_at: Utc::now(),
    })
}

fn canonical_action_id(domain: &str, command: &str) -> String {
    match (domain, command) {
        ("cockpit", "profile.upsert") => "mfg.cockpit.profile.upsert".to_string(),
        ("cockpit", "profile.clone") => "mfg.cockpit.profile.clone".to_string(),
        ("cockpit", "profile.share") => "mfg.cockpit.profile.share".to_string(),
        ("cockpit", "profile.delete") => "mfg.cockpit.profile.delete".to_string(),
        ("alert", "rule.upsert") => "mfg.alert_rule.upsert".to_string(),
        ("alert", "subscription.upsert") => "mfg.alert_subscription.upsert".to_string(),
        ("assignment", "assignment.upsert") => "mfg.assignment.upsert".to_string(),
        ("assignment", "requestupdate") => "mfg.assignment.request_update".to_string(),
        ("alert", command) => format!("mfg.alert.{command}"),
        ("assignment", command) => format!("mfg.assignment.{command}"),
        _ => format!("mfg.{domain}.{}", command.replace('_', ".")),
    }
}

fn canonical_upsert_action_id(domain: &str, command: &str, previous_revision: u64) -> String {
    let base = canonical_action_id(domain, command);
    if !command.ends_with(".upsert") {
        return base;
    }
    format!(
        "{}.{}",
        base.trim_end_matches(".upsert"),
        if previous_revision == 0 {
            "create"
        } else {
            "update"
        }
    )
}

fn stable_payload_digest<T: serde::Serialize>(value: &T) -> Result<String, MfgRepositoryError> {
    use sha2::{Digest, Sha256};
    let encoded = serde_json::to_vec(value)?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

fn stable_upsert_payload_digest<T: serde::Serialize>(
    resource: &T,
    expected_revision: Option<u64>,
    command: &str,
) -> Result<String, MfgRepositoryError> {
    let mut resource = serde_json::to_value(resource)?;
    if let Some(object) = resource.as_object_mut() {
        for server_owned in [
            "revision",
            "created_at",
            "updated_at",
            "created_by",
            "status",
        ] {
            object.remove(server_owned);
        }
    }
    stable_payload_digest(&(resource, expected_revision, command))
}

#[allow(clippy::too_many_arguments)]
fn claim_mutation_receipt(
    connection: &Connection,
    idempotency_key: &str,
    actor_principal: &str,
    action_id: &str,
    resource_ref: &str,
    expected_revision: Option<u64>,
    payload_digest: &str,
    correlation_id: &str,
) -> Result<MfgMutationClaim, MfgRepositoryError> {
    let action_id_value = app_mfg_contract::MfgActionId::parse(action_id).ok_or_else(|| {
        MfgRepositoryError::CommandRejected(format!(
            "action is not in the canonical MFG contract: {action_id}"
        ))
    })?;
    let now = Utc::now();
    let pending_response = serde_json::json!({
        "kind": "mfg.mutation.accepted",
        "correlation_id": correlation_id,
        "resource_ref": resource_ref,
    });
    let receipt = app_mfg_contract::MfgReceiptV1 {
        receipt_id: format!("receipt-{}", uuid::Uuid::new_v4()),
        idempotency_key: idempotency_key.to_string(),
        actor_principal: actor_principal.to_string(),
        action_id: action_id_value,
        resource_ref: resource_ref.to_string(),
        expected_revision,
        result_revision: None,
        payload_digest: payload_digest.to_string(),
        correlation_id: Some(correlation_id.to_string()),
        status: app_mfg_contract::MfgReceiptStatus::Accepted,
        response: pending_response.clone(),
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        created_at: now,
        updated_at: now,
    };
    let inserted = connection.execute(
        "INSERT OR IGNORE INTO mfg_mutation_receipt (
            receipt_id, idempotency_key, actor_principal, action_id, resource_ref,
            expected_revision, result_revision, payload_digest, status, response_json,
            contract_version, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, 'accepted', ?8, ?9, ?10, ?10)",
        params![
            receipt.receipt_id,
            receipt.idempotency_key,
            receipt.actor_principal,
            action_id,
            receipt.resource_ref,
            expected_revision.map(|value| value as i64),
            receipt.payload_digest,
            serde_json::to_string(&pending_response)?,
            receipt.contract_version.0,
            now.to_rfc3339(),
        ],
    )?;
    if inserted == 1 {
        return Ok(MfgMutationClaim::Acquired(receipt));
    }
    let native_recovery = connection
        .query_row(
            "SELECT actor_principal, action_id, resource_ref, payload_digest, response_json
             FROM mfg_mutation_receipt WHERE idempotency_key = ?1",
            params![idempotency_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?
        .and_then(|(actor, action, resource, digest, json)| {
            serde_json::from_str::<MfgCommandReceipt>(&json)
                .ok()
                .map(|business| (actor, action, resource, digest, business))
        });
    if let Some((stored_actor, stored_action, stored_resource, stored_digest, business)) =
        native_recovery
    {
        if stored_actor == actor_principal
            && stored_action == action_id
            && stored_resource == resource_ref
            && stored_digest == payload_digest
            && business.actor_ref == actor_principal
            && business.action_id == action_id
            && business.subject_ref == resource_ref
        {
            return Ok(MfgMutationClaim::NativeRecovery(business));
        }
        return Err(MfgRepositoryError::CommandRejected(
            "idempotency key is already bound to another native business command".to_string(),
        ));
    }
    match find_mutation_receipt(
        connection,
        idempotency_key,
        actor_principal,
        action_id,
        resource_ref,
        payload_digest,
    )? {
        Some((receipt, _)) if receipt.status == app_mfg_contract::MfgReceiptStatus::Accepted => {
            Ok(MfgMutationClaim::Pending(receipt))
        }
        Some((receipt, response)) => Ok(MfgMutationClaim::Replayed(receipt, response)),
        None => Err(MfgRepositoryError::CommandRejected(
            "idempotency key is occupied by an unfinished business command".to_string(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn find_mutation_receipt(
    connection: &Connection,
    idempotency_key: &str,
    actor_principal: &str,
    action_id: &str,
    resource_ref: &str,
    payload_digest: &str,
) -> Result<Option<(app_mfg_contract::MfgReceiptV1, serde_json::Value)>, MfgRepositoryError> {
    let stored = connection
        .query_row(
            "SELECT receipt_id, actor_principal, action_id, resource_ref,
                    expected_revision, result_revision, payload_digest, status,
                    response_json, contract_version, created_at, updated_at
             FROM mfg_mutation_receipt WHERE idempotency_key = ?1",
            params![idempotency_key],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )
        .optional()?;
    let Some((
        receipt_id,
        stored_actor,
        stored_action,
        stored_resource,
        expected_revision,
        result_revision,
        stored_digest,
        status,
        response_json,
        contract_version,
        created_at,
        updated_at,
    )) = stored
    else {
        return Ok(None);
    };
    // Native command handlers persist a crash-recovery record before the
    // Gateway middleware can finalize the canonical governance receipt. Let
    // the handler replay that command and upgrade this same row instead of
    // treating the temporary record as a conflicting second authority.
    if let Ok(business) = serde_json::from_str::<MfgCommandReceipt>(&response_json) {
        if business.actor_ref == actor_principal && business.action_id == action_id {
            return Ok(None);
        }
        return Err(MfgRepositoryError::CommandRejected(
            "idempotency key is already bound to another actor or action".to_string(),
        ));
    }
    if stored_actor != actor_principal
        || stored_action != action_id
        || stored_resource != resource_ref
        || stored_digest != payload_digest
    {
        return Err(MfgRepositoryError::CommandRejected(
            "idempotency key is already bound to another actor/action/resource/payload".to_string(),
        ));
    }
    let action_id = app_mfg_contract::MfgActionId::parse(&stored_action).ok_or_else(|| {
        MfgRepositoryError::CommandRejected(format!(
            "stored receipt action is not in the canonical contract: {stored_action}"
        ))
    })?;
    let parse_time = |value: String| {
        chrono::DateTime::parse_from_rfc3339(&value)
            .map(|value| value.with_timezone(&Utc))
            .map_err(|error| {
                MfgRepositoryError::CommandRejected(format!(
                    "stored receipt timestamp is invalid: {error}"
                ))
            })
    };
    let receipt_status = match status.as_str() {
        "preview" => app_mfg_contract::MfgReceiptStatus::Preview,
        "accepted" => app_mfg_contract::MfgReceiptStatus::Accepted,
        "replayed" => app_mfg_contract::MfgReceiptStatus::Replayed,
        "conflict" => app_mfg_contract::MfgReceiptStatus::Conflict,
        "rejected" => app_mfg_contract::MfgReceiptStatus::Rejected,
        "failed" => app_mfg_contract::MfgReceiptStatus::Failed,
        _ => app_mfg_contract::MfgReceiptStatus::Completed,
    };
    let response = serde_json::from_str::<serde_json::Value>(&response_json)?;
    Ok(Some((
        app_mfg_contract::MfgReceiptV1 {
            receipt_id,
            idempotency_key: idempotency_key.to_string(),
            actor_principal: stored_actor,
            action_id,
            resource_ref: stored_resource,
            expected_revision: expected_revision.map(|value| value as u64),
            result_revision: result_revision.map(|value| value as u64),
            payload_digest: stored_digest,
            correlation_id: find_string_recursive(&response, "correlation_id"),
            status: receipt_status,
            response: response.clone(),
            contract_version: app_mfg_contract::MfgContractVersion(contract_version),
            created_at: parse_time(created_at)?,
            updated_at: parse_time(updated_at)?,
        },
        response,
    )))
}

#[allow(clippy::too_many_arguments)]
fn record_mutation_receipt(
    connection: &Connection,
    idempotency_key: &str,
    actor_principal: &str,
    action_id: &str,
    resource_ref: &str,
    expected_revision: Option<u64>,
    result_revision: Option<u64>,
    payload_digest: &str,
    response: &serde_json::Value,
) -> Result<app_mfg_contract::MfgReceiptV1, MfgRepositoryError> {
    let transitional_identity = connection
        .query_row(
            "SELECT receipt_id, created_at
             FROM mfg_mutation_receipt
             WHERE idempotency_key = ?1
               AND status IN ('accepted', 'business_completed')",
            params![idempotency_key],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let existing = find_mutation_receipt(
        connection,
        idempotency_key,
        actor_principal,
        action_id,
        resource_ref,
        payload_digest,
    )?;
    if let Some((mut receipt, _)) = existing
        .as_ref()
        .filter(|(receipt, _)| receipt.status != app_mfg_contract::MfgReceiptStatus::Accepted)
        .cloned()
    {
        receipt.status = app_mfg_contract::MfgReceiptStatus::Replayed;
        return Ok(receipt);
    }
    let action_id_value = app_mfg_contract::MfgActionId::parse(action_id).ok_or_else(|| {
        MfgRepositoryError::CommandRejected(format!(
            "action is not in the canonical MFG contract: {action_id}"
        ))
    })?;
    let now = Utc::now();
    let (receipt_id, created_at) = existing
        .map(|(receipt, _)| (receipt.receipt_id, receipt.created_at))
        .or_else(|| {
            transitional_identity.and_then(|(receipt_id, created_at)| {
                chrono::DateTime::parse_from_rfc3339(&created_at)
                    .ok()
                    .map(|created_at| (receipt_id, created_at.with_timezone(&Utc)))
            })
        })
        .unwrap_or_else(|| (format!("receipt-{}", uuid::Uuid::new_v4()), now));
    let receipt = app_mfg_contract::MfgReceiptV1 {
        receipt_id,
        idempotency_key: idempotency_key.to_string(),
        actor_principal: actor_principal.to_string(),
        action_id: action_id_value,
        resource_ref: resource_ref.to_string(),
        expected_revision,
        result_revision,
        payload_digest: payload_digest.to_string(),
        correlation_id: find_string_recursive(response, "correlation_id"),
        status: app_mfg_contract::MfgReceiptStatus::Completed,
        response: response.clone(),
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        created_at,
        updated_at: now,
    };
    connection.execute(
        "INSERT INTO mfg_mutation_receipt (
            receipt_id, idempotency_key, actor_principal, action_id, resource_ref,
            expected_revision, result_revision, payload_digest, status, response_json,
            contract_version, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'completed', ?9, ?10, ?11, ?12)
         ON CONFLICT(idempotency_key) DO UPDATE SET
            receipt_id = excluded.receipt_id,
            actor_principal = excluded.actor_principal,
            action_id = excluded.action_id,
            resource_ref = excluded.resource_ref,
            expected_revision = excluded.expected_revision,
            result_revision = excluded.result_revision,
            payload_digest = excluded.payload_digest,
            status = excluded.status,
            response_json = excluded.response_json,
            contract_version = excluded.contract_version,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at",
        params![
            receipt.receipt_id,
            idempotency_key,
            receipt.actor_principal,
            action_id,
            receipt.resource_ref,
            expected_revision.map(|value| value as i64),
            result_revision.map(|value| value as i64),
            receipt.payload_digest,
            serde_json::to_string(response)?,
            receipt.contract_version.0,
            receipt.created_at.to_rfc3339(),
            now.to_rfc3339(),
        ],
    )?;
    append_projection_event(
        connection,
        "receipt",
        &format!("mfg:receipt:{}", receipt.receipt_id),
        "receipt.completed",
        serde_json::json!({"receipt": receipt}),
    )?;
    Ok(receipt)
}

fn find_string_recursive(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(object) => object
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                object
                    .values()
                    .find_map(|child| find_string_recursive(child, key))
            }),
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_string_recursive(child, key)),
        _ => None,
    }
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
    let previous_revision = find_cockpit_profile(connection, &profile.profile_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let action_id = canonical_upsert_action_id(
        "cockpit",
        command,
        if expected_revision.is_some() { 1 } else { 0 },
    );
    let payload_digest = stable_upsert_payload_digest(profile, expected_revision, command)?;
    if let Some(receipt) = find_command_receipt(
        connection,
        idempotency_key,
        actor_ref,
        &action_id,
        &subject_ref,
        &payload_digest,
    )? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let profile = command_receipt_snapshot(&receipt)
            .or(find_cockpit_profile(connection, &profile.profile_id)?)
            .ok_or_else(|| MfgRepositoryError::NotFound(profile.profile_id.clone()))?;
        return Ok((profile, receipt));
    }
    let profile = upsert_cockpit_profile(connection, profile, expected_revision)?;
    let mut receipt = mutation_receipt(
        "cockpit",
        subject_ref.clone(),
        command,
        actor_ref,
        idempotency_key,
        payload_digest,
        previous_revision,
        profile.revision,
    )?;
    receipt.response_snapshot = serde_json::to_value(&profile)?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "cockpit",
        &subject_ref,
        "profile.receipted",
        serde_json::json!({ "profile": profile, "receipt": receipt }),
    )?;
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
    let action_id = canonical_action_id("cockpit", "profile.delete");
    let payload_digest = stable_payload_digest(&(profile_id, expected_revision))?;
    if let Some(receipt) = find_command_receipt(
        connection,
        idempotency_key,
        actor_ref,
        &action_id,
        &subject_ref,
        &payload_digest,
    )? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let profile = command_receipt_snapshot(&receipt);
        return Ok((profile, receipt));
    }
    let profile = find_cockpit_profile(connection, profile_id)?
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
    let mut receipt = mutation_receipt(
        "cockpit",
        subject_ref.clone(),
        "profile.delete",
        actor_ref,
        idempotency_key,
        payload_digest,
        profile.revision,
        profile.revision,
    )?;
    receipt.response_snapshot = serde_json::to_value(&profile)?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "cockpit",
        &subject_ref,
        "profile.deleted",
        serde_json::json!({ "profile": profile, "receipt": receipt }),
    )?;
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
    let previous_revision = find_alert_rule(connection, &rule.rule_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let action_id = canonical_upsert_action_id(
        "alert",
        "rule.upsert",
        if expected_revision.is_some() { 1 } else { 0 },
    );
    let payload_digest = stable_upsert_payload_digest(rule, expected_revision, "rule.upsert")?;
    if let Some(receipt) = find_command_receipt(
        connection,
        idempotency_key,
        actor_ref,
        &action_id,
        &subject_ref,
        &payload_digest,
    )? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let rule = command_receipt_snapshot(&receipt)
            .or(find_alert_rule(connection, &rule.rule_id)?)
            .ok_or_else(|| MfgRepositoryError::NotFound(rule.rule_id.clone()))?;
        return Ok((rule, receipt));
    }
    let rule = upsert_alert_rule(connection, rule, expected_revision)?;
    let mut receipt = mutation_receipt(
        "alert",
        subject_ref.clone(),
        "rule.upsert",
        actor_ref,
        idempotency_key,
        payload_digest,
        previous_revision,
        rule.revision,
    )?;
    receipt.response_snapshot = serde_json::to_value(&rule)?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "alert",
        &subject_ref,
        "alert_rule.receipted",
        serde_json::json!({ "rule": rule, "receipt": receipt }),
    )?;
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
    let previous_revision = find_alert_subscription(connection, &subscription.subscription_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let action_id = canonical_upsert_action_id(
        "alert",
        "subscription.upsert",
        if expected_revision.is_some() { 1 } else { 0 },
    );
    let payload_digest =
        stable_upsert_payload_digest(subscription, expected_revision, "subscription.upsert")?;
    if let Some(receipt) = find_command_receipt(
        connection,
        idempotency_key,
        actor_ref,
        &action_id,
        &subject_ref,
        &payload_digest,
    )? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let subscription = command_receipt_snapshot(&receipt)
            .or(find_alert_subscription(
                connection,
                &subscription.subscription_id,
            )?)
            .ok_or_else(|| MfgRepositoryError::NotFound(subscription.subscription_id.clone()))?;
        return Ok((subscription, receipt));
    }
    let subscription = upsert_alert_subscription(connection, subscription, expected_revision)?;
    let mut receipt = mutation_receipt(
        "alert",
        subject_ref.clone(),
        "subscription.upsert",
        actor_ref,
        idempotency_key,
        payload_digest,
        previous_revision,
        subscription.revision,
    )?;
    receipt.response_snapshot = serde_json::to_value(&subscription)?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "alert",
        &subject_ref,
        "alert_subscription.receipted",
        serde_json::json!({ "subscription": subscription, "receipt": receipt }),
    )?;
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
    let previous_revision = find_assignment(connection, &assignment.assignment_id)?
        .map(|item| item.revision)
        .unwrap_or_default();
    let action_id = canonical_upsert_action_id(
        "assignment",
        "assignment.upsert",
        if expected_revision.is_some() { 1 } else { 0 },
    );
    let payload_digest =
        stable_upsert_payload_digest(assignment, expected_revision, "assignment.upsert")?;
    if let Some(receipt) = find_command_receipt(
        connection,
        idempotency_key,
        actor_ref,
        &action_id,
        &subject_ref,
        &payload_digest,
    )? {
        if receipt.actor_ref != actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let assignment = command_receipt_snapshot(&receipt)
            .or(find_assignment(connection, &assignment.assignment_id)?)
            .ok_or_else(|| MfgRepositoryError::NotFound(assignment.assignment_id.clone()))?;
        return Ok((assignment, receipt));
    }
    let assignment = upsert_assignment(connection, assignment, expected_revision)?;
    let mut receipt = mutation_receipt(
        "assignment",
        subject_ref.clone(),
        "assignment.upsert",
        actor_ref,
        idempotency_key,
        payload_digest,
        previous_revision,
        assignment.revision,
    )?;
    receipt.response_snapshot = serde_json::to_value(&assignment)?;
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "assignment",
        &subject_ref,
        "assignment.receipted",
        serde_json::json!({ "assignment": assignment, "receipt": receipt }),
    )?;
    Ok((assignment, receipt))
}

fn record_command_notifications(
    connection: &Connection,
    idempotency_key: &str,
    notification_refs: Vec<String>,
) -> Result<MfgCommandReceipt, MfgRepositoryError> {
    let value = connection
        .query_row(
            "SELECT response_json FROM mfg_mutation_receipt WHERE idempotency_key = ?1",
            params![idempotency_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| MfgRepositoryError::NotFound(idempotency_key.to_string()))?;
    let mut receipt: MfgCommandReceipt = serde_json::from_str(&value)?;
    receipt.notification_refs = notification_refs;
    connection.execute(
        "UPDATE mfg_mutation_receipt
         SET response_json = ?2, updated_at = ?3
         WHERE idempotency_key = ?1",
        params![
            idempotency_key,
            serde_json::to_string(&receipt)?,
            Utc::now().to_rfc3339()
        ],
    )?;
    append_projection_event(
        connection,
        &receipt.domain,
        &receipt.subject_ref,
        "notification.delivery_observed",
        serde_json::json!({"receipt": receipt}),
    )?;
    Ok(receipt)
}

fn command_notification_refs_for_resource(
    connection: &Connection,
    resource_ref: &str,
) -> Result<Vec<String>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT response_json
         FROM mfg_mutation_receipt
         WHERE resource_ref = ?1
         ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![resource_ref], |row| row.get::<_, String>(0))?;
    for row in rows {
        let json = row?;
        if let Ok(value) = serde_json::from_str::<Value>(&json) {
            if let Some(notification_refs) = find_notification_refs(&value) {
                return Ok(notification_refs);
            }
        }
    }
    Ok(Vec::new())
}

fn find_notification_refs(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Object(object) => {
            if let Some(notification_refs) =
                object.get("notification_refs").and_then(Value::as_array)
            {
                let notification_refs = notification_refs
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                if !notification_refs.is_empty() {
                    return Some(notification_refs);
                }
            }
            // The finalized Gateway row nests the native MfgCommandReceipt as
            // response.business_receipt.response. Recursive typed traversal
            // keeps the durable backlink available after that in-place
            // governance upgrade.
            object.values().find_map(find_notification_refs)
        }
        Value::Array(items) => items.iter().find_map(find_notification_refs),
        _ => None,
    }
}

fn upsert_alert_rule(
    connection: &Connection,
    rule: &MfgAlertRule,
    expected_revision: Option<u64>,
) -> Result<MfgAlertRule, MfgRepositoryError> {
    let mut rule = rule.clone();
    validate_alert_rule_condition(&rule.condition)?;
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
        None if expected_revision.is_some() => {
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
        serde_json::json!({"rule": rule}),
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
        None if expected_revision.is_some() => {
            return Err(MfgRepositoryError::RevisionConflict {
                domain: "alert_subscription".to_string(),
                subject_id: subscription.subscription_id.clone(),
                expected: expected_revision,
                actual: None,
            });
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
        serde_json::json!({"subscription": subscription}),
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
    rows.map(|row| -> Result<MfgAlertSubscription, MfgRepositoryError> {
        Ok(serde_json::from_str::<MfgAlertSubscription>(&row?)?)
    })
    .filter(|item| {
        item.as_ref().map_or(true, |subscription| {
            subscriber_ref.is_none_or(|filter| subscription.subscriber_ref == filter)
        })
    })
    .collect::<Result<Vec<_>, MfgRepositoryError>>()
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
            && attention_matches_alert_condition(item, &rule.condition)
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
            serde_json::json!({
                "occurrence": occurrence,
                "owner_ref": rule.owner_ref,
            }),
        )?;
    }
    Ok(())
}

fn validate_alert_rule_condition(condition: &Value) -> Result<(), MfgRepositoryError> {
    if condition.is_null() {
        return Ok(());
    }
    let object = condition.as_object().ok_or_else(|| {
        MfgRepositoryError::CommandRejected("alert rule condition must be an object".to_string())
    })?;
    if object.is_empty() {
        return Ok(());
    }
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "field" | "operator" | "threshold" | "severity_in" | "status_in" | "window_minutes"
        ) {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "unsupported alert rule condition key: {key}"
            )));
        }
    }
    let numeric_keys_present = object.contains_key("field")
        || object.contains_key("operator")
        || object.contains_key("threshold");
    if numeric_keys_present {
        let field = object.get("field").and_then(Value::as_str).ok_or_else(|| {
            MfgRepositoryError::CommandRejected(
                "alert rule numeric condition requires field".to_string(),
            )
        })?;
        if !matches!(
            field,
            "priority_score" | "urgency" | "confidence" | "strategic_weight"
        ) {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "unsupported alert rule condition field: {field}"
            )));
        }
        let operator = object
            .get("operator")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                MfgRepositoryError::CommandRejected(
                    "alert rule numeric condition requires operator".to_string(),
                )
            })?;
        if !matches!(operator, "gt" | "gte" | "lt" | "lte" | "eq") {
            return Err(MfgRepositoryError::CommandRejected(format!(
                "unsupported alert rule condition operator: {operator}"
            )));
        }
        if object.get("threshold").and_then(Value::as_f64).is_none() {
            return Err(MfgRepositoryError::CommandRejected(
                "alert rule numeric condition requires a numeric threshold".to_string(),
            ));
        }
    }
    for key in ["severity_in", "status_in"] {
        if let Some(value) = object.get(key) {
            let valid = value.as_array().is_some_and(|values| {
                !values.is_empty() && values.iter().all(|value| value.as_str().is_some())
            });
            if !valid {
                return Err(MfgRepositoryError::CommandRejected(format!(
                    "alert rule condition {key} must be a non-empty string array"
                )));
            }
        }
    }
    if object.get("window_minutes").is_some_and(|value| {
        !value
            .as_u64()
            .is_some_and(|minutes| (1..=10_080).contains(&minutes))
    }) {
        return Err(MfgRepositoryError::CommandRejected(
            "alert rule condition window_minutes must be between 1 and 10080".to_string(),
        ));
    }
    Ok(())
}

fn attention_matches_alert_condition(attention: &MatrixAttentionItem, condition: &Value) -> bool {
    let Some(object) = condition.as_object() else {
        return condition.is_null();
    };
    if object.is_empty() {
        return true;
    }
    if let Some(window_minutes) = object.get("window_minutes").and_then(Value::as_u64) {
        let oldest = Utc::now() - chrono::Duration::minutes(window_minutes as i64);
        if attention.updated_at < oldest {
            return false;
        }
    }
    if let Some(values) = object.get("severity_in").and_then(Value::as_array) {
        let severity = match attention.severity {
            MatrixSeverity::Normal => "normal",
            MatrixSeverity::Warning => "warning",
            MatrixSeverity::Critical => "critical",
            MatrixSeverity::Unknown => "unknown",
        };
        if !values.iter().any(|value| value.as_str() == Some(severity)) {
            return false;
        }
    }
    if let Some(values) = object.get("status_in").and_then(Value::as_array) {
        if !values
            .iter()
            .any(|value| value.as_str() == Some(attention.status.as_str()))
        {
            return false;
        }
    }
    let Some(field) = object.get("field").and_then(Value::as_str) else {
        return true;
    };
    let Some(operator) = object.get("operator").and_then(Value::as_str) else {
        return false;
    };
    let Some(threshold) = object.get("threshold").and_then(Value::as_f64) else {
        return false;
    };
    let actual = match field {
        "priority_score" => f64::from(attention.priority_score),
        "urgency" => f64::from(attention.urgency),
        "confidence" => f64::from(attention.confidence),
        "strategic_weight" => f64::from(attention.strategic_weight),
        _ => return false,
    };
    match operator {
        "gt" => actual > threshold,
        "gte" => actual >= threshold,
        "lt" => actual < threshold,
        "lte" => actual <= threshold,
        "eq" => (actual - threshold).abs() <= f64::EPSILON,
        _ => false,
    }
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
    let command = format!("{:?}", input.command).to_lowercase();
    let action_id = canonical_action_id("alert", &command);
    let payload_digest = stable_payload_digest(&input)?;
    if let Some(receipt) = find_command_receipt(
        connection,
        &input.idempotency_key,
        &input.actor_ref,
        &action_id,
        &subject_ref,
        &payload_digest,
    )? {
        if receipt.actor_ref != input.actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let occurrence = command_receipt_snapshot(&receipt)
            .or(find_alert_occurrence(connection, occurrence_id)?)
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
    let receipt = MfgCommandReceipt {
        receipt_id: format!("receipt-{}", uuid::Uuid::new_v4()),
        domain: "alert".to_string(),
        subject_ref: subject_ref.clone(),
        command: command.clone(),
        action_id,
        actor_ref: input.actor_ref,
        idempotency_key: input.idempotency_key,
        payload_digest,
        correlation_id: None,
        contract_version: app_mfg_contract::MFG_CONTRACT_VERSION.to_string(),
        idempotent_replay: false,
        previous_revision,
        current_revision: occurrence.revision,
        audit_ref: format!("audit://mfg/alert/{occurrence_id}/{}", occurrence.revision),
        notification_refs: Vec::new(),
        response_snapshot: serde_json::to_value(&occurrence)?,
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
    for metric_ref in metric_refs {
        if forecasts.iter().any(|item| item.metric_ref == *metric_ref) {
            continue;
        }
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
    validate_assignment(assignment)?;
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
        None if expected_revision.is_some() => {
            return Err(MfgRepositoryError::RevisionConflict {
                domain: "assignment".to_string(),
                subject_id: assignment.assignment_id.clone(),
                expected: expected_revision,
                actual: None,
            });
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
        serde_json::json!({"assignment": assignment}),
    )?;
    Ok(assignment)
}

fn validate_assignment(assignment: &MfgAssignment) -> Result<(), MfgRepositoryError> {
    if assignment.task_ref.trim().is_empty() || assignment.assignee_ref.trim().is_empty() {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment requires task_ref and assignee_ref".to_string(),
        ));
    }
    if !matches!(
        assignment.assignee_kind.as_str(),
        "user" | "agent" | "team" | "role" | "organization"
    ) {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment assignee_kind must be user, agent, team, role, or organization".to_string(),
        ));
    }
    if !matches!(
        assignment.priority.as_str(),
        "low" | "normal" | "high" | "critical" | "urgent"
    ) {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment priority must be low, normal, high, critical, or urgent".to_string(),
        ));
    }
    if !matches!(
        assignment.visibility.as_str(),
        "private" | "team" | "public"
    ) {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment visibility must be private, team, or public".to_string(),
        ));
    }
    if assignment.sla_minutes == Some(0) {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment sla_minutes must be greater than zero".to_string(),
        ));
    }
    let mut watchers = BTreeSet::new();
    if assignment
        .watcher_refs
        .iter()
        .any(|watcher| watcher.trim().is_empty() || !watchers.insert(watcher))
    {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment watcher_refs must be unique and non-empty".to_string(),
        ));
    }
    if assignment
        .notification_targets
        .iter()
        .any(|target| target.surface.trim().is_empty() || target.recipient.trim().is_empty())
    {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment notification targets require surface and recipient".to_string(),
        ));
    }
    Ok(())
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
    let command = format!("{:?}", input.command).to_lowercase();
    let action_id = canonical_action_id("assignment", &command);
    let payload_digest = stable_payload_digest(&input)?;
    if let Some(receipt) = find_command_receipt(
        connection,
        &input.idempotency_key,
        &input.actor_ref,
        &action_id,
        &subject_ref,
        &payload_digest,
    )? {
        if receipt.actor_ref != input.actor_ref {
            return Err(MfgRepositoryError::CommandRejected(
                "idempotency key is bound to another actor".to_string(),
            ));
        }
        let assignment = command_receipt_snapshot(&receipt)
            .or(find_assignment(connection, assignment_id)?)
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
            | MfgAssignmentCommand::Start
            | MfgAssignmentCommand::Complete
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
    if assignment.status == "completion_pending"
        && (!matches!(input.command, MfgAssignmentCommand::Complete)
            || assignment.lifecycle_correlation_id.as_deref() != Some(&input.correlation_id))
    {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment completion is reserved by another lifecycle command".to_string(),
        ));
    }
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
            if let Some(kind) = assignment_kind_from_ref(&assignment.assignee_ref) {
                assignment.assignee_kind = kind.to_string();
            }
            assignment.status = "assigned".to_string();
        }
        MfgAssignmentCommand::Claim => {
            assignment.assignee_ref = input.actor_ref.clone();
            assignment.assignee_kind = "user".to_string();
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
        MfgAssignmentCommand::Start => {
            if !matches!(
                assignment.status.as_str(),
                "assigned" | "claimed" | "update_requested"
            ) {
                return Err(MfgRepositoryError::CommandRejected(format!(
                    "assignment cannot start from status {}",
                    assignment.status
                )));
            }
            assignment.status = "in_progress".to_string();
            assignment.lifecycle_correlation_id = Some(input.correlation_id.clone());
        }
        MfgAssignmentCommand::Complete => {
            if assignment.status != "completion_pending" {
                return Err(MfgRepositoryError::CommandRejected(format!(
                    "assignment cannot complete from status {}",
                    assignment.status
                )));
            }
            let evidence = input.completion_evidence.as_ref().ok_or_else(|| {
                MfgRepositoryError::CommandRejected(
                    "mfg_assignment_task_transition_required".to_string(),
                )
            })?;
            if evidence.task_ref != assignment.task_ref
                || evidence.workflow_node_id != assignment.workflow_node_id
                || evidence.correlation_id != input.correlation_id
                || evidence.receipt_ref.trim().is_empty()
                || !matches!(
                    evidence.terminal_status.as_str(),
                    "completed" | "blocked" | "failed" | "cancelled"
                )
                || evidence.owner_kind != "runtime_assignment_terminal_observation"
            {
                return Err(MfgRepositoryError::CommandRejected(
                    "assignment completion evidence does not match the canonical task binding"
                        .to_string(),
                ));
            }
            assignment.status = "completed".to_string();
            assignment.completion_ref = Some(evidence.receipt_ref.clone());
            assignment.lifecycle_correlation_id = Some(evidence.correlation_id.clone());
        }
    }
    assignment.revision = assignment.revision.saturating_add(1);
    assignment.updated_at = Utc::now();
    save_assignment(connection, &assignment)?;
    let receipt = MfgCommandReceipt {
        receipt_id: format!("receipt-{}", uuid::Uuid::new_v4()),
        domain: "assignment".to_string(),
        subject_ref: subject_ref.clone(),
        command: command.clone(),
        action_id,
        actor_ref: input.actor_ref,
        idempotency_key: input.idempotency_key,
        payload_digest,
        correlation_id: Some(input.correlation_id.clone()),
        contract_version: app_mfg_contract::MFG_CONTRACT_VERSION.to_string(),
        idempotent_replay: false,
        previous_revision,
        current_revision: assignment.revision,
        audit_ref: format!(
            "audit://mfg/assignment/{assignment_id}/{}",
            assignment.revision
        ),
        notification_refs: Vec::new(),
        response_snapshot: serde_json::to_value(&assignment)?,
        created_at: Utc::now(),
    };
    insert_command_receipt(connection, &receipt)?;
    append_projection_event(
        connection,
        "assignment",
        &subject_ref,
        &format!("assignment.{command}"),
        serde_json::json!({
            "assignment": assignment,
            "receipt": receipt,
            "reason": input.reason,
            "correlation_id": input.correlation_id,
        }),
    )?;
    Ok((assignment, receipt))
}

fn reserve_assignment_completion(
    connection: &Connection,
    assignment_id: &str,
    expected_revision: u64,
    actor_ref: &str,
    correlation_id: &str,
) -> Result<MfgAssignment, MfgRepositoryError> {
    let mut assignment = find_assignment(connection, assignment_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(assignment_id.to_string()))?;
    if assignment.revision != expected_revision {
        return Err(MfgRepositoryError::RevisionConflict {
            domain: "assignment".to_string(),
            subject_id: assignment_id.to_string(),
            expected: Some(expected_revision),
            actual: Some(assignment.revision),
        });
    }
    if assignment.status != "in_progress" {
        return Err(MfgRepositoryError::CommandRejected(format!(
            "assignment cannot reserve completion from status {}",
            assignment.status
        )));
    }
    if actor_ref != assignment.created_by && actor_ref != assignment.assignee_ref {
        return Err(MfgRepositoryError::CommandRejected(
            "assignment completion requires the owner or current assignee".to_string(),
        ));
    }
    assignment.status = "completion_pending".to_string();
    assignment.lifecycle_correlation_id = Some(correlation_id.to_string());
    assignment.revision = assignment.revision.saturating_add(1);
    assignment.updated_at = Utc::now();
    let assignment_json = serde_json::to_string(&assignment)?;
    let changed = connection.execute(
        "UPDATE mfg_assignment
         SET status = 'completion_pending',
             revision = ?2,
             assignment_json = ?3,
             updated_at = ?4
         WHERE assignment_id = ?1
           AND revision = ?5
           AND status = 'in_progress'",
        params![
            assignment_id,
            assignment.revision as i64,
            assignment_json,
            assignment.updated_at.to_rfc3339(),
            expected_revision as i64,
        ],
    )?;
    if changed != 1 {
        let actual = find_assignment(connection, assignment_id)?.map(|item| item.revision);
        return Err(MfgRepositoryError::RevisionConflict {
            domain: "assignment".to_string(),
            subject_id: assignment_id.to_string(),
            expected: Some(expected_revision),
            actual,
        });
    }
    append_projection_event(
        connection,
        "assignment",
        &format!("mfg:assignment:{assignment_id}"),
        "assignment.completion_reserved",
        serde_json::json!({
            "assignment": assignment,
            "correlation_id": correlation_id,
        }),
    )?;
    Ok(assignment)
}

fn assignment_kind_from_ref(reference: &str) -> Option<&'static str> {
    ["user", "agent", "team", "role", "organization"]
        .into_iter()
        .find(|kind| reference.starts_with(&format!("{kind}:")))
}

fn build_live_snapshot_read(
    connection: &Connection,
) -> Result<MfgLiveSnapshotRead, MfgRepositoryError> {
    let epoch = load_live_epoch(connection)?;
    let high_cursor = connection.query_row(
        "SELECT COALESCE(MAX(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let state = MfgLiveSnapshotStateV1 {
        cockpit: serde_json::json!({
            "profiles": json_column_rows(
                connection,
                "SELECT profile_json FROM mfg_cockpit_profile ORDER BY updated_at DESC",
            )?,
        }),
        alerts: serde_json::json!({
            "rules": json_column_rows(
                connection,
                "SELECT rule_json FROM mfg_alert_rule ORDER BY updated_at DESC",
            )?,
            "subscriptions": json_column_rows(
                connection,
                "SELECT subscription_json FROM mfg_alert_subscription ORDER BY updated_at DESC",
            )?,
            "occurrences": json_column_rows(
                connection,
                "SELECT occurrence_json FROM mfg_alert_occurrence ORDER BY updated_at DESC",
            )?,
        }),
        assignments: serde_json::json!({
            "items": json_column_rows(
                connection,
                "SELECT assignment_json FROM mfg_assignment ORDER BY updated_at DESC",
            )?,
        }),
        incidents: serde_json::json!({
            "items": json_column_rows(
                connection,
                "SELECT incident_json FROM mfg_incident ORDER BY updated_at DESC",
            )?,
            "workflows": json_column_rows(
                connection,
                "SELECT graph_json FROM mfg_workflow_graph ORDER BY updated_at DESC",
            )?,
            "analyses": json_column_rows(
                connection,
                "SELECT analysis_json FROM mfg_operational_analysis ORDER BY created_at DESC",
            )?,
            "memory_cases": json_column_rows(
                connection,
                "SELECT memory_case_json FROM mfg_memory_case ORDER BY created_at DESC",
            )?,
            "playbooks": json_column_rows(
                connection,
                "SELECT playbook_json FROM mfg_playbook ORDER BY updated_at DESC",
            )?,
        }),
        executions: serde_json::json!({
            "actions": json_column_rows(
                connection,
                "SELECT execution_json FROM mfg_action_execution ORDER BY updated_at DESC",
            )?,
            "skills": json_column_rows(
                connection,
                "SELECT execution_json FROM mfg_skill_execution ORDER BY updated_at DESC",
            )?,
        }),
        reports: serde_json::json!({
            "items": json_column_rows(
                connection,
                "SELECT report_json FROM mfg_cockpit_report ORDER BY created_at DESC",
            )?,
        }),
        reviews: serde_json::json!({
            "items": json_column_rows(
                connection,
                "SELECT review_json FROM mfg_report_delivery_review ORDER BY updated_at DESC",
            )?,
        }),
        receipts: serde_json::json!({
            "commands": json_column_rows(
                connection,
                "SELECT receipt_json FROM mfg_command_receipt ORDER BY created_at DESC",
            )?,
            "mutations": mutation_receipt_rows(connection)?,
        }),
        data_compute: serde_json::json!({
            "entities": json_column_rows(
                connection,
                "SELECT entity_json FROM matrix_entity ORDER BY updated_at DESC",
            )?,
            "relations": json_column_rows(
                connection,
                "SELECT relation_json FROM matrix_relation ORDER BY updated_at DESC",
            )?,
            "facts": serde_json::to_value(list_facts(connection, i64::MAX as usize)?)?,
            "attention": json_column_rows(
                connection,
                "SELECT attention_json FROM matrix_attention_item ORDER BY updated_at DESC",
            )?,
            "evidence": json_column_rows(
                connection,
                "SELECT packet_json FROM matrix_evidence_packet ORDER BY created_at DESC",
            )?,
            "quality_gates": json_column_rows(
                connection,
                "SELECT gate_json FROM matrix_quality_gate ORDER BY created_at DESC",
            )?,
            "metric_definitions": json_column_rows(
                connection,
                "SELECT definition_json FROM matrix_metric_definition ORDER BY updated_at DESC",
            )?,
            "metric_dependencies": json_column_rows(
                connection,
                "SELECT dependency_json FROM matrix_metric_dependency ORDER BY updated_at DESC",
            )?,
            "metric_states": json_column_rows(
                connection,
                "SELECT state_json FROM matrix_metric_state ORDER BY computed_at DESC",
            )?,
            "metric_snapshots": json_column_rows(
                connection,
                "SELECT snapshot_json FROM matrix_metric_snapshot ORDER BY created_at DESC",
            )?,
            "watermarks": json_column_rows(
                connection,
                "SELECT watermark_json FROM matrix_data_plane_watermark ORDER BY updated_at DESC",
            )?,
            "jobs": json_column_rows(
                connection,
                "SELECT job_json FROM matrix_compute_job ORDER BY updated_at DESC",
            )?,
            "changes": json_column_rows(
                connection,
                "SELECT change_json FROM matrix_change_event ORDER BY detected_at DESC",
            )?,
            "source_packs": json_column_rows(
                connection,
                "SELECT source_pack_json FROM matrix_source_pack ORDER BY updated_at DESC",
            )?,
            "connector_runs": json_column_rows(
                connection,
                "SELECT run_json FROM matrix_connector_run ORDER BY updated_at DESC",
            )?,
            "ontology_packs": json_column_rows(
                connection,
                "SELECT pack_json FROM matrix_ontology_pack ORDER BY updated_at DESC",
            )?,
            "entity_match_candidates": json_column_rows(
                connection,
                "SELECT candidate_json FROM matrix_entity_match_candidate ORDER BY created_at DESC",
            )?,
            "entity_conflict_decisions": json_column_rows(
                connection,
                "SELECT decision_json FROM matrix_entity_conflict_decision ORDER BY decided_at DESC",
            )?,
        }),
    };
    Ok(MfgLiveSnapshotRead {
        epoch,
        high_cursor,
        state,
    })
}

fn build_live_delta_read(
    connection: &Connection,
    cursor: u64,
    limit: usize,
) -> Result<MfgLiveDeltaRead, MfgRepositoryError> {
    let epoch = load_live_epoch(connection)?;
    let high_cursor = connection.query_row(
        "SELECT COALESCE(MAX(event_id), 0) FROM mfg_projection_event",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    let resync_reason = if cursor < epoch.retention_low_cursor {
        Some("cursor_below_retention_low_watermark".to_string())
    } else if cursor > high_cursor {
        Some("cursor_ahead_of_retained_log".to_string())
    } else {
        None
    };
    let events = if resync_reason.is_some() {
        Vec::new()
    } else {
        let mut statement = connection.prepare(
            "SELECT event_id, event_type, subject_ref, event_json, created_at
             FROM mfg_projection_event
             WHERE event_id > ?1
             ORDER BY event_id ASC
             LIMIT ?2",
        )?;
        let rows = statement
            .query_map(params![cursor, limit.clamp(1, 500)], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .map(|row| {
                let (cursor, event_type, subject_ref, event_json, created_at) = row?;
                let event_json: Value = serde_json::from_str(&event_json)?;
                Ok(MfgLiveProjectionEvent {
                    cursor,
                    event_type,
                    subject_ref,
                    payload: event_json.get("payload").cloned().unwrap_or(Value::Null),
                    created_at: parse_rfc3339_utc(&created_at)?,
                })
            })
            .collect::<Result<Vec<_>, MfgRepositoryError>>()?;
        rows
    };
    Ok(MfgLiveDeltaRead {
        epoch,
        base_cursor: cursor,
        high_cursor,
        events,
        resync_reason,
    })
}

fn json_column_rows(connection: &Connection, sql: &str) -> Result<Vec<Value>, MfgRepositoryError> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|row| -> Result<Value, MfgRepositoryError> {
            let json = row?;
            serde_json::from_str::<Value>(&json).map_err(MfgRepositoryError::from)
        })
        .collect::<Result<Vec<_>, MfgRepositoryError>>()?;
    Ok(rows)
}

fn mutation_receipt_rows(connection: &Connection) -> Result<Vec<Value>, MfgRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT receipt_id, idempotency_key, actor_principal, action_id,
                resource_ref, expected_revision, result_revision, payload_digest,
                status, response_json, contract_version, created_at, updated_at
         FROM mfg_mutation_receipt ORDER BY updated_at DESC",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<u64>>(5)?,
                row.get::<_, Option<u64>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
            ))
        })?
        .map(|row| -> Result<Value, MfgRepositoryError> {
            let (
                receipt_id,
                idempotency_key,
                actor_principal,
                action_id,
                resource_ref,
                expected_revision,
                result_revision,
                payload_digest,
                status,
                response_json,
                contract_version,
                created_at,
                updated_at,
            ) = row?;
            Ok(serde_json::json!({
                "receipt_id": receipt_id,
                "idempotency_key": idempotency_key,
                "actor_principal": actor_principal,
                "action_id": action_id,
                "resource_ref": resource_ref,
                "expected_revision": expected_revision,
                "result_revision": result_revision,
                "payload_digest": payload_digest,
                "status": status,
                "response": serde_json::from_str::<Value>(&response_json)?,
                "contract_version": contract_version,
                "created_at": created_at,
                "updated_at": updated_at,
            }))
        })
        .collect::<Result<Vec<_>, MfgRepositoryError>>()?;
    Ok(rows)
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:ontology:{}", pack.ontology_id),
        "ontology.updated",
        serde_json::json!({"ontology": pack}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:entity-match:{}", candidate.candidate_id),
        "entity.match_candidate_updated",
        serde_json::json!({"entity_match_candidate": candidate}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:entity-conflict:{}", decision.decision_id),
        "entity.conflict_decided",
        serde_json::json!({"entity_conflict_decision": decision}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:entity:{}", entity.entity_id),
        "entity.updated",
        serde_json::json!({"entity": entity}),
    )?;
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:relation:{}", relation.relation_id),
        "relation.updated",
        serde_json::json!({"relation": relation}),
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
    for rule in list_alert_rules(connection, None, 500)? {
        materialize_alert_occurrences(connection, &rule)?;
    }
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:attention:{}", item.attention_id),
        "attention.updated",
        serde_json::json!({"attention": item}),
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

fn build_evidence_packet_transaction(
    connection: &Connection,
    attention_id: Option<&str>,
    problem_statement: Option<&str>,
    packet_id: Option<&str>,
) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
    let attention = match attention_id {
        Some(id) => Some(
            find_attention(connection, id)?
                .ok_or_else(|| MfgRepositoryError::NotFound(id.to_string()))?,
        ),
        None => latest_attention(connection)?,
    };
    let mut packet = MatrixEvidencePacket::new(problem_statement.unwrap_or_else(|| {
        attention
            .as_ref()
            .map(|item| item.title.as_str())
            .unwrap_or("MATRIX operational evidence packet")
    }));
    if let Some(packet_id) = packet_id {
        packet.packet_id = packet_id.to_string();
    }
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
                if let Some(change) = find_change(connection, change_id)? {
                    packet.change_evidence.push(serde_json::to_value(&change)?);
                    if let Some(metric_id) = change.metric_id.as_deref() {
                        if let Some(state) = latest_metric_state_for_metric(connection, metric_id)?
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
    insert_evidence_packet(connection, &packet)?;
    Ok(packet)
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:evidence:{}", packet.packet_id),
        "evidence.updated",
        serde_json::json!({"evidence": packet}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:quality-gate:{}", gate.gate_id),
        "quality_gate.updated",
        serde_json::json!({"quality_gate": gate}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:metric-definition:{}", definition.metric_id),
        "metric_definition.updated",
        serde_json::json!({"metric_definition": definition}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:metric-dependency:{}", dependency.dependency_id),
        "metric_dependency.updated",
        serde_json::json!({"metric_dependency": dependency}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:metric-snapshot:{}", snapshot.snapshot_id),
        "metric_snapshot.updated",
        serde_json::json!({"metric_snapshot": snapshot}),
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
    append_projection_event(
        connection,
        "execution",
        &format!("mfg:skill-execution:{execution_id}"),
        "skill_run.updated",
        serde_json::json!({"skill_run": run}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:compute-job:{}", job.job_id),
        "compute_job.updated",
        serde_json::json!({"job": job}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:metric-state:{}", state.state_id),
        "metric_state.updated",
        serde_json::json!({"metric_state": state}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:change:{}", change.change_id),
        "metric_change.detected",
        serde_json::json!({"change": change}),
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
    append_projection_event(
        connection,
        "incident",
        &format!("mfg:incident:{}", incident.incident_id),
        "incident.updated",
        serde_json::json!({"incident": incident}),
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
    append_projection_event(
        connection,
        "incident",
        &format!("mfg:analysis:{}", analysis.analysis_id),
        "analysis.updated",
        serde_json::json!({"analysis": analysis}),
    )?;
    Ok(())
}

fn analyze_incident_transaction(
    connection: &Connection,
    incident_id: &str,
    analysis_id: Option<&str>,
) -> Result<MfgOperationalAnalysis, MfgRepositoryError> {
    let mut incident = find_incident(connection, incident_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(incident_id.to_string()))?;
    let packet_id = incident
        .evidence_packet_id
        .clone()
        .ok_or_else(|| MfgRepositoryError::NotFound("incident evidence packet".to_string()))?;
    let mut packet = find_evidence_packet(connection, &packet_id)?
        .ok_or_else(|| MfgRepositoryError::NotFound(packet_id.clone()))?;
    let mut analysis = MfgOperationalAnalysis::from_evidence(incident_id, &packet);
    if let Some(analysis_id) = analysis_id {
        analysis.analysis_id = analysis_id.to_string();
    }
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
        !item.contains("attribution_not_computed") && !item.contains("impact_paths_not_computed")
    });
    packet.confidence = packet.confidence.max(analysis.confidence);
    insert_evidence_packet(connection, &packet)?;
    insert_analysis(connection, &analysis)?;
    incident.status = "analyzed".to_string();
    incident.revision = incident.revision.saturating_add(1);
    incident.updated_at = Utc::now();
    upsert_incident(connection, &incident)?;
    Ok(analysis)
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
    append_projection_event(
        connection,
        "execution",
        &format!("mfg:execution:{}", execution.execution_id),
        "execution.updated",
        serde_json::json!({"execution": execution}),
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
    append_projection_event(
        connection,
        "incident",
        &format!("mfg:memory-case:{}", memory_case.case_id),
        "memory_case.updated",
        serde_json::json!({"memory_case": memory_case}),
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
    append_projection_event(
        connection,
        "incident",
        &format!("mfg:playbook:{}", playbook.playbook_id),
        "playbook.updated",
        serde_json::json!({"playbook": playbook}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:source-pack:{}", source_pack.source_pack_id),
        "source_pack.updated",
        serde_json::json!({"source_pack": source_pack}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!("matrix:connector-run:{}", run.run_id),
        "connector_run.updated",
        serde_json::json!({"connector_run": run}),
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
    append_projection_event(
        connection,
        "data_compute",
        &format!(
            "matrix:watermark:{}:{}:{}",
            watermark.source_ref, watermark.fact_type, watermark.partition_ref
        ),
        "data_watermark.updated",
        serde_json::json!({"watermark": watermark}),
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
        default_mfg_widget_instances, MfgAlertRuleInput, MfgAlertSubscriptionInput,
        MfgAssignmentInput, MfgCockpitProfileInput, MfgCockpitReportDeliveryPayload,
        MfgCockpitReportDeliveryPayloadRequest, MfgCockpitReportDeliveryReceipt,
        MfgCockpitReportDeliveryState, MfgCockpitReportRequest, MfgDashboardScope,
    };
    use matrix_core::{
        MatrixComputeJobInput, MatrixDataPlaneIngestPlanInput, MatrixEntityInput, MatrixFactInput,
        MatrixMetricStatus, MatrixRelationInput, MatrixSourceFactMapping, MatrixSourceKey,
    };
    use std::sync::{Arc, Barrier};

    #[test]
    fn cockpit_clone_keeps_its_canonical_action_id() {
        assert_eq!(
            canonical_upsert_action_id("cockpit", "profile.clone", 0),
            "mfg.cockpit.profile.clone"
        );
        assert_eq!(
            canonical_upsert_action_id("cockpit", "profile.upsert", 0),
            "mfg.cockpit.profile.create"
        );
    }

    fn insert_live_snapshot_sentinel(
        connection: &Connection,
        table: &str,
        json_column: &str,
        marker: &str,
    ) {
        connection
            .execute_batch("PRAGMA foreign_keys = OFF; PRAGMA ignore_check_constraints = ON;")
            .unwrap();
        let mut statement = connection
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap();
        let columns = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?.to_ascii_uppercase(),
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let names = columns
            .iter()
            .map(|(name, _)| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = (1..=columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let now = Utc::now().to_rfc3339();
        let values = columns
            .iter()
            .map(|(name, data_type)| {
                if name == json_column {
                    rusqlite::types::Value::Text(
                        serde_json::json!({"_live_snapshot_sentinel": marker}).to_string(),
                    )
                } else if name.ends_with("_json") {
                    rusqlite::types::Value::Text("{}".to_string())
                } else if data_type.contains("INT") {
                    rusqlite::types::Value::Integer(1)
                } else if data_type.contains("REAL")
                    || data_type.contains("FLOAT")
                    || data_type.contains("DOUBLE")
                {
                    rusqlite::types::Value::Real(1.0)
                } else if data_type.contains("BLOB") {
                    rusqlite::types::Value::Blob(vec![1])
                } else if name.ends_with("_at") {
                    rusqlite::types::Value::Text(now.clone())
                } else {
                    rusqlite::types::Value::Text(format!("{marker}:{name}"))
                }
            })
            .collect::<Vec<_>>();
        connection
            .execute(
                &format!("INSERT INTO \"{table}\" ({names}) VALUES ({placeholders})"),
                rusqlite::params_from_iter(values),
            )
            .unwrap_or_else(|error| panic!("insert live sentinel into {table}: {error}"));
    }

    #[test]
    fn live_epoch_survives_restart_and_rotates_only_for_identity_change() {
        let path = std::env::temp_dir().join(format!(
            "cowd-mfg-live-epoch-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let repository = MfgRepository::open(&path).unwrap();
        let initial = repository.live_epoch().unwrap();
        drop(repository);

        let reopened = MfgRepository::open(&path).unwrap();
        let stable = reopened.live_epoch().unwrap();
        assert_eq!(stable.epoch_id, initial.epoch_id);
        let rotated = reopened.rotate_live_epoch("cursor_key_recreated").unwrap();
        assert_ne!(rotated.epoch_id, initial.epoch_id);
        assert_eq!(rotated.rotation_reason, "cursor_key_recreated");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn live_epoch_rotates_when_the_event_log_is_rewritten() {
        let path = std::env::temp_dir().join(format!(
            "cowd-mfg-live-rewrite-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let repository = MfgRepository::open(&path).unwrap();
        {
            let connection = repository.connection.lock().unwrap();
            append_projection_event(
                &connection,
                "assignment",
                "mfg:assignment:before-rewrite",
                "assignment.receipted",
                serde_json::json!({"revision": 1}),
            )
            .unwrap();
        }
        let initial = repository.live_epoch().unwrap();
        drop(repository);
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute("DELETE FROM mfg_projection_event", [])
                .unwrap();
        }
        let reopened = MfgRepository::open(&path).unwrap();
        let rotated = reopened.live_epoch().unwrap();
        assert_ne!(rotated.epoch_id, initial.epoch_id);
        assert_eq!(rotated.rotation_reason, "event_log_rewritten");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn live_snapshot_and_delta_share_high_watermark_and_cover_every_domain() {
        let repository = MfgRepository::in_memory().unwrap();
        repository.seed_mfg_domain().unwrap();
        let snapshot_fields = [
            ("cockpit", "profiles", "mfg_cockpit_profile", "profile_json"),
            ("alerts", "rules", "mfg_alert_rule", "rule_json"),
            (
                "alerts",
                "subscriptions",
                "mfg_alert_subscription",
                "subscription_json",
            ),
            (
                "alerts",
                "occurrences",
                "mfg_alert_occurrence",
                "occurrence_json",
            ),
            ("assignments", "items", "mfg_assignment", "assignment_json"),
            ("incidents", "items", "mfg_incident", "incident_json"),
            ("incidents", "workflows", "mfg_workflow_graph", "graph_json"),
            (
                "incidents",
                "analyses",
                "mfg_operational_analysis",
                "analysis_json",
            ),
            (
                "incidents",
                "memory_cases",
                "mfg_memory_case",
                "memory_case_json",
            ),
            ("incidents", "playbooks", "mfg_playbook", "playbook_json"),
            (
                "executions",
                "actions",
                "mfg_action_execution",
                "execution_json",
            ),
            (
                "executions",
                "skills",
                "mfg_skill_execution",
                "execution_json",
            ),
            ("reports", "items", "mfg_cockpit_report", "report_json"),
            (
                "reviews",
                "items",
                "mfg_report_delivery_review",
                "review_json",
            ),
            (
                "receipts",
                "commands",
                "mfg_command_receipt",
                "receipt_json",
            ),
            ("data_compute", "entities", "matrix_entity", "entity_json"),
            (
                "data_compute",
                "relations",
                "matrix_relation",
                "relation_json",
            ),
            (
                "data_compute",
                "attention",
                "matrix_attention_item",
                "attention_json",
            ),
            (
                "data_compute",
                "evidence",
                "matrix_evidence_packet",
                "packet_json",
            ),
            (
                "data_compute",
                "quality_gates",
                "matrix_quality_gate",
                "gate_json",
            ),
            (
                "data_compute",
                "metric_definitions",
                "matrix_metric_definition",
                "definition_json",
            ),
            (
                "data_compute",
                "metric_dependencies",
                "matrix_metric_dependency",
                "dependency_json",
            ),
            (
                "data_compute",
                "metric_states",
                "matrix_metric_state",
                "state_json",
            ),
            (
                "data_compute",
                "metric_snapshots",
                "matrix_metric_snapshot",
                "snapshot_json",
            ),
            (
                "data_compute",
                "watermarks",
                "matrix_data_plane_watermark",
                "watermark_json",
            ),
            ("data_compute", "jobs", "matrix_compute_job", "job_json"),
            (
                "data_compute",
                "changes",
                "matrix_change_event",
                "change_json",
            ),
            (
                "data_compute",
                "source_packs",
                "matrix_source_pack",
                "source_pack_json",
            ),
            (
                "data_compute",
                "connector_runs",
                "matrix_connector_run",
                "run_json",
            ),
            (
                "data_compute",
                "ontology_packs",
                "matrix_ontology_pack",
                "pack_json",
            ),
            (
                "data_compute",
                "entity_match_candidates",
                "matrix_entity_match_candidate",
                "candidate_json",
            ),
            (
                "data_compute",
                "entity_conflict_decisions",
                "matrix_entity_conflict_decision",
                "decision_json",
            ),
        ];
        {
            let connection = repository.connection.lock().unwrap();
            for (domain, field, table, json_column) in snapshot_fields {
                insert_live_snapshot_sentinel(
                    &connection,
                    table,
                    json_column,
                    &format!("{domain}.{field}"),
                );
            }
            insert_live_snapshot_sentinel(
                &connection,
                "mfg_mutation_receipt",
                "response_json",
                "receipts.mutations",
            );
        }
        let event_cursor = {
            let connection = repository.connection.lock().unwrap();
            append_projection_event(
                &connection,
                "assignment",
                "mfg:assignment:assignment-live-1",
                "assignment.receipted",
                serde_json::json!({
                    "assignment": {
                        "assignment_id": "assignment-live-1",
                        "status": "assigned",
                        "revision": 1
                    }
                }),
            )
            .unwrap()
        };
        let snapshot = repository.live_snapshot_read().unwrap();
        assert_eq!(snapshot.high_cursor, event_cursor);
        let state = serde_json::to_value(&snapshot.state).unwrap();
        for (domain, field, _, _) in snapshot_fields {
            let marker = format!("{domain}.{field}");
            assert!(
                state[domain][field].as_array().is_some_and(|items| {
                    items.iter().any(|item| {
                        item["_live_snapshot_sentinel"].as_str() == Some(marker.as_str())
                    })
                }),
                "snapshot field {marker} is not wired to its durable table"
            );
        }
        assert!(!state["data_compute"]["facts"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(state["receipts"]["mutations"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| {
                item["response"]["_live_snapshot_sentinel"].as_str() == Some("receipts.mutations")
            })));
        let delta = repository
            .live_delta_read(event_cursor.saturating_sub(1), 100)
            .unwrap();
        assert_eq!(delta.high_cursor, snapshot.high_cursor);
        assert_eq!(delta.events.len(), 1);
        assert_eq!(delta.events[0].cursor, event_cursor);
        assert_eq!(delta.events[0].event_type, "assignment.receipted");
    }

    #[test]
    fn live_delta_below_retention_watermark_requires_resync_without_rotating_epoch() {
        let repository = MfgRepository::in_memory().unwrap();
        let epoch = repository.live_epoch().unwrap();
        {
            let connection = repository.connection.lock().unwrap();
            connection
                .execute(
                    "UPDATE mfg_live_epoch
                     SET retention_low_cursor = 50, retention_high_cursor = 75
                     WHERE singleton_id = 1",
                    [],
                )
                .unwrap();
        }
        let delta = repository.live_delta_read(49, 100).unwrap();
        assert_eq!(
            delta.resync_reason.as_deref(),
            Some("cursor_below_retention_low_watermark")
        );
        assert_eq!(delta.epoch.epoch_id, epoch.epoch_id);
    }

    #[test]
    fn every_completed_gateway_mutation_produces_one_durable_receipt_event() {
        let repository = MfgRepository::in_memory().unwrap();
        let claim = repository
            .claim_mutation_receipt(
                "live-receipt-key",
                "principal:operator",
                "mfg.incident.create",
                "mfg:incident:live-receipt",
                None,
                "sha256:live-receipt",
                "correlation:live-receipt",
            )
            .unwrap();
        assert!(matches!(claim, MfgMutationClaim::Acquired(_)));
        let before = repository.live_epoch().unwrap().retention_high_cursor;
        let receipt = repository
            .record_mutation_receipt(
                "live-receipt-key",
                "principal:operator",
                "mfg.incident.create",
                "mfg:incident:live-receipt",
                None,
                Some(1),
                "sha256:live-receipt",
                &serde_json::json!({"revision": 1}),
            )
            .unwrap();
        let delta = repository.live_delta_read(before, 10).unwrap();
        assert_eq!(delta.events.len(), 1);
        assert_eq!(delta.events[0].event_type, "receipt.completed");
        assert_eq!(
            delta.events[0].payload["receipt"]["receipt_id"],
            receipt.receipt_id
        );
        let after = delta.high_cursor;

        let replay = repository
            .record_mutation_receipt(
                "live-receipt-key",
                "principal:operator",
                "mfg.incident.create",
                "mfg:incident:live-receipt",
                None,
                Some(1),
                "sha256:live-receipt",
                &serde_json::json!({"revision": 1}),
            )
            .unwrap();
        assert_eq!(replay.status, app_mfg_contract::MfgReceiptStatus::Replayed);
        assert_eq!(
            repository.live_delta_read(after, 10).unwrap().events.len(),
            0
        );
    }

    #[test]
    fn completed_gateway_receipt_rolls_back_when_its_live_event_cannot_commit() {
        let repository = MfgRepository::in_memory().unwrap();
        repository
            .claim_mutation_receipt(
                "live-atomicity-key",
                "principal:operator",
                "mfg.incident.create",
                "mfg:incident:live-atomicity",
                None,
                "sha256:live-atomicity",
                "correlation:live-atomicity",
            )
            .unwrap();
        {
            let connection = repository.connection.lock().unwrap();
            connection
                .execute_batch(
                    "CREATE TRIGGER reject_live_receipt_event
                     BEFORE INSERT ON mfg_projection_event
                     BEGIN
                         SELECT RAISE(ABORT, 'live event unavailable');
                     END;",
                )
                .unwrap();
        }

        assert!(repository
            .record_mutation_receipt(
                "live-atomicity-key",
                "principal:operator",
                "mfg.incident.create",
                "mfg:incident:live-atomicity",
                None,
                Some(1),
                "sha256:live-atomicity",
                &serde_json::json!({"revision": 1}),
            )
            .is_err());

        let connection = repository.connection.lock().unwrap();
        let status = connection
            .query_row(
                "SELECT status FROM mfg_mutation_receipt
                 WHERE idempotency_key = 'live-atomicity-key'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(status, "accepted");
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM mfg_projection_event", [], |row| {
                    row.get::<_, u64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn live_retention_deletes_only_old_events_outside_the_latest_fifty_thousand() {
        let repository = MfgRepository::in_memory().unwrap();
        {
            let connection = repository.connection.lock().unwrap();
            connection
                .execute_batch(
                    "WITH RECURSIVE seq(value) AS (
                         VALUES(1)
                         UNION ALL
                         SELECT value + 1 FROM seq WHERE value < 50001
                     )
                     INSERT INTO mfg_projection_event (
                         domain, subject_ref, event_type, event_json, created_at
                     )
                     SELECT
                         'retention',
                         'mfg:retention:' || value,
                         'retention.test',
                         '{\"payload\":{}}',
                         CASE WHEN value = 1
                              THEN datetime('now', '-8 days')
                              ELSE datetime('now')
                         END
                     FROM seq;
                     UPDATE mfg_live_epoch
                     SET retention_low_cursor = 0,
                         retention_high_cursor = 50001,
                         updated_at = datetime('now', '-6 minutes')
                     WHERE singleton_id = 1;",
                )
                .unwrap();
            compact_live_events_if_due(&connection).unwrap();
            let count = connection
                .query_row("SELECT COUNT(*) FROM mfg_projection_event", [], |row| {
                    row.get::<_, u64>(0)
                })
                .unwrap();
            assert_eq!(count, 50_000);
            assert_eq!(
                connection
                    .query_row(
                        "SELECT MIN(event_id) FROM mfg_projection_event",
                        [],
                        |row| row.get::<_, u64>(0),
                    )
                    .unwrap(),
                2
            );
        }
        let epoch = repository.live_epoch().unwrap();
        assert_eq!(epoch.retention_low_cursor, 1);
        assert_eq!(epoch.retention_high_cursor, 50_001);
    }

    #[test]
    fn gateway_governance_receipt_upgrades_the_business_recovery_row_in_place() {
        let repository = MfgRepository::from_connection(
            Connection::open_in_memory().expect("in-memory database"),
        )
        .unwrap();
        let business = mutation_receipt(
            "alert",
            "mfg:alert-occurrence:alert-1".to_string(),
            "resolve",
            "principal:tui",
            "shared-key",
            "sha256:business-body".to_string(),
            1,
            2,
        )
        .unwrap();
        {
            let connection = repository.connection.lock().unwrap();
            insert_command_receipt(&connection, &business).unwrap();
        }
        let receipt = repository
            .record_mutation_receipt(
                "shared-key",
                "principal:tui",
                "mfg.alert.resolve",
                "mfg:alert-occurrence:alert-1",
                Some(1),
                Some(2),
                "sha256:gateway-body",
                &serde_json::json!({"correlation_id": "correlation-1"}),
            )
            .unwrap();
        assert_eq!(receipt.idempotency_key, "shared-key");
        let connection = repository.connection.lock().unwrap();
        let stored = connection
            .query_row(
                "SELECT COUNT(*), idempotency_key, response_json
                 FROM mfg_mutation_receipt WHERE idempotency_key = ?1",
                params!["shared-key"],
                |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(stored.0, 1);
        assert_eq!(stored.1, "shared-key");
        assert_eq!(
            serde_json::from_str::<Value>(&stored.2).unwrap()["correlation_id"],
            "correlation-1"
        );
    }

    #[test]
    fn finalized_gateway_receipt_preserves_assignment_notification_backlinks() {
        let repository = MfgRepository::from_connection(
            Connection::open_in_memory().expect("in-memory database"),
        )
        .unwrap();
        let mut business = mutation_receipt(
            "assignment",
            "mfg:assignment:assignment-1".to_string(),
            "start",
            "principal:tui",
            "assignment-notification-key",
            "sha256:business-body".to_string(),
            1,
            2,
        )
        .unwrap();
        business.notification_refs =
            vec!["surface://feishu/delivery/surface-delivery-1".to_string()];
        {
            let connection = repository.connection.lock().unwrap();
            insert_command_receipt(&connection, &business).unwrap();
        }
        let canonical_business = business.canonical_receipt().unwrap();
        repository
            .record_mutation_receipt(
                "assignment-notification-key",
                "principal:tui",
                "mfg.assignment.start",
                "mfg:assignment:assignment-1",
                Some(1),
                Some(2),
                "sha256:gateway-body",
                &serde_json::json!({
                    "kind": "mfg.assignment_command_receipt",
                    "business_receipt": canonical_business,
                    "assignment": {"assignment_id": "assignment-1", "revision": 2}
                }),
            )
            .unwrap();

        assert_eq!(
            repository
                .command_notification_refs_for_resource("mfg:assignment:assignment-1")
                .unwrap(),
            vec!["surface://feishu/delivery/surface-delivery-1".to_string()]
        );
    }

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
    fn legacy_receipt_binding_conflict_rolls_back_and_persists_repair_report() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                r"CREATE TABLE mfg_command_receipt (
                    idempotency_key TEXT PRIMARY KEY,
                    domain TEXT NOT NULL,
                    subject_ref TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE mfg_mutation_receipt (
                    receipt_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    actor_principal TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    resource_ref TEXT NOT NULL,
                    expected_revision INTEGER,
                    result_revision INTEGER,
                    payload_digest TEXT NOT NULL,
                    status TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    contract_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE mfg_mutation_receipt_alias (
                    legacy_idempotency_key TEXT PRIMARY KEY,
                    receipt_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE mfg_mutation_receipt_repair_report (
                    report_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL,
                    existing_receipt_json TEXT NOT NULL,
                    incoming_receipt_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );",
            )
            .unwrap();
        let legacy = MfgCommandReceipt {
            receipt_id: "legacy-receipt".to_string(),
            domain: "alert".to_string(),
            subject_ref: "mfg:alert-rule:rule-a".to_string(),
            command: "rule.upsert".to_string(),
            action_id: "mfg.alert_rule.create".to_string(),
            actor_ref: "principal:legacy".to_string(),
            idempotency_key: "conflicting-key".to_string(),
            payload_digest: "sha256:legacy".to_string(),
            correlation_id: None,
            contract_version: app_mfg_contract::MFG_CONTRACT_VERSION.to_string(),
            idempotent_replay: false,
            previous_revision: 0,
            current_revision: 1,
            audit_ref: "audit://legacy".to_string(),
            notification_refs: Vec::new(),
            response_snapshot: Value::Null,
            created_at: Utc::now(),
        };
        connection
            .execute(
                "INSERT INTO mfg_command_receipt (
                    idempotency_key, domain, subject_ref, receipt_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    legacy.idempotency_key,
                    legacy.domain,
                    legacy.subject_ref,
                    serde_json::to_string(&legacy).unwrap(),
                    legacy.created_at.to_rfc3339(),
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO mfg_mutation_receipt (
                    receipt_id, idempotency_key, actor_principal, action_id, resource_ref,
                    expected_revision, result_revision, payload_digest, status, response_json,
                    contract_version, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, 1, ?6, 'completed', ?7, ?8, ?9, ?9)",
                params![
                    "existing-receipt",
                    "conflicting-key",
                    "principal:other",
                    "mfg.alert_rule.create",
                    "mfg:alert-rule:rule-a",
                    "sha256:other",
                    serde_json::json!({"existing": true}).to_string(),
                    app_mfg_contract::MFG_CONTRACT_VERSION,
                    Utc::now().to_rfc3339(),
                ],
            )
            .unwrap();

        assert!(initialize_schema(&connection).is_err());
        let repair_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM mfg_mutation_receipt_repair_report
                 WHERE idempotency_key = 'conflicting-key'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let alias_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM mfg_mutation_receipt_alias",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(repair_count, 1);
        assert_eq!(alias_count, 0);
    }

    #[test]
    fn legacy_four_widget_profile_migrates_losslessly_without_dual_write() {
        let repository = MfgRepository::in_memory().unwrap();
        let now = Utc::now();
        let legacy = serde_json::json!({
            "profile_id": "cockpit-profile-legacy-four",
            "owner_ref": "user:legacy-planner",
            "display_name": "Legacy operations cockpit",
            "focus_refs": ["entity:legacy-line"],
            "focus_metric_ids": ["metric:legacy-output"],
            "thresholds": { "metric:legacy-output": { "critical": 42 } },
            "template_id": "mfg.legacy_ops",
            "cadence": "weekly",
            "revision": 0,
            "created_at": now,
            "updated_at": now
        });
        let mut profile: MfgCockpitProfile = serde_json::from_value(legacy).unwrap();
        profile.normalize_legacy();

        assert_eq!(profile.revision, 1);
        assert_eq!(profile.widget_instances, default_mfg_widget_instances());
        assert_eq!(profile.focus_refs, vec!["entity:legacy-line"]);
        assert_eq!(profile.focus_metric_ids, vec!["metric:legacy-output"]);
        assert_eq!(profile.thresholds["metric:legacy-output"]["critical"], 42);

        let saved = repository
            .upsert_cockpit_profile(&profile, None)
            .expect("legacy profile saves through the canonical profile writer");
        let loaded = repository
            .get_cockpit_profile(&saved.profile_id)
            .unwrap()
            .unwrap();
        assert_eq!(loaded.focus_refs, profile.focus_refs);
        assert_eq!(loaded.focus_metric_ids, profile.focus_metric_ids);
        assert_eq!(loaded.thresholds, profile.thresholds);
        assert_eq!(loaded.widget_instances, default_mfg_widget_instances());

        let connection = repository.connection.lock().unwrap();
        let row_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM mfg_cockpit_profile WHERE profile_id = ?1",
                params![saved.profile_id],
                |row| row.get(0),
            )
            .unwrap();
        let profile_json: String = connection
            .query_row(
                "SELECT profile_json FROM mfg_cockpit_profile WHERE profile_id = ?1",
                params![saved.profile_id],
                |row| row.get(0),
            )
            .unwrap();
        let persisted: Value = serde_json::from_str(&profile_json).unwrap();
        assert_eq!(row_count, 1);
        assert_eq!(persisted["widget_instances"].as_array().unwrap().len(), 4);
        assert!(persisted.get("widgets").is_none());
    }

    #[test]
    fn alert_conditions_trigger_for_future_attention_and_deduplicate() {
        let repository = MfgRepository::in_memory().unwrap();
        let rule = MfgAlertRule::from_input(MfgAlertRuleInput {
            rule_id: Some("alert-rule-priority".to_string()),
            owner_ref: "user:planner".to_string(),
            name: "High priority attention".to_string(),
            metric_refs: vec!["metric:output".to_string()],
            entity_refs: vec!["entity:line-a".to_string()],
            condition: serde_json::json!({
                "field": "priority_score",
                "operator": "gte",
                "threshold": 0.8,
                "window_minutes": 30
            }),
            severity: "critical".to_string(),
            enabled: true,
            expected_revision: None,
        });
        repository.upsert_alert_rule(&rule, None).unwrap();
        assert!(repository
            .list_alert_occurrences(None, 10)
            .unwrap()
            .is_empty());

        let now = Utc::now();
        let matching = MatrixAttentionItem {
            attention_id: "attention-priority-high".to_string(),
            title: "Line A output risk".to_string(),
            business_domain: "manufacturing".to_string(),
            entity_ref: Some("entity:line-a".to_string()),
            metric_refs: vec!["metric:output".to_string()],
            period: None,
            priority_score: 0.91,
            severity: MatrixSeverity::Critical,
            urgency: 0.9,
            strategic_weight: 0.8,
            confidence: 0.95,
            reason_codes: vec!["threshold".to_string()],
            linked_changes: vec!["matrix:change:output".to_string()],
            linked_anomalies: Vec::new(),
            linked_impacts: Vec::new(),
            owner_roles: vec!["operations".to_string()],
            status: "open".to_string(),
            created_at: now,
            updated_at: now,
        };
        {
            let connection = repository.connection.lock().unwrap();
            upsert_attention(&connection, &matching).unwrap();
            upsert_attention(&connection, &matching).unwrap();
            let mut below = matching.clone();
            below.attention_id = "attention-priority-low".to_string();
            below.priority_score = 0.4;
            upsert_attention(&connection, &below).unwrap();
            let mut stale = matching.clone();
            stale.attention_id = "attention-priority-stale".to_string();
            stale.updated_at = now - chrono::Duration::hours(2);
            upsert_attention(&connection, &stale).unwrap();
        }
        let occurrences = repository.list_alert_occurrences(None, 10).unwrap();
        assert_eq!(occurrences.len(), 1);
        assert_eq!(occurrences[0].rule_id, rule.rule_id);
        assert_eq!(occurrences[0].severity, "critical");
    }

    #[test]
    fn assignment_contract_validates_operational_fields_and_transfer_kind() {
        let repository = MfgRepository::in_memory().unwrap();
        let mut invalid = MfgAssignment::from_input(
            MfgAssignmentInput {
                assignment_id: Some("assignment-invalid".to_string()),
                task_ref: "task:canonical".to_string(),
                workflow_id: None,
                workflow_node_id: None,
                incident_id: None,
                assignee_ref: "operator-a".to_string(),
                assignee_kind: "unknown".to_string(),
                watcher_refs: Vec::new(),
                priority: "normal".to_string(),
                due_at: None,
                sla_minutes: Some(30),
                notification_targets: Vec::new(),
                visibility: "team".to_string(),
                expected_revision: None,
            },
            "user:dispatcher".to_string(),
        );
        assert!(matches!(
            repository.upsert_assignment(&invalid, None),
            Err(MfgRepositoryError::CommandRejected(_))
        ));

        invalid.assignment_id = "assignment-valid".to_string();
        invalid.assignee_kind = "agent".to_string();
        invalid.assignee_ref = "agent:planner".to_string();
        let saved = repository.upsert_assignment(&invalid, None).unwrap();
        let (transferred, receipt) = repository
            .command_assignment(
                &saved.assignment_id,
                MfgAssignmentCommandInput {
                    command: MfgAssignmentCommand::Transfer,
                    actor_ref: "user:dispatcher".to_string(),
                    expected_revision: saved.revision,
                    idempotency_key: "assignment-transfer-kind".to_string(),
                    target_ref: Some("team:operations".to_string()),
                    reason: Some("move ownership to the operations team".to_string()),
                    correlation_id: "assignment-transfer-correlation".to_string(),
                    completion_evidence: None,
                },
            )
            .unwrap();
        assert_eq!(transferred.assignee_ref, "team:operations");
        assert_eq!(transferred.assignee_kind, "team");
        assert_eq!(receipt.current_revision, saved.revision + 1);
    }

    #[test]
    fn assignment_lifecycle_never_fabricates_task_completion_and_binds_canonical_evidence() {
        let repository = MfgRepository::in_memory().unwrap();
        let mut incident = MfgIncident::new("canonical assignment lifecycle");
        incident.task_id = Some("canonical-1".to_string());
        let mut workflow = MfgWorkflowGraph::for_incident(&incident).unwrap();
        workflow.workflow_id = "workflow-1".to_string();
        let workflow_node_id = workflow.nodes[0].node_id.clone();
        repository.save_workflow_graph(&workflow, None).unwrap();
        let assignment = MfgAssignment::from_input(
            MfgAssignmentInput {
                assignment_id: Some("assignment-lifecycle".to_string()),
                task_ref: "task:canonical-1".to_string(),
                workflow_id: Some("workflow-1".to_string()),
                workflow_node_id: Some(workflow_node_id.clone()),
                incident_id: None,
                assignee_ref: "agent:operator".to_string(),
                assignee_kind: "agent".to_string(),
                watcher_refs: Vec::new(),
                priority: "high".to_string(),
                due_at: None,
                sla_minutes: Some(30),
                notification_targets: Vec::new(),
                visibility: "team".to_string(),
                expected_revision: None,
            },
            "user:dispatcher".to_string(),
        );
        let saved = repository.upsert_assignment(&assignment, None).unwrap();
        let correlation_id = "assignment-lifecycle-correlation".to_string();
        let (started, start_receipt) = repository
            .command_assignment(
                &saved.assignment_id,
                MfgAssignmentCommandInput {
                    command: MfgAssignmentCommand::Start,
                    actor_ref: "user:dispatcher".to_string(),
                    expected_revision: saved.revision,
                    idempotency_key: "assignment-start".to_string(),
                    target_ref: None,
                    reason: Some("work accepted".to_string()),
                    correlation_id: correlation_id.clone(),
                    completion_evidence: None,
                },
            )
            .unwrap();
        assert_eq!(started.status, "in_progress");
        assert_eq!(started.completion_ref, None);
        assert_eq!(
            started.lifecycle_correlation_id.as_deref(),
            Some(correlation_id.as_str())
        );
        assert_eq!(
            start_receipt.correlation_id.as_deref(),
            Some(correlation_id.as_str())
        );
        let pending = repository
            .reserve_assignment_completion(
                &started.assignment_id,
                started.revision,
                "user:dispatcher",
                &correlation_id,
            )
            .unwrap();

        let missing = repository.command_assignment(
            &started.assignment_id,
            MfgAssignmentCommandInput {
                command: MfgAssignmentCommand::Complete,
                actor_ref: "user:dispatcher".to_string(),
                expected_revision: pending.revision,
                idempotency_key: "assignment-complete-missing".to_string(),
                target_ref: None,
                reason: None,
                correlation_id: correlation_id.clone(),
                completion_evidence: None,
            },
        );
        assert!(matches!(
            missing,
            Err(MfgRepositoryError::CommandRejected(message))
                if message == "mfg_assignment_task_transition_required"
        ));

        let completion_evidence = app_mfg_contract::MfgAssignmentCompletionEvidenceV1 {
            correlation_id: correlation_id.clone(),
            owner_kind: "runtime_assignment_terminal_observation".to_string(),
            task_ref: started.task_ref.clone(),
            workflow_node_id: started.workflow_node_id.clone(),
            terminal_status: "completed".to_string(),
            receipt_ref: format!(
                "execution://{}/nodes/{}?revision=7",
                workflow.workflow_id, workflow_node_id,
            ),
        };
        let complete_input = MfgAssignmentCommandInput {
            command: MfgAssignmentCommand::Complete,
            actor_ref: "user:dispatcher".to_string(),
            expected_revision: pending.revision,
            idempotency_key: "assignment-complete".to_string(),
            target_ref: None,
            reason: Some("canonical runtime node completed".to_string()),
            correlation_id: correlation_id.clone(),
            completion_evidence: Some(completion_evidence.clone()),
        };
        let (completed, complete_receipt) = repository
            .command_assignment(&started.assignment_id, complete_input.clone())
            .unwrap();
        assert_eq!(completed.status, "completed");
        assert_eq!(
            completed.completion_ref.as_deref(),
            Some(completion_evidence.receipt_ref.as_str())
        );
        assert_eq!(
            complete_receipt.correlation_id.as_deref(),
            Some(correlation_id.as_str())
        );

        let (replayed, replay_receipt) = repository
            .command_assignment(&started.assignment_id, complete_input)
            .unwrap();
        assert_eq!(replayed, completed);
        assert!(replay_receipt.idempotent_replay);
        assert_eq!(replay_receipt.receipt_id, complete_receipt.receipt_id);

        let task_assignment = MfgAssignment::from_input(
            MfgAssignmentInput {
                assignment_id: Some("assignment-task-command".to_string()),
                task_ref: "task://canonical-task-2".to_string(),
                workflow_id: None,
                workflow_node_id: None,
                incident_id: None,
                assignee_ref: "agent:operator".to_string(),
                assignee_kind: "agent".to_string(),
                watcher_refs: Vec::new(),
                priority: "normal".to_string(),
                due_at: None,
                sla_minutes: None,
                notification_targets: Vec::new(),
                visibility: "team".to_string(),
                expected_revision: None,
            },
            "user:dispatcher".to_string(),
        );
        let task_assignment = repository
            .upsert_assignment(&task_assignment, None)
            .unwrap();
        let (task_started, _) = repository
            .command_assignment(
                &task_assignment.assignment_id,
                MfgAssignmentCommandInput {
                    command: MfgAssignmentCommand::Start,
                    actor_ref: "user:dispatcher".to_string(),
                    expected_revision: task_assignment.revision,
                    idempotency_key: "assignment-task-start".to_string(),
                    target_ref: None,
                    reason: None,
                    correlation_id: correlation_id.clone(),
                    completion_evidence: None,
                },
            )
            .unwrap();
        let runtime_receipt = "runtime-event://event-1?cursor=9&transaction=tx-1";
        let task_pending = repository
            .reserve_assignment_completion(
                &task_started.assignment_id,
                task_started.revision,
                "user:dispatcher",
                &correlation_id,
            )
            .unwrap();
        let (task_completed, task_receipt) = repository
            .command_assignment(
                &task_started.assignment_id,
                MfgAssignmentCommandInput {
                    command: MfgAssignmentCommand::Complete,
                    actor_ref: "user:dispatcher".to_string(),
                    expected_revision: task_pending.revision,
                    idempotency_key: "assignment-task-complete".to_string(),
                    target_ref: None,
                    reason: None,
                    correlation_id: correlation_id.clone(),
                    completion_evidence: Some(
                        app_mfg_contract::MfgAssignmentCompletionEvidenceV1 {
                            correlation_id: correlation_id.clone(),
                            owner_kind: "runtime_assignment_terminal_observation".to_string(),
                            task_ref: task_started.task_ref.clone(),
                            workflow_node_id: None,
                            terminal_status: "completed".to_string(),
                            receipt_ref: runtime_receipt.to_string(),
                        },
                    ),
                },
            )
            .unwrap();
        assert_eq!(
            task_completed.completion_ref.as_deref(),
            Some(runtime_receipt)
        );
        assert_eq!(
            task_receipt.correlation_id.as_deref(),
            Some(correlation_id.as_str())
        );
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
    fn data_plane_ingest_plan_includes_metric_declared_by_source_pack() {
        let store = MfgRepository::in_memory().expect("store opens");
        let source_pack_id = "source-pack-ingest-metric";
        store
            .upsert_source_pack(MatrixSourcePack {
                source_pack_id: source_pack_id.to_string(),
                source_name: "manufacturing events".to_string(),
                owner: "test".to_string(),
                access_mode: "manual".to_string(),
                refresh_mode: "manual".to_string(),
                entity_mappings: Vec::new(),
                fact_mappings: vec![MatrixSourceFactMapping {
                    source_table: "manufacturing_events".to_string(),
                    fact_type: "manufacturing.event".to_string(),
                    metric_key: "manufacturing_event_count".to_string(),
                    entity_ref_fields: vec!["asset_id".to_string()],
                    measure_fields: Vec::new(),
                    event_time_field: None,
                    dedup_key: "event_id".to_string(),
                    delta_signature: "updated_at".to_string(),
                }],
                relation_mappings: Vec::new(),
                reconciliation_rules: Vec::new(),
                quality_rules: Vec::new(),
                freshness_sla: None,
                security_policy: None,
                metadata: Value::Null,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .expect("source pack saves");

        let plan = store
            .plan_data_plane_ingest(MatrixDataPlaneIngestPlanInput {
                source_ref: format!("source-pack://{source_pack_id}"),
                fact_type: "manufacturing.event".to_string(),
                partition_ref: None,
                high_watermark: None,
                estimated_rows: None,
                raw_checksum: None,
                expected_revision: None,
                adapter_id: None,
                strategy: None,
                table: None,
                cursor: None,
                offset: None,
                metric_ids: Vec::new(),
            })
            .expect("ingest plan builds");

        assert!(plan
            .affected_metric_ids
            .iter()
            .any(|metric_id| metric_id == "manufacturing_event_count"));
        assert!(plan.compute_jobs.iter().any(|job| {
            job.metric_ids
                .iter()
                .any(|metric_id| metric_id == "manufacturing_event_count")
        }));
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
            .find(|widget| widget.definition_id == "attention.queue")
            .expect("attention widget exists");
        assert!(attention_widget.data["count"].as_u64().unwrap_or(0) >= 1);
        assert!(!attention_widget.source_refs.is_empty());
        let quality_widget = projection
            .widgets
            .iter()
            .find(|widget| widget.definition_id == "quality.gates")
            .expect("quality widget exists");
        assert_eq!(quality_widget.data["pass_count"], 1);
        let action_widget = projection
            .widgets
            .iter()
            .find(|widget| widget.definition_id == "action.executions")
            .expect("action widget exists");
        assert_eq!(action_widget.data["active_count"], 1);
        let threshold_widget = projection
            .widgets
            .iter()
            .find(|widget| widget.definition_id == "focus.summary")
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
        assert_eq!(delivery_state.retry_attempt_count, 0);
        let delivered_revision = delivered.revision;
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
        assert_eq!(delivered.revision, delivered_revision);
        assert!(store
            .attach_cockpit_report_delivery(
                &report.report_id,
                MfgCockpitReportDeliveryReceipt::new(
                    report.report_id.clone(),
                    "cpx-report-test",
                    "dispatched",
                    "sent",
                    Some("cpa-report-test".to_string()),
                ),
            )
            .is_err());
        let unchanged = store
            .get_cockpit_report(&report.report_id)
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.status, "delivery_planned");
        assert_eq!(unchanged.revision, delivered_revision);
        for attempt in 1..=3 {
            store
                .attach_cockpit_report_delivery(
                    &report.report_id,
                    MfgCockpitReportDeliveryReceipt::new(
                        report.report_id.clone(),
                        format!("cpx-report-failure-{attempt}"),
                        "blocked",
                        "runtime_unavailable",
                        Some(format!("cpa-report-failure-{attempt}")),
                    ),
                )
                .expect("retryable report delivery failure attaches");
        }
        let dead_lettered = store
            .get_cockpit_report(&report.report_id)
            .expect("dead-lettered report loads")
            .expect("dead-lettered report exists");
        let delivery_state = MfgCockpitReportDeliveryState::from_report(&dead_lettered);
        assert_eq!(delivery_state.classification, "delivery_dead_lettered");
        assert!(delivery_state.dead_lettered);
        assert_eq!(delivery_state.attempt_count, 4);
        assert_eq!(
            delivery_state.retry_attempt_count,
            delivery_state.max_attempts
        );
        assert!(!delivery_state.retryable);
        assert_eq!(delivery_state.recommended_mode, "manual_review");
        assert!(delivery_state
            .reasons
            .contains(&"delivery:retry_attempts_exhausted:3".to_string()));
    }

    #[test]
    fn cockpit_widget_projection_isolated_retry_and_filter_override_are_contractual() {
        let store = MfgRepository::in_memory().expect("store opens");
        let mut profile = MfgCockpitProfile::from_input(MfgCockpitProfileInput {
            profile_id: Some("cockpit-profile-isolated".to_string()),
            owner_ref: "user:planner".to_string(),
            display_name: Some("Isolated widgets".to_string()),
            focus_refs: vec!["entity:legacy".to_string()],
            focus_metric_ids: vec!["metric:legacy".to_string()],
            thresholds: Value::Null,
            template_id: None,
            cadence: None,
            expected_revision: None,
            scope: None,
            layout: None,
            global_filters: serde_json::json!({
                "entity_refs": ["entity:global"],
                "metric_ids": ["metric:global"]
            }),
            widget_instances: Vec::new(),
            sharing_policy: None,
        });
        profile.widget_instances[0].query = serde_json::json!({
            "entity_refs": ["entity:widget"],
            "metric_ids": ["metric:widget"],
            "limit": 5
        });
        let scoped = effective_cockpit_profile(&profile, &profile.widget_instances[0]);
        assert_eq!(scoped.focus_refs, vec!["entity:widget"]);
        assert_eq!(scoped.focus_metric_ids, vec!["metric:widget"]);

        let saved = store
            .upsert_cockpit_profile(&profile, None)
            .expect("profile saves");
        let projection = store
            .cockpit_widget_projection(&saved.profile_id, "default-attention")
            .expect("single widget projects");
        assert_eq!(projection.profile_revision, saved.revision);
        assert_eq!(projection.widget.instance_id, "default-attention");
        assert!(store
            .cockpit_widget_projection(&saved.profile_id, "default-quality")
            .is_ok());
        assert!(matches!(
            store.cockpit_widget_projection(&saved.profile_id, "missing"),
            Err(MfgRepositoryError::NotFound(_))
        ));
    }

    fn dead_letter_report(repository: &MfgRepository, suffix: &str) -> MfgCockpitReportSnapshot {
        let profile = MfgCockpitProfile::from_input(MfgCockpitProfileInput {
            profile_id: Some(format!("review-profile-{suffix}")),
            owner_ref: "principal:review-requester".to_string(),
            display_name: Some("Review fixture".to_string()),
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
        repository.upsert_cockpit_profile(&profile, None).unwrap();
        let report = repository
            .generate_cockpit_report(
                &profile.profile_id,
                MfgCockpitReportRequest {
                    report_id: Some(format!("review-report-{suffix}")),
                    cadence: Some("daily".to_string()),
                    delivery_ref: Some("channel://feishu/user/review-target".to_string()),
                    note: None,
                },
            )
            .unwrap();
        for attempt in 1..=3 {
            repository
                .attach_cockpit_report_delivery(
                    &report.report_id,
                    MfgCockpitReportDeliveryReceipt::new(
                        report.report_id.clone(),
                        format!("review-failure-{suffix}-{attempt}"),
                        "blocked",
                        "runtime_unavailable",
                        Some(format!("review-audit-{suffix}-{attempt}")),
                    ),
                )
                .unwrap();
        }
        repository
            .get_cockpit_report(&report.report_id)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn report_delivery_review_saga_is_idempotent_cas_bound_and_effect_recoverable() {
        let repository = MfgRepository::in_memory().unwrap();
        let report = dead_letter_report(&repository, "force");
        let review = repository
            .create_report_delivery_review(
                &report,
                report.revision,
                "principal:review-requester",
                "retry after manual evidence review",
                vec!["evidence:delivery-failure".to_string()],
                "review-create-force",
            )
            .unwrap();
        let replay = repository
            .create_report_delivery_review(
                &report,
                report.revision,
                "principal:review-requester",
                "retry after manual evidence review",
                vec!["evidence:delivery-failure".to_string()],
                "review-create-force",
            )
            .unwrap();
        assert_eq!(review.review_id, replay.review_id);
        let pending = repository
            .bind_report_delivery_review_approval(
                &review.review_id,
                review.revision,
                "mfg-approval:force",
                "principal:review-requester",
                "review-bind-force",
            )
            .unwrap();
        assert!(matches!(
            repository.bind_report_delivery_review_approval(
                &review.review_id,
                review.revision,
                "mfg-approval:force",
                "principal:review-requester",
                "review-bind-stale",
            ),
            Err(MfgRepositoryError::RevisionConflict { .. })
        ));
        let prepared = repository
            .prepare_report_delivery_review_decision(
                &review.review_id,
                pending.revision,
                MfgReportDeliveryReviewDecision::ForceRetry,
                "principal:reviewer",
                "approved retry",
                vec!["evidence:reviewed".to_string()],
                None,
                "lease:force",
                "review-decision-force",
            )
            .unwrap();
        let activated = repository
            .activate_report_delivery_review_decision(
                &review.review_id,
                prepared.revision,
                "principal:reviewer",
                "review-activate-force",
            )
            .unwrap();
        assert_eq!(
            activated.status,
            MfgReportDeliveryReviewStatus::DecisionPendingEffect
        );
        let effects = repository.claim_report_delivery_review_effects(10).unwrap();
        assert_eq!(effects.len(), 1);
        let completed = repository
            .complete_report_delivery_review_effect(
                &effects[0].effect_key,
                "cross-plane:receipt:force",
                "principal:reviewer",
            )
            .unwrap();
        assert_eq!(
            completed.status,
            MfgReportDeliveryReviewStatus::EffectAppliedForceRetry
        );
        assert_eq!(
            completed.effect_receipt_ref.as_deref(),
            Some("cross-plane:receipt:force")
        );
    }

    #[test]
    fn report_delivery_review_reroute_outbox_reopens_and_keeps_one_effect_key() {
        let path =
            std::env::temp_dir().join(format!("mfg-review-reopen-{}.sqlite", uuid::Uuid::new_v4()));
        let effect_key;
        {
            let repository = MfgRepository::open(&path).unwrap();
            let report = dead_letter_report(&repository, "reroute-reopen");
            let requested = repository
                .create_report_delivery_review(
                    &report,
                    report.revision,
                    "principal:review-requester",
                    "reroute after provider outage",
                    vec!["evidence:provider-outage".to_string()],
                    "review-create-reroute-reopen",
                )
                .unwrap();
            let pending = repository
                .bind_report_delivery_review_approval(
                    &requested.review_id,
                    requested.revision,
                    "mfg-approval:reroute-reopen",
                    "principal:review-requester",
                    "review-bind-reroute-reopen",
                )
                .unwrap();
            let prepared = repository
                .prepare_report_delivery_review_decision(
                    &pending.review_id,
                    pending.revision,
                    MfgReportDeliveryReviewDecision::Reroute,
                    "principal:reviewer",
                    "reroute to the fallback provider",
                    vec!["evidence:fallback-validated".to_string()],
                    Some(MfgReportDeliveryReviewRerouteTarget {
                        target_ref: "channel://feishu/user/fallback".to_string(),
                        provider_account: "provider:fallback".to_string(),
                        channel: "feishu".to_string(),
                        requested_capability: "channel.send".to_string(),
                    }),
                    "lease:reroute-reopen",
                    "review-decision-reroute-reopen",
                )
                .unwrap();
            repository
                .activate_report_delivery_review_decision(
                    &prepared.review_id,
                    prepared.revision,
                    "principal:reviewer",
                    "review-activate-reroute-reopen",
                )
                .unwrap();
            let claimed = repository.claim_report_delivery_review_effects(10).unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(claimed[0].action, MfgReportDeliveryReviewDecision::Reroute);
            assert_eq!(
                claimed[0].payload["target_ref"],
                "channel://feishu/user/fallback"
            );
            effect_key = claimed[0].effect_key.clone();
        }

        let reopened = MfgRepository::open(&path).unwrap();
        let reclaimed = reopened.claim_report_delivery_review_effects(10).unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].effect_key, effect_key);
        assert_eq!(reclaimed[0].attempt_count, 2);
        let terminal = reopened
            .complete_report_delivery_review_effect(
                &effect_key,
                "cross-plane:receipt:reroute-reopen",
                "principal:reviewer",
            )
            .unwrap();
        assert_eq!(
            terminal.status,
            MfgReportDeliveryReviewStatus::EffectAppliedReroute
        );
        assert!(reopened
            .claim_report_delivery_review_effects(10)
            .unwrap()
            .is_empty());
        drop(reopened);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    }

    #[test]
    fn review_local_terminals_preserve_delivery_semantics() {
        for (decision, expected_review, expected_report) in [
            (
                MfgReportDeliveryReviewDecision::Reject,
                MfgReportDeliveryReviewStatus::Rejected,
                "delivery_dead_lettered",
            ),
            (
                MfgReportDeliveryReviewDecision::Abandon,
                MfgReportDeliveryReviewStatus::Abandoned,
                "delivery_abandoned",
            ),
            (
                MfgReportDeliveryReviewDecision::Resolve,
                MfgReportDeliveryReviewStatus::ResolvedExternal,
                "delivery_resolved_external",
            ),
        ] {
            let repository = MfgRepository::in_memory().unwrap();
            let suffix = review_decision_string(decision);
            let report = dead_letter_report(&repository, &suffix);
            let requested = repository
                .create_report_delivery_review(
                    &report,
                    report.revision,
                    "principal:review-requester",
                    "manual review",
                    vec!["evidence:failure".to_string()],
                    &format!("create-{suffix}"),
                )
                .unwrap();
            let pending = repository
                .bind_report_delivery_review_approval(
                    &requested.review_id,
                    requested.revision,
                    &format!("approval-{suffix}"),
                    "principal:review-requester",
                    &format!("bind-{suffix}"),
                )
                .unwrap();
            let prepared = repository
                .prepare_report_delivery_review_decision(
                    &pending.review_id,
                    pending.revision,
                    decision,
                    "principal:reviewer",
                    if decision == MfgReportDeliveryReviewDecision::Resolve {
                        "resolved by external operator"
                    } else {
                        "reviewed"
                    },
                    vec!["evidence:reviewed".to_string()],
                    None,
                    &format!("lease-{suffix}"),
                    &format!("decision-{suffix}"),
                )
                .unwrap();
            let terminal = repository
                .activate_report_delivery_review_decision(
                    &prepared.review_id,
                    prepared.revision,
                    "principal:reviewer",
                    &format!("activate-{suffix}"),
                )
                .unwrap();
            assert_eq!(terminal.status, expected_review);
            assert!(terminal.effect_receipt_ref.is_some());
            let report = repository
                .get_cockpit_report(&report.report_id)
                .unwrap()
                .unwrap();
            assert_eq!(
                MfgCockpitReportDeliveryState::from_report(&report).classification,
                expected_report
            );
        }
    }

    #[test]
    fn cockpit_catalog_and_validation_expose_only_supported_query_contracts() {
        let attention = mfg_widget_catalog()
            .into_iter()
            .find(|definition| definition.definition_id == "attention.queue")
            .expect("attention definition");
        assert!(attention.query_schema["properties"]["severities"].is_object());
        assert_eq!(attention.query_schema["additionalProperties"], false);

        let store = MfgRepository::in_memory().expect("store opens");
        let mut profile = MfgCockpitProfile::from_input(MfgCockpitProfileInput {
            profile_id: Some("cockpit-profile-invalid-query".to_string()),
            owner_ref: "user:planner".to_string(),
            display_name: None,
            focus_refs: Vec::new(),
            focus_metric_ids: Vec::new(),
            thresholds: Value::Null,
            template_id: None,
            cadence: None,
            expected_revision: None,
            scope: None,
            layout: None,
            global_filters: Value::Null,
            widget_instances: Vec::new(),
            sharing_policy: None,
        });
        profile.widget_instances[1].query = serde_json::json!({ "metric_ids": ["not-supported"] });
        assert!(matches!(
            store.upsert_cockpit_profile(&profile, None),
            Err(MfgRepositoryError::CommandRejected(_))
        ));

        profile.widget_instances[1].query = Value::Null;
        profile.widget_instances[0].config = serde_json::json!({ "refresh_interval_seconds": 5 });
        assert!(matches!(
            store.upsert_cockpit_profile(&profile, None),
            Err(MfgRepositoryError::CommandRejected(_))
        ));

        profile.widget_instances[0].config = serde_json::json!({ "refresh_interval_seconds": 60 });
        profile.scope = MfgDashboardScope {
            kind: "team".to_string(),
            scope_ref: None,
        };
        assert!(matches!(
            store.upsert_cockpit_profile(&profile, None),
            Err(MfgRepositoryError::CommandRejected(_))
        ));

        profile.scope.scope_ref = Some("operations".to_string());
        profile.global_filters = serde_json::json!({ "from": "not-a-timestamp" });
        assert!(matches!(
            store.upsert_cockpit_profile(&profile, None),
            Err(MfgRepositoryError::CommandRejected(_))
        ));

        profile.global_filters = serde_json::json!({ "from": "2026-07-16T00:00:00Z" });
        assert!(store.upsert_cockpit_profile(&profile, None).is_ok());
    }

    #[test]
    fn playbook_upsert_distinguishes_create_from_revision_checked_update() {
        let repository = MfgRepository::in_memory().unwrap();
        let now = Utc::now();
        let playbook = MfgPlaybook {
            playbook_id: "playbook-revision".to_string(),
            revision: 1,
            domain: "supply".to_string(),
            scenario: "shortage".to_string(),
            trigger_fact_types: Vec::new(),
            metric_keys: vec!["shortage_risk".to_string()],
            recommended_steps: Vec::new(),
            required_evidence: Vec::new(),
            quality_gate_policy: "quality_gate".to_string(),
            cross_plane_policy: "dry_run_first".to_string(),
            success_metrics: vec!["shortage_risk".to_string()],
            created_from_case_id: None,
            created_at: now,
            updated_at: now,
        };
        let created = repository.upsert_playbook(&playbook, None).unwrap();
        assert_eq!(created.revision, 1);
        assert!(matches!(
            repository.upsert_playbook(&playbook, None),
            Err(MfgRepositoryError::RevisionConflict { .. })
        ));
        let updated = repository
            .upsert_playbook(&playbook, Some(created.revision))
            .unwrap();
        assert_eq!(updated.revision, 2);
    }

    #[test]
    fn alert_subscription_and_assignment_upserts_emit_create_then_update_actions() {
        let repository = MfgRepository::in_memory().unwrap();
        let actor = "principal:operator";

        let rule = MfgAlertRule::from_input(MfgAlertRuleInput {
            rule_id: Some("alert-rule-revision".to_string()),
            owner_ref: actor.to_string(),
            name: "Revision rule".to_string(),
            metric_refs: Vec::new(),
            entity_refs: Vec::new(),
            condition: Value::Null,
            severity: "warning".to_string(),
            enabled: true,
            expected_revision: None,
        });
        let (created_rule, created_rule_receipt) = repository
            .upsert_alert_rule_receipted(&rule, None, actor, "rule-create-key")
            .unwrap();
        assert_eq!(created_rule_receipt.action_id, "mfg.alert_rule.create");
        let mut changed_rule = created_rule.clone();
        changed_rule.name = "Revision rule updated".to_string();
        let (updated_rule, updated_rule_receipt) = repository
            .upsert_alert_rule_receipted(
                &changed_rule,
                Some(created_rule.revision),
                actor,
                "rule-update-key",
            )
            .unwrap();
        assert_eq!(updated_rule.revision, 2);
        assert_eq!(updated_rule_receipt.action_id, "mfg.alert_rule.update");

        let subscription = MfgAlertSubscription::from_input(
            MfgAlertSubscriptionInput {
                subscription_id: Some("alert-subscription-revision".to_string()),
                rule_id: created_rule.rule_id,
                channels: vec!["webui".to_string()],
                enabled: true,
                expected_revision: None,
            },
            actor.to_string(),
        );
        let (created_subscription, created_subscription_receipt) = repository
            .upsert_alert_subscription_receipted(
                &subscription,
                None,
                actor,
                "subscription-create-key",
            )
            .unwrap();
        assert_eq!(
            created_subscription_receipt.action_id,
            "mfg.alert_subscription.create"
        );
        let mut changed_subscription = created_subscription.clone();
        changed_subscription.channels.push("tui".to_string());
        let (updated_subscription, updated_subscription_receipt) = repository
            .upsert_alert_subscription_receipted(
                &changed_subscription,
                Some(created_subscription.revision),
                actor,
                "subscription-update-key",
            )
            .unwrap();
        assert_eq!(updated_subscription.revision, 2);
        assert_eq!(
            updated_subscription_receipt.action_id,
            "mfg.alert_subscription.update"
        );

        let assignment = MfgAssignment::from_input(
            MfgAssignmentInput {
                assignment_id: Some("assignment-revision".to_string()),
                task_ref: "task:revision-fixture".to_string(),
                workflow_id: None,
                workflow_node_id: None,
                incident_id: None,
                assignee_ref: "principal:worker".to_string(),
                assignee_kind: "user".to_string(),
                watcher_refs: Vec::new(),
                priority: "normal".to_string(),
                due_at: None,
                sla_minutes: None,
                notification_targets: Vec::new(),
                visibility: "private".to_string(),
                expected_revision: None,
            },
            actor.to_string(),
        );
        let (created_assignment, created_assignment_receipt) = repository
            .upsert_assignment_receipted(&assignment, None, actor, "assignment-create-key")
            .unwrap();
        assert_eq!(
            created_assignment_receipt.action_id,
            "mfg.assignment.create"
        );
        let mut changed_assignment = created_assignment.clone();
        changed_assignment.priority = "high".to_string();
        let (updated_assignment, updated_assignment_receipt) = repository
            .upsert_assignment_receipted(
                &changed_assignment,
                Some(created_assignment.revision),
                actor,
                "assignment-update-key",
            )
            .unwrap();
        assert_eq!(updated_assignment.revision, 2);
        assert_eq!(
            updated_assignment_receipt.action_id,
            "mfg.assignment.update"
        );
        repository
            .record_command_notifications(
                "assignment-update-key",
                vec!["surface://feishu/message-42?status=sent".to_string()],
            )
            .unwrap();
        assert_eq!(
            repository
                .command_notification_refs_for_resource(&format!(
                    "mfg:assignment:{}",
                    updated_assignment.assignment_id
                ))
                .unwrap(),
            vec!["surface://feishu/message-42?status=sent".to_string()]
        );
        assert!(matches!(
            repository.upsert_assignment(
                &MfgAssignment::from_input(
                    MfgAssignmentInput {
                        assignment_id: Some("assignment-create-with-revision".to_string()),
                        task_ref: "task:revision-fixture".to_string(),
                        workflow_id: None,
                        workflow_node_id: None,
                        incident_id: None,
                        assignee_ref: "principal:worker".to_string(),
                        assignee_kind: "user".to_string(),
                        watcher_refs: Vec::new(),
                        priority: "normal".to_string(),
                        due_at: None,
                        sla_minutes: None,
                        notification_targets: Vec::new(),
                        visibility: "private".to_string(),
                        expected_revision: Some(0),
                    },
                    actor.to_string(),
                ),
                Some(0)
            ),
            Err(MfgRepositoryError::RevisionConflict { .. })
        ));
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
        assert_eq!(receipt.action_id, "mfg.cockpit.profile.create");
        assert_eq!(replay_receipt.action_id, "mfg.cockpit.profile.create");
        assert!(receipt.audit_ref.contains("cockpit-profile-idempotency"));

        let mut changed = saved.clone();
        changed.display_name = "Idempotent cockpit updated".to_string();
        let (updated, update_receipt) = store
            .upsert_cockpit_profile_receipted(
                &changed,
                Some(saved.revision),
                "profile.upsert",
                "user:planner",
                "cockpit-update-key",
            )
            .expect("profile updates with a receipt");
        assert_eq!(updated.revision, 2);
        assert_eq!(update_receipt.action_id, "mfg.cockpit.profile.update");

        let (deleted, deletion_receipt) = store
            .delete_cockpit_profile_receipted(
                &updated.profile_id,
                updated.revision,
                "user:planner",
                "cockpit-delete-key",
            )
            .expect("profile deletes with a receipt");
        let (deleted_replay, deletion_replay) = store
            .delete_cockpit_profile_receipted(
                &updated.profile_id,
                updated.revision,
                "user:planner",
                "cockpit-delete-key",
            )
            .expect("profile deletion replays");
        assert_eq!(
            deleted.expect("first deletion returns profile").profile_id,
            saved.profile_id
        );
        assert_eq!(
            deleted_replay
                .expect("replay returns the original deleted snapshot")
                .profile_id,
            saved.profile_id
        );
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
        assert_eq!(updated_incident.revision, incident.revision + 1);
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
        let bridge_updated_at = execution.updated_at;
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
        assert_eq!(execution.updated_at, bridge_updated_at);
        assert!(store
            .attach_cross_plane_receipt(
                &execution.execution_id,
                MfgCrossPlaneBridgeReceipt::new(
                    execution.execution_id.clone(),
                    "cpx-matrix-test",
                    "dispatched",
                    "sent",
                    Some("cpa-matrix-test".to_string()),
                ),
            )
            .is_err());
        let unchanged = store
            .get_execution(&execution.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.status, "cross_plane_planned");
        assert_eq!(unchanged.updated_at, bridge_updated_at);

        let feedback = MfgActionFeedback::new("resolved", "supplier commit secured", Some(-260.0));
        let execution = store
            .record_execution_feedback(&execution.execution_id, feedback.clone())
            .expect("feedback saves");
        assert_eq!(execution.status, "feedback_resolved");
        assert_eq!(execution.feedback.as_ref().unwrap().outcome, "resolved");
        let feedback_updated_at = execution.updated_at;
        let replayed = store
            .record_execution_feedback(&execution.execution_id, feedback)
            .expect("identical feedback replays");
        assert_eq!(replayed.updated_at, feedback_updated_at);
        assert!(store
            .record_execution_feedback(
                &execution.execution_id,
                MfgActionFeedback::new(
                    "needs_followup",
                    "attempt to replace immutable outcome",
                    Some(40.0),
                ),
            )
            .is_err());
        let unchanged = store
            .get_execution(&execution.execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.status, "feedback_resolved");
        assert_eq!(unchanged.updated_at, feedback_updated_at);
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
        assert_eq!(incident.revision, 3);
    }
}
