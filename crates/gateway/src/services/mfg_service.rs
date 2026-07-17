use std::{
    collections::BTreeMap,
    path::Path,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};

use super::ServiceEnvelope;
use app_mfg::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_skill_pack, MfgActionExecution, MfgActionExecutionRequest,
    MfgActionFeedback, MfgAlertCommandInput, MfgAlertOccurrence, MfgAlertRule,
    MfgAlertSubscription, MfgAssignment, MfgAssignmentCommandInput, MfgCasePromotion,
    MfgCockpitProfile, MfgCockpitProjection, MfgCockpitReportDeliveryReceipt,
    MfgCockpitReportRequest, MfgCockpitReportSnapshot, MfgCockpitWidgetProjection,
    MfgCommandReceipt, MfgCrossPlaneBridgeReceipt, MfgDomainSeedResult, MfgForecastProjection,
    MfgHealth, MfgIncident, MfgMemoryCase, MfgMetricRecomputeResult, MfgOperationalAnalysis,
    MfgPlaybook, MfgRepositoryError, MfgSkillManifest, MfgSkillPlan, MfgSkillRun, MfgStore,
};
use app_mfg_contract::{
    MfgReportDeliveryReview, MfgReportDeliveryReviewDecision, MfgReportDeliveryReviewEffect,
    MfgReportDeliveryReviewRerouteTarget,
};
use connector::{CrossPlaneRisk, DataClassification};
use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixEntity, MatrixEvidencePacket, MatrixFact,
    MatrixMetricDefinition, MatrixOntologyPack, MatrixQualityGateDecision, MatrixSourcePack,
};
use runtime::{CrossPlaneAction, CrossPlaneExecutionReceipt, IdentityTrust};
use serde::Serialize;

mod cross_plane;
mod delivery;
mod live;

pub(crate) use live::{MfgLivePrincipalContext, MfgLiveServiceError};

/// Internal execution request. Gateway API handlers construct this only after
/// deriving the audit principal from authenticated middleware.
#[derive(Debug)]
pub(crate) struct MfgCrossPlaneBridgeRequest {
    pub(crate) mode: String,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) actor_principal: String,
    pub(crate) actor_identity_ref: Option<String>,
    pub(crate) source_channel: Option<String>,
    pub(crate) requested_capability: Option<String>,
    pub(crate) provider_account: Option<String>,
    pub(crate) target_ref: Option<String>,
    pub(crate) resource_ref: Option<String>,
}

/// Internal delivery request. It is intentionally not deserializable from an
/// HTTP payload because the actor is a Gateway-owned security boundary.
#[derive(Debug)]
pub(crate) struct MfgCockpitReportDeliveryRequest {
    pub(crate) mode: String,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) actor_principal: String,
    pub(crate) actor_identity_ref: Option<String>,
    pub(crate) source_channel: Option<String>,
    pub(crate) requested_capability: Option<String>,
    pub(crate) provider_account: Option<String>,
    pub(crate) target_ref: Option<String>,
    pub(crate) resource_ref: Option<String>,
    pub(crate) channel: Option<String>,
    pub(crate) template_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MfgCockpitReportDeliveryOutcome {
    pub(crate) mode: String,
    pub(crate) status: String,
    pub(crate) dispatch_status: String,
    pub(crate) report: MfgCockpitReportSnapshot,
    pub(crate) delivery_payload: app_mfg::MfgCockpitReportDeliveryPayload,
    pub(crate) cross_plane_execution_receipt: CrossPlaneExecutionReceipt,
    pub(crate) idempotent_replay: bool,
}

fn default_mfg_bridge_mode() -> String {
    "dry_run".to_string()
}

#[derive(Clone)]
pub(crate) struct MfgService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    review_reconciler: Arc<MfgReviewReconcilerLifecycle>,
    live_stores: Arc<Mutex<BTreeMap<PathBuf, Arc<MfgStore>>>>,
    live_key_lock: Arc<Mutex<()>>,
}

pub(crate) struct MfgReviewReconcilerLifecycle {
    started: AtomicBool,
    cancelled: AtomicBool,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MfgReviewReconcilerLifecycle {
    pub(crate) fn begin(self: &Arc<Self>) -> Option<Weak<Self>> {
        self.started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            .then(|| Arc::downgrade(self))
    }

    fn install(&self, handle: tokio::task::JoinHandle<()>) {
        let mut current = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancelled.load(Ordering::Acquire) {
            handle.abort();
        } else {
            *current = Some(handle);
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    async fn shutdown(&self) {
        self.cancelled.store(true, Ordering::Release);
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for MfgReviewReconcilerLifecycle {
    fn drop(&mut self) {
        if let Some(handle) = self
            .handle
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            handle.abort();
        }
    }
}

pub(crate) struct MfgIncidentContext {
    pub(crate) incident: MfgIncident,
    pub(crate) analysis: Option<MfgOperationalAnalysis>,
    pub(crate) packet: Option<MatrixEvidencePacket>,
}

impl MfgService {
    pub(crate) fn new() -> Self {
        Self {
            label: "mfg",
            owner: "0.9.380 GatewayServices",
            review_reconciler: Arc::new(MfgReviewReconcilerLifecycle {
                started: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                handle: Mutex::new(None),
            }),
            live_stores: Arc::new(Mutex::new(BTreeMap::new())),
            live_key_lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn begin_review_reconciler(&self) -> Option<Weak<MfgReviewReconcilerLifecycle>> {
        self.review_reconciler.begin()
    }

    pub(crate) fn install_review_reconciler(&self, handle: tokio::task::JoinHandle<()>) {
        self.review_reconciler.install(handle);
    }

    pub(crate) async fn shutdown_review_reconciler(&self) {
        self.review_reconciler.shutdown().await;
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: "service_boundary_ready",
            owner: self.owner,
            boundary_status: "0620_final_boundary",
        }
    }

    pub(crate) fn skill_pack(&self) -> Vec<MfgSkillManifest> {
        server_manufacturing_skill_pack()
    }

    pub(crate) fn app_descriptor(&self) -> app_mfg::MfgApplicationDescriptor {
        app_mfg::manufacturing_app_descriptor()
    }

    pub(crate) fn domain_pack(&self) -> app_mfg::MfgDomainPack {
        app_mfg::server_manufacturing_domain_pack()
    }

    pub(crate) fn ontology_pack(&self) -> MatrixOntologyPack {
        app_mfg::server_manufacturing_ontology_pack()
    }

    pub(crate) fn skill_manifest(&self, skill_id: &str) -> Option<MfgSkillManifest> {
        self.skill_pack()
            .into_iter()
            .find(|skill| skill.skill_id == skill_id)
    }

    pub(crate) fn plan_server_skills(
        &self,
        incident: &MfgIncident,
        analysis: Option<&MfgOperationalAnalysis>,
        packet: Option<&MatrixEvidencePacket>,
        limit: usize,
    ) -> MfgSkillPlan {
        plan_server_manufacturing_skills(incident, analysis, packet, limit)
    }

    pub(crate) fn run_server_skill(
        &self,
        incident: &MfgIncident,
        skill: &MfgSkillManifest,
        analysis: Option<&MfgOperationalAnalysis>,
        packet: Option<&MatrixEvidencePacket>,
    ) -> MfgSkillRun {
        run_server_manufacturing_skill(incident, skill, analysis, packet)
    }

    pub(crate) fn open_store(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MfgStore, MfgRepositoryError> {
        let registry = storage::StorageRegistry::default_for_config_home(config_home);
        registry
            .layout
            .ensure_directories()
            .map_err(to_mfg_storage_error)?;
        let handle = registry
            .sqlite_handle("mfg")
            .map_err(to_mfg_storage_error)?;
        MfgStore::open_storage_handle(handle)
    }

    fn open_live_store(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<Arc<MfgStore>, MfgRepositoryError> {
        let config_home = config_home.as_ref().to_path_buf();
        if let Some(store) = self
            .live_stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&config_home)
            .cloned()
        {
            return Ok(store);
        }
        let registry = storage::StorageRegistry::default_for_config_home(&config_home);
        registry
            .layout
            .ensure_directories()
            .map_err(to_mfg_storage_error)?;
        let handle = registry
            .sqlite_handle("mfg")
            .map_err(to_mfg_storage_error)?;
        let store = Arc::new(MfgStore::open_storage_handle(handle)?);
        let mut stores = self
            .live_stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(stores
            .entry(config_home)
            .or_insert_with(|| Arc::clone(&store))
            .clone())
    }

    pub(crate) fn incident_context(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
    ) -> Result<Option<MfgIncidentContext>, MfgRepositoryError> {
        let store = self.open_store(config_home)?;
        let Some(incident) = store.get_incident(incident_id)? else {
            return Ok(None);
        };
        let analysis = store.analyze_incident(incident_id).ok();
        let packet = incident
            .evidence_packet_id
            .as_deref()
            .and_then(|packet_id| store.get_evidence_packet(packet_id).ok().flatten());
        Ok(Some(MfgIncidentContext {
            incident,
            analysis,
            packet,
        }))
    }

    pub(crate) fn health(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MfgHealth, MfgRepositoryError> {
        self.open_store(config_home)?.health()
    }

    pub(crate) fn list_attention(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, MfgRepositoryError> {
        self.open_store(config_home)?.list_attention(limit)
    }

    pub(crate) fn list_changes(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixChangeEvent>, MfgRepositoryError> {
        self.open_store(config_home)?.list_changes(limit)
    }

    pub(crate) fn list_source_packs(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, MfgRepositoryError> {
        self.open_store(config_home)?.list_source_packs(limit)
    }

    pub(crate) fn list_facts(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixFact>, MfgRepositoryError> {
        self.open_store(config_home)?.list_facts(limit)
    }

    pub(crate) fn list_entities(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixEntity>, MfgRepositoryError> {
        self.open_store(config_home)?.list_entities(limit)
    }

    pub(crate) fn list_metric_definitions(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<Vec<MatrixMetricDefinition>, MfgRepositoryError> {
        self.open_store(config_home)?.list_metric_definitions()
    }

    pub(crate) fn list_evidence_packets(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, MfgRepositoryError> {
        self.open_store(config_home)?.list_evidence_packets(limit)
    }

    pub(crate) fn get_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, MfgRepositoryError> {
        self.open_store(config_home)?.get_evidence_packet(packet_id)
    }

    pub(crate) fn upsert_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        packet: &MatrixEvidencePacket,
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        self.open_store(config_home)?.upsert_evidence_packet(packet)
    }

    pub(crate) fn build_evidence_packet(
        &self,
        config_home: impl AsRef<Path>,
        attention_id: Option<&str>,
        title: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        self.open_store(config_home)?
            .build_evidence_packet(attention_id, title)
    }

    pub(crate) fn build_evidence_packet_idempotent(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
        attention_id: Option<&str>,
        title: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        self.open_store(config_home)?
            .build_evidence_packet_idempotent(packet_id, attention_id, title)
    }

    pub(crate) fn evaluate_evidence_quality(
        &self,
        config_home: impl AsRef<Path>,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, MfgRepositoryError> {
        self.open_store(config_home)?
            .evaluate_evidence_quality(packet_id)
    }

    pub(crate) fn seed_mfg_domain(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MfgDomainSeedResult, MfgRepositoryError> {
        self.open_store(config_home)?.seed_mfg_domain()
    }

    pub(crate) fn seed_mfg_ontology(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MatrixOntologyPack, MfgRepositoryError> {
        self.open_store(config_home)?.seed_mfg_ontology()
    }

    pub(crate) fn recompute_metrics(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<MfgMetricRecomputeResult, MfgRepositoryError> {
        self.open_store(config_home)?.recompute_metrics()
    }

    pub(crate) fn create_incident(
        &self,
        config_home: impl AsRef<Path>,
        incident: &MfgIncident,
    ) -> Result<MfgIncident, MfgRepositoryError> {
        self.open_store(config_home)?.create_incident(incident)
    }

    pub(crate) fn get_incident(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
    ) -> Result<Option<MfgIncident>, MfgRepositoryError> {
        self.open_store(config_home)?.get_incident(incident_id)
    }

    pub(crate) fn list_incidents(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MfgIncident>, MfgRepositoryError> {
        self.open_store(config_home)?.list_incidents(limit)
    }

    pub(crate) fn analyze_incident(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
    ) -> Result<MfgOperationalAnalysis, MfgRepositoryError> {
        self.open_store(config_home)?.analyze_incident(incident_id)
    }

    pub(crate) fn analyze_incident_idempotent(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
        analysis_id: &str,
    ) -> Result<MfgOperationalAnalysis, MfgRepositoryError> {
        self.open_store(config_home)?
            .analyze_incident_idempotent(incident_id, analysis_id)
    }

    pub(crate) fn latest_analysis_for_incident(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
        self.open_store(config_home)?
            .latest_analysis_for_incident(incident_id)
    }

    pub(crate) fn get_analysis(
        &self,
        config_home: impl AsRef<Path>,
        analysis_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
        self.open_store(config_home)?.get_analysis(analysis_id)
    }

    pub(crate) fn execute_recommended_action(
        &self,
        config_home: impl AsRef<Path>,
        analysis_id: &str,
        action_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.open_store(config_home)?
            .execute_recommended_action(analysis_id, action_id, request)
    }

    pub(crate) fn preview_recommended_action(
        &self,
        config_home: impl AsRef<Path>,
        analysis_id: &str,
        action_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.open_store(config_home)?
            .preview_recommended_action(analysis_id, action_id, request)
    }

    pub(crate) fn execute_recommended_action_idempotent(
        &self,
        config_home: impl AsRef<Path>,
        analysis_id: &str,
        action_id: &str,
        execution_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.open_store(config_home)?
            .execute_recommended_action_idempotent(analysis_id, action_id, execution_id, request)
    }

    pub(crate) fn get_execution(
        &self,
        config_home: impl AsRef<Path>,
        execution_id: &str,
    ) -> Result<Option<MfgActionExecution>, MfgRepositoryError> {
        self.open_store(config_home)?.get_execution(execution_id)
    }

    pub(crate) fn list_executions_for_incident(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_executions_for_incident(incident_id, limit)
    }

    pub(crate) fn list_recent_action_executions(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_recent_action_executions(limit)
    }

    pub(crate) fn attach_cross_plane_receipt(
        &self,
        config_home: impl AsRef<Path>,
        execution_id: &str,
        receipt: MfgCrossPlaneBridgeReceipt,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.open_store(config_home)?
            .attach_cross_plane_receipt(execution_id, receipt)
    }

    pub(crate) fn attach_execution_cross_plane_receipt(
        &self,
        config_home: impl AsRef<Path>,
        execution: &MfgActionExecution,
        receipt: &CrossPlaneExecutionReceipt,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.attach_cross_plane_receipt(
            config_home,
            &execution.execution_id,
            MfgCrossPlaneBridgeReceipt::new(
                execution.execution_id.clone(),
                receipt.id.clone(),
                receipt.status.clone(),
                receipt.dispatch_status.clone(),
                receipt.audit_record_id.clone(),
            ),
        )
    }

    pub(crate) fn record_execution_feedback(
        &self,
        config_home: impl AsRef<Path>,
        execution_id: &str,
        feedback: MfgActionFeedback,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.open_store(config_home)?
            .record_execution_feedback(execution_id, feedback)
    }

    pub(crate) fn record_skill_run(
        &self,
        config_home: impl AsRef<Path>,
        run: &MfgSkillRun,
    ) -> Result<MfgSkillRun, MfgRepositoryError> {
        self.open_store(config_home)?.record_skill_run(run)
    }

    pub(crate) fn get_skill_run(
        &self,
        config_home: impl AsRef<Path>,
        execution_id: &str,
    ) -> Result<Option<MfgSkillRun>, MfgRepositoryError> {
        self.open_store(config_home)?.get_skill_run(execution_id)
    }

    pub(crate) fn list_recent_skill_runs(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
        self.open_store(config_home)?.list_recent_skill_runs(limit)
    }

    pub(crate) fn list_skill_runs_for_incident(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_skill_runs_for_incident(incident_id, limit)
    }

    pub(crate) fn promote_incident_to_memory_case(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
    ) -> Result<MfgCasePromotion, MfgRepositoryError> {
        self.open_store(config_home)?
            .promote_incident_to_memory_case(incident_id)
    }

    pub(crate) fn get_memory_case(
        &self,
        config_home: impl AsRef<Path>,
        case_id: &str,
    ) -> Result<Option<MfgMemoryCase>, MfgRepositoryError> {
        self.open_store(config_home)?.get_memory_case(case_id)
    }

    pub(crate) fn search_memory_cases(
        &self,
        config_home: impl AsRef<Path>,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgMemoryCase>, MfgRepositoryError> {
        self.open_store(config_home)?
            .search_memory_cases(query, limit)
    }

    pub(crate) fn upsert_playbook(
        &self,
        config_home: impl AsRef<Path>,
        playbook: &MfgPlaybook,
        expected_revision: Option<u64>,
    ) -> Result<MfgPlaybook, MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_playbook(playbook, expected_revision)
    }

    pub(crate) fn get_playbook(
        &self,
        config_home: impl AsRef<Path>,
        playbook_id: &str,
    ) -> Result<Option<MfgPlaybook>, MfgRepositoryError> {
        self.open_store(config_home)?.get_playbook(playbook_id)
    }

    pub(crate) fn recommend_playbooks_for_incident(
        &self,
        config_home: impl AsRef<Path>,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgPlaybook>, MfgRepositoryError> {
        self.open_store(config_home)?
            .recommend_playbooks_for_incident(incident_id, limit)
    }

    pub(crate) fn upsert_cockpit_profile(
        &self,
        config_home: impl AsRef<Path>,
        profile: &MfgCockpitProfile,
        expected_revision: Option<u64>,
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_cockpit_profile(profile, expected_revision)
    }

    pub(crate) fn upsert_cockpit_profile_receipted(
        &self,
        config_home: impl AsRef<Path>,
        profile: &MfgCockpitProfile,
        expected_revision: Option<u64>,
        command: &str,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgCockpitProfile, MfgCommandReceipt), MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_cockpit_profile_receipted(
                profile,
                expected_revision,
                command,
                actor_ref,
                idempotency_key,
            )
    }

    pub(crate) fn get_cockpit_profile(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
    ) -> Result<Option<MfgCockpitProfile>, MfgRepositoryError> {
        self.open_store(config_home)?
            .get_cockpit_profile(profile_id)
    }

    pub(crate) fn list_cockpit_profiles(
        &self,
        config_home: impl AsRef<Path>,
        cadence: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgCockpitProfile>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_cockpit_profiles(cadence, limit)
    }

    pub(crate) fn cockpit_projection(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
    ) -> Result<MfgCockpitProjection, MfgRepositoryError> {
        self.open_store(config_home)?.cockpit_projection(profile_id)
    }

    pub(crate) fn cockpit_projection_with_filters(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        filters: serde_json::Value,
    ) -> Result<MfgCockpitProjection, MfgRepositoryError> {
        self.open_store(config_home)?
            .cockpit_projection_with_filters(profile_id, filters)
    }

    pub(crate) fn cockpit_widget_projection(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        instance_id: &str,
    ) -> Result<MfgCockpitWidgetProjection, MfgRepositoryError> {
        self.open_store(config_home)?
            .cockpit_widget_projection(profile_id, instance_id)
    }

    pub(crate) fn cockpit_widget_projection_with_filters(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        instance_id: &str,
        filters: serde_json::Value,
    ) -> Result<MfgCockpitWidgetProjection, MfgRepositoryError> {
        self.open_store(config_home)?
            .cockpit_widget_projection_with_filters(profile_id, instance_id, filters)
    }

    pub(crate) fn generate_cockpit_report(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.open_store(config_home)?
            .generate_cockpit_report(profile_id, request)
    }

    pub(crate) fn generate_cockpit_report_idempotent(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        report_id: &str,
        request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.open_store(config_home)?
            .generate_cockpit_report_idempotent(profile_id, report_id, request)
    }

    pub(crate) fn get_cockpit_report(
        &self,
        config_home: impl AsRef<Path>,
        report_id: &str,
    ) -> Result<Option<MfgCockpitReportSnapshot>, MfgRepositoryError> {
        self.open_store(config_home)?.get_cockpit_report(report_id)
    }

    pub(crate) fn list_cockpit_reports(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgCockpitReportSnapshot>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_cockpit_reports(profile_id, limit)
    }

    pub(crate) fn attach_cockpit_report_delivery(
        &self,
        config_home: impl AsRef<Path>,
        report_id: &str,
        receipt: MfgCockpitReportDeliveryReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.open_store(config_home)?
            .attach_cockpit_report_delivery(report_id, receipt)
    }

    pub(crate) fn create_report_delivery_review(
        &self,
        config_home: impl AsRef<Path>,
        report: &MfgCockpitReportSnapshot,
        expected_report_revision: u64,
        requester_principal: &str,
        reason: &str,
        evidence_refs: Vec<String>,
        idempotency_key: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        self.open_store(config_home)?.create_report_delivery_review(
            report,
            expected_report_revision,
            requester_principal,
            reason,
            evidence_refs,
            idempotency_key,
        )
    }

    pub(crate) fn bind_report_delivery_review_approval(
        &self,
        config_home: impl AsRef<Path>,
        review_id: &str,
        expected_revision: u64,
        approval_id: &str,
        actor_principal: &str,
        idempotency_key: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        self.open_store(config_home)?
            .bind_report_delivery_review_approval(
                review_id,
                expected_revision,
                approval_id,
                actor_principal,
                idempotency_key,
            )
    }

    pub(crate) fn get_report_delivery_review(
        &self,
        config_home: impl AsRef<Path>,
        review_id: &str,
    ) -> Result<Option<MfgReportDeliveryReview>, MfgRepositoryError> {
        self.open_store(config_home)?
            .get_report_delivery_review(review_id)
    }

    pub(crate) fn report_delivery_review_by_transition_key(
        &self,
        config_home: impl AsRef<Path>,
        review_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<MfgReportDeliveryReview>, MfgRepositoryError> {
        self.open_store(config_home)?
            .report_delivery_review_by_transition_key(review_id, idempotency_key)
    }

    pub(crate) fn list_report_delivery_reviews(
        &self,
        config_home: impl AsRef<Path>,
        report_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgReportDeliveryReview>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_report_delivery_reviews(report_id, limit)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_report_delivery_review_decision(
        &self,
        config_home: impl AsRef<Path>,
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
        self.open_store(config_home)?
            .prepare_report_delivery_review_decision(
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

    pub(crate) fn activate_report_delivery_review_decision(
        &self,
        config_home: impl AsRef<Path>,
        review_id: &str,
        expected_revision: u64,
        actor_principal: &str,
        idempotency_key: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        self.open_store(config_home)?
            .activate_report_delivery_review_decision(
                review_id,
                expected_revision,
                actor_principal,
                idempotency_key,
            )
    }

    pub(crate) fn claim_report_delivery_review_effects(
        &self,
        config_home: impl AsRef<Path>,
        limit: usize,
    ) -> Result<Vec<MfgReportDeliveryReviewEffect>, MfgRepositoryError> {
        self.open_store(config_home)?
            .claim_report_delivery_review_effects(limit)
    }

    pub(crate) fn complete_report_delivery_review_effect(
        &self,
        config_home: impl AsRef<Path>,
        effect_key: &str,
        receipt_ref: &str,
        actor_principal: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        self.open_store(config_home)?
            .complete_report_delivery_review_effect(effect_key, receipt_ref, actor_principal)
    }

    pub(crate) fn fail_report_delivery_review_effect(
        &self,
        config_home: impl AsRef<Path>,
        effect_key: &str,
        error: &str,
        actor_principal: &str,
    ) -> Result<MfgReportDeliveryReview, MfgRepositoryError> {
        self.open_store(config_home)?
            .fail_report_delivery_review_effect(effect_key, error, actor_principal)
    }

    pub(crate) fn delete_cockpit_profile(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        expected_revision: u64,
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        self.open_store(config_home)?
            .delete_cockpit_profile(profile_id, expected_revision)
    }

    pub(crate) fn delete_cockpit_profile_receipted(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        expected_revision: u64,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(Option<MfgCockpitProfile>, MfgCommandReceipt), MfgRepositoryError> {
        self.open_store(config_home)?
            .delete_cockpit_profile_receipted(
                profile_id,
                expected_revision,
                actor_ref,
                idempotency_key,
            )
    }

    pub(crate) fn upsert_alert_rule(
        &self,
        config_home: impl AsRef<Path>,
        rule: &MfgAlertRule,
        expected_revision: Option<u64>,
    ) -> Result<MfgAlertRule, MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_alert_rule(rule, expected_revision)
    }

    pub(crate) fn upsert_alert_rule_receipted(
        &self,
        config_home: impl AsRef<Path>,
        rule: &MfgAlertRule,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAlertRule, MfgCommandReceipt), MfgRepositoryError> {
        self.open_store(config_home)?.upsert_alert_rule_receipted(
            rule,
            expected_revision,
            actor_ref,
            idempotency_key,
        )
    }

    pub(crate) fn list_alert_rules(
        &self,
        config_home: impl AsRef<Path>,
        owner_ref: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAlertRule>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_alert_rules(owner_ref, limit)
    }

    pub(crate) fn list_alert_occurrences(
        &self,
        config_home: impl AsRef<Path>,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAlertOccurrence>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_alert_occurrences(status, limit)
    }

    pub(crate) fn upsert_alert_subscription(
        &self,
        config_home: impl AsRef<Path>,
        subscription: &MfgAlertSubscription,
        expected_revision: Option<u64>,
    ) -> Result<MfgAlertSubscription, MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_alert_subscription(subscription, expected_revision)
    }

    pub(crate) fn upsert_alert_subscription_receipted(
        &self,
        config_home: impl AsRef<Path>,
        subscription: &MfgAlertSubscription,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAlertSubscription, MfgCommandReceipt), MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_alert_subscription_receipted(
                subscription,
                expected_revision,
                actor_ref,
                idempotency_key,
            )
    }

    pub(crate) fn list_alert_subscriptions(
        &self,
        config_home: impl AsRef<Path>,
        subscriber_ref: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAlertSubscription>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_alert_subscriptions(subscriber_ref, limit)
    }

    pub(crate) fn command_alert(
        &self,
        config_home: impl AsRef<Path>,
        occurrence_id: &str,
        command: MfgAlertCommandInput,
    ) -> Result<(MfgAlertOccurrence, MfgCommandReceipt), MfgRepositoryError> {
        self.open_store(config_home)?
            .command_alert(occurrence_id, command)
    }

    pub(crate) fn forecasts(
        &self,
        config_home: impl AsRef<Path>,
        metric_refs: &[String],
        horizon: &str,
        limit: usize,
    ) -> Result<Vec<MfgForecastProjection>, MfgRepositoryError> {
        self.open_store(config_home)?
            .forecasts(metric_refs, horizon, limit)
    }

    pub(crate) fn upsert_assignment(
        &self,
        config_home: impl AsRef<Path>,
        assignment: &MfgAssignment,
        expected_revision: Option<u64>,
    ) -> Result<MfgAssignment, MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_assignment(assignment, expected_revision)
    }

    pub(crate) fn upsert_assignment_receipted(
        &self,
        config_home: impl AsRef<Path>,
        assignment: &MfgAssignment,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
        self.open_store(config_home)?.upsert_assignment_receipted(
            assignment,
            expected_revision,
            actor_ref,
            idempotency_key,
        )
    }

    pub(crate) fn get_assignment(
        &self,
        config_home: impl AsRef<Path>,
        assignment_id: &str,
    ) -> Result<Option<MfgAssignment>, MfgRepositoryError> {
        self.open_store(config_home)?.get_assignment(assignment_id)
    }

    pub(crate) fn list_assignments(
        &self,
        config_home: impl AsRef<Path>,
        assignee_ref: Option<&str>,
        incident_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAssignment>, MfgRepositoryError> {
        self.open_store(config_home)?
            .list_assignments(assignee_ref, incident_id, limit)
    }

    pub(crate) fn command_assignment(
        &self,
        config_home: impl AsRef<Path>,
        assignment_id: &str,
        command: MfgAssignmentCommandInput,
    ) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
        self.open_store(config_home)?
            .command_assignment(assignment_id, command)
    }

    pub(crate) fn reserve_assignment_completion(
        &self,
        config_home: impl AsRef<Path>,
        assignment_id: &str,
        expected_revision: u64,
        actor_ref: &str,
        correlation_id: &str,
    ) -> Result<MfgAssignment, MfgRepositoryError> {
        self.open_store(config_home)?.reserve_assignment_completion(
            assignment_id,
            expected_revision,
            actor_ref,
            correlation_id,
        )
    }

    pub(crate) fn live_epoch(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<app_mfg::MfgLiveEpoch, MfgRepositoryError> {
        self.open_live_store(config_home)?.live_epoch()
    }

    pub(crate) fn rotate_live_epoch(
        &self,
        config_home: impl AsRef<Path>,
        reason: &str,
    ) -> Result<app_mfg::MfgLiveEpoch, MfgRepositoryError> {
        self.open_live_store(config_home)?.rotate_live_epoch(reason)
    }

    pub(crate) fn live_snapshot_read(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<app_mfg::MfgLiveSnapshotRead, MfgRepositoryError> {
        self.open_live_store(config_home)?.live_snapshot_read()
    }

    pub(crate) fn live_delta_read(
        &self,
        config_home: impl AsRef<Path>,
        cursor: u64,
        limit: usize,
    ) -> Result<app_mfg::MfgLiveDeltaRead, MfgRepositoryError> {
        self.open_live_store(config_home)?
            .live_delta_read(cursor, limit)
    }

    pub(crate) fn record_command_notifications(
        &self,
        config_home: impl AsRef<Path>,
        idempotency_key: &str,
        notification_refs: Vec<String>,
    ) -> Result<MfgCommandReceipt, MfgRepositoryError> {
        self.open_store(config_home)?
            .record_command_notifications(idempotency_key, notification_refs)
    }

    pub(crate) fn command_notification_refs_for_resource(
        &self,
        config_home: impl AsRef<Path>,
        resource_ref: &str,
    ) -> Result<Vec<String>, MfgRepositoryError> {
        self.open_store(config_home)?
            .command_notification_refs_for_resource(resource_ref)
    }

    pub(crate) fn native_command_receipt_by_identity(
        &self,
        config_home: impl AsRef<Path>,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
    ) -> Result<Option<app_mfg::MfgCommandReceipt>, MfgRepositoryError> {
        self.open_store(config_home)?
            .native_command_receipt_by_identity(
                idempotency_key,
                actor_principal,
                action_id,
                resource_ref,
            )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn claim_mutation_receipt(
        &self,
        config_home: impl AsRef<Path>,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
        expected_revision: Option<u64>,
        payload_digest: &str,
        correlation_id: &str,
    ) -> Result<app_mfg::MfgMutationClaim, MfgRepositoryError> {
        self.open_store(config_home)?.claim_mutation_receipt(
            idempotency_key,
            actor_principal,
            action_id,
            resource_ref,
            expected_revision,
            payload_digest,
            correlation_id,
        )
    }

    pub(crate) fn release_mutation_claim(
        &self,
        config_home: impl AsRef<Path>,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        payload_digest: &str,
    ) -> Result<bool, MfgRepositoryError> {
        self.open_store(config_home)?.release_mutation_claim(
            idempotency_key,
            actor_principal,
            action_id,
            payload_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn find_mutation_receipt(
        &self,
        config_home: impl AsRef<Path>,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
        payload_digest: &str,
    ) -> Result<Option<(app_mfg_contract::MfgReceiptV1, serde_json::Value)>, MfgRepositoryError>
    {
        self.open_store(config_home)?.find_mutation_receipt(
            idempotency_key,
            actor_principal,
            action_id,
            resource_ref,
            payload_digest,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_mutation_receipt(
        &self,
        config_home: impl AsRef<Path>,
        idempotency_key: &str,
        actor_principal: &str,
        action_id: &str,
        resource_ref: &str,
        expected_revision: Option<u64>,
        result_revision: Option<u64>,
        payload_digest: &str,
        response: &serde_json::Value,
    ) -> Result<app_mfg_contract::MfgReceiptV1, MfgRepositoryError> {
        self.open_store(config_home)?.record_mutation_receipt(
            idempotency_key,
            actor_principal,
            action_id,
            resource_ref,
            expected_revision,
            result_revision,
            payload_digest,
            response,
        )
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.envelope("health"),
            self.envelope("incident"),
            self.envelope("analysis"),
            self.envelope("skill_run"),
            self.envelope("action_execution"),
            self.envelope("cockpit_report"),
            self.envelope("memory_case"),
            self.envelope("playbook"),
        ]
    }
}

fn to_mfg_storage_error(error: storage::StorageError) -> MfgRepositoryError {
    MfgRepositoryError::Storage(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn review_reconciler_has_one_owner_and_weak_shutdown_boundary() {
        let service = MfgService::new();
        let lifecycle = service
            .begin_review_reconciler()
            .expect("first caller starts reconciler");
        assert!(service.begin_review_reconciler().is_none());
        assert!(lifecycle.upgrade().is_some());
        drop(service);
        assert!(lifecycle.upgrade().is_none());
    }

    #[tokio::test]
    async fn review_reconciler_shutdown_cancels_and_awaits_task() {
        let service = MfgService::new();
        let lifecycle = service
            .begin_review_reconciler()
            .expect("first caller starts reconciler");
        let observer = lifecycle.clone();
        let handle = tokio::spawn(async move {
            while observer
                .upgrade()
                .is_some_and(|owner| !owner.is_cancelled())
            {
                tokio::task::yield_now().await;
            }
        });
        service.install_review_reconciler(handle);
        service.shutdown_review_reconciler().await;
        assert!(lifecycle.upgrade().unwrap().is_cancelled());
        assert!(service.review_reconciler.handle.lock().unwrap().is_none());
    }
}
