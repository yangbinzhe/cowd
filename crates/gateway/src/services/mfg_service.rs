use std::path::Path;

use super::ServiceEnvelope;
use app_mfg::{
    plan_server_manufacturing_skills, run_server_manufacturing_skill,
    server_manufacturing_skill_pack, MfgActionExecution, MfgActionExecutionRequest,
    MfgActionFeedback, MfgCasePromotion, MfgCockpitProfile, MfgCockpitProjection,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportRequest, MfgCockpitReportSnapshot,
    MfgCrossPlaneBridgeReceipt, MfgDomainSeedResult, MfgHealth, MfgIncident, MfgMemoryCase,
    MfgMetricRecomputeResult, MfgOperationalAnalysis, MfgPlaybook, MfgRepositoryError,
    MfgSkillManifest, MfgSkillPlan, MfgSkillRun, MfgStore,
};
use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixEntity, MatrixEvidencePacket, MatrixFact,
    MatrixMetricDefinition, MatrixOntologyPack, MatrixQualityGateDecision, MatrixSourcePack,
};
use runtime::{
    CrossPlaneAction, CrossPlaneControlPlane, CrossPlaneDecisionEvidence,
    CrossPlaneExecutionReceipt, CrossPlanePolicyDecision, CrossPlaneRisk, DataClassification,
    IdentityTrust, PolicyDecisionKind,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub(crate) struct MfgCrossPlaneBridgeRequest {
    #[serde(default = "default_mfg_bridge_mode")]
    pub(crate) mode: String,
    #[serde(default)]
    pub(crate) idempotency_key: Option<String>,
    #[serde(default)]
    pub(crate) actor_principal: Option<String>,
    #[serde(default)]
    pub(crate) actor_identity_ref: Option<String>,
    #[serde(default)]
    pub(crate) source_channel: Option<String>,
    #[serde(default)]
    pub(crate) requested_capability: Option<String>,
    #[serde(default)]
    pub(crate) provider_account: Option<String>,
    #[serde(default)]
    pub(crate) target_ref: Option<String>,
    #[serde(default)]
    pub(crate) resource_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MfgCockpitReportDeliveryRequest {
    #[serde(default = "default_mfg_bridge_mode")]
    pub(crate) mode: String,
    #[serde(default)]
    pub(crate) idempotency_key: Option<String>,
    #[serde(default)]
    pub(crate) actor_principal: Option<String>,
    #[serde(default)]
    pub(crate) actor_identity_ref: Option<String>,
    #[serde(default)]
    pub(crate) source_channel: Option<String>,
    #[serde(default)]
    pub(crate) requested_capability: Option<String>,
    #[serde(default)]
    pub(crate) provider_account: Option<String>,
    #[serde(default)]
    pub(crate) target_ref: Option<String>,
    #[serde(default)]
    pub(crate) resource_ref: Option<String>,
    #[serde(default)]
    pub(crate) channel: Option<String>,
    #[serde(default)]
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

fn default_cross_plane_capability(execution: &MfgActionExecution) -> &'static str {
    match execution.action_type.as_str() {
        "supplier_recovery" | "plan_bom_reconciliation" | "evidence_review" => {
            "channel.feishu.send_text"
        }
        _ => "channel.feishu.send_text",
    }
}

fn default_bridge_message(execution: &MfgActionExecution) -> String {
    format!(
        "MFG action {} [{}]: {}; incident={}; execution={}",
        execution.action_type,
        execution.owner_role,
        execution.title,
        execution.incident_id,
        execution.execution_id
    )
}

fn cross_plane_risk(execution: &MfgActionExecution) -> CrossPlaneRisk {
    if execution.governance.contains("human_review") || execution.mode == "commit" {
        CrossPlaneRisk::Medium
    } else {
        CrossPlaneRisk::Low
    }
}

#[derive(Clone)]
pub(crate) struct MfgService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
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
            owner: "0.9.292 Gateway RuntimeHost",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: "service_boundary_ready",
            owner: self.owner,
            boundary_status: "0618_final_boundary",
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

    pub(crate) fn normalize_bridge_mode(&self, mode: &str) -> String {
        match mode.trim().to_ascii_lowercase().as_str() {
            "commit" | "live" | "execute" => "commit".to_string(),
            _ => "dry_run".to_string(),
        }
    }

    pub(crate) fn cross_plane_action_from_execution(
        &self,
        execution: &MfgActionExecution,
        request: &MfgCrossPlaneBridgeRequest,
    ) -> CrossPlaneAction {
        let actor_principal = request
            .actor_principal
            .as_deref()
            .or(execution.operator_id.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("mfg:operator");
        let requested_capability = request
            .requested_capability
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| default_cross_plane_capability(execution));
        let mut action = CrossPlaneAction::new(actor_principal, requested_capability);
        action.actor_identity_ref = request.actor_identity_ref.clone();
        action.source_channel = Some(
            request
                .source_channel
                .clone()
                .unwrap_or_else(|| "mfg".to_string()),
        );
        action.session_id = Some(execution.incident_id.clone());
        action.provider_account = request.provider_account.clone();
        action.target_ref = request.target_ref.clone();
        action.resource_ref = request
            .resource_ref
            .clone()
            .or_else(|| Some(format!("text://{}", default_bridge_message(execution))));
        action.risk = cross_plane_risk(execution);
        action.data_classification = DataClassification::Internal;
        action.identity_trust = IdentityTrust::Unknown;
        action
    }

    pub(crate) fn bridge_outcome(
        &self,
        mode: &str,
        decision: &CrossPlanePolicyDecision,
    ) -> (String, String, Vec<String>, String, String) {
        if decision.decision == PolicyDecisionKind::Allow {
            if mode == "dry_run" {
                return (
                    "planned".to_string(),
                    "dry_run".to_string(),
                    Vec::new(),
                    "dry_run".to_string(),
                    "mfg_cross_plane_bridge_dry_run_plan".to_string(),
                );
            }
            return (
                "planned".to_string(),
                "human_review_required".to_string(),
                vec!["mfg:human_review_required".to_string()],
                "planned".to_string(),
                "mfg_cross_plane_bridge_queued_for_human_review".to_string(),
            );
        }
        (
            "blocked".to_string(),
            "policy_blocked".to_string(),
            vec![format!("policy:{}", decision.reason)],
            "blocked".to_string(),
            "mfg_cross_plane_bridge_policy_blocked".to_string(),
        )
    }

    pub(crate) fn record_cross_plane_bridge_receipt(
        &self,
        control: &CrossPlaneControlPlane,
        idempotency_key: Option<String>,
        mode: String,
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
        evidence: CrossPlaneDecisionEvidence,
    ) -> CrossPlaneExecutionReceipt {
        let (status, dispatch_status, blockers, audit_result, audit_summary) =
            self.bridge_outcome(&mode, &decision);
        let audit_record = runtime::CrossPlaneAuditRecord::new(
            action.clone(),
            decision.clone(),
            audit_result,
            audit_summary,
        )
        .with_evidence(evidence);
        let audit_record_id = audit_record.id.clone();
        control.record_audit(audit_record);
        let receipt = CrossPlaneExecutionReceipt::new(
            idempotency_key,
            mode,
            status,
            dispatch_status,
            action,
            decision,
            blockers,
            Some(audit_record_id),
        );
        control.record_execution(receipt.clone());
        receipt
    }

    pub(crate) fn report_delivery_payload(
        &self,
        report: &MfgCockpitReportSnapshot,
        request: &MfgCockpitReportDeliveryRequest,
    ) -> app_mfg::MfgCockpitReportDeliveryPayload {
        app_mfg::MfgCockpitReportDeliveryPayload::from_report(
            report,
            app_mfg::MfgCockpitReportDeliveryPayloadRequest {
                channel: request.channel.clone(),
                template_id: request.template_id.clone(),
                target_ref: request
                    .target_ref
                    .clone()
                    .or_else(|| report.delivery_ref.clone()),
                requested_capability: request.requested_capability.clone(),
            },
        )
    }

    pub(crate) fn report_delivery_action(
        &self,
        report: &MfgCockpitReportSnapshot,
        request: &MfgCockpitReportDeliveryRequest,
        delivery_payload: &app_mfg::MfgCockpitReportDeliveryPayload,
    ) -> CrossPlaneAction {
        let actor_principal = request
            .actor_principal
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(report.owner_ref.as_str());
        let requested_capability = request
            .requested_capability
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(delivery_payload.requested_capability.as_str());
        let mut action = CrossPlaneAction::new(actor_principal, requested_capability);
        action.actor_identity_ref = request.actor_identity_ref.clone();
        action.source_channel = Some(
            request
                .source_channel
                .clone()
                .unwrap_or_else(|| "mfg.report".to_string()),
        );
        action.session_id = Some(report.report_id.clone());
        action.provider_account = request.provider_account.clone();
        action.target_ref = request
            .target_ref
            .clone()
            .or_else(|| delivery_payload.target_ref.clone())
            .or_else(|| report.delivery_ref.clone());
        action.resource_ref = request
            .resource_ref
            .clone()
            .or_else(|| Some(delivery_payload.resource_ref.clone()));
        action.risk = CrossPlaneRisk::Low;
        action.data_classification = DataClassification::Internal;
        action.identity_trust = IdentityTrust::Unknown;
        action
    }

    pub(crate) fn report_delivery_receipt_matches(
        &self,
        receipt: &CrossPlaneExecutionReceipt,
        report: &MfgCockpitReportSnapshot,
    ) -> bool {
        receipt.action.session_id.as_deref() == Some(report.report_id.as_str())
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
    ) -> Result<MfgPlaybook, MfgRepositoryError> {
        self.open_store(config_home)?.upsert_playbook(playbook)
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
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        self.open_store(config_home)?
            .upsert_cockpit_profile(profile)
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

    pub(crate) fn generate_cockpit_report(
        &self,
        config_home: impl AsRef<Path>,
        profile_id: &str,
        request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.open_store(config_home)?
            .generate_cockpit_report(profile_id, request)
    }

    pub(crate) fn get_cockpit_report(
        &self,
        config_home: impl AsRef<Path>,
        report_id: &str,
    ) -> Result<Option<MfgCockpitReportSnapshot>, MfgRepositoryError> {
        self.open_store(config_home)?.get_cockpit_report(report_id)
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

    pub(crate) fn attach_report_delivery_receipt(
        &self,
        config_home: impl AsRef<Path>,
        report: &MfgCockpitReportSnapshot,
        receipt: &CrossPlaneExecutionReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.attach_cockpit_report_delivery(
            config_home,
            &report.report_id,
            MfgCockpitReportDeliveryReceipt::new(
                report.report_id.clone(),
                receipt.id.clone(),
                receipt.status.clone(),
                receipt.dispatch_status.clone(),
                receipt.audit_record_id.clone(),
            ),
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
