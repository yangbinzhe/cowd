use std::path::Path;

use crate::repository::{MfgHealth, MfgMetricRecomputeResult, MfgRepository, MfgRepositoryError};
use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixEntity, MatrixEvidencePacket, MatrixFact,
    MatrixMetricDefinition, MatrixOntologyPack, MatrixQualityGateDecision, MatrixSourcePack,
};

use crate::{
    MfgActionExecution, MfgActionExecutionRequest, MfgActionFeedback, MfgCasePromotion,
    MfgCockpitProfile, MfgCockpitProjection, MfgCockpitReportDeliveryReceipt,
    MfgCockpitReportRequest, MfgCockpitReportSnapshot, MfgCrossPlaneBridgeReceipt,
    MfgDomainSeedResult, MfgIncident, MfgMemoryCase, MfgOperationalAnalysis, MfgPlaybook,
    MfgSkillRun,
};

/// Application-layer store facade for MFG.
///
/// The facade keeps gateway and app code from depending directly on MfgRepository
/// for manufacturing operations.
#[derive(Debug)]
pub struct MfgStore {
    repository: MfgRepository,
}

impl MfgStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MfgRepositoryError> {
        Ok(Self {
            repository: MfgRepository::open(path)?,
        })
    }

    pub fn open_storage_handle(
        handle: &storage::StorageHandle,
    ) -> Result<Self, MfgRepositoryError> {
        Ok(Self {
            repository: MfgRepository::open_storage_handle(handle)?,
        })
    }

    pub fn seed_mfg_domain(&self) -> Result<MfgDomainSeedResult, MfgRepositoryError> {
        self.repository.seed_mfg_domain()
    }

    pub fn seed_mfg_ontology(&self) -> Result<MatrixOntologyPack, MfgRepositoryError> {
        self.repository.seed_mfg_ontology()
    }

    pub fn health(&self) -> Result<MfgHealth, MfgRepositoryError> {
        self.repository.health()
    }

    pub fn list_attention(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, MfgRepositoryError> {
        self.repository.list_attention(limit)
    }

    pub fn list_changes(&self, limit: usize) -> Result<Vec<MatrixChangeEvent>, MfgRepositoryError> {
        self.repository.list_changes(limit)
    }

    pub fn list_source_packs(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, MfgRepositoryError> {
        self.repository.list_source_packs(limit)
    }

    pub fn list_facts(&self, limit: usize) -> Result<Vec<MatrixFact>, MfgRepositoryError> {
        self.repository.list_facts(limit)
    }

    pub fn list_entities(&self, limit: usize) -> Result<Vec<MatrixEntity>, MfgRepositoryError> {
        self.repository.list_entities(limit)
    }

    pub fn list_metric_definitions(
        &self,
    ) -> Result<Vec<MatrixMetricDefinition>, MfgRepositoryError> {
        self.repository.list_metric_definitions()
    }

    pub fn recompute_metrics(&self) -> Result<MfgMetricRecomputeResult, MfgRepositoryError> {
        self.repository.recompute_metrics()
    }

    pub fn list_evidence_packets(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, MfgRepositoryError> {
        self.repository.list_evidence_packets(limit)
    }

    pub fn get_evidence_packet(
        &self,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, MfgRepositoryError> {
        self.repository.get_evidence_packet(packet_id)
    }

    pub fn build_evidence_packet(
        &self,
        attention_id: Option<&str>,
        title: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        self.repository.build_evidence_packet(attention_id, title)
    }

    pub fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, MfgRepositoryError> {
        self.repository.evaluate_evidence_quality(packet_id)
    }

    pub fn create_incident(
        &self,
        incident: &MfgIncident,
    ) -> Result<MfgIncident, MfgRepositoryError> {
        self.repository.create_incident(incident)
    }

    pub fn get_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgIncident>, MfgRepositoryError> {
        self.repository.get_incident(incident_id)
    }

    pub fn list_incidents(&self, limit: usize) -> Result<Vec<MfgIncident>, MfgRepositoryError> {
        self.repository.list_incidents(limit)
    }

    pub fn analyze_incident(
        &self,
        incident_id: &str,
    ) -> Result<MfgOperationalAnalysis, MfgRepositoryError> {
        self.repository.analyze_incident(incident_id)
    }

    pub fn latest_analysis_for_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
        self.repository.latest_analysis_for_incident(incident_id)
    }

    pub fn get_analysis(
        &self,
        analysis_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MfgRepositoryError> {
        self.repository.get_analysis(analysis_id)
    }

    pub fn execute_recommended_action(
        &self,
        analysis_id: &str,
        action_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.repository
            .execute_recommended_action(analysis_id, action_id, request)
    }

    pub fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgActionExecution>, MfgRepositoryError> {
        self.repository.get_execution(execution_id)
    }

    pub fn list_executions_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
        self.repository
            .list_executions_for_incident(incident_id, limit)
    }

    pub fn list_recent_action_executions(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgRepositoryError> {
        self.repository.list_recent_action_executions(limit)
    }

    pub fn attach_cross_plane_receipt(
        &self,
        execution_id: &str,
        receipt: MfgCrossPlaneBridgeReceipt,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.repository
            .attach_cross_plane_receipt(execution_id, receipt)
    }

    pub fn record_execution_feedback(
        &self,
        execution_id: &str,
        feedback: MfgActionFeedback,
    ) -> Result<MfgActionExecution, MfgRepositoryError> {
        self.repository
            .record_execution_feedback(execution_id, feedback)
    }

    pub fn record_skill_run(&self, run: &MfgSkillRun) -> Result<MfgSkillRun, MfgRepositoryError> {
        self.repository.record_skill_run(run)
    }

    pub fn get_skill_run(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgSkillRun>, MfgRepositoryError> {
        self.repository.get_skill_run(execution_id)
    }

    pub fn list_skill_runs_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
        self.repository
            .list_skill_runs_for_incident(incident_id, limit)
    }

    pub fn list_recent_skill_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgRepositoryError> {
        self.repository.list_recent_skill_runs(limit)
    }

    pub fn promote_incident_to_memory_case(
        &self,
        incident_id: &str,
    ) -> Result<MfgCasePromotion, MfgRepositoryError> {
        self.repository.promote_incident_to_memory_case(incident_id)
    }

    pub fn get_memory_case(
        &self,
        case_id: &str,
    ) -> Result<Option<MfgMemoryCase>, MfgRepositoryError> {
        self.repository.get_memory_case(case_id)
    }

    pub fn search_memory_cases(
        &self,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgMemoryCase>, MfgRepositoryError> {
        self.repository.search_memory_cases(query, limit)
    }

    pub fn upsert_playbook(
        &self,
        playbook: &MfgPlaybook,
    ) -> Result<MfgPlaybook, MfgRepositoryError> {
        self.repository.upsert_playbook(playbook)
    }

    pub fn get_playbook(
        &self,
        playbook_id: &str,
    ) -> Result<Option<MfgPlaybook>, MfgRepositoryError> {
        self.repository.get_playbook(playbook_id)
    }

    pub fn recommend_playbooks_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgPlaybook>, MfgRepositoryError> {
        self.repository
            .recommend_playbooks_for_incident(incident_id, limit)
    }

    pub fn upsert_cockpit_profile(
        &self,
        profile: &MfgCockpitProfile,
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        self.repository.upsert_cockpit_profile(profile)
    }

    pub fn get_cockpit_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<MfgCockpitProfile>, MfgRepositoryError> {
        self.repository.get_cockpit_profile(profile_id)
    }

    pub fn list_cockpit_profiles(
        &self,
        cadence: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgCockpitProfile>, MfgRepositoryError> {
        self.repository.list_cockpit_profiles(cadence, limit)
    }

    pub fn cockpit_projection(
        &self,
        profile_id: &str,
    ) -> Result<MfgCockpitProjection, MfgRepositoryError> {
        self.repository.cockpit_projection(profile_id)
    }

    pub fn generate_cockpit_report(
        &self,
        profile_id: &str,
        request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.repository.generate_cockpit_report(profile_id, request)
    }

    pub fn get_cockpit_report(
        &self,
        report_id: &str,
    ) -> Result<Option<MfgCockpitReportSnapshot>, MfgRepositoryError> {
        self.repository.get_cockpit_report(report_id)
    }

    pub fn attach_cockpit_report_delivery(
        &self,
        report_id: &str,
        receipt: MfgCockpitReportDeliveryReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.repository
            .attach_cockpit_report_delivery(report_id, receipt)
    }
}
