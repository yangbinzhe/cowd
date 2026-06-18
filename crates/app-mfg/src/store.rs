use std::path::Path;

use matrix::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixEntity, MatrixEvidencePacket, MatrixFact,
    MatrixMetricDefinition, MatrixOntologyPack, MatrixQualityGateDecision, MatrixSourcePack,
};
use runtime::{
    open_mfg_matrix_adapter, MatrixHealth, MfgActionExecution, MfgActionExecutionRequest,
    MfgActionFeedback, MfgCasePromotion, MfgCockpitProfile, MfgCockpitProjection,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportRequest, MfgCockpitReportSnapshot,
    MfgCrossPlaneBridgeReceipt, MfgDomainSeedResult, MfgIncident,
    MfgMatrixAdapter as RuntimeMfgMatrixAdapter, MfgMatrixAdapterError, MfgMemoryCase,
    MfgOperationalAnalysis, MfgPlaybook, MfgSkillRun,
};

/// Application-layer store facade for MFG.
///
/// The facade keeps gateway and app code from depending directly on MfgMatrixAdapter
/// for manufacturing operations while the underlying SQLite schema is still
/// shared during migration.
#[derive(Debug)]
pub struct MfgStore {
    matrix: RuntimeMfgMatrixAdapter,
}

impl MfgStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MfgMatrixAdapterError> {
        Ok(Self {
            matrix: open_mfg_matrix_adapter(path)?,
        })
    }

    pub fn seed_mfg_domain(&self) -> Result<MfgDomainSeedResult, MfgMatrixAdapterError> {
        self.matrix.seed_mfg_domain()
    }

    pub fn seed_mfg_ontology(&self) -> Result<MatrixOntologyPack, MfgMatrixAdapterError> {
        self.matrix.seed_mfg_ontology()
    }

    pub fn health(&self) -> Result<MatrixHealth, MfgMatrixAdapterError> {
        self.matrix.health()
    }

    pub fn list_attention(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, MfgMatrixAdapterError> {
        self.matrix.list_attention(limit)
    }

    pub fn list_changes(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixChangeEvent>, MfgMatrixAdapterError> {
        self.matrix.list_changes(limit)
    }

    pub fn list_source_packs(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, MfgMatrixAdapterError> {
        self.matrix.list_source_packs(limit)
    }

    pub fn list_facts(&self, limit: usize) -> Result<Vec<MatrixFact>, MfgMatrixAdapterError> {
        self.matrix.list_facts(limit)
    }

    pub fn list_entities(&self, limit: usize) -> Result<Vec<MatrixEntity>, MfgMatrixAdapterError> {
        self.matrix.list_entities(limit)
    }

    pub fn list_metric_definitions(
        &self,
    ) -> Result<Vec<MatrixMetricDefinition>, MfgMatrixAdapterError> {
        self.matrix.list_metric_definitions()
    }

    pub fn list_evidence_packets(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, MfgMatrixAdapterError> {
        self.matrix.list_evidence_packets(limit)
    }

    pub fn get_evidence_packet(
        &self,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, MfgMatrixAdapterError> {
        self.matrix.get_evidence_packet(packet_id)
    }

    pub fn build_evidence_packet(
        &self,
        attention_id: Option<&str>,
        title: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MfgMatrixAdapterError> {
        self.matrix.build_evidence_packet(attention_id, title)
    }

    pub fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, MfgMatrixAdapterError> {
        self.matrix.evaluate_evidence_quality(packet_id)
    }

    pub fn create_incident(
        &self,
        incident: &MfgIncident,
    ) -> Result<MfgIncident, MfgMatrixAdapterError> {
        self.matrix.create_incident(incident)
    }

    pub fn get_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgIncident>, MfgMatrixAdapterError> {
        self.matrix.get_incident(incident_id)
    }

    pub fn list_incidents(&self, limit: usize) -> Result<Vec<MfgIncident>, MfgMatrixAdapterError> {
        self.matrix.list_incidents(limit)
    }

    pub fn analyze_incident(
        &self,
        incident_id: &str,
    ) -> Result<MfgOperationalAnalysis, MfgMatrixAdapterError> {
        self.matrix.analyze_incident(incident_id)
    }

    pub fn latest_analysis_for_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MfgMatrixAdapterError> {
        self.matrix.latest_analysis_for_incident(incident_id)
    }

    pub fn get_analysis(
        &self,
        analysis_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MfgMatrixAdapterError> {
        self.matrix.get_analysis(analysis_id)
    }

    pub fn execute_recommended_action(
        &self,
        analysis_id: &str,
        action_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MfgMatrixAdapterError> {
        self.matrix
            .execute_recommended_action(analysis_id, action_id, request)
    }

    pub fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgActionExecution>, MfgMatrixAdapterError> {
        self.matrix.get_execution(execution_id)
    }

    pub fn list_executions_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgMatrixAdapterError> {
        self.matrix.list_executions_for_incident(incident_id, limit)
    }

    pub fn list_recent_action_executions(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MfgMatrixAdapterError> {
        self.matrix.list_recent_action_executions(limit)
    }

    pub fn attach_cross_plane_receipt(
        &self,
        execution_id: &str,
        receipt: MfgCrossPlaneBridgeReceipt,
    ) -> Result<MfgActionExecution, MfgMatrixAdapterError> {
        self.matrix
            .attach_cross_plane_receipt(execution_id, receipt)
    }

    pub fn record_execution_feedback(
        &self,
        execution_id: &str,
        feedback: MfgActionFeedback,
    ) -> Result<MfgActionExecution, MfgMatrixAdapterError> {
        self.matrix
            .record_execution_feedback(execution_id, feedback)
    }

    pub fn record_skill_run(
        &self,
        run: &MfgSkillRun,
    ) -> Result<MfgSkillRun, MfgMatrixAdapterError> {
        self.matrix.record_skill_run(run)
    }

    pub fn get_skill_run(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgSkillRun>, MfgMatrixAdapterError> {
        self.matrix.get_skill_run(execution_id)
    }

    pub fn list_skill_runs_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgMatrixAdapterError> {
        self.matrix.list_skill_runs_for_incident(incident_id, limit)
    }

    pub fn list_recent_skill_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MfgMatrixAdapterError> {
        self.matrix.list_recent_skill_runs(limit)
    }

    pub fn promote_incident_to_memory_case(
        &self,
        incident_id: &str,
    ) -> Result<MfgCasePromotion, MfgMatrixAdapterError> {
        self.matrix.promote_incident_to_memory_case(incident_id)
    }

    pub fn get_memory_case(
        &self,
        case_id: &str,
    ) -> Result<Option<MfgMemoryCase>, MfgMatrixAdapterError> {
        self.matrix.get_memory_case(case_id)
    }

    pub fn search_memory_cases(
        &self,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgMemoryCase>, MfgMatrixAdapterError> {
        self.matrix.search_memory_cases(query, limit)
    }

    pub fn upsert_playbook(
        &self,
        playbook: &MfgPlaybook,
    ) -> Result<MfgPlaybook, MfgMatrixAdapterError> {
        self.matrix.upsert_playbook(playbook)
    }

    pub fn get_playbook(
        &self,
        playbook_id: &str,
    ) -> Result<Option<MfgPlaybook>, MfgMatrixAdapterError> {
        self.matrix.get_playbook(playbook_id)
    }

    pub fn recommend_playbooks_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgPlaybook>, MfgMatrixAdapterError> {
        self.matrix
            .recommend_playbooks_for_incident(incident_id, limit)
    }

    pub fn upsert_cockpit_profile(
        &self,
        profile: &MfgCockpitProfile,
    ) -> Result<MfgCockpitProfile, MfgMatrixAdapterError> {
        self.matrix.upsert_cockpit_profile(profile)
    }

    pub fn get_cockpit_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<MfgCockpitProfile>, MfgMatrixAdapterError> {
        self.matrix.get_cockpit_profile(profile_id)
    }

    pub fn list_cockpit_profiles(
        &self,
        cadence: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgCockpitProfile>, MfgMatrixAdapterError> {
        self.matrix.list_cockpit_profiles(cadence, limit)
    }

    pub fn cockpit_projection(
        &self,
        profile_id: &str,
    ) -> Result<MfgCockpitProjection, MfgMatrixAdapterError> {
        self.matrix.cockpit_projection(profile_id)
    }

    pub fn generate_cockpit_report(
        &self,
        profile_id: &str,
        request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MfgMatrixAdapterError> {
        self.matrix.generate_cockpit_report(profile_id, request)
    }

    pub fn get_cockpit_report(
        &self,
        report_id: &str,
    ) -> Result<Option<MfgCockpitReportSnapshot>, MfgMatrixAdapterError> {
        self.matrix.get_cockpit_report(report_id)
    }

    pub fn attach_cockpit_report_delivery(
        &self,
        report_id: &str,
        receipt: MfgCockpitReportDeliveryReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgMatrixAdapterError> {
        self.matrix
            .attach_cockpit_report_delivery(report_id, receipt)
    }
}
