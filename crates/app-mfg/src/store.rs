use std::path::Path;

use runtime::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixEntity, MatrixEvidencePacket, MatrixFact,
    MatrixHealth, MatrixMetricDefinition, MatrixOntologyPack, MatrixQualityGateDecision,
    MatrixSourcePack, MatrixStore, MatrixStoreError, MfgActionExecution, MfgActionExecutionRequest,
    MfgActionFeedback, MfgCasePromotion, MfgCockpitProfile, MfgCockpitProjection,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportRequest, MfgCockpitReportSnapshot,
    MfgCrossPlaneBridgeReceipt, MfgDomainSeedResult, MfgIncident, MfgMemoryCase,
    MfgOperationalAnalysis, MfgPlaybook, MfgSkillRun,
};

/// Application-layer store facade for MFG.
///
/// The facade keeps gateway and app code from depending directly on MatrixStore
/// for manufacturing operations while the underlying SQLite schema is still
/// shared during migration.
#[derive(Debug)]
pub struct MfgStore {
    matrix: MatrixStore,
}

impl MfgStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MatrixStoreError> {
        Ok(Self {
            matrix: MatrixStore::open(path)?,
        })
    }

    pub fn seed_mfg_domain(&self) -> Result<MfgDomainSeedResult, MatrixStoreError> {
        self.matrix.seed_mfg_domain()
    }

    pub fn seed_mfg_ontology(&self) -> Result<MatrixOntologyPack, MatrixStoreError> {
        self.matrix.seed_mfg_ontology()
    }

    pub fn health(&self) -> Result<MatrixHealth, MatrixStoreError> {
        self.matrix.health()
    }

    pub fn list_attention(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixAttentionItem>, MatrixStoreError> {
        self.matrix.list_attention(limit)
    }

    pub fn list_changes(&self, limit: usize) -> Result<Vec<MatrixChangeEvent>, MatrixStoreError> {
        self.matrix.list_changes(limit)
    }

    pub fn list_source_packs(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixSourcePack>, MatrixStoreError> {
        self.matrix.list_source_packs(limit)
    }

    pub fn list_facts(&self, limit: usize) -> Result<Vec<MatrixFact>, MatrixStoreError> {
        self.matrix.list_facts(limit)
    }

    pub fn list_entities(&self, limit: usize) -> Result<Vec<MatrixEntity>, MatrixStoreError> {
        self.matrix.list_entities(limit)
    }

    pub fn list_metric_definitions(&self) -> Result<Vec<MatrixMetricDefinition>, MatrixStoreError> {
        self.matrix.list_metric_definitions()
    }

    pub fn list_evidence_packets(
        &self,
        limit: usize,
    ) -> Result<Vec<MatrixEvidencePacket>, MatrixStoreError> {
        self.matrix.list_evidence_packets(limit)
    }

    pub fn get_evidence_packet(
        &self,
        packet_id: &str,
    ) -> Result<Option<MatrixEvidencePacket>, MatrixStoreError> {
        self.matrix.get_evidence_packet(packet_id)
    }

    pub fn build_evidence_packet(
        &self,
        attention_id: Option<&str>,
        title: Option<&str>,
    ) -> Result<MatrixEvidencePacket, MatrixStoreError> {
        self.matrix.build_evidence_packet(attention_id, title)
    }

    pub fn evaluate_evidence_quality(
        &self,
        packet_id: &str,
    ) -> Result<MatrixQualityGateDecision, MatrixStoreError> {
        self.matrix.evaluate_evidence_quality(packet_id)
    }

    pub fn create_incident(&self, incident: &MfgIncident) -> Result<MfgIncident, MatrixStoreError> {
        self.matrix.create_incident(incident)
    }

    pub fn get_incident(&self, incident_id: &str) -> Result<Option<MfgIncident>, MatrixStoreError> {
        self.matrix.get_incident(incident_id)
    }

    pub fn list_incidents(&self, limit: usize) -> Result<Vec<MfgIncident>, MatrixStoreError> {
        self.matrix.list_incidents(limit)
    }

    pub fn analyze_incident(
        &self,
        incident_id: &str,
    ) -> Result<MfgOperationalAnalysis, MatrixStoreError> {
        self.matrix.analyze_incident(incident_id)
    }

    pub fn latest_analysis_for_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MatrixStoreError> {
        self.matrix.latest_analysis_for_incident(incident_id)
    }

    pub fn get_analysis(
        &self,
        analysis_id: &str,
    ) -> Result<Option<MfgOperationalAnalysis>, MatrixStoreError> {
        self.matrix.get_analysis(analysis_id)
    }

    pub fn execute_recommended_action(
        &self,
        analysis_id: &str,
        action_id: &str,
        request: &MfgActionExecutionRequest,
    ) -> Result<MfgActionExecution, MatrixStoreError> {
        self.matrix
            .execute_recommended_action(analysis_id, action_id, request)
    }

    pub fn get_execution(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgActionExecution>, MatrixStoreError> {
        self.matrix.get_execution(execution_id)
    }

    pub fn list_executions_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MatrixStoreError> {
        self.matrix.list_executions_for_incident(incident_id, limit)
    }

    pub fn list_recent_action_executions(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgActionExecution>, MatrixStoreError> {
        self.matrix.list_recent_action_executions(limit)
    }

    pub fn attach_cross_plane_receipt(
        &self,
        execution_id: &str,
        receipt: MfgCrossPlaneBridgeReceipt,
    ) -> Result<MfgActionExecution, MatrixStoreError> {
        self.matrix
            .attach_cross_plane_receipt(execution_id, receipt)
    }

    pub fn record_execution_feedback(
        &self,
        execution_id: &str,
        feedback: MfgActionFeedback,
    ) -> Result<MfgActionExecution, MatrixStoreError> {
        self.matrix
            .record_execution_feedback(execution_id, feedback)
    }

    pub fn record_skill_run(&self, run: &MfgSkillRun) -> Result<MfgSkillRun, MatrixStoreError> {
        self.matrix.record_skill_run(run)
    }

    pub fn get_skill_run(
        &self,
        execution_id: &str,
    ) -> Result<Option<MfgSkillRun>, MatrixStoreError> {
        self.matrix.get_skill_run(execution_id)
    }

    pub fn list_skill_runs_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MatrixStoreError> {
        self.matrix.list_skill_runs_for_incident(incident_id, limit)
    }

    pub fn list_recent_skill_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgSkillRun>, MatrixStoreError> {
        self.matrix.list_recent_skill_runs(limit)
    }

    pub fn promote_incident_to_memory_case(
        &self,
        incident_id: &str,
    ) -> Result<MfgCasePromotion, MatrixStoreError> {
        self.matrix.promote_incident_to_memory_case(incident_id)
    }

    pub fn get_memory_case(
        &self,
        case_id: &str,
    ) -> Result<Option<MfgMemoryCase>, MatrixStoreError> {
        self.matrix.get_memory_case(case_id)
    }

    pub fn search_memory_cases(
        &self,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgMemoryCase>, MatrixStoreError> {
        self.matrix.search_memory_cases(query, limit)
    }

    pub fn upsert_playbook(&self, playbook: &MfgPlaybook) -> Result<MfgPlaybook, MatrixStoreError> {
        self.matrix.upsert_playbook(playbook)
    }

    pub fn get_playbook(&self, playbook_id: &str) -> Result<Option<MfgPlaybook>, MatrixStoreError> {
        self.matrix.get_playbook(playbook_id)
    }

    pub fn recommend_playbooks_for_incident(
        &self,
        incident_id: &str,
        limit: usize,
    ) -> Result<Vec<MfgPlaybook>, MatrixStoreError> {
        self.matrix
            .recommend_playbooks_for_incident(incident_id, limit)
    }

    pub fn upsert_cockpit_profile(
        &self,
        profile: &MfgCockpitProfile,
    ) -> Result<MfgCockpitProfile, MatrixStoreError> {
        self.matrix.upsert_cockpit_profile(profile)
    }

    pub fn get_cockpit_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<MfgCockpitProfile>, MatrixStoreError> {
        self.matrix.get_cockpit_profile(profile_id)
    }

    pub fn list_cockpit_profiles(
        &self,
        cadence: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgCockpitProfile>, MatrixStoreError> {
        self.matrix.list_cockpit_profiles(cadence, limit)
    }

    pub fn cockpit_projection(
        &self,
        profile_id: &str,
    ) -> Result<MfgCockpitProjection, MatrixStoreError> {
        self.matrix.cockpit_projection(profile_id)
    }

    pub fn generate_cockpit_report(
        &self,
        profile_id: &str,
        request: MfgCockpitReportRequest,
    ) -> Result<MfgCockpitReportSnapshot, MatrixStoreError> {
        self.matrix.generate_cockpit_report(profile_id, request)
    }

    pub fn get_cockpit_report(
        &self,
        report_id: &str,
    ) -> Result<Option<MfgCockpitReportSnapshot>, MatrixStoreError> {
        self.matrix.get_cockpit_report(report_id)
    }

    pub fn attach_cockpit_report_delivery(
        &self,
        report_id: &str,
        receipt: MfgCockpitReportDeliveryReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MatrixStoreError> {
        self.matrix
            .attach_cockpit_report_delivery(report_id, receipt)
    }
}
