use std::path::Path;

use crate::repository::{MfgHealth, MfgMetricRecomputeResult, MfgRepository, MfgRepositoryError};
use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixEntity, MatrixEvidencePacket, MatrixFact,
    MatrixMetricDefinition, MatrixOntologyPack, MatrixQualityGateDecision, MatrixSourcePack,
};

use crate::{
    MfgActionExecution, MfgActionExecutionRequest, MfgActionFeedback, MfgAlertCommandInput,
    MfgAlertOccurrence, MfgAlertRule, MfgAlertSubscription, MfgAssignment,
    MfgAssignmentCommandInput, MfgCasePromotion, MfgCockpitProfile, MfgCockpitProjection,
    MfgCockpitReportDeliveryReceipt, MfgCockpitReportRequest, MfgCockpitReportSnapshot,
    MfgCockpitWidgetProjection, MfgCommandReceipt, MfgCrossPlaneBridgeReceipt, MfgDomainSeedResult,
    MfgForecastProjection, MfgIncident, MfgLiveProjection, MfgMemoryCase, MfgOperationalAnalysis,
    MfgPlaybook, MfgSkillRun, MfgWorkflowGraph,
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

    pub fn in_memory() -> Result<Self, MfgRepositoryError> {
        Ok(Self {
            repository: MfgRepository::in_memory()?,
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

    pub fn upsert_evidence_packet(
        &self,
        packet: &MatrixEvidencePacket,
    ) -> Result<MatrixEvidencePacket, MfgRepositoryError> {
        self.repository.upsert_evidence_packet(packet)
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
        expected_revision: Option<u64>,
    ) -> Result<MfgPlaybook, MfgRepositoryError> {
        self.repository.upsert_playbook(playbook, expected_revision)
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
        expected_revision: Option<u64>,
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        self.repository
            .upsert_cockpit_profile(profile, expected_revision)
    }

    pub fn upsert_cockpit_profile_receipted(
        &self,
        profile: &MfgCockpitProfile,
        expected_revision: Option<u64>,
        command: &str,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgCockpitProfile, MfgCommandReceipt), MfgRepositoryError> {
        self.repository.upsert_cockpit_profile_receipted(
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

    pub fn cockpit_projection_with_filters(
        &self,
        profile_id: &str,
        filters: serde_json::Value,
    ) -> Result<MfgCockpitProjection, MfgRepositoryError> {
        self.repository
            .cockpit_projection_with_filters(profile_id, filters)
    }

    pub fn cockpit_widget_projection(
        &self,
        profile_id: &str,
        instance_id: &str,
    ) -> Result<MfgCockpitWidgetProjection, MfgRepositoryError> {
        self.repository
            .cockpit_widget_projection(profile_id, instance_id)
    }

    pub fn cockpit_widget_projection_with_filters(
        &self,
        profile_id: &str,
        instance_id: &str,
        filters: serde_json::Value,
    ) -> Result<MfgCockpitWidgetProjection, MfgRepositoryError> {
        self.repository
            .cockpit_widget_projection_with_filters(profile_id, instance_id, filters)
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

    pub fn list_cockpit_reports(
        &self,
        profile_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgCockpitReportSnapshot>, MfgRepositoryError> {
        self.repository.list_cockpit_reports(profile_id, limit)
    }

    pub fn attach_cockpit_report_delivery(
        &self,
        report_id: &str,
        receipt: MfgCockpitReportDeliveryReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.repository
            .attach_cockpit_report_delivery(report_id, receipt)
    }

    pub fn delete_cockpit_profile(
        &self,
        profile_id: &str,
        expected_revision: u64,
    ) -> Result<MfgCockpitProfile, MfgRepositoryError> {
        self.repository
            .delete_cockpit_profile(profile_id, expected_revision)
    }

    pub fn delete_cockpit_profile_receipted(
        &self,
        profile_id: &str,
        expected_revision: u64,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(Option<MfgCockpitProfile>, MfgCommandReceipt), MfgRepositoryError> {
        self.repository.delete_cockpit_profile_receipted(
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
        self.repository.upsert_alert_rule(rule, expected_revision)
    }

    pub fn upsert_alert_rule_receipted(
        &self,
        rule: &MfgAlertRule,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAlertRule, MfgCommandReceipt), MfgRepositoryError> {
        self.repository.upsert_alert_rule_receipted(
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
        self.repository.list_alert_rules(owner_ref, limit)
    }

    pub fn list_alert_occurrences(
        &self,
        status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAlertOccurrence>, MfgRepositoryError> {
        self.repository.list_alert_occurrences(status, limit)
    }

    pub fn upsert_alert_subscription(
        &self,
        subscription: &MfgAlertSubscription,
        expected_revision: Option<u64>,
    ) -> Result<MfgAlertSubscription, MfgRepositoryError> {
        self.repository
            .upsert_alert_subscription(subscription, expected_revision)
    }

    pub fn upsert_alert_subscription_receipted(
        &self,
        subscription: &MfgAlertSubscription,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAlertSubscription, MfgCommandReceipt), MfgRepositoryError> {
        self.repository.upsert_alert_subscription_receipted(
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
        self.repository
            .list_alert_subscriptions(subscriber_ref, limit)
    }

    pub fn command_alert(
        &self,
        occurrence_id: &str,
        command: MfgAlertCommandInput,
    ) -> Result<(MfgAlertOccurrence, MfgCommandReceipt), MfgRepositoryError> {
        self.repository.command_alert(occurrence_id, command)
    }

    pub fn forecasts(
        &self,
        metric_refs: &[String],
        horizon: &str,
        limit: usize,
    ) -> Result<Vec<MfgForecastProjection>, MfgRepositoryError> {
        self.repository.forecasts(metric_refs, horizon, limit)
    }

    pub fn upsert_assignment(
        &self,
        assignment: &MfgAssignment,
        expected_revision: Option<u64>,
    ) -> Result<MfgAssignment, MfgRepositoryError> {
        self.repository
            .upsert_assignment(assignment, expected_revision)
    }

    pub fn upsert_assignment_receipted(
        &self,
        assignment: &MfgAssignment,
        expected_revision: Option<u64>,
        actor_ref: &str,
        idempotency_key: &str,
    ) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
        self.repository.upsert_assignment_receipted(
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
        self.repository.get_assignment(assignment_id)
    }

    pub fn list_assignments(
        &self,
        assignee_ref: Option<&str>,
        incident_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MfgAssignment>, MfgRepositoryError> {
        self.repository
            .list_assignments(assignee_ref, incident_id, limit)
    }

    pub fn command_assignment(
        &self,
        assignment_id: &str,
        command: MfgAssignmentCommandInput,
    ) -> Result<(MfgAssignment, MfgCommandReceipt), MfgRepositoryError> {
        self.repository.command_assignment(assignment_id, command)
    }

    pub fn live_projection(
        &self,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<MfgLiveProjection, MfgRepositoryError> {
        self.repository.live_projection(cursor, limit)
    }

    pub fn record_command_notifications(
        &self,
        idempotency_key: &str,
        notification_refs: Vec<String>,
    ) -> Result<MfgCommandReceipt, MfgRepositoryError> {
        self.repository
            .record_command_notifications(idempotency_key, notification_refs)
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
        self.repository.find_mutation_receipt(
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
        self.repository.record_mutation_receipt(
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

    pub fn save_workflow_graph(
        &self,
        graph: &MfgWorkflowGraph,
        expected_revision: Option<u64>,
    ) -> Result<MfgWorkflowGraph, MfgRepositoryError> {
        self.repository
            .save_workflow_graph(graph, expected_revision)
    }

    pub fn create_incident_workflow(
        &self,
        incident: &MfgIncident,
        packet: &MatrixEvidencePacket,
    ) -> Result<(MfgIncident, MfgWorkflowGraph), MfgRepositoryError> {
        self.repository.create_incident_workflow(incident, packet)
    }

    pub fn plan_incident_workflow_skills(
        &self,
        incident_id: &str,
        plan: &crate::MfgSkillPlan,
    ) -> Result<MfgWorkflowGraph, MfgRepositoryError> {
        self.repository
            .plan_incident_workflow_skills(incident_id, plan)
    }

    pub fn complete_incident_workflow_skill(
        &self,
        incident_id: &str,
        run: &MfgSkillRun,
    ) -> Result<MfgWorkflowGraph, MfgRepositoryError> {
        self.repository
            .complete_incident_workflow_skill(incident_id, run)
    }

    pub fn record_skill_run_and_complete_workflow(
        &self,
        run: &MfgSkillRun,
    ) -> Result<(MfgSkillRun, MfgWorkflowGraph), MfgRepositoryError> {
        self.repository.record_skill_run_and_complete_workflow(run)
    }

    pub fn get_workflow_graph(
        &self,
        workflow_id: &str,
    ) -> Result<Option<MfgWorkflowGraph>, MfgRepositoryError> {
        self.repository.get_workflow_graph(workflow_id)
    }

    pub fn workflow_graph_for_incident(
        &self,
        incident_id: &str,
    ) -> Result<Option<MfgWorkflowGraph>, MfgRepositoryError> {
        self.repository.workflow_graph_for_incident(incident_id)
    }

    pub fn workflow_graph_for_task(
        &self,
        task_id: &str,
    ) -> Result<Option<MfgWorkflowGraph>, MfgRepositoryError> {
        self.repository.workflow_graph_for_task(task_id)
    }

    pub fn list_workflow_graphs(
        &self,
        limit: usize,
    ) -> Result<Vec<MfgWorkflowGraph>, MfgRepositoryError> {
        self.repository.list_workflow_graphs(limit)
    }
}

#[cfg(test)]
mod workflow_tests {
    use super::*;
    use crate::{run_server_manufacturing_skill, server_manufacturing_skill_pack, MfgSkillPlan};

    #[test]
    fn workflow_store_isolates_incidents_and_task_lookup() {
        let store = MfgStore::in_memory().unwrap();
        let mut first_incident = MfgIncident::new("GPU shortage");
        first_incident.task_id = Some("task-gpu".to_string());
        let mut second_incident = MfgIncident::new("DIMM quality");
        second_incident.task_id = Some("task-dimm".to_string());
        let first = MfgWorkflowGraph::for_incident(&first_incident).unwrap();
        let second = MfgWorkflowGraph::for_incident(&second_incident).unwrap();

        store.save_workflow_graph(&first, None).unwrap();
        store.save_workflow_graph(&second, None).unwrap();

        assert_eq!(
            store
                .workflow_graph_for_task("task-gpu")
                .unwrap()
                .unwrap()
                .incident_id,
            first_incident.incident_id
        );
        assert_eq!(
            store
                .workflow_graph_for_incident(&second_incident.incident_id)
                .unwrap()
                .unwrap()
                .workflow_id,
            second.workflow_id
        );
        assert_eq!(store.list_workflow_graphs(10).unwrap().len(), 2);
    }

    #[test]
    fn workflow_store_rejects_stale_writer() {
        let store = MfgStore::in_memory().unwrap();
        let incident = MfgIncident::new("Supplier recovery");
        let graph = MfgWorkflowGraph::for_incident(&incident).unwrap();
        store.save_workflow_graph(&graph, None).unwrap();

        let mut current = store
            .get_workflow_graph(&graph.workflow_id)
            .unwrap()
            .unwrap();
        let stale = current.clone();
        let expected = current.revision;
        current
            .add_evidence("planner", "decision", "mfg:decision:1", "expedite")
            .unwrap();
        store.save_workflow_graph(&current, Some(expected)).unwrap();

        let error = store
            .save_workflow_graph(&stale, Some(expected))
            .unwrap_err();
        assert!(matches!(
            error,
            MfgRepositoryError::WorkflowRevisionConflict { .. }
        ));
    }

    #[test]
    fn workflow_store_owns_the_incident_skill_lifecycle() {
        let store = MfgStore::in_memory().unwrap();
        let incident = MfgIncident::new("GPU supply risk");
        let packet = MatrixEvidencePacket::new("GPU supply risk affects weekly build");
        let (incident, graph) = store.create_incident_workflow(&incident, &packet).unwrap();
        assert_eq!(
            incident.workflow_graph_id.as_deref(),
            Some(graph.workflow_id.as_str())
        );
        assert!(store.get_incident(&incident.incident_id).unwrap().is_some());

        let skill = server_manufacturing_skill_pack().remove(0);
        let plan = MfgSkillPlan {
            incident_id: incident.incident_id.clone(),
            selected_skills: vec![skill.clone()],
            evidence_requirements: skill.required_evidence.clone(),
            planned_agent_nodes: vec![crate::skill_agent_node_id(&skill.skill_id)],
        };
        let mut planned = store
            .plan_incident_workflow_skills(&incident.incident_id, &plan)
            .unwrap();
        let expected_revision = planned.revision;
        planned
            .set_node_terminal_result("mfg_researcher", "researched")
            .unwrap();
        planned
            .set_node_terminal_result("mfg_reviewer", "reviewed")
            .unwrap();
        let planned = store
            .save_workflow_graph(&planned, Some(expected_revision))
            .unwrap();
        let run = run_server_manufacturing_skill(&incident, &skill, None, Some(&packet));
        let (recorded_run, completed) = store.record_skill_run_and_complete_workflow(&run).unwrap();

        assert!(completed.revision > planned.revision);
        assert_eq!(completed.incident_id, incident.incident_id);
        assert_eq!(recorded_run.execution_id, run.execution_id);
        assert!(store
            .get_skill_run(run.execution_id.as_deref().unwrap())
            .unwrap()
            .is_some());
        assert!(completed
            .evidence
            .iter()
            .any(|item| item.kind == "mfg_skill_run"));
    }
}
