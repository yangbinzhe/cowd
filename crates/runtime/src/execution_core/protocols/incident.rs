use super::{
    validate_protocol_request, OutputSpec, ProtocolAvailability, ProtocolCompileError,
    ProtocolCompileRequest, ProtocolGraphBuilder, ProtocolId, ProtocolSpec, RepairPolicy,
    RepairTrigger, RoleDependencySpec, RoleEvidenceMode, RoleSpec, StopPolicy,
};
use harness_contract::execution_graph::ExecutionGraph;

#[derive(Debug, Default, Clone, Copy)]
pub struct IncidentProtocolCompiler;

impl IncidentProtocolCompiler {
    pub fn compile(
        &self,
        request: &ProtocolCompileRequest,
    ) -> Result<ExecutionGraph, ProtocolCompileError> {
        compile(request)
    }
}

pub fn compile(request: &ProtocolCompileRequest) -> Result<ExecutionGraph, ProtocolCompileError> {
    let spec = spec();
    validate_protocol_request(&spec, request)?;
    let mut builder = ProtocolGraphBuilder::new(&spec, request);

    let triage = builder.add_agent("triage", 0, &[])?;
    let evidence_logs = builder.add_agent("evidence_logs", 0, std::slice::from_ref(&triage))?;
    let evidence_code = builder.add_agent("evidence_code", 0, std::slice::from_ref(&triage))?;
    let evidence_state = builder.add_agent("evidence_state", 0, std::slice::from_ref(&triage))?;
    let evidence = [evidence_logs, evidence_code, evidence_state];
    let hypothesis_inputs = std::iter::once(triage)
        .chain(evidence.iter().cloned())
        .collect::<Vec<_>>();
    let hypotheses = builder.add_agent("hypotheses", 0, &hypothesis_inputs)?;
    let mitigation = builder.add_agent("mitigation", 0, std::slice::from_ref(&hypotheses))?;
    let review_inputs = vec![hypotheses.clone(), mitigation.clone()];
    let review = builder.add_agent("review", 0, &review_inputs)?;
    let report_inputs = vec![mitigation, review];
    let report = builder.add_agent("report", 0, &report_inputs)?;
    builder.add_terminal_chain(&[report]);
    builder.finish()
}

pub(crate) fn spec() -> ProtocolSpec {
    ProtocolSpec {
        id: ProtocolId::Incident,
        version: 1,
        summary: "Incident triage with parallel evidence, hypotheses, bounded mitigation planning, review, and an honest report.".to_string(),
        availability: ProtocolAvailability::Available,
        roles: vec![
            RoleSpec::agent(
                "triage",
                "Establish severity, scope, and initial hypotheses without claiming unobserved facts.",
                1,
                1,
                OutputSpec::structured(&["severity", "scope", "initial_hypotheses"], true),
            )
            .with_evidence_mode(RoleEvidenceMode::ObjectiveOnly),
            RoleSpec::agent(
                "evidence_logs",
                "Collect and assess relevant log evidence, including collection failures.",
                1,
                1,
                OutputSpec::evidence_backed(&["log_evidence", "gaps", "collection_status"], true),
            )
            .with_evidence_mode(RoleEvidenceMode::Acquire),
            RoleSpec::agent(
                "evidence_code",
                "Inspect code-path evidence and identify uncertainty or unavailable sources.",
                1,
                1,
                OutputSpec::evidence_backed(&["code_evidence", "gaps", "collection_status"], true),
            )
            .with_evidence_mode(RoleEvidenceMode::Acquire),
            RoleSpec::agent(
                "evidence_state",
                "Inspect service and runtime state evidence and record unavailable observations.",
                1,
                1,
                OutputSpec::evidence_backed(&["state_evidence", "gaps", "collection_status"], true),
            )
            .with_evidence_mode(RoleEvidenceMode::Acquire),
            RoleSpec::agent(
                "hypotheses",
                "Rank hypotheses by evidence and state disconfirming observations.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["ranked_hypotheses", "supporting_evidence", "uncertainty"],
                    true,
                ),
            )
            .with_evidence_mode(RoleEvidenceMode::UpstreamOnly),
            RoleSpec::agent(
                "mitigation",
                "Produce a bounded mitigation plan with action gates and rollback criteria.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["mitigation_plan", "action_gates", "rollback_criteria"],
                    true,
                ),
            )
            .with_evidence_mode(RoleEvidenceMode::UpstreamOnly),
            RoleSpec::agent(
                "review",
                "Review the mitigation plan for safety, evidence sufficiency, and unaddressed impact.",
                1,
                1,
                OutputSpec::evidence_backed(&["review_findings", "safety_risks", "gaps"], true),
            )
            .with_evidence_mode(RoleEvidenceMode::UpstreamOnly),
            RoleSpec::agent(
                "report",
                "Produce an honest incident report, retaining partial or unresolved findings.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["incident_report", "confirmed_facts", "unresolved", "next_actions"],
                    true,
                ),
            )
            .with_evidence_mode(RoleEvidenceMode::UpstreamOnly),
        ],
        dependencies: vec![
            RoleDependencySpec::all("evidence_logs", "triage"),
            RoleDependencySpec::all("evidence_code", "triage"),
            RoleDependencySpec::all("evidence_state", "triage"),
            RoleDependencySpec::all("hypotheses", "triage"),
            RoleDependencySpec::all("hypotheses", "evidence_logs"),
            RoleDependencySpec::all("hypotheses", "evidence_code"),
            RoleDependencySpec::all("hypotheses", "evidence_state"),
            RoleDependencySpec::all("mitigation", "hypotheses"),
            RoleDependencySpec::all("review", "hypotheses"),
            RoleDependencySpec::all("review", "mitigation"),
            RoleDependencySpec::all("report", "mitigation"),
            RoleDependencySpec::all("report", "review"),
        ],
        verify_after_roles: vec!["report".to_string()],
        output: OutputSpec::evidence_backed(
            &["incident_report", "confirmed_facts", "unresolved", "next_actions"],
            true,
        ),
        stop_policy: StopPolicy {
            max_agent_attempts: 1,
            stop_on_verification_failure: true,
            allows_unresolved: true,
        },
        repair_policy: RepairPolicy {
            max_revisions: 1,
            repair_role: Some("mitigation".to_string()),
            triggers: vec![RepairTrigger::MissingEvidence, RepairTrigger::VerificationFailure],
        },
    }
}
