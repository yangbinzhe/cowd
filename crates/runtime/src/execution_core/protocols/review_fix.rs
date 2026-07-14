use super::{
    validate_protocol_request, OutputSpec, ProtocolAvailability, ProtocolCompileError,
    ProtocolCompileRequest, ProtocolGraphBuilder, ProtocolId, ProtocolSpec, RepairPolicy,
    RepairTrigger, RoleDependencySpec, RoleEvidenceMode, RoleSpec, StopPolicy,
};
use harness_contract::execution_graph::ExecutionGraph;

#[derive(Debug, Default, Clone, Copy)]
pub struct ReviewFixProtocolCompiler;

impl ReviewFixProtocolCompiler {
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

    let implementation = builder.add_agent("implement", 0, &[])?;
    let reviewers = (0..request.fanout)
        .map(|slot| builder.add_agent("review", slot, std::slice::from_ref(&implementation)))
        .collect::<Result<Vec<_>, _>>()?;
    let fix_inputs = std::iter::once(implementation)
        .chain(reviewers)
        .collect::<Vec<_>>();
    let fix = builder.add_agent("fix", 0, &fix_inputs)?;
    builder.add_terminal_chain(&[fix]);
    builder.finish()
}

pub(crate) fn spec() -> ProtocolSpec {
    ProtocolSpec {
        id: ProtocolId::ReviewFix,
        version: 1,
        summary: "Bounded implementation, independent review, one repair, verification, and final synthesis.".to_string(),
        availability: ProtocolAvailability::Available,
        roles: vec![
            RoleSpec::agent(
                "implement",
                "Implement the bounded change and map the result to acceptance criteria.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["change_summary", "change_evidence", "acceptance_mapping"],
                    false,
                ),
            )
            .with_evidence_mode(RoleEvidenceMode::Acquire),
            RoleSpec::agent(
                "review",
                "Independently review the implementation for defects, evidence gaps, and regressions.",
                2,
                4,
                OutputSpec::evidence_backed(&["findings", "evidence", "regression_risk"], true),
            )
            .with_evidence_mode(RoleEvidenceMode::UpstreamOnly),
            RoleSpec::agent(
                "fix",
                "Apply at most one bounded remediation and state what still requires verification.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["remediation", "verification_plan", "remaining_risk"],
                    true,
                ),
            )
            .with_evidence_mode(RoleEvidenceMode::Acquire),
        ],
        dependencies: vec![
            RoleDependencySpec::all("review", "implement"),
            RoleDependencySpec::all("fix", "implement"),
            RoleDependencySpec::all("fix", "review"),
        ],
        verify_after_roles: vec!["fix".to_string()],
        output: OutputSpec::evidence_backed(
            &["remediation", "verification_plan", "remaining_risk"],
            true,
        ),
        stop_policy: StopPolicy {
            max_agent_attempts: 1,
            stop_on_verification_failure: true,
            allows_unresolved: true,
        },
        repair_policy: RepairPolicy {
            max_revisions: 1,
            repair_role: Some("fix".to_string()),
            triggers: vec![RepairTrigger::VerificationFailure, RepairTrigger::ConstraintConflict],
        },
    }
}
