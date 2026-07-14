use super::{
    validate_protocol_request, OutputSpec, ProtocolAvailability, ProtocolCompileError,
    ProtocolCompileRequest, ProtocolGraphBuilder, ProtocolId, ProtocolSpec, RepairPolicy,
    RoleDependencySpec, RoleEvidenceMode, RoleSpec, StopPolicy,
};
use harness_contract::execution_graph::ExecutionGraph;

#[derive(Debug, Default, Clone, Copy)]
pub struct JpsProtocolCompiler;

impl JpsProtocolCompiler {
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

    let solutions = (0..request.fanout)
        .map(|slot| builder.add_agent("solution", slot, &[]))
        .collect::<Result<Vec<_>, _>>()?;
    let synthesis = builder.add_agent("decision_synthesis", 0, &solutions)?;
    builder.add_terminal_chain(&[synthesis]);
    builder.finish()
}

pub(crate) fn spec() -> ProtocolSpec {
    ProtocolSpec {
        id: ProtocolId::Jps,
        version: 1,
        summary: "Parallel evidence lanes with one bounded decision synthesis that reconciles tradeoffs, contradictions, and unresolved evidence.".to_string(),
        availability: ProtocolAvailability::Available,
        roles: vec![
            RoleSpec::agent(
                "solution",
                "Independently acquire the minimum concrete evidence for one solution lane, then return a solution, tradeoffs, risks, and source-backed evidence. Do not wait for a framing role; the objective is the shared frame.",
                2,
                4,
                OutputSpec::evidence_backed(&["solution", "tradeoffs", "evidence"], true),
            )
            .with_evidence_mode(RoleEvidenceMode::Acquire),
            RoleSpec::agent(
                "decision_synthesis",
                "Synthesize the independent evidence lanes into one decision. Reconcile contradictions, assess evidence confidence, and preserve unsupported claims as explicit unresolved items instead of re-reading the workspace.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["decision", "rationale", "conflict_resolution", "unresolved"],
                    true,
                ),
            )
            .with_evidence_mode(RoleEvidenceMode::UpstreamOnly),
        ],
        dependencies: vec![
            RoleDependencySpec::all("decision_synthesis", "solution"),
        ],
        verify_after_roles: vec!["decision_synthesis".to_string()],
        output: OutputSpec::evidence_backed(&["decision", "rationale", "unresolved"], true),
        stop_policy: StopPolicy {
            max_agent_attempts: 1,
            stop_on_verification_failure: true,
            allows_unresolved: true,
        },
        repair_policy: RepairPolicy {
            max_revisions: 0,
            repair_role: None,
            triggers: Vec::new(),
        },
    }
}
