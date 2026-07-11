use super::{
    validate_protocol_request, OutputSpec, ProtocolAvailability, ProtocolCompileError,
    ProtocolCompileRequest, ProtocolGraphBuilder, ProtocolId, ProtocolSpec, RepairPolicy,
    RoleDependencySpec, RoleSpec, StopPolicy,
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

    let frame = builder.add_agent("frame", 0, &[])?;
    let solutions = (0..request.fanout)
        .map(|slot| builder.add_agent("solution", slot, std::slice::from_ref(&frame)))
        .collect::<Result<Vec<_>, _>>()?;
    let evaluation = builder.add_agent("evaluation", 0, &solutions)?;
    let conflict_inputs = solutions
        .iter()
        .cloned()
        .chain(std::iter::once(evaluation.clone()))
        .collect::<Vec<_>>();
    let conflict_matrix = builder.add_agent("conflict_matrix", 0, &conflict_inputs)?;
    let synthesis_inputs = vec![evaluation, conflict_matrix];
    let synthesis = builder.add_agent("decision_synthesis", 0, &synthesis_inputs)?;
    builder.add_terminal_chain(&[synthesis]);
    builder.finish()
}

pub(crate) fn spec() -> ProtocolSpec {
    ProtocolSpec {
        id: ProtocolId::Jps,
        version: 1,
        summary: "Joint problem solving through a shared frame, independent solutions, evidence scoring, and conflict synthesis.".to_string(),
        availability: ProtocolAvailability::Available,
        roles: vec![
            RoleSpec::agent(
                "frame",
                "Frame the problem, constraints, and unknowns before solutions are proposed.",
                1,
                1,
                OutputSpec::structured(&["problem_frame", "constraints", "unknowns"], true),
            ),
            RoleSpec::agent(
                "solution",
                "Produce an independent solution with tradeoffs and supporting evidence.",
                2,
                4,
                OutputSpec::evidence_backed(&["solution", "tradeoffs", "evidence"], true),
            ),
            RoleSpec::agent(
                "evaluation",
                "Evaluate solution evidence quality, confidence, and remaining gaps.",
                1,
                1,
                OutputSpec::evidence_backed(&["evidence_scorecard", "confidence", "gaps"], true),
            ),
            RoleSpec::agent(
                "conflict_matrix",
                "Produce a typed conflict matrix and explicit resolution options.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["conflict_matrix", "disputed_assumptions", "resolution_options"],
                    true,
                ),
            ),
            RoleSpec::agent(
                "decision_synthesis",
                "Synthesize a constrained decision while preserving unresolved conflicts.",
                1,
                1,
                OutputSpec::evidence_backed(&["decision", "rationale", "unresolved"], true),
            ),
        ],
        dependencies: vec![
            RoleDependencySpec::all("solution", "frame"),
            RoleDependencySpec::all("evaluation", "solution"),
            RoleDependencySpec::all("conflict_matrix", "solution"),
            RoleDependencySpec::all("conflict_matrix", "evaluation"),
            RoleDependencySpec::all("decision_synthesis", "evaluation"),
            RoleDependencySpec::all("decision_synthesis", "conflict_matrix"),
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
