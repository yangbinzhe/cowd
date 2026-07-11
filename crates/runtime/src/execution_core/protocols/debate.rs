use super::{
    validate_protocol_request, OutputSpec, ProtocolAvailability, ProtocolCompileError,
    ProtocolCompileRequest, ProtocolGraphBuilder, ProtocolId, ProtocolSpec, RepairPolicy,
    RepairTrigger, RoleDependencySpec, RoleSpec, StopPolicy,
};
use harness_contract::execution_graph::ExecutionGraph;

#[derive(Debug, Default, Clone, Copy)]
pub struct DebateProtocolCompiler;

impl DebateProtocolCompiler {
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

    let proposers = (0..request.fanout)
        .map(|slot| builder.add_agent("proposer", slot, &[]))
        .collect::<Result<Vec<_>, _>>()?;
    let critics = (0..request.fanout)
        .map(|slot| {
            let cross_proposals = proposers
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != slot)
                .map(|(_, node)| node.clone())
                .collect::<Vec<_>>();
            builder.add_agent("critic", slot, &cross_proposals)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let evidence_inputs = proposers
        .iter()
        .chain(&critics)
        .cloned()
        .collect::<Vec<_>>();
    let evidence_gap = builder.add_agent("evidence_gap", 0, &evidence_inputs)?;
    let arbiter_inputs = evidence_inputs
        .iter()
        .cloned()
        .chain(std::iter::once(evidence_gap.clone()))
        .collect::<Vec<_>>();
    let arbiter = builder.add_agent("arbiter", 0, &arbiter_inputs)?;
    let verification_inputs = if request.enable_repair {
        // The protocol exposes a single, explicit repair branch. It is never
        // used as a hidden retry loop.
        let repair = builder.add_agent("repair", 0, &[arbiter.clone()])?;
        vec![arbiter, repair]
    } else {
        vec![arbiter]
    };
    builder.add_terminal_chain(&verification_inputs);
    builder.finish()
}

pub(crate) fn spec() -> ProtocolSpec {
    ProtocolSpec {
        id: ProtocolId::Debate,
        version: 1,
        summary: "Evidence-arbitrated debate with one bounded repair revision.".to_string(),
        availability: ProtocolAvailability::Available,
        roles: vec![
            RoleSpec::agent(
                "proposer",
                "Produce an independent structured proposal without hidden peer reasoning.",
                2,
                4,
                OutputSpec::evidence_backed(&["proposal", "constraints", "risks"], true),
            ),
            RoleSpec::agent(
                "critic",
                "Cross-review another proposal for counterexamples, missing evidence, and risk.",
                2,
                4,
                OutputSpec::evidence_backed(
                    &["counterexamples", "missing_evidence", "risks"],
                    true,
                ),
            ),
            RoleSpec::agent(
                "evidence_gap",
                "Identify unresolved evidence gaps and request the bounded evidence needed to decide.",
                1,
                1,
                OutputSpec::evidence_backed(&["evidence_gaps", "evidence_requests", "confidence"], true),
            ),
            RoleSpec::agent(
                "arbiter",
                "Select, combine, or leave unresolved based on evidence quality, constraints, counterexamples, and risk rather than votes.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &[
                        "decision",
                        "evidence_arbitration",
                        "constraint_assessment",
                        "unresolved",
                    ],
                    true,
                ),
            ),
            RoleSpec::agent(
                "repair",
                "Perform at most one evidence- or constraint-driven revision, or explicitly report that no revision is needed.",
                1,
                1,
                OutputSpec::evidence_backed(
                    &["revision", "repair_status", "remaining_unresolved"],
                    true,
                ),
            ),
        ],
        dependencies: vec![
            RoleDependencySpec::cross_fanout("critic", "proposer"),
            RoleDependencySpec::all("evidence_gap", "proposer"),
            RoleDependencySpec::all("evidence_gap", "critic"),
            RoleDependencySpec::all("arbiter", "proposer"),
            RoleDependencySpec::all("arbiter", "critic"),
            RoleDependencySpec::all("arbiter", "evidence_gap"),
            RoleDependencySpec::all("repair", "arbiter"),
        ],
        verify_after_roles: vec!["arbiter".to_string()],
        output: OutputSpec::evidence_backed(
            &["decision", "evidence_arbitration", "unresolved"],
            true,
        ),
        stop_policy: StopPolicy {
            max_agent_attempts: 1,
            stop_on_verification_failure: true,
            allows_unresolved: true,
        },
        repair_policy: RepairPolicy {
            max_revisions: 1,
            repair_role: Some("repair".to_string()),
            triggers: vec![
                RepairTrigger::MissingEvidence,
                RepairTrigger::ConstraintConflict,
                RepairTrigger::VerificationFailure,
            ],
        },
    }
}
