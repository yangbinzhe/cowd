//! Delegated Agent session and task-contract preparation stages.

use super::*;

pub(super) fn delegated_child_session(
    session_id: &str,
    model: &str,
    workspace_root: &std::path::Path,
) -> Session {
    let mut session = Session::new().with_workspace_root(workspace_root);
    session.session_id = session_id.to_string();
    session.model = Some(model.to_string());
    session
}

pub(super) fn packet_focus_novelty_target_bp(packet: &AgentTaskPacket) -> u16 {
    packet
        .team_role_assignment()
        .map(|assignment| assignment.identity.novelty_target_bp)
        .unwrap_or(0)
        .min(10_000)
}

pub(super) fn packet_focus_acceptance_scopes(packet: &AgentTaskPacket) -> Vec<String> {
    let mut scopes = packet
        .required_acceptance
        .evidence_obligations
        .iter()
        .map(crate::path_identity::obligation_scope_key)
        .collect::<Vec<_>>();
    scopes.sort();
    scopes.dedup();
    scopes
}

pub(super) fn packet_required_output_fields(packet: &AgentTaskPacket) -> Vec<String> {
    let mut fields = packet_acceptance_contract(packet)
        .into_iter()
        .filter_map(|requirement| match requirement.check {
            harness_contract::team::TeamAcceptanceCheck::StructuredField { field }
            | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, .. } => {
                Some(field.as_str().to_string())
            }
            harness_contract::team::TeamAcceptanceCheck::StructuredArtifact { name } => Some(name),
            harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. } => Some(
                harness_contract::team::TeamStructuredOutputField::SourceVerification
                    .as_str()
                    .to_string(),
            ),
            harness_contract::team::TeamAcceptanceCheck::UpstreamReview => Some(
                harness_contract::team::TeamStructuredOutputField::Review
                    .as_str()
                    .to_string(),
            ),
            harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. }
            | harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence => None,
        })
        .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
}
