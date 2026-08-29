//! Agent terminal classification and verified narrative normalization.

use super::*;

pub(super) fn agent_terminal_outcome(
    completion: harness_contract::goal::GoalCompletion,
    terminal_answer: &str,
) -> (AgentTerminalStatus, Option<String>) {
    match completion {
        harness_contract::goal::GoalCompletion::Satisfied => (AgentTerminalStatus::Completed, None),
        harness_contract::goal::GoalCompletion::Partial => (
            AgentTerminalStatus::Blocked,
            Some(terminal_answer.to_string()),
        ),
        harness_contract::goal::GoalCompletion::WaitingExternalDecision => (
            AgentTerminalStatus::Blocked,
            Some(terminal_answer.to_string()),
        ),
        harness_contract::goal::GoalCompletion::Cancelled => (
            AgentTerminalStatus::Cancelled,
            Some(terminal_answer.to_string()),
        ),
        harness_contract::goal::GoalCompletion::Open => (
            AgentTerminalStatus::Failed,
            Some("child turn returned an open goal as a terminal result".to_string()),
        ),
    }
}

pub(super) fn needs_managed_escalation_recovery(
    requires_escalation: bool,
    has_successful_escalation: bool,
    has_source_evidence: bool,
) -> bool {
    requires_escalation && !has_successful_escalation && has_source_evidence
}

pub(super) fn managed_escalation_recovery_input(packet: &AgentTaskPacket) -> String {
    let focus = packet
        .team_role_identity
        .as_ref()
        .map(|identity| identity.focus_id.as_str())
        .filter(|focus| !focus.trim().is_empty())
        .unwrap_or("the bounded source-evidence focus");
    let node_digest = format!("{:x}", Sha256::digest(packet.node_id().as_bytes()));
    serde_json::json!({
        "reason": format!(
            "Runtime requires an independent follow-up verification of {focus} after the managed Agent's durable source-evidence pass."
        ),
        "requested_add_team": {
            "semantic_node_id": format!("managed-follow-up-{}", &node_digest[..16]),
            "objective": format!("Independently verify the bounded evidence for {focus}."),
        }
    })
    .to_string()
}

pub(super) fn agent_input_text(input: &AgentInput) -> String {
    match input {
        AgentInput::UserSupplement(text) => text.clone(),
        AgentInput::PeerMessage {
            from_agent_id,
            message,
        } => format!("Peer message from {from_agent_id}: {message}"),
        AgentInput::ControlContext(value) => format!("Control context: {value}"),
        AgentInput::ApprovalResult {
            approval_id,
            approved,
        } => format!(
            "Approval {approval_id}: {}",
            if *approved { "approved" } else { "denied" }
        ),
    }
}

pub(super) fn normalize_verified_narrative_terminal(
    packet: &AgentTaskPacket,
    tool_executor: &ScopedRuntimeToolExecutor,
    summary: &mut crate::TurnSummary,
) {
    if summary.terminal_completion != harness_contract::goal::GoalCompletion::Satisfied {
        return;
    }
    let has_typed_receipt = tool_executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .any(|receipt| !receipt.observed_evidence.is_empty());
    let has_upstream_evidence = packet
        .evidence_refs
        .iter()
        .any(crate::agent_result_validator::is_materialized_durable_evidence);
    if !has_typed_receipt && !has_upstream_evidence {
        return;
    }
    let mut required_fields = packet_acceptance_contract(packet)
        .iter()
        .filter_map(narrative_field_for_requirement)
        .collect::<Vec<_>>();
    required_fields.sort_by_key(|field| field.as_str());
    required_fields.dedup();
    if required_fields.is_empty() {
        return;
    }

    let Some(normalized) =
        normalized_narrative_terminal_body(&summary.final_answer, &required_fields)
    else {
        return;
    };
    summary.final_answer = normalized;
}

/// Maps only presentation-bearing checks to the field which accompanies the
/// Runtime-owned fact.  The fact itself is still checked independently below:
/// write/change checks require receipts, source verification requires the
/// pre/post read chain, and reviews require durable upstream evidence.
pub(super) fn narrative_field_for_requirement(
    requirement: &harness_contract::team::TeamAcceptanceRequirement,
) -> Option<harness_contract::team::TeamStructuredOutputField> {
    match &requirement.check {
        harness_contract::team::TeamAcceptanceCheck::StructuredField { field }
        | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, .. } => {
            Some(*field)
        }
        harness_contract::team::TeamAcceptanceCheck::StructuredArtifact { .. } => None,
        harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. } => {
            Some(harness_contract::team::TeamStructuredOutputField::SourceVerification)
        }
        harness_contract::team::TeamAcceptanceCheck::UpstreamReview => {
            Some(harness_contract::team::TeamStructuredOutputField::Review)
        }
        harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. }
        | harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence => None,
    }
}

/// A terminal answer may carry these presentation fields as natural language
/// after Runtime has independently verified the corresponding facts. The
/// remaining fields represent a deliberate risk/unknown/legacy declaration;
/// they must already be present in the Agent's structured terminal result.
/// A mixed contract may therefore preserve an explicit declaration while
/// Runtime supplies only a missing presentation field such as `review`.
const fn narrative_field_can_be_normalized(
    field: harness_contract::team::TeamStructuredOutputField,
) -> bool {
    matches!(
        field,
        harness_contract::team::TeamStructuredOutputField::Findings
            | harness_contract::team::TeamStructuredOutputField::Summary
            | harness_contract::team::TeamStructuredOutputField::Plan
            | harness_contract::team::TeamStructuredOutputField::Implementation
            | harness_contract::team::TeamStructuredOutputField::SourceVerification
            | harness_contract::team::TeamStructuredOutputField::Review
            | harness_contract::team::TeamStructuredOutputField::Proposal
            | harness_contract::team::TeamStructuredOutputField::Critique
            | harness_contract::team::TeamStructuredOutputField::Mitigation
            | harness_contract::team::TeamStructuredOutputField::Checkpoint
    )
}

pub(super) fn normalized_narrative_terminal_body(
    candidate: &str,
    fields: &[harness_contract::team::TeamStructuredOutputField],
) -> Option<String> {
    if fields.is_empty() {
        return None;
    }
    let body = candidate.trim();
    if body.is_empty()
        || body.starts_with("<synthesized_terminal")
        || body.contains("<tool_call>")
        || body.contains("```tool_use")
        || body.contains("<function=")
    {
        return None;
    }
    let mut output = structured_agent_output(body).unwrap_or_default();
    for field in fields {
        if structured_field_materialized(*field, output.get(field.as_str())) {
            continue;
        }
        // Never manufacture a risk, unknown, or decision declaration. This
        // check is deliberately per-field: a valid explicit `unresolved: []`
        // must not prevent Runtime from adding a receipt-backed `review` to
        // the same terminal result.
        if !narrative_field_can_be_normalized(*field) {
            return None;
        }
        let value = match field {
            harness_contract::team::TeamStructuredOutputField::Findings => output
                .get("summary")
                .filter(|value| materialized_json_value(value))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
            harness_contract::team::TeamStructuredOutputField::Summary => output
                .get("findings")
                .filter(|value| materialized_json_value(value))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
            // These are presentation carriers, never independently trusted
            // acceptance facts.  Copying the Agent's own terminal wording is
            // safe only because callers have already established the
            // corresponding receipt/upstream evidence chain.
            harness_contract::team::TeamStructuredOutputField::Plan
            | harness_contract::team::TeamStructuredOutputField::Implementation
            | harness_contract::team::TeamStructuredOutputField::SourceVerification
            | harness_contract::team::TeamStructuredOutputField::Review
            | harness_contract::team::TeamStructuredOutputField::Proposal
            | harness_contract::team::TeamStructuredOutputField::Critique
            | harness_contract::team::TeamStructuredOutputField::Mitigation
            | harness_contract::team::TeamStructuredOutputField::Checkpoint => {
                serde_json::Value::String(body.to_string())
            }
            harness_contract::team::TeamStructuredOutputField::Risks
            | harness_contract::team::TeamStructuredOutputField::Unresolved
            | harness_contract::team::TeamStructuredOutputField::KeyDecisions
            | harness_contract::team::TeamStructuredOutputField::UnresolvedOrRisks => {
                unreachable!("non-presentation terminal fields are rejected before normalization")
            }
        };
        output.insert(field.as_str().to_string(), value);
    }
    serde_json::to_string(&output).ok()
}

#[cfg(test)]
mod structured_output_probe {
    use super::*;

    #[test]
    fn mandatory_escalation_recovery_requires_evidence_and_an_unsatisfied_contract() {
        assert!(needs_managed_escalation_recovery(true, false, true));
        assert!(!needs_managed_escalation_recovery(false, false, true));
        assert!(!needs_managed_escalation_recovery(true, true, true));
        assert!(!needs_managed_escalation_recovery(true, false, false));
    }

    #[test]
    fn arbiter_terminal_text_extracts_key_decisions() {
        let text = "Write and read-back verification complete: `cross-team-decision-report.html` confirmed on disk (215 lines, sha256 d6340e87…), covering summary / evidence / key_decisions (K1-K8) / unresolved_or_risks (U1-U7, R1-R10) with all six roles' evidence citations and arbitration reasons. Terminal synthesis follows.\n\n{\"summary\":\"convergence_arbiter 终态收敛完成\",\"evidence\":[\"tool://tool-raw-call_00_GPhgxF1uJefA7wiTBDTR0830-2b7d0e1f4574cf50（write_file 成功）\"],\"key_decisions\":[{\"id\":\"K1\",\"decision\":\"保持自研确定性 Rust 内核\"}],\"unresolved_or_risks\":[{\"id\":\"U1\",\"item\":\"无真实数据集\"}]}";
        let parsed = structured_agent_output(text);
        assert!(
            parsed.is_some(),
            "contract JSON must be extracted from prose+JSON terminal"
        );
        let object = parsed.expect("parsed");
        assert!(object.contains_key("summary"));
        assert!(object.contains_key("evidence"));
        assert!(object.contains_key("key_decisions"));
        assert!(object.contains_key("unresolved_or_risks"));
        assert!(materialized_json_value(
            object.get("key_decisions").expect("kd")
        ));
        assert!(materialized_json_value(
            object.get("unresolved_or_risks").expect("ur")
        ));
    }

    #[test]
    fn real_arbiter_terminal_extracts_all_contract_fields() {
        let Ok(text) = std::fs::read_to_string("/tmp/arbiter_final.txt") else {
            return;
        };
        let parsed = structured_agent_output(&text);
        assert!(
            parsed.is_some(),
            "real arbiter terminal must yield a contract object"
        );
        let object = parsed.expect("parsed");
        for field in [
            "summary",
            "evidence",
            "key_decisions",
            "unresolved_or_risks",
        ] {
            assert!(
                object.contains_key(field),
                "missing {field}; keys={:?}",
                object.keys().collect::<Vec<_>>()
            );
        }
    }
}

pub(super) fn normalized_scope(value: &str) -> &str {
    let value = value.trim();
    let value = ["read:", "write:", "workspace:"]
        .into_iter()
        .find_map(|prefix| value.strip_prefix(prefix))
        .unwrap_or(value);
    value.trim_start_matches("./").trim_end_matches('/')
}

pub(super) fn path_within_scope(path: &str, scope: &str) -> bool {
    let path = normalized_scope(path);
    let scope = normalized_scope(scope);
    !path.is_empty()
        && !scope.is_empty()
        && (scope == "."
            || path == scope
            || path
                .strip_prefix(scope)
                .is_some_and(|suffix| suffix.starts_with('/')))
}
