use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentResultValidationError {
    BindingMismatch,
    MissingOutcome,
    MissingAcceptance,
    MissingEvidence,
    MissingToolExecution,
    AcceptanceMismatch,
}

impl std::fmt::Display for AgentResultValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::BindingMismatch => "agent return does not match the task graph binding",
            Self::MissingOutcome => "completed agent return has no outcome",
            Self::MissingAcceptance => "completed agent return omitted acceptance evaluation",
            Self::MissingEvidence => "completed agent return omitted required evidence",
            Self::MissingToolExecution => {
                "completed Team agent return has no successful evidence-producing tool execution"
            }
            Self::AcceptanceMismatch => {
                "completed agent return did not satisfy every Runtime-evaluated acceptance criterion"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for AgentResultValidationError {}

/// Pure validation before V3's `ExecutionCommitService` commits the graph/node
/// transition. This module never mutates graph state.
pub fn validate_agent_return(
    task: &AgentTaskPacket,
    returned: &AgentReturnPacket,
) -> Result<(), AgentResultValidationError> {
    if returned.run_id != task.run_id()
        || returned.agent_id != task.agent_id()
        || returned.task_id != task.task_id()
        || returned.session_id != task.session_id()
        || returned.mission_id != task.mission_id()
        || returned.graph_id != task.graph_id()
        || returned.node_id != task.node_id()
        || returned.attempt != task.attempt
        || returned.expected_graph_revision != task.expected_graph_revision
    {
        return Err(AgentResultValidationError::BindingMismatch);
    }
    if returned.status == AgentTerminalStatus::Completed && returned.outcome.trim().is_empty() {
        return Err(AgentResultValidationError::MissingOutcome);
    }
    if returned.status == AgentTerminalStatus::Completed {
        if task.team_id().is_some() {
            let requirements = task
                .constraints
                .iter()
                .find_map(|constraint| constraint.strip_prefix("team_acceptance_contract:"))
                .and_then(|value| {
                    serde_json::from_str::<Vec<harness_contract::team::TeamAcceptanceRequirement>>(
                        value,
                    )
                    .ok()
                })
                .filter(|requirements| {
                    requirements.len() == task.acceptance.len()
                        && requirements
                            .iter()
                            .all(|requirement| task.acceptance.contains(&requirement.criterion))
                })
                .ok_or(AgentResultValidationError::AcceptanceMismatch)?;
            if !task
                .acceptance
                .iter()
                .all(|criterion| returned.acceptance.contains(criterion))
            {
                return Err(AgentResultValidationError::AcceptanceMismatch);
            }
            let requires_new_tool_evidence = requirements.iter().any(|requirement| {
                matches!(
                    &requirement.check,
                    harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. }
                        | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { .. }
                        | harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. }
                        | harness_contract::team::TeamAcceptanceCheck::UpstreamReview
                        | harness_contract::team::TeamAcceptanceCheck::LegacyEvidenceBound { .. }
                )
            });
            let consumes_upstream = requirements.iter().any(|requirement| {
                matches!(
                    &requirement.check,
                    harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence
                )
            });
            // Evidence refs are content-addressed. A fresh verification read
            // of unchanged upstream content therefore legitimately returns
            // the same ref. For Cowd-native Team agents the observed scopes
            // and acceptance vector are derived from Runtime tool receipts;
            // together with a real tool call they prove reacquisition even
            // when ref identity is unchanged.
            let fresh_runtime_tool_observed =
                returned.tool_calls > 0 && !returned.runtime_observed_resource_scopes.is_empty();
            let produced = returned.evidence_refs.iter().any(|evidence| {
                is_materialized_durable_evidence(evidence)
                    && (fresh_runtime_tool_observed
                        || !task
                            .evidence_refs
                            .iter()
                            .any(|input| input.evidence_ref == evidence.evidence_ref))
            });
            if requires_new_tool_evidence && returned.tool_calls == 0 {
                return Err(AgentResultValidationError::MissingToolExecution);
            }
            if requires_new_tool_evidence && !produced {
                return Err(AgentResultValidationError::MissingEvidence);
            }
            if consumes_upstream
                && !task.evidence_refs.iter().any(|input| {
                    is_materialized_durable_evidence(input)
                        && returned
                            .evidence_refs
                            .iter()
                            .any(|evidence| evidence.evidence_ref == input.evidence_ref)
                })
            {
                return Err(AgentResultValidationError::MissingEvidence);
            }
            if !requires_new_tool_evidence && !consumes_upstream {
                return Err(AgentResultValidationError::AcceptanceMismatch);
            }
        } else {
            if !task.acceptance.is_empty() && returned.acceptance.is_empty() {
                return Err(AgentResultValidationError::MissingAcceptance);
            }
            if !task.evidence_refs.is_empty() && returned.evidence_refs.is_empty() {
                return Err(AgentResultValidationError::MissingEvidence);
            }
        }
    }
    Ok(())
}

#[must_use]
pub(crate) fn is_materialized_durable_evidence(
    evidence: &harness_contract::context::EvidenceAccessRef,
) -> bool {
    evidence.is_durable()
        && evidence.bytes > 0
        && !evidence.sha256.trim().is_empty()
        && !evidence.retrieval_selector.trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{
        agent::{AgentReturnPacket, AgentTaskPacket},
        context::{ContextBudgetLeaseRef, EvidenceAccessRef, EvidenceRef},
    };

    fn team_task() -> AgentTaskPacket {
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "agent",
                "run",
                "task",
                "session",
                "mission",
                Some("team"),
                "graph",
                "node",
            ),
            attempt: 1,
            expected_graph_revision: 0,
            objective: "inspect".to_string(),
            acceptance: vec!["evidence".to_string()],
            constraints: vec![format!(
                "team_acceptance_contract:{}",
                serde_json::to_string(&vec![harness_contract::team::TeamAcceptanceRequirement {
                    criterion: "evidence".to_string(),
                    check: harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                        scopes: vec!["read:src".to_string()],
                    },
                },])
                .expect("acceptance contract")
            )],
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: vec!["read:src".to_string()],
            allowed_tools: vec!["read_file".to_string()],
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model".to_string(),
            budget_lease: ContextBudgetLeaseRef::new("budget", "agent", "team", 100, 1),
            binding: None,
            managed_invocation: None,
            idempotency_key: "team-task".to_string(),
        }
    }

    fn team_return(task: &AgentTaskPacket) -> AgentReturnPacket {
        AgentReturnPacket {
            run_id: task.run_id().to_string(),
            agent_id: task.agent_id().to_string(),
            task_id: task.task_id().to_string(),
            session_id: task.session_id().to_string(),
            mission_id: task.mission_id().to_string(),
            team_id: task.team_id().map(ToString::to_string),
            graph_id: task.graph_id().to_string(),
            node_id: task.node_id().to_string(),
            attempt: task.attempt,
            expected_graph_revision: task.expected_graph_revision,
            status: AgentTerminalStatus::Completed,
            outcome: r#"{"evidence":"checked"}"#.to_string(),
            acceptance: task.acceptance.clone(),
            evidence_refs: vec![EvidenceAccessRef::durable(
                EvidenceRef::observed("tool", "read-1"),
                "a".repeat(64),
                1,
                "text/plain",
                "artifact://art_result_validator_1",
                "session:session",
            )],
            changes: Vec::new(),
            runtime_change_receipts: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 1,
            output_tokens: 1,
            cached_tokens: 0,
            model: "model".to_string(),
            provider: "provider".to_string(),
            tool_calls: 1,
            duplicate_tool_calls: 0,
            max_tool_concurrency_observed: 1,
            parallel_tool_batches: 0,
            runtime_write_attempt_paths: Vec::new(),
            runtime_observed_resource_scopes: Vec::new(),
            failure: None,
        }
    }

    #[test]
    fn team_text_and_self_report_cannot_replace_tool_or_durable_evidence() {
        let task = team_task();
        let mut returned = team_return(&task);
        returned.tool_calls = 0;
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::MissingToolExecution)
        );

        returned.tool_calls = 1;
        returned.evidence_refs.clear();
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::MissingEvidence)
        );
    }

    #[test]
    fn team_requires_every_runtime_evaluated_acceptance_criterion() {
        let task = team_task();
        let mut returned = team_return(&task);
        returned.acceptance.clear();
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::AcceptanceMismatch)
        );
        assert_eq!(validate_agent_return(&task, &team_return(&task)), Ok(()));
    }

    #[test]
    fn return_cannot_change_the_canonical_mission_binding() {
        let task = team_task();
        let mut returned = team_return(&task);
        returned.mission_id = "another-mission".to_string();
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::BindingMismatch)
        );
    }

    #[test]
    fn upstream_synthesis_allows_zero_tools_but_never_missing_predecessor_evidence() {
        let mut task = team_task();
        task.constraints = vec![format!(
            "team_acceptance_contract:{}",
            serde_json::to_string(&vec![harness_contract::team::TeamAcceptanceRequirement {
                criterion: "evidence".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence,
            },])
            .expect("upstream contract")
        )];
        let upstream = EvidenceAccessRef::durable(
            EvidenceRef::observed("tool", "upstream"),
            "b".repeat(64),
            1,
            "text/plain",
            "artifact://art_result_validator_2",
            "session:session",
        );
        task.evidence_refs = vec![upstream.clone()];
        let mut returned = team_return(&task);
        returned.tool_calls = 0;
        returned.evidence_refs = vec![upstream];
        assert_eq!(validate_agent_return(&task, &returned), Ok(()));

        task.evidence_refs.clear();
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::MissingEvidence)
        );
    }

    #[test]
    fn fresh_runtime_read_accepts_the_same_content_addressed_evidence_ref() {
        let mut task = team_task();
        let shared = EvidenceAccessRef::durable(
            EvidenceRef::observed("tool", "same-content"),
            "c".repeat(64),
            10,
            "text/plain",
            "artifact://art_result_validator_shared",
            "session:session",
        );
        task.evidence_refs = vec![shared.clone()];
        let mut returned = team_return(&task);
        returned.evidence_refs = vec![shared];
        returned.runtime_observed_resource_scopes = vec!["read:src".to_string()];

        assert_eq!(validate_agent_return(&task, &returned), Ok(()));

        returned.runtime_observed_resource_scopes.clear();
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::MissingEvidence)
        );
    }
}
