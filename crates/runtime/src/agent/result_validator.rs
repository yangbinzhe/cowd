use harness_contract::acceptance::AcceptanceVerdict;
use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TeamEvidencePolicy {
    pub requires_new_tool_evidence: bool,
    pub consumes_upstream: bool,
}

/// Compile the evidence policy once for both Agent terminal admission and
/// Team delivery verification. Keeping this classification shared prevents a
/// role from becoming Completed under a weaker interpretation than the Team
/// verifier applies to the same frozen packet.
#[must_use]
pub(crate) fn team_evidence_policy(
    requirements: &[harness_contract::team::TeamAcceptanceRequirement],
) -> TeamEvidencePolicy {
    let consumes_upstream = requirements.iter().any(|requirement| {
        matches!(
            &requirement.check,
            harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence
        )
    });
    let requires_new_tool_evidence = requirements.iter().any(|requirement| {
        matches!(
            &requirement.check,
            harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. }
                | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { .. }
                | harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. }
                | harness_contract::team::TeamAcceptanceCheck::UpstreamReview
        )
    });
    TeamEvidencePolicy {
        requires_new_tool_evidence,
        consumes_upstream,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentResultValidationError {
    BindingMismatch,
    MissingOutcome,
    MissingAcceptanceEvaluation,
    UnknownAcceptanceEvaluator,
    MissingEvidence,
    MissingToolExecution,
    UnsatisfiedAcceptance,
    UpstreamOnlyOutcomeRequestsTool,
}

impl std::fmt::Display for AgentResultValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::BindingMismatch => "agent return does not match the task graph binding",
            Self::MissingOutcome => "completed agent return has no outcome",
            Self::MissingAcceptanceEvaluation => {
                "completed agent return omitted the Runtime acceptance evaluation"
            }
            Self::UnknownAcceptanceEvaluator => {
                "completed agent return has an unknown Runtime acceptance evaluator revision"
            }
            Self::MissingEvidence => "completed agent return omitted required evidence",
            Self::MissingToolExecution => {
                "completed Team agent return has no successful evidence-producing tool execution"
            }
            Self::UnsatisfiedAcceptance => {
                "completed agent return has a non-satisfied Runtime acceptance verdict"
            }
            Self::UpstreamOnlyOutcomeRequestsTool => {
                "upstream-only Team result simulated or requested a forbidden tool invocation"
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
    // An upstream-only role has no reacquisition lease by construction. Its
    // conclusion must be based on Runtime-attached evidence, not on a
    // model-written pseudo tool call that cannot be authorized, executed, or
    // audited. This is deliberately keyed by the typed task constraint, not
    // a role name, template id, or display label.
    if returned.status == AgentTerminalStatus::Completed
        && task
            .constraints
            .iter()
            .any(|constraint| constraint == "upstream_evidence_only:no_tool_reacquisition")
        && upstream_only_outcome_requests_tool(&returned.outcome)
    {
        return Err(AgentResultValidationError::UpstreamOnlyOutcomeRequestsTool);
    }
    if returned.status == AgentTerminalStatus::Completed {
        let evaluation = returned
            .acceptance_evaluation
            .as_ref()
            .ok_or(AgentResultValidationError::MissingAcceptanceEvaluation)?;
        if evaluation.evaluator_revision
            != crate::acceptance_evaluator::AcceptanceEvaluator::REVISION
        {
            return Err(AgentResultValidationError::UnknownAcceptanceEvaluator);
        }
        if evaluation.verdict != AcceptanceVerdict::Satisfied {
            return Err(AgentResultValidationError::UnsatisfiedAcceptance);
        }
        if task.team_id().is_some() {
            let requirements = (!task.output_acceptance.is_empty())
                .then(|| task.output_acceptance.clone())
                .filter(|requirements| {
                    requirements.len() == task.acceptance.len()
                        && requirements
                            .iter()
                            .all(|requirement| task.acceptance.contains(&requirement.criterion))
                })
                .ok_or(AgentResultValidationError::UnsatisfiedAcceptance)?;
            let evidence_policy = team_evidence_policy(&requirements);
            let consumes_upstream = evidence_policy.consumes_upstream;
            let requires_new_tool_evidence = evidence_policy.requires_new_tool_evidence;
            // Evidence refs are content-addressed. A fresh verification read
            // of unchanged upstream content therefore legitimately returns
            // the same ref. For Cowd-native Team agents the observed scopes
            // and acceptance vector are derived from Runtime tool receipts;
            // together with a real tool call they prove reacquisition even
            // when ref identity is unchanged.
            let fresh_runtime_tool_observed = returned.tool_calls > 0
                && !returned.observed_acceptance.observed_evidence.is_empty();
            // A durable artifact is only evidence-producing when Runtime also
            // observed a successful tool effect for this execution. Failed
            // tool attempts are durably recorded for audit and therefore can
            // introduce a new content address, but that failure artifact must
            // never satisfy a Team acceptance contract. Keep this admission
            // rule aligned with Team delivery verification so an Agent cannot
            // become Completed and later make its Team terminal Partial.
            let produced = fresh_runtime_tool_observed
                && returned
                    .evidence_refs
                    .iter()
                    .any(is_materialized_durable_evidence);
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
                return Err(AgentResultValidationError::UnsatisfiedAcceptance);
            }
        } else {
            if !task.evidence_refs.is_empty() && returned.evidence_refs.is_empty() {
                return Err(AgentResultValidationError::MissingEvidence);
            }
        }
    }
    Ok(())
}

fn upstream_only_outcome_requests_tool(outcome: &str) -> bool {
    let compact = outcome
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let simulated_runtime_tools = [
        "runtime_capabilities",
        "tool_search",
        "context_retrieve",
        "evidence_retrieve",
        "runtime_orchestrate",
        "submit_collaboration_decision",
        "request_collaboration_escalation",
    ];
    simulated_runtime_tools.iter().any(|tool| {
        compact.contains(&format!(r#""name":"{tool}""#))
            || compact.contains(&format!("<tool_call>{tool}"))
            || compact.contains(&format!("<function_call>{tool}"))
    })
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
        context::{ChildExecutionBudgetReservation, EvidenceAccessRef, EvidenceRef},
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
            policy_revision: 1,
            objective: "inspect".to_string(),
            required_acceptance: Default::default(),
            output_acceptance: vec![harness_contract::team::TeamAcceptanceRequirement {
                criterion: "evidence".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                    scopes: vec!["read:src".to_string()],
                },
            }],
            requires_managed_collaboration_escalation: false,
            acceptance: vec!["evidence".to_string()],
            team_role_identity: None,
            team_role: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: vec!["read:src".to_string()],
            allowed_tools: vec!["read_file".to_string()],
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model".to_string(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "team",
                100,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "team-task".to_string(),
        }
    }

    fn team_return(task: &AgentTaskPacket) -> AgentReturnPacket {
        let observed_acceptance = harness_contract::context::ObservedAcceptance {
            satisfied_criteria: task.acceptance.clone(),
            observed_evidence: vec![harness_contract::context::ObservedEvidence {
                obligation_id: "fixture-read".to_string(),
                target: harness_contract::context::EvidenceTargetIdentity::Network {
                    endpoint: "fixture".to_string(),
                },
                observed_at_sequence: 1,
                tool_name: "read_file".to_string(),
                provenance: harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
                evidence_ref: None,
                model_observation: None,
                workspace_prior_state: None,
            }],
            unresolved_obligation_ids: Vec::new(),
        };
        let required = if task.required_acceptance.is_empty() {
            harness_contract::context::RequiredAcceptance {
                criteria: task.acceptance.clone(),
                evidence_obligations: Vec::new(),
            }
        } else {
            task.required_acceptance.clone()
        };
        let (_, acceptance_evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_terminal(
                &required,
                observed_acceptance.satisfied_criteria.clone(),
                observed_acceptance.observed_evidence.clone(),
            );
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
            answer_candidate: None,
            observed_acceptance,
            acceptance_evaluation: Some(acceptance_evaluation),
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
    fn failed_tool_attempt_artifact_cannot_satisfy_team_evidence() {
        let task = team_task();
        let mut returned = team_return(&task);

        // ToolHost persists failed outputs for audit, so both a call count and
        // a new durable artifact can exist without any successful observed
        // evidence. That combination must remain ineligible for acceptance.
        returned.observed_acceptance.observed_evidence.clear();
        let required = harness_contract::context::RequiredAcceptance {
            criteria: task.acceptance.clone(),
            evidence_obligations: Vec::new(),
        };
        let (_, evaluation) = crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_terminal(
            &required,
            returned.observed_acceptance.satisfied_criteria.clone(),
            returned.observed_acceptance.observed_evidence.clone(),
        );
        returned.acceptance_evaluation = Some(evaluation);

        assert_eq!(returned.tool_calls, 1);
        assert!(!returned.evidence_refs.is_empty());
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::MissingEvidence)
        );
    }

    #[test]
    fn team_requires_every_runtime_evaluated_acceptance_criterion() {
        let task = team_task();
        let mut returned = team_return(&task);
        returned.observed_acceptance.satisfied_criteria.clear();
        let required = harness_contract::context::RequiredAcceptance {
            criteria: task.acceptance.clone(),
            evidence_obligations: Vec::new(),
        };
        let (_, evaluation) = crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_terminal(
            &required,
            returned.observed_acceptance.satisfied_criteria.clone(),
            returned.observed_acceptance.observed_evidence.clone(),
        );
        returned.acceptance_evaluation = Some(evaluation);
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::UnsatisfiedAcceptance)
        );
        assert_eq!(validate_agent_return(&task, &team_return(&task)), Ok(()));
    }

    #[test]
    fn completed_return_rejects_an_unknown_evaluator_even_if_its_verdict_is_satisfied() {
        let task = team_task();
        let mut returned = team_return(&task);
        returned
            .acceptance_evaluation
            .as_mut()
            .expect("fixture carries the canonical evaluation")
            .evaluator_revision = 0;
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::UnknownAcceptanceEvaluator)
        );
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
        task.output_acceptance = vec![harness_contract::team::TeamAcceptanceRequirement {
            criterion: "evidence".to_string(),
            check: harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence,
        }];
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
    fn custom_artifact_requires_fresh_tools_or_upstream_grounding() {
        let mut task = team_task();
        task.acceptance = vec!["artifact:runtime_findings".to_string()];
        task.output_acceptance = vec![
            harness_contract::team::TeamAcceptanceRequirement {
                criterion: "artifact:runtime_findings".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::StructuredArtifact {
                    name: "runtime_findings".to_string(),
                },
            },
            harness_contract::team::TeamAcceptanceRequirement {
                criterion: "evidence".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                    scopes: vec!["read:src".to_string()],
                },
            },
        ];
        task.acceptance.push("evidence".to_string());
        let mut returned = team_return(&task);
        returned.outcome = r#"{"runtime_findings":"receipt-grounded result"}"#.to_string();
        assert_eq!(validate_agent_return(&task, &returned), Ok(()));

        returned.tool_calls = 0;
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::MissingToolExecution)
        );

        task.output_acceptance[1] = harness_contract::team::TeamAcceptanceRequirement {
            criterion: "upstream:evidence".to_string(),
            check: harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence,
        };
        task.acceptance[1] = "upstream:evidence".to_string();
        let upstream = EvidenceAccessRef::durable(
            EvidenceRef::observed("tool", "custom-upstream"),
            "e".repeat(64),
            1,
            "application/json",
            "artifact://art_result_validator_custom_upstream",
            "session:session",
        );
        task.evidence_refs = vec![upstream.clone()];
        let mut returned = team_return(&task);
        returned.outcome = r#"{"runtime_findings":"upstream-grounded result"}"#.to_string();
        returned.tool_calls = 0;
        returned.evidence_refs = vec![upstream];
        assert_eq!(validate_agent_return(&task, &returned), Ok(()));
    }

    #[test]
    fn upstream_only_synthesis_rejects_simulated_runtime_tool_payloads() {
        let mut task = team_task();
        task.output_acceptance = vec![harness_contract::team::TeamAcceptanceRequirement {
            criterion: "evidence".to_string(),
            check: harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence,
        }];
        task.constraints = vec!["upstream_evidence_only:no_tool_reacquisition".to_string()];
        let upstream = EvidenceAccessRef::durable(
            EvidenceRef::observed("tool", "upstream"),
            "d".repeat(64),
            1,
            "text/plain",
            "artifact://art_result_validator_3",
            "session:session",
        );
        task.evidence_refs = vec![upstream.clone()];
        let mut returned = team_return(&task);
        returned.tool_calls = 0;
        returned.evidence_refs = vec![upstream];
        returned.outcome = r#"```json
        { "name": "runtime_capabilities", "arguments": {} }
        ```"#
            .to_string();

        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::UpstreamOnlyOutcomeRequestsTool)
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

        assert_eq!(validate_agent_return(&task, &returned), Ok(()));

        returned.observed_acceptance.observed_evidence.clear();
        let required = if task.required_acceptance.is_empty() {
            harness_contract::context::RequiredAcceptance {
                criteria: task.acceptance.clone(),
                evidence_obligations: Vec::new(),
            }
        } else {
            task.required_acceptance.clone()
        };
        let (_, evaluation) = crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_terminal(
            &required,
            returned.observed_acceptance.satisfied_criteria.clone(),
            returned.observed_acceptance.observed_evidence.clone(),
        );
        returned.acceptance_evaluation = Some(evaluation);
        assert_eq!(
            validate_agent_return(&task, &returned),
            Err(AgentResultValidationError::MissingEvidence)
        );
    }
}
