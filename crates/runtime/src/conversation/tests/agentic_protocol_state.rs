use super::agentic_program_owns_root_terminal;
use super::host_backend::{
    delegated_agentic_protocol_state, pending_delegated_action_is_ready,
    DelegatedAgenticProtocolState,
};
use harness_contract::agent::{AgentTaskPacket, AgenticExecutionBinding, AgenticExecutionFocus};
use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
    AgentInviteInput, ArtifactCommitInput, TaskClaimInput, TaskPublishInput, TaskReviewDecision,
    TaskReviewInput, TaskSubmitInput, TeamCreateInput,
};
use harness_contract::context::{ArtifactWriteDescriptor, ChildExecutionBudgetReservation};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionParentBinding,
};
use harness_contract::policy::PermissionMode;

#[test]
fn bound_agentic_program_exclusively_owns_the_root_goal_terminal() {
    let services = crate::RuntimeServices::in_memory().expect("Runtime services");
    let session_id = "session-terminal-owner";
    let turn_id = "turn-terminal-owner";
    let root_execution_id = "root-terminal-owner";
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    services
        .agent_action_service()
        .apply(&AgentActionEnvelope {
            action_id: "bind-terminal-owner".to_string(),
            actor: AgentActorBinding {
                objective_id,
                program_id,
                session_id: session_id.to_string(),
                turn_id: turn_id.to_string(),
                root_execution_id: Some(root_execution_id.to_string()),
                required_team_count: 0,
                objective_summary: "Agent-first terminal ownership".to_string(),
                model_lease: "test-model".to_string(),
                permission_ceiling: Some(PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: "root-terminal-owner".to_string(),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
            expected_revision: None,
            action: AgentAction::TeamCreate(TeamCreateInput {
                name: "Terminal owner Team".to_string(),
                mission: "prove presentation cannot complete the Objective early".to_string(),
                objective: None,
            }),
        })
        .expect("bind Agent-first Program");

    assert!(agentic_program_owns_root_terminal(
        services.as_ref(),
        session_id,
        turn_id,
        root_execution_id,
    )
    .expect("terminal owner"));
    assert!(
        super::host_presentation::root_terminal_owned_elsewhere(
            services.as_ref(),
            true,
            session_id,
            turn_id,
            root_execution_id,
            "goal:root-terminal-owner",
            harness_contract::goal::GoalCompletion::Satisfied,
        )
        .is_err(),
        "presentation cannot turn an Open Program into visible success"
    );
    assert!(
        super::host_presentation::root_terminal_owned_elsewhere(
            services.as_ref(),
            true,
            session_id,
            turn_id,
            root_execution_id,
            "goal:root-terminal-owner",
            harness_contract::goal::GoalCompletion::Partial,
        )
        .unwrap(),
        "failure presentation preserves Program terminal ownership"
    );
    assert!(!agentic_program_owns_root_terminal(
        services.as_ref(),
        "session-direct",
        "turn-direct",
        "root-direct",
    )
    .expect("direct turn retains presentation writer"));
}

struct AgenticProtocolFixture {
    services: std::sync::Arc<crate::RuntimeServices>,
    root: AgentActorBinding,
    team_ref: String,
    author_ref: String,
    reviewer_ref: String,
    task_ref: String,
}

impl AgenticProtocolFixture {
    fn envelope(&self, action_id: &str, action: AgentAction) -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: action_id.to_string(),
            actor: self.root.clone(),
            expected_revision: None,
            action,
        }
    }

    fn agent_envelope(
        &self,
        action_id: &str,
        agent_ref: &str,
        execution_id: &str,
        action: AgentAction,
    ) -> AgentActionEnvelope {
        let mut envelope = self.envelope(action_id, action);
        envelope.actor.actor_id = agent_ref.to_string();
        envelope.actor.kind = AgentActorKind::Agent;
        envelope.actor.execution_id = Some(execution_id.to_string());
        envelope.actor.team_id = Some(self.team_ref.clone());
        envelope.actor.agent_id = Some(agent_ref.to_string());
        envelope
    }
}

fn applied_ref(services: &crate::RuntimeServices, envelope: &AgentActionEnvelope) -> String {
    services
        .agent_action_service()
        .apply(envelope)
        .expect("apply Agent action")
        .changed_refs
        .into_iter()
        .next()
        .expect("action entity ref")
}

fn protocol_fixture() -> AgenticProtocolFixture {
    let root = AgentActorBinding {
        objective_id: "objective-protocol-state".to_string(),
        program_id: "program-protocol-state".to_string(),
        session_id: "session-protocol-state".to_string(),
        turn_id: "turn-protocol-state".to_string(),
        root_execution_id: None,
        required_team_count: 1,
        objective_summary: "prove the delegated Agent protocol".to_string(),
        model_lease: "test-model".to_string(),
        permission_ceiling: Some(PermissionMode::ReadOnly),
        resource_scopes: vec!["session:session-protocol-state".to_string()],
        actor_id: "root:session-protocol-state".to_string(),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    protocol_fixture_for_root(root)
}

fn protocol_fixture_for_root(root: AgentActorBinding) -> AgenticProtocolFixture {
    let services = crate::RuntimeServices::in_memory().expect("Runtime services");
    let envelope = |action_id: &str, action: AgentAction| AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: root.clone(),
        expected_revision: None,
        action,
    };
    let team_ref = applied_ref(
        services.as_ref(),
        &envelope(
            "team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Protocol Team".to_string(),
                mission: "close delegated work through durable actions".to_string(),
                objective: None,
            }),
        ),
    );
    let author_ref = applied_ref(
        services.as_ref(),
        &envelope(
            "author",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_ref.clone(),
                role: "Author".to_string(),
                mission: "produce evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ),
    );
    let reviewer_ref = applied_ref(
        services.as_ref(),
        &envelope(
            "reviewer",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_ref.clone(),
                role: "Reviewer".to_string(),
                mission: "independently verify evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ),
    );
    let task_ref = applied_ref(
        services.as_ref(),
        &envelope(
            "task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_ref.clone(),
                title: "Protocol result".to_string(),
                objective: "produce one durable result".to_string(),
                acceptance: "an independent reviewer accepts the submitted artifact".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ),
    );
    AgenticProtocolFixture {
        services,
        root,
        team_ref,
        author_ref,
        reviewer_ref,
        task_ref,
    }
}

fn register_protocol_graph(
    fixture: &AgenticProtocolFixture,
    mode: &str,
    agent_ref: &str,
    parent_graph_id: &str,
    conversation_graph_id: &str,
    attempt: u32,
) -> NodeExecutionTicket {
    let lineage = ExecutionGraphLineage {
        session_id: fixture.root.session_id.clone(),
        turn_id: fixture.root.turn_id.clone(),
        root_task_id: fixture.task_ref.clone(),
        task_id: fixture.task_ref.clone(),
        generation: 1,
    };
    let node_id = format!("{parent_graph_id}:agent");
    let packet = AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None,
            &format!("instance:{agent_ref}"),
            &format!("run:{parent_graph_id}"),
            &fixture.task_ref,
            &fixture.root.session_id,
            "mission:protocol-state",
            None,
            parent_graph_id,
            &node_id,
        ),
        attempt,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "close the assigned Agent-first task".to_string(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        acceptance: vec!["durable submission".to_string()],
        cohort_prompt_package: None,
        constraints: Vec::new(),
        context_refs: vec![format!("task:{}", fixture.task_ref)],
        evidence_refs: Vec::new(),
        resource_scopes: fixture.root.resource_scopes.clone(),
        allowed_tools: vec![
            harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID.to_string(),
            harness_contract::agent_action::TASK_SUBMIT_TOOL_ID.to_string(),
            harness_contract::agent_action::TASK_REVIEW_TOOL_ID.to_string(),
            "evidence_retrieve".to_string(),
        ],
        allowed_skills: Vec::new(),
        permission_ceiling: PermissionMode::ReadOnly,
        model_lease: "test-model".to_string(),
        budget_lease: ChildExecutionBudgetReservation::single(
            format!("budget:{parent_graph_id}"),
            agent_ref,
            fixture.task_ref.clone(),
            1_000_000,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: format!("protocol:{parent_graph_id}"),
        agentic_binding: Some(AgenticExecutionBinding {
            program_id: fixture.root.program_id.clone(),
            agent_id: agent_ref.to_string(),
            membership_id: format!("membership:{agent_ref}:{}", fixture.team_ref),
            team_id: fixture.team_ref.clone(),
            task_team_id: fixture.team_ref.clone(),
            source_spec_revision: 1,
            focus: match mode {
                "execute" => AgenticExecutionFocus::TaskExecute {
                    task_ref: fixture.task_ref.clone(),
                },
                "review" => AgenticExecutionFocus::TaskReview {
                    task_ref: fixture.task_ref.clone(),
                },
                _ => panic!("unsupported protocol fixture mode {mode}"),
            },
        }),
    };
    let mut parent_node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
        serde_json::to_string(&packet).expect("encode AgentTask packet"),
    );
    parent_node.id = node_id.clone();
    parent_node.idempotency_key = format!("node:{parent_graph_id}");
    let mut parent_graph = ExecutionGraph::new("Agent-first parent");
    parent_graph.id = parent_graph_id.to_string();
    parent_graph.lineage = Some(lineage.clone());
    parent_graph
        .node_statuses
        .insert(node_id.clone(), ExecutionNodeStatus::Planned);
    parent_graph.nodes.push(parent_node);
    fixture
        .services
        .commit_service()
        .register_graph(parent_graph)
        .expect("register AgentTask parent graph");

    let current_node_id = format!("{conversation_graph_id}:model");
    let mut current_node = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "payload:model",
    );
    current_node.id = current_node_id.clone();
    current_node.idempotency_key = format!("node:{conversation_graph_id}");
    let mut conversation_graph = ExecutionGraph::new("delegated conversation");
    conversation_graph.id = conversation_graph_id.to_string();
    conversation_graph.lineage = Some(ExecutionGraphLineage {
        generation: lineage.generation + 1,
        ..lineage
    });
    conversation_graph.parent_execution = Some(ExecutionParentBinding {
        execution_id: parent_graph_id.to_string(),
        node_id,
    });
    conversation_graph
        .node_statuses
        .insert(current_node_id.clone(), ExecutionNodeStatus::Planned);
    conversation_graph.nodes.push(current_node);
    fixture
        .services
        .commit_service()
        .register_graph(conversation_graph)
        .expect("register delegated conversation graph");
    NodeExecutionTicket {
        graph_id: conversation_graph_id.to_string(),
        node_id: current_node_id.clone(),
        executor_kind: "inline_model".to_string(),
        service_class: harness_contract::execution_graph::ExecutionServiceClass::Foreground,
        attempt: 1,
        idempotency_key: format!("ticket:{current_node_id}"),
        payload_ref: "payload:model".to_string(),
    }
}

#[tokio::test]
async fn delegated_content_uses_parent_agent_task_session_visibility() {
    let fixture = protocol_fixture();
    let ticket = register_protocol_graph(
        &fixture,
        "execute",
        &fixture.author_ref,
        "agent-content-parent",
        "agent-content-conversation",
        1,
    );
    let message = ConversationMessage::assistant(vec![ContentBlock::Text {
        text: "shared reviewable Agent result".to_string(),
    }]);

    let selector = persist_agentic_content_draft(fixture.services.as_ref(), &ticket, &message)
        .await
        .expect("persist delegated content")
        .expect("content selector");
    let artifact = fixture
        .services
        .artifact_store()
        .resolve(&selector)
        .expect("resolve delegated content");

    assert_eq!(
        artifact.visibility_scope,
        format!("session:{}", fixture.root.session_id),
        "a submitted Agent artifact must be readable by a different reviewer execution in the same Session"
    );
    fixture
        .services
        .artifact_store()
        .read(
            &artifact,
            &format!("session:{}", fixture.root.session_id),
            None,
        )
        .await
        .expect("same-Session reviewer can read committed content");
}

async fn commit_protocol_artifact(
    fixture: &AgenticProtocolFixture,
    action_id: &str,
    actor_ref: &str,
    execution_id: &str,
    relates_to: Vec<String>,
    content: &str,
) -> (String, String) {
    let physical = fixture
        .services
        .artifact_store()
        .write_bytes(
            ArtifactWriteDescriptor {
                media_type: "text/markdown".to_string(),
                visibility_scope: format!("session:{}", fixture.root.session_id),
                expected_bytes: Some(content.len() as u64),
                original_name: Some(format!("{action_id}.md")),
            },
            content.as_bytes(),
        )
        .await
        .expect("write physical artifact");
    let content_ref = physical.selector;
    let envelope = fixture.agent_envelope(
        action_id,
        actor_ref,
        execution_id,
        AgentAction::ArtifactCommit(ArtifactCommitInput {
            content_ref: content_ref.clone(),
            kind: "report".to_string(),
            title: action_id.to_string(),
            relates_to,
        }),
    );
    let artifact_ref = applied_ref(fixture.services.as_ref(), &envelope);
    (artifact_ref, content_ref)
}

#[tokio::test]
async fn delegated_protocol_reads_real_parent_and_selects_all_runtime_bound_task_artifacts() {
    let fixture = protocol_fixture();
    let execute_graph = "agent-execute-active";
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "claim",
            &fixture.author_ref,
            execute_graph,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: fixture.task_ref.clone(),
                reason: Some("execute the task".to_string()),
            }),
        ))
        .expect("claim Task");
    let (included_ref, included_evidence) = commit_protocol_artifact(
        &fixture,
        "included-artifact",
        &fixture.author_ref,
        execute_graph,
        vec![fixture.task_ref.clone()],
        "included evidence",
    )
    .await;
    let (auto_bound_ref, auto_bound_evidence) = commit_protocol_artifact(
        &fixture,
        "auto-bound-artifact",
        &fixture.author_ref,
        execute_graph,
        vec!["task:unrelated".to_string()],
        "same execution evidence",
    )
    .await;
    let (foreign_ref, _) = commit_protocol_artifact(
        &fixture,
        "foreign-artifact",
        &fixture.reviewer_ref,
        "reviewer-unrelated-execution",
        vec![fixture.task_ref.clone()],
        "wrong actor",
    )
    .await;
    let ticket = register_protocol_graph(
        &fixture,
        "execute",
        &fixture.author_ref,
        execute_graph,
        "conversation-execute-active",
        1,
    );

    let state = delegated_agentic_protocol_state(fixture.services.as_ref(), &ticket)
        .expect("derive protocol state")
        .expect("Agent-first protocol");
    assert_eq!(state.program_id, fixture.root.program_id);
    assert_eq!(state.task_id, fixture.task_ref);
    assert_eq!(state.mode, "execute");
    assert_eq!(state.status, crate::AgenticTaskStatus::Claimed);
    assert!(state.owns_active_attempt);
    assert!(!state.is_terminal());
    let mut expected_artifacts = vec![
        (included_ref.clone(), included_evidence.clone()),
        (auto_bound_ref.clone(), auto_bound_evidence.clone()),
    ];
    expected_artifacts.sort();
    let expected_refs = expected_artifacts
        .iter()
        .map(|(artifact_ref, _)| artifact_ref.clone())
        .collect::<Vec<_>>();
    let expected_evidence = expected_artifacts
        .iter()
        .map(|(_, evidence_ref)| evidence_ref.clone())
        .collect::<Vec<_>>();
    assert_eq!(state.artifact_refs, expected_refs);
    assert_eq!(state.artifact_evidence_refs, expected_evidence);
    assert!(state.artifact_refs.contains(&auto_bound_ref));
    assert!(!state.artifact_refs.contains(&foreign_ref));
    let instruction = state.continuation_instruction();
    assert!(instruction.contains(&included_ref));
    assert!(instruction.contains(&included_evidence));
}

#[tokio::test]
async fn rework_attempt_cannot_reuse_artifacts_from_an_earlier_claim() {
    let fixture = protocol_fixture();
    let first_execution = "agent-attempt-one";
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "claim-attempt-one",
            &fixture.author_ref,
            first_execution,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: fixture.task_ref.clone(),
                reason: Some("first attempt".to_string()),
            }),
        ))
        .expect("claim first attempt");
    let (stale_artifact, stale_evidence) = commit_protocol_artifact(
        &fixture,
        "attempt-one-artifact",
        &fixture.author_ref,
        first_execution,
        vec![fixture.task_ref.clone()],
        "first attempt evidence",
    )
    .await;
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "submit-attempt-one",
            &fixture.author_ref,
            first_execution,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: fixture.task_ref.clone(),
                artifact_refs: vec![stale_artifact.clone()],
                evidence_refs: vec![stale_evidence.clone()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("submit first attempt");
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "rework-attempt-one",
            &fixture.reviewer_ref,
            "review-attempt-one",
            AgentAction::TaskReview(TaskReviewInput {
                task_ref: fixture.task_ref.clone(),
                decision: TaskReviewDecision::Rework,
                reason: "the first attempt lacks a required acceptance item".to_string(),
                evidence_refs: vec![stale_evidence],
            }),
        ))
        .expect("review requires rework");

    let second_execution = "agent-attempt-two";
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "claim-attempt-two",
            &fixture.author_ref,
            second_execution,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: fixture.task_ref.clone(),
                reason: Some("rework the rejected first attempt".to_string()),
            }),
        ))
        .expect("claim rework attempt");

    let stale_submit = fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "submit-stale-attempt-one-artifact",
            &fixture.author_ref,
            second_execution,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: fixture.task_ref.clone(),
                artifact_refs: vec![stale_artifact],
                evidence_refs: vec!["artifact://not-needed-for-rejection".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("stale submission returns a receipt");
    assert_eq!(stale_submit.status, AgentActionStatus::Rejected);
    assert_eq!(
        stale_submit.error.expect("rejection detail").code,
        "artifact_not_bound_to_active_claim"
    );

    let ticket = register_protocol_graph(
        &fixture,
        "execute",
        &fixture.author_ref,
        second_execution,
        "conversation-attempt-two",
        2,
    );
    let state = delegated_agentic_protocol_state(fixture.services.as_ref(), &ticket)
        .expect("derive rework protocol")
        .expect("Agent-first protocol");
    assert!(state.owns_active_attempt);
    assert!(state.artifact_refs.is_empty());
    assert_eq!(
        state.closure_tool_ids(),
        std::collections::BTreeSet::from([
            harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID.to_string()
        ])
    );
}

#[test]
fn delegated_protocol_terminal_classification_is_mode_and_fence_aware() {
    let state = |mode: &str, status, owns_active_attempt| DelegatedAgenticProtocolState {
        program_id: "program".to_string(),
        task_id: "task".to_string(),
        mode: mode.to_string(),
        status,
        artifact_refs: Vec::new(),
        artifact_evidence_refs: Vec::new(),
        owns_active_attempt,
    };

    assert!(!state("execute", crate::AgenticTaskStatus::Claimed, true).is_terminal());
    assert!(state("execute", crate::AgenticTaskStatus::Submitted, false).is_terminal());
    assert!(state("execute", crate::AgenticTaskStatus::Accepted, false).is_terminal());
    assert!(state("execute", crate::AgenticTaskStatus::Rework, false).is_terminal());
    assert!(state("execute", crate::AgenticTaskStatus::Blocked, false).is_terminal());
    assert!(!state("review", crate::AgenticTaskStatus::Submitted, true).is_terminal());
    assert!(state("review", crate::AgenticTaskStatus::Accepted, false).is_terminal());
    assert!(state("review", crate::AgenticTaskStatus::Rework, false).is_terminal());
    assert!(state("review", crate::AgenticTaskStatus::Blocked, false).is_terminal());
    assert!(state("execute", crate::AgenticTaskStatus::Claimed, false).is_terminal());
}

#[test]
fn saturated_delegated_work_commits_its_next_action_without_text_only_detour() {
    let state = |mode: &str, status, owns_active_attempt| DelegatedAgenticProtocolState {
        program_id: "program".to_string(),
        task_id: "task".to_string(),
        mode: mode.to_string(),
        status,
        artifact_refs: vec!["artifact:one".to_string()],
        artifact_evidence_refs: vec!["artifact://content".to_string()],
        owns_active_attempt,
    };
    let pending_review = state("review", crate::AgenticTaskStatus::Submitted, true);
    assert!(pending_delegated_action_is_ready(
        Some(&pending_review),
        true,
        true,
    ));
    assert!(!pending_delegated_action_is_ready(
        Some(&pending_review),
        false,
        true,
    ));
    assert!(!pending_delegated_action_is_ready(
        Some(&pending_review),
        true,
        false,
    ));

    let accepted_review = state("review", crate::AgenticTaskStatus::Accepted, false);
    assert!(!pending_delegated_action_is_ready(
        Some(&accepted_review),
        true,
        true,
    ));
    let executing = state("execute", crate::AgenticTaskStatus::Claimed, true);
    assert!(pending_delegated_action_is_ready(
        Some(&executing),
        true,
        true,
    ));
    assert!(!pending_delegated_action_is_ready(None, true, true));
}

#[test]
fn active_delegated_protocol_exposes_only_its_irreducible_closure_action() {
    let state = |mode: &str, artifact_refs: Vec<String>| DelegatedAgenticProtocolState {
        program_id: "program".to_string(),
        task_id: "task".to_string(),
        mode: mode.to_string(),
        status: crate::AgenticTaskStatus::Claimed,
        artifact_refs,
        artifact_evidence_refs: Vec::new(),
        owns_active_attempt: true,
    };

    assert_eq!(
        state("execute", Vec::new()).closure_tool_ids(),
        std::collections::BTreeSet::from([
            harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID.to_string()
        ])
    );
    assert_eq!(
        state("execute", vec!["artifact:ready".to_string()]).closure_tool_ids(),
        std::collections::BTreeSet::from([
            harness_contract::agent_action::TASK_SUBMIT_TOOL_ID.to_string()
        ])
    );
    assert_eq!(
        state("review", vec!["artifact:submitted".to_string()]).closure_tool_ids(),
        std::collections::BTreeSet::from([
            harness_contract::agent_action::TASK_REVIEW_TOOL_ID.to_string()
        ])
    );
}

#[test]
fn delegated_protocol_rejects_a_stale_claim_execution_fence() {
    let fixture = protocol_fixture();
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "claim-active",
            &fixture.author_ref,
            "agent-execute-current",
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: fixture.task_ref.clone(),
                reason: None,
            }),
        ))
        .expect("claim Task");
    let stale_ticket = register_protocol_graph(
        &fixture,
        "execute",
        &fixture.author_ref,
        "agent-execute-stale",
        "conversation-execute-stale",
        1,
    );

    let state = delegated_agentic_protocol_state(fixture.services.as_ref(), &stale_ticket)
        .expect("derive stale protocol")
        .expect("Agent-first protocol");
    assert_eq!(state.status, crate::AgenticTaskStatus::Claimed);
    assert!(!state.owns_active_attempt);
    assert!(state.is_terminal());
}

#[test]
fn delegated_protocol_rejects_a_stale_claim_generation_fence() {
    let fixture = protocol_fixture();
    let execute_graph = "agent-execute-generation";
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "claim-generation-one",
            &fixture.author_ref,
            execute_graph,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: fixture.task_ref.clone(),
                reason: None,
            }),
        ))
        .expect("claim Task at generation one");
    let stale_ticket = register_protocol_graph(
        &fixture,
        "execute",
        &fixture.author_ref,
        execute_graph,
        "conversation-execute-generation-two",
        2,
    );

    let state = delegated_agentic_protocol_state(fixture.services.as_ref(), &stale_ticket)
        .expect("derive stale generation protocol")
        .expect("Agent-first protocol");
    assert_eq!(state.status, crate::AgenticTaskStatus::Claimed);
    assert!(!state.owns_active_attempt);
    assert!(state.is_terminal());
}

#[tokio::test]
async fn delegated_review_is_pending_only_until_its_durable_verdict() {
    let fixture = protocol_fixture();
    let execute_graph = "agent-execute-for-review";
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "claim-review-source",
            &fixture.author_ref,
            execute_graph,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: fixture.task_ref.clone(),
                reason: None,
            }),
        ))
        .expect("claim Task");
    let (artifact_ref, content_ref) = commit_protocol_artifact(
        &fixture,
        "submitted-artifact",
        &fixture.author_ref,
        execute_graph,
        vec![fixture.task_ref.clone()],
        "review this exact evidence",
    )
    .await;
    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "submit",
            &fixture.author_ref,
            execute_graph,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: fixture.task_ref.clone(),
                artifact_refs: vec![artifact_ref.clone()],
                evidence_refs: vec![content_ref.clone()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("submit Task");
    let ticket = register_protocol_graph(
        &fixture,
        "review",
        &fixture.reviewer_ref,
        "agent-review-active",
        "conversation-review-active",
        1,
    );

    let pending = delegated_agentic_protocol_state(fixture.services.as_ref(), &ticket)
        .expect("derive review protocol")
        .expect("Agent-first protocol");
    assert_eq!(pending.status, crate::AgenticTaskStatus::Submitted);
    assert!(pending.owns_active_attempt);
    assert!(!pending.is_terminal());
    assert_eq!(pending.artifact_refs, vec![artifact_ref]);
    assert_eq!(pending.artifact_evidence_refs, vec![content_ref.clone()]);

    fixture
        .services
        .agent_action_service()
        .apply(&fixture.agent_envelope(
            "accept",
            &fixture.reviewer_ref,
            "agent-review-active",
            AgentAction::TaskReview(TaskReviewInput {
                task_ref: fixture.task_ref.clone(),
                decision: TaskReviewDecision::Accept,
                reason: "the exact submitted artifact satisfies acceptance".to_string(),
                evidence_refs: vec![content_ref],
            }),
        ))
        .expect("review Task");
    let terminal = delegated_agentic_protocol_state(fixture.services.as_ref(), &ticket)
        .expect("derive terminal review protocol")
        .expect("Agent-first protocol");
    assert_eq!(terminal.status, crate::AgenticTaskStatus::Accepted);
    assert!(!terminal.owns_active_attempt);
    assert!(terminal.is_terminal());
}

#[test]
fn topic_transport_preserves_unacknowledged_pages_and_rechecks_private_scope() {
    let fixture = protocol_fixture();
    let ticket = register_protocol_graph(
        &fixture,
        "execute",
        &fixture.author_ref,
        "topic-port-parent",
        "topic-port-conversation",
        1,
    );
    let parent = fixture
        .services
        .graph_state_store()
        .load("topic-port-parent")
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&parent.nodes[0].payload_ref).unwrap();
    let actions = fixture.services.agent_action_service();
    let publish = |id: &str, recipients: Vec<String>| {
        applied_ref(
            fixture.services.as_ref(),
            &fixture.envelope(
                id,
                AgentAction::MessagePublish(harness_contract::agent_action::MessagePublishInput {
                    topic_ref: format!("topic:{}", fixture.root.program_id),
                    summary: Some(id.into()),
                    content_ref: None,
                    refs: vec![fixture.task_ref.clone()],
                    recipients,
                    intent: None,
                    issue_dispositions: vec![],
                }),
            ),
        )
    };
    let mut transport = crate::agentic::topic_delivery::TopicTransport::default();
    assert!(transport.issue(&actions, &packet).unwrap().is_none());
    let first_ref = publish("first-peer-evidence", vec![fixture.author_ref.clone()]);
    let first = transport.issue(&actions, &packet).unwrap().unwrap();
    let id = first["delivery_id"].as_str().unwrap();
    assert_eq!(
        first["context"]["entries"][0]["read_request"]["input"]["entry_ref"],
        first_ref
    );
    publish("second-peer-evidence", vec![fixture.author_ref.clone()]);
    assert_eq!(
        transport.issue(&actions, &packet).unwrap().unwrap(),
        first,
        "a new event cannot replace the unacknowledged page"
    );
    assert!(transport
        .acknowledge(&actions, "topic-delivery:forged")
        .is_err());
    assert!(actions
        .topic_observations(
            &fixture.root.program_id,
            &fixture.author_ref,
            &packet.agentic_binding.as_ref().unwrap().team_id,
            packet.graph_id(),
            16,
            48 * 1024
        )
        .unwrap()
        .is_some());
    transport.acknowledge(&actions, id).unwrap();
    transport.acknowledge(&actions, id).unwrap();
    let cursor_events = fixture
        .services
        .event_store()
        .list_stream(&format!(
            "agentic-topic-cursor:{}:{}",
            fixture.root.program_id,
            packet.graph_id()
        ))
        .unwrap();
    assert_eq!(cursor_events.len(), 1);
    assert_eq!(
        cursor_events[0].payload["observation_kind"],
        "worker_transport"
    );

    let second = transport.issue(&actions, &packet).unwrap().unwrap();
    assert_eq!(second["context"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(
        second["context"]["entries"][0]["entry"]["summary"],
        "second-peer-evidence"
    );
    transport
        .acknowledge(&actions, second["delivery_id"].as_str().unwrap())
        .unwrap();
    publish("reviewer-private", vec![fixture.reviewer_ref.clone()]);
    assert!(transport.issue(&actions, &packet).unwrap().is_none());
    assert!(crate::agentic::topic_delivery::prepare(&actions, &packet)
        .unwrap()
        .is_none());
    assert_eq!(ticket.graph_id, "topic-port-conversation");
}

#[derive(Clone)]
struct TopicSafePointClient {
    requests: Arc<Mutex<Vec<ApiRequest>>>,
    services: Arc<crate::RuntimeServices>,
    publication: AgentActionEnvelope,
    fail_first: bool,
}
impl ApiClient for TopicSafePointClient {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let step = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request);
            requests.len()
        };
        if self.fail_first || step >= 3 {
            return Box::pin(stream::iter(vec![Err(RuntimeError::new(
                "HTTP 402 Payment Required: safe-point test",
            ))]));
        }
        if step == 1 {
            assert_eq!(
                self.services
                    .agent_action_service()
                    .apply(&self.publication)
                    .unwrap()
                    .status,
                harness_contract::agent_action::AgentActionStatus::Applied
            );
        }
        Box::pin(stream::iter(vec![
            Ok(AssistantEvent::ToolUse {
                id: format!("topic-read-{step}"),
                name: "read_file".into(),
                input: format!("{{\"path\":\"part-{step}.txt\"}}"),
            }),
            Ok(AssistantEvent::MessageStop),
        ]))
    }
}
struct TopicSafePointReader;
#[async_trait::async_trait]
impl ToolExecutor for TopicSafePointReader {
    async fn execute_output(
        &self,
        name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if name != "read_file" {
            return Err(ToolError::new("unexpected tool"));
        }
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            "fixture source evidence".to_string(),
        ))
    }
    fn available_tool_names(&self) -> Vec<String> {
        vec!["read_file".into()]
    }
}

#[tokio::test]
async fn actual_delegated_model_safe_point_receives_delta_and_only_success_acknowledges() {
    for fail_first in [false, true] {
        let fixture = protocol_fixture();
        let parent_id = "topic-model-parent";
        register_protocol_graph(
            &fixture,
            "execute",
            &fixture.author_ref,
            parent_id,
            "topic-model-unused-conversation",
            1,
        );
        let services = Arc::clone(&fixture.services);
        let claim = fixture.agent_envelope(
            "safe-point-claim",
            &fixture.author_ref,
            parent_id,
            AgentAction::TaskClaim(harness_contract::agent_action::TaskClaimInput {
                task_ref: fixture.task_ref.clone(),
                reason: Some("inspect before work".into()),
            }),
        );
        assert_eq!(
            services
                .agent_action_service()
                .apply(&claim)
                .unwrap()
                .status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        services.publish_session_execution_policy(
            &fixture.root.session_id,
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Supervised,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                ),
            ),
        );
        let spec = services
            .task_runtime_port()
            .bind_task_spec(
                &fixture.root.session_id,
                Some(harness_contract::policy::PermissionMode::ReadOnly),
                harness_contract::task::TaskSpec::new(
                    "Read the bounded source and preserve peer evidence",
                ),
            )
            .unwrap();
        services
            .task_runtime_port()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: fixture.task_ref.clone(),
                mission_id: services.mission_runtime().default_mission_id().into(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: fixture.root.session_id.clone(),
                origin_turn_id: fixture.root.turn_id.clone(),
                root_task_id: fixture.task_ref.clone(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                mission_assigned_by: "topic-test".into(),
                spec,
                evidence_refs: vec![],
            })
            .unwrap();
        let publication = fixture.envelope(
            "live-peer-change",
            AgentAction::MessagePublish(harness_contract::agent_action::MessagePublishInput {
                topic_ref: format!("topic:{}", fixture.root.program_id),
                summary: Some("peer-counterexample-arrived-at-tool-boundary".into()),
                content_ref: None,
                refs: vec![fixture.task_ref.clone()],
                recipients: vec![fixture.author_ref.clone()],
                intent: None,
                issue_dispositions: vec![],
            }),
        );
        if fail_first {
            assert_eq!(
                services
                    .agent_action_service()
                    .apply(&publication)
                    .unwrap()
                    .status,
                harness_contract::agent_action::AgentActionStatus::Applied
            );
        }
        let captured = Arc::new(Mutex::new(vec![]));
        let mut session = Session::new();
        session.session_id = fixture.root.session_id.clone();
        let mut runtime = crate::ConversationRuntime::new(
            session,
            TopicSafePointClient {
                requests: Arc::clone(&captured),
                services: Arc::clone(&services),
                publication,
                fail_first,
            },
            TopicSafePointReader,
            PermissionPolicy::new(crate::PermissionMode::ReadOnly),
            canonical_host_system_prompt(vec![]),
        )
        .without_memory();
        runtime.set_active_model("test-model");
        runtime.set_context_profile(ContextProfile::SubAgent);
        let parent = services.graph_state_store().load(parent_id).unwrap();
        let lineage = parent.lineage.clone().unwrap();
        let (_runtime, result) = submit_owned_conversation_turn_with_ingress(
            runtime,
            Arc::clone(&services),
            "Read the bounded source and preserve peer evidence",
            &SharedPrompter::none(),
            None,
            Some(ExecutionParentBinding {
                execution_id: parent_id.into(),
                node_id: parent.nodes[0].id.clone(),
            }),
            Some(lineage),
            TurnExecutionRole::DelegatedLeaf,
            0,
            false,
        )
        .await;
        if let Ok(summary) = &result {
            assert_ne!(
                summary.terminal_completion,
                harness_contract::goal::GoalCompletion::Satisfied
            );
        }
        let requests = captured.lock().unwrap();
        let wanted = if fail_first { 0 } else { 1 };
        assert!(
            requests.len() > wanted,
            "provider requests missing: {result:?}"
        );
        assert!(
            format!("{:?}", requests[wanted].prompt.contextual_packets)
                .contains("peer-counterexample-arrived-at-tool-boundary"),
            "actual next request must contain peer delta"
        );
        let unread = services
            .agent_action_service()
            .topic_observations(
                &fixture.root.program_id,
                &fixture.author_ref,
                &fixture.team_ref,
                parent_id,
                16,
                48 * 1024,
            )
            .unwrap();
        assert_eq!(
            unread.is_some(),
            fail_first,
            "failed provider delivery must remain unread; selected success acknowledges"
        );
        let cursor_events = services
            .event_store()
            .list_stream(&format!(
                "agentic-topic-cursor:{}:{parent_id}",
                fixture.root.program_id
            ))
            .unwrap();
        if fail_first {
            assert!(cursor_events.is_empty());
        } else {
            assert_eq!(cursor_events.len(), 1);
            assert_eq!(
                cursor_events[0].payload["observation_kind"],
                "provider_model"
            );
            assert!(requests.len() >= 3);
            assert!(
                !format!("{:?}", requests[2].prompt.contextual_packets)
                    .contains("peer-counterexample-arrived-at-tool-boundary"),
                "acknowledged peer page must not be reinjected at the next model safe point"
            );
        }
    }
}

#[derive(Clone)]
struct RootClosureResearchClient {
    requests: Arc<Mutex<Vec<ApiRequest>>>,
    choices: Arc<Mutex<Vec<(bool, Option<String>)>>>,
}
impl ApiClient for RootClosureResearchClient {
    fn configure_tool_choice(&mut self, required: bool, name: Option<String>) {
        self.choices.lock().unwrap().push((required, name));
    }
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        self.requests.lock().unwrap().push(request);
        Box::pin(stream::iter(vec![Err(RuntimeError::new(
            "HTTP 402 Payment Required: fixture unavailable after recording actual request",
        ))]))
    }
}

#[tokio::test]
async fn accepted_root_work_does_not_force_a_terminal_tool_or_promote_its_checkpoint_to_system() {
    let mut root = protocol_fixture().root;
    root.session_id = "root-research-session".into();
    root.turn_id = "turn-test".into();
    root.objective_id =
        harness_contract::agent_action::root_objective_id(&root.session_id, &root.turn_id);
    root.program_id = harness_contract::agent_action::program_id_for_objective(&root.objective_id);
    root.resource_scopes = vec![format!("session:{}", root.session_id)];
    let fixture = protocol_fixture_for_root(root);
    let actions = fixture.services.agent_action_service();
    assert_eq!(
        actions
            .apply(&fixture.agent_envelope(
                "root-check-claim",
                &fixture.author_ref,
                "root-check-author",
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: fixture.task_ref.clone(),
                    reason: None
                })
            ))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    let (artifact, content) = commit_protocol_artifact(
        &fixture,
        "root-check-artifact",
        &fixture.author_ref,
        "root-check-author",
        vec![fixture.task_ref.clone()],
        "Evidence for the accepted task; new counterexamples still require research",
    )
    .await;
    assert_eq!(
        actions
            .apply(&fixture.agent_envelope(
                "root-check-submit",
                &fixture.author_ref,
                "root-check-author",
                AgentAction::TaskSubmit(TaskSubmitInput {
                    task_ref: fixture.task_ref.clone(),
                    artifact_refs: vec![artifact],
                    evidence_refs: vec![content.clone()],
                    unresolved: vec![]
                })
            ))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    assert_eq!(
        actions
            .apply(&fixture.agent_envelope(
                "root-check-review",
                &fixture.reviewer_ref,
                "root-check-reviewer",
                AgentAction::TaskReview(TaskReviewInput {
                    task_ref: fixture.task_ref.clone(),
                    decision: TaskReviewDecision::Accept,
                    reason: "source supports this task".into(),
                    evidence_refs: vec![content]
                })
            ))
            .unwrap()
            .status,
        AgentActionStatus::Applied
    );
    let projection = actions.project(&fixture.root.program_id).unwrap();
    assert!(matches!(
        super::root_agentic_terminal_action(&projection),
        Some(super::RootAgenticTerminalAction::RequestObjectiveCompletion { .. })
    ));
    let requests = Arc::new(Mutex::new(vec![]));
    let choices = Arc::new(Mutex::new(vec![]));
    let mut session = Session::new();
    session.session_id = fixture.root.session_id.clone();
    let mut runtime = crate::ConversationRuntime::new(
        session,
        RootClosureResearchClient {
            requests: Arc::clone(&requests),
            choices: Arc::clone(&choices),
        },
        TopicSafePointReader,
        PermissionPolicy::new(PermissionMode::ReadOnly),
        canonical_host_system_prompt(vec![]),
    )
    .without_memory();
    runtime.set_active_model("test-model");
    fixture
        .services
        .working_context_command(
            &runtime.memory_turn_context(),
            "root-working-pin",
            crate::working_context::WorkingContextInput::Pin {
                source: crate::working_context::WorkingSource::Artifact {
                    content_ref: projection
                        .artifacts
                        .values()
                        .next()
                        .unwrap()
                        .content_ref
                        .clone(),
                },
            },
        )
        .await
        .unwrap();
    let (_, result) = submit_test_owned_conversation_turn(
        runtime,
        Arc::clone(&fixture.services),
        "Inspect new counterevidence before deciding whether the original objective is complete",
        &SharedPrompter::none(),
        test_execution_lineage(),
    )
    .await;
    let captured = requests.lock().unwrap();
    assert!(
        !captured.is_empty(),
        "missing real root request: {result:?}"
    );
    assert!(captured[0]
        .prompt
        .contextual_packets
        .iter()
        .any(|p| p.content.contains("agentic_program_checkpoint")));
    assert!(!captured[0]
        .prompt
        .trusted_system
        .iter()
        .any(|p| p.contains("agentic_program_checkpoint")));
    assert!(captured[0].prompt.contextual_packets.iter().any(|p| p
        .content
        .contains("runtime.working_context")
        && p.content
            .contains("new counterexamples still require research")));
    let choices = choices.lock().unwrap();
    assert!(!choices.is_empty());
    assert!(
        choices
            .iter()
            .all(|(required, name)| !required && name.is_none()),
        "root must retain research and disposition choices: {choices:?}"
    );
}
