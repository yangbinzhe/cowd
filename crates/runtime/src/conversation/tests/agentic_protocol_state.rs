use super::host_backend::{delegated_agentic_protocol_state, DelegatedAgenticProtocolState};
use super::agentic_program_owns_root_terminal;
use harness_contract::agent::AgentTaskPacket;
use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, AgentInviteInput,
    ArtifactCommitInput, TaskClaimInput, TaskPublishInput, TaskReviewDecision, TaskReviewInput,
    TaskSubmitInput, TeamCreateInput,
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
    let services = crate::RuntimeServices::in_memory().expect("Runtime services");
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
            agent_ref,
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
        requires_managed_collaboration_escalation: false,
        acceptance: vec!["durable submission".to_string()],
        cohort_prompt_package: None,
        constraints: Vec::new(),
        context_refs: vec![
            format!("agentic_program:{}", fixture.root.program_id),
            format!("agentic_team:{}", fixture.team_ref),
            format!("agentic_member:{agent_ref}"),
            format!("agentic_task:{}", fixture.task_ref),
            format!("agentic_mode:{mode}"),
        ],
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
async fn delegated_protocol_reads_real_parent_and_selects_only_owned_task_artifacts() {
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
    let (unrelated_ref, _) = commit_protocol_artifact(
        &fixture,
        "unrelated-artifact",
        &fixture.author_ref,
        execute_graph,
        vec!["task:unrelated".to_string()],
        "wrong task",
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
    assert_eq!(state.artifact_refs, vec![included_ref.clone()]);
    assert_eq!(
        state.artifact_evidence_refs,
        vec![included_evidence.clone()]
    );
    assert!(!state.artifact_refs.contains(&unrelated_ref));
    assert!(!state.artifact_refs.contains(&foreign_ref));
    assert_eq!(
        state.required_terminal_tools(),
        std::collections::BTreeSet::from([
            harness_contract::agent_action::TASK_SUBMIT_TOOL_ID.to_string()
        ])
    );
    let instruction = state.continuation_instruction();
    assert!(instruction.contains(&included_ref));
    assert!(instruction.contains(&included_evidence));
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
    assert_eq!(
        pending.required_terminal_tools(),
        std::collections::BTreeSet::from([
            "evidence_retrieve".to_string(),
            harness_contract::agent_action::TASK_REVIEW_TOOL_ID.to_string(),
        ])
    );

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
