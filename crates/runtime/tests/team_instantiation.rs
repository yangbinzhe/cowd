#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{AgentTaskPacket, DefinitionScope};
use harness_contract::team::{
    FocusPartitionPlan, FocusPartitionSlot, RoleCardinalityPolicy, TeamInstantiationRequest,
    TeamRoleCardinalityOverride, TeamSelectionMode, TeamStrategyBinding, TeamTemplateDefinitionId,
    TeamTemplateSelector,
};
use runtime::RuntimeServices;

#[path = "support/canonical_agent_fixture.rs"]
mod canonical_agent_fixture;

fn request(template_id: &str, mission_id: &str) -> TeamInstantiationRequest {
    TeamInstantiationRequest {
        request_id: "team-instantiation-test".to_string(),
        team_id: "team-instantiation-test".to_string(),
        mission_id: mission_id.to_string(),
        lineage: harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-team-instantiation".to_string(),
            turn_id: "turn-team-instantiation".to_string(),
            root_task_id: "task:root:team-instantiation".to_string(),
            task_id: "task:root:team-instantiation".to_string(),
            generation: 1,
        },
        parent_execution: None,
        selection_mode: TeamSelectionMode::Explicit,
        strategy_binding: None,
        template_selector: TeamTemplateSelector::LatestStable {
            template_id: TeamTemplateDefinitionId::new(DefinitionScope::Builtin, template_id)
                .expect("builtin template id"),
        },
        objective: "Investigate independent architecture options and reconcile them.".to_string(),
        acceptance: vec!["summary".to_string(), "evidence".to_string()],
        risk: None,
        role_binding_overrides: Vec::new(),
        display_name: None,
        role_display_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "deepseek-v4-flash".to_string(),
        execution_budget: harness_contract::context::ParentExecutionBudget::new(
            "team-instantiation-budget",
            65_536,
            u64::MAX,
            32,
            1,
        ),
        deadline_at_ms: u64::MAX,
        managed_invocation: None,
        resource_scopes: vec![
            "read:crates/runtime".to_string(),
            "session:session-team-instantiation".to_string(),
        ],
        allow_whole_workspace_scope: false,
        upstream_evidence_refs: Vec::new(),
        upstream_artifact_refs: Vec::new(),
    }
}

#[test]
fn explicit_template_creates_bound_non_overlapping_role_slots() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut request = request(
        "cowd/parallel-research-synthesis",
        services.mission_runtime().default_mission_id(),
    );
    request.cardinality_overrides = vec![TeamRoleCardinalityOverride {
        role_id: "researcher".to_string(),
        cardinality: RoleCardinalityPolicy::Fixed { count: 3 },
    }];
    request.focus_partition_plans = vec![FocusPartitionPlan {
        role_id: "researcher".to_string(),
        shared_baseline: vec!["compare the same target architecture".to_string()],
        slots: vec![
            FocusPartitionSlot {
                focus_id: "storage".to_string(),
                boundary: "persistence and consistency only".to_string(),
                evidence_responsibility: "schema and repository evidence".to_string(),
                capability_cropped_refs: vec!["read:storage".to_string()],
                scope_hash: harness_contract::team::focus_scope_hash(
                    "researcher",
                    "persistence and consistency only",
                    &["read:storage".to_string()],
                ),
                overlap_budget_bp: 0,
                novelty_target_bp: 2_500,
                output_contract: vec!["findings".to_string(), "evidence".to_string()],
                output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
            },
            FocusPartitionSlot {
                focus_id: "runtime".to_string(),
                boundary: "execution lifecycle and scheduling only".to_string(),
                evidence_responsibility: "runtime call-chain evidence".to_string(),
                capability_cropped_refs: vec!["read:runtime".to_string()],
                scope_hash: harness_contract::team::focus_scope_hash(
                    "researcher",
                    "execution lifecycle and scheduling only",
                    &["read:runtime".to_string()],
                ),
                overlap_budget_bp: 0,
                novelty_target_bp: 2_500,
                output_contract: vec!["findings".to_string(), "evidence".to_string()],
                output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
            },
            FocusPartitionSlot {
                focus_id: "surface".to_string(),
                boundary: "API and surface projection only".to_string(),
                evidence_responsibility: "route and UI evidence".to_string(),
                capability_cropped_refs: vec!["read:surface".to_string()],
                scope_hash: harness_contract::team::focus_scope_hash(
                    "researcher",
                    "API and surface projection only",
                    &["read:surface".to_string()],
                ),
                overlap_budget_bp: 0,
                novelty_target_bp: 2_500,
                output_contract: vec!["findings".to_string(), "evidence".to_string()],
                output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
            },
        ],
    }];
    request.resource_scopes.extend([
        "read:storage".to_string(),
        "read:runtime".to_string(),
        "read:surface".to_string(),
    ]);

    let instantiated = services
        .team_runtime()
        .plan(request)
        .expect("canonical Team instantiation");

    let researchers = instantiated
        .role_slots
        .iter()
        .filter(|slot| slot.role_id == "researcher")
        .collect::<Vec<_>>();
    assert_eq!(researchers.len(), 3);
    assert_eq!(
        researchers
            .iter()
            .map(|slot| slot.focus_partition.focus_id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["runtime", "storage", "surface"])
    );
    assert!(researchers.iter().all(|slot| {
        slot.definition_ref.definition_id.as_str() == "builtin/cowd/explore"
            && !slot.focus_partition.evidence_responsibility.is_empty()
    }));
    assert!(instantiated
        .cardinality_resolutions
        .iter()
        .any(|resolution| resolution.role_id == "researcher" && resolution.resolved_count == 3));
    assert!(instantiated.graph.nodes.iter().all(|node| {
        if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
            return true;
        }
        let packet = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
            .expect("AgentTask packet must decode");
        let search_required = packet.node_id().contains(":researcher:");
        let binding = packet
            .binding
            .as_ref()
            .expect("AgentTask must carry its Binding");
        let upstream_only = packet
            .constraints
            .iter()
            .any(|constraint| constraint == "upstream_evidence_only:no_tool_reacquisition");
        packet.allowed_tools == binding.tool_contract_refs
            && packet.allowed_skills == binding.skill_refs
            && if upstream_only {
                packet.allowed_tools.is_empty()
            } else {
                !packet.allowed_tools.is_empty()
                    && packet.allowed_tools.iter().any(|tool| tool == "read_file")
                    && (!search_required
                        || packet
                            .allowed_tools
                            .iter()
                            .any(|tool| tool == "grep_search" || tool == "glob_search"))
            }
    }));
}

#[test]
fn builtin_template_default_pointer_resolves_the_verified_stable_release() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut request = request(
        "cowd/parallel-research-synthesis",
        services.mission_runtime().default_mission_id(),
    );
    request.template_selector = TeamTemplateSelector::Default {
        template_id: TeamTemplateDefinitionId::new(
            DefinitionScope::Builtin,
            "cowd/parallel-research-synthesis",
        )
        .expect("builtin template id"),
    };

    let instantiated = services
        .team_runtime()
        .plan(request)
        .expect("builtin default pointer must resolve");
    assert_eq!(
        instantiated.template_ref.template_id.as_str(),
        "builtin/cowd/parallel-research-synthesis"
    );
    assert_eq!(instantiated.template_ref.revision, 2);
}

#[test]
fn whole_workspace_scope_requires_the_full_trust_flag_and_plans_when_granted() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut request = request(
        "cowd/execute-review",
        services.mission_runtime().default_mission_id(),
    );
    request.permission_ceiling = harness_contract::policy::PermissionMode::DangerFullAccess;
    request.resource_scopes = vec![
        "write:.".to_string(),
        "session:session-team-instantiation".to_string(),
    ];
    assert!(
        services.team_runtime().plan(request.clone()).is_err(),
        "whole-workspace scope must be rejected without the Runtime full-trust flag"
    );
    request.allow_whole_workspace_scope = true;
    let instantiated = services
        .team_runtime()
        .plan(request)
        .expect("full-trust whole-workspace Team must plan");
    assert!(!instantiated.graph.nodes.is_empty());
    assert!(instantiated.graph.id.contains("team"));
}

#[test]
fn strategy_bound_team_tasks_inherit_the_canonical_turn_scope() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut request = request(
        "cowd/direct-executor",
        services.mission_runtime().default_mission_id(),
    );
    let canonical_turn_id = request.lineage.turn_id.clone();
    request.strategy_binding = Some(TeamStrategyBinding {
        decision_id: "decision-team-instantiation".to_string(),
        decision_revision: 1,
        decision_lease: "lease-team-instantiation".to_string(),
        turn_ref: canonical_turn_id.clone(),
    });

    let instantiated = services
        .team_runtime()
        .plan(request)
        .expect("strategy-bound Team instantiation");

    assert!(!instantiated.task_commands.is_empty());
    assert!(instantiated
        .task_commands
        .iter()
        .all(|task| task.origin_turn_id == canonical_turn_id));
}

#[tokio::test]
async fn terminal_role_transition_commits_team_working_state_with_graph() {
    let (services, _provider) =
        canonical_agent_fixture::services_with_canonical_agent("session-team-instantiation").await;
    let projection = services
        .team_runtime()
        .instantiate(request(
            "cowd/direct-executor",
            services.mission_runtime().default_mission_id(),
        ))
        .await
        .expect("team execution");
    assert_eq!(projection.status, "completed", "{projection:?}");
    let tasks = services
        .task_runtime_port()
        .list()
        .expect("canonical Team tasks");
    assert_eq!(tasks.len(), 2, "{tasks:#?}");
    let root_task = tasks
        .iter()
        .find(|task| task.kind == harness_contract::task::TaskKind::Root)
        .expect("Team root Task");
    let delegated_task = tasks
        .iter()
        .find(|task| task.kind == harness_contract::task::TaskKind::Delegated)
        .expect("Team delegated Agent Task");
    assert_eq!(
        root_task.mission_id,
        services.mission_runtime().default_mission_id()
    );
    assert_eq!(root_task.origin_session_id, "session-team-instantiation");
    assert_eq!(root_task.origin_turn_id, "turn-team-instantiation");
    assert_eq!(root_task.root_task_id, "task:root:team-instantiation");
    assert_eq!(delegated_task.mission_id, root_task.mission_id);
    assert_eq!(
        delegated_task.origin_session_id,
        root_task.origin_session_id
    );
    assert_eq!(delegated_task.origin_turn_id, root_task.origin_turn_id);
    assert_eq!(delegated_task.root_task_id, root_task.task_id);
    assert_eq!(
        delegated_task.parent_task_id.as_deref(),
        Some(root_task.task_id.as_str())
    );
    assert_eq!(delegated_task.graph_refs.len(), 2, "{delegated_task:#?}");
    assert_eq!(delegated_task.graph_refs[0].graph_id, projection.graph_id);
    assert!(
        delegated_task.graph_refs[0].revision > 0,
        "Task must retain the registered Team graph revision instead of a placeholder"
    );
    assert!(
        delegated_task.graph_refs[1]
            .graph_id
            .starts_with("execution-graph-"),
        "canonical InProcess execution must link its child execution graph"
    );
    assert!(delegated_task.graph_refs[1].revision > 0);
    let state = services
        .team_runtime()
        .working_state("team-instantiation-test")
        .expect("durable TeamWorkingState");
    assert_eq!(state.graph_id, projection.graph_id);
    assert_eq!(state.entries.len(), 1);
    assert!(
        state.entries[0]
            .summary
            .contains("completed with evidence reference"),
        "{state:#?}"
    );
    assert!(
        !state.entries[0].producer_instance_id.is_empty(),
        "working state records the immutable producing Agent instance"
    );
    assert!(
        services.agent_runtime().list().is_empty(),
        "terminal Agent projections must leave bounded hot state"
    );
    let agent_run = services
        .agent_runtime()
        .get(&state.entries[0].producer_instance_id)
        .expect("durable terminal Agent projection");
    assert_eq!(agent_run.task_id, delegated_task.task_id);
    assert_eq!(agent_run.root_task_id, root_task.task_id);
    assert_eq!(agent_run.session_id, root_task.origin_session_id);
    assert!(
        agent_run.run_id.starts_with("team-instantiation-test:run:"),
        "Agent runtime must retain the canonical Team run identity"
    );
    assert!(state.entries[0]
        .boundary
        .contains("no raw chain-of-thought"));
    let replayed = services
        .team_runtime()
        .working_state("team-instantiation-test")
        .expect("replay working state");
    assert_eq!(
        replayed, state,
        "event replay must not duplicate semantic state"
    );
    let mut outcome_samples = 0_u64;
    for _ in 0..50 {
        services
            .outcome_projector()
            .project_available(128)
            .expect("Outcome projection");
        outcome_samples = services
            .outcome_projector()
            .snapshot()
            .segments
            .values()
            .filter(|segment| {
                segment.key.as_ref().is_some_and(|key| {
                    key.candidate == harness_contract::strategy::ExecutionCandidateKind::Team
                })
            })
            .map(|segment| segment.sample_count)
            .sum();
        if outcome_samples >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        outcome_samples >= 2,
        "Team execution must persist both Agent and Team terminal Outcomes"
    );
}
