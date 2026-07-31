#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::{
    AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus, DefinitionScope,
};
use harness_contract::team::{
    FocusPartitionPlan, FocusPartitionSlot, RoleCardinalityPolicy, TeamInstantiationRequest,
    TeamRoleCardinalityOverride, TeamSelectionMode, TeamTemplateDefinitionId, TeamTemplateSelector,
};
use runtime::{
    AgentBackendCapabilities, AgentBackendKind, AgentModelSelection, AgentRuntimeBackend,
    RuntimeServices,
};

fn services_with_provider() -> Arc<RuntimeServices> {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let root = root.keep();
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let providers = model_protocol::provider_config::ProvidersConfig {
        providers: HashMap::from([(
            "test".to_string(),
            model_protocol::provider_config::ProviderConfig {
                name: "test".to_string(),
                base_url: "https://example.test/v1".to_string(),
                api_key: "test".to_string(),
                models: vec!["default".to_string()],
                protocol: Some("responses".to_string()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    RuntimeServices::builder(&root, &workspace)
        .provider_registry(Arc::new(
            runtime::ProviderRegistry::new(providers).expect("provider registry"),
        ))
        .build()
        .expect("runtime services")
}

fn request(template_id: &str, mission_id: &str) -> TeamInstantiationRequest {
    TeamInstantiationRequest {
        request_id: "team-instantiation-test".to_string(),
        team_id: "team-instantiation-test".to_string(),
        session_id: "session-team-instantiation".to_string(),
        mission_id: mission_id.to_string(),
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
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        permission_lease: "read_only".to_string(),
        model_lease: "default".to_string(),
        budget_lease: None,
        managed_invocation: None,
        resource_scopes: vec![
            "read:crates/runtime".to_string(),
            "session:session-team-instantiation".to_string(),
        ],
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
        !packet.allowed_tools.is_empty()
            && packet.allowed_tools == binding.tool_contract_refs
            && packet.allowed_tools.iter().any(|tool| tool == "read_file")
            && (!search_required
                || packet
                    .allowed_tools
                    .iter()
                    .any(|tool| tool == "grep_search" || tool == "glob_search"))
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
    assert_eq!(instantiated.template_ref.revision, 1);
}

struct CompletedBackend;

#[async_trait]
impl AgentRuntimeBackend for CompletedBackend {
    fn kind(&self) -> AgentBackendKind {
        AgentBackendKind::InProcess
    }

    fn capabilities(&self) -> AgentBackendCapabilities {
        AgentBackendCapabilities::in_process()
    }

    async fn execute(
        &self,
        packet: AgentTaskPacket,
        selection: AgentModelSelection,
    ) -> Result<AgentReturnPacket, String> {
        let mut evidence_refs = packet.evidence_refs.clone();
        evidence_refs.push(harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed(
                "tool",
                format!("materialized:{}", packet.node_id()),
            ),
            "a".repeat(64),
            1,
            "application/json",
            "artifact://art_team_instantiation",
            format!("session:{}", packet.session_id()),
        ));
        Ok(AgentReturnPacket {
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            session_id: packet.session_id().to_string(),
            mission_id: packet.mission_id().to_string(),
            team_id: packet.team_id().map(ToString::to_string),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status: AgentTerminalStatus::Completed,
            outcome: serde_json::json!({
                "summary": "completed with evidence reference",
                "evidence": "materialized durable tool evidence"
            })
            .to_string(),
            acceptance: packet.acceptance,
            evidence_refs,
            changes: Vec::new(),
            runtime_change_receipts: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 1,
            output_tokens: 1,
            cached_tokens: 0,
            model: selection.model,
            provider: selection.provider,
            tool_calls: 1,
            duplicate_tool_calls: 0,
            runtime_write_attempt_paths: Vec::new(),
            runtime_observed_resource_scopes: Vec::new(),
            failure: None,
        })
    }
}

#[tokio::test]
async fn terminal_role_transition_commits_team_working_state_with_graph() {
    let services = services_with_provider();
    services
        .agent_runtime()
        .register_backend(Arc::new(CompletedBackend));
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
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].mission_id,
        services.mission_runtime().default_mission_id()
    );
    assert_eq!(tasks[0].source_session_id, "session-team-instantiation");
    assert_eq!(tasks[0].source_turn_id, "team-instantiation-test");
    assert_eq!(tasks[0].graph_refs.len(), 1);
    assert_eq!(tasks[0].graph_refs[0].graph_id, projection.graph_id);
    assert!(
        tasks[0].graph_refs[0].revision > 0,
        "Task must retain the registered graph revision instead of a placeholder"
    );
    let mission = services
        .mission_runtime()
        .aggregate(services.mission_runtime().default_mission_id())
        .expect("Team execution must materialize its Mission aggregate");
    assert!(
        mission
            .team_run_refs
            .iter()
            .any(|reference| reference.id == "team-instantiation-test"),
        "normal Team execution must link its run into the canonical Mission"
    );
    assert_eq!(
        mission.agent_run_refs.len(),
        1,
        "the Team's Agent execution must link its run into the same Mission"
    );
    assert!(
        mission.agent_run_refs[0]
            .id
            .starts_with("team-instantiation-test:run:"),
        "Mission must retain the canonical Agent run identity"
    );

    let state = services
        .team_runtime()
        .working_state("team-instantiation-test")
        .expect("durable TeamWorkingState");
    assert_eq!(state.graph_id, projection.graph_id);
    assert_eq!(state.entries.len(), 1);
    assert!(state.entries[0]
        .summary
        .contains("completed with evidence reference"));
    assert!(
        !state.entries[0].producer_instance_id.is_empty(),
        "working state records the immutable producing Agent instance"
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
