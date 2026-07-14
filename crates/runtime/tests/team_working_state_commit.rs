#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::{
    AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus, DefinitionScope,
};
use harness_contract::team::{
    TeamInstantiationRequest, TeamSelectionMode, TeamTemplateDefinitionId, TeamTemplateSelector,
};
use runtime::{
    AgentBackendCapabilities, AgentBackendKind, AgentModelSelection, AgentRuntimeBackend,
    RuntimeServices,
};

fn services_with_provider() -> Arc<RuntimeServices> {
    let root = tempfile::tempdir().expect("runtime root").keep();
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
            },
        )]),
    };
    RuntimeServices::builder(&root, &workspace)
        .provider_registry(Arc::new(
            runtime::ProviderRegistry::new(providers).expect("provider"),
        ))
        .build()
        .expect("runtime")
}

fn request() -> TeamInstantiationRequest {
    TeamInstantiationRequest {
        request_id: "working-state-commit".to_string(),
        team_id: "team:working-state-commit".to_string(),
        session_id: "session:working-state-commit".to_string(),
        mission_id: None,
        parent_execution: None,
        selection_mode: TeamSelectionMode::Explicit,
        template_selector: TeamTemplateSelector::LatestStable {
            template_id: TeamTemplateDefinitionId::new(
                DefinitionScope::Builtin,
                "cowd/direct-executor",
            )
            .unwrap(),
        },
        objective: "Produce an evidence-backed bounded conclusion.".to_string(),
        acceptance: vec!["summary".to_string(), "evidence".to_string()],
        risk: None,
        role_binding_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        permission_lease: "read_only".to_string(),
        model_lease: "default".to_string(),
        budget_lease: None,
        managed_invocation: None,
        resource_scopes: vec!["session:working-state-commit".to_string()],
    }
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
        Ok(AgentReturnPacket {
            run_id: packet.run_id,
            agent_id: packet.agent_id,
            task_id: packet.task_id,
            session_id: packet.session_id,
            mission_id: packet.mission_id,
            team_id: packet.team_id,
            graph_id: packet.graph_id,
            node_id: packet.node_id,
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status: AgentTerminalStatus::Completed,
            outcome: "completed with evidence".to_string(),
            acceptance: vec!["summary".to_string(), "evidence".to_string()],
            evidence_refs: Vec::new(),
            changes: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 1,
            output_tokens: 1,
            model: selection.model,
            provider: selection.provider,
            tool_calls: 0,
            failure: None,
        })
    }
}

#[tokio::test]
async fn terminal_graph_transition_commits_exactly_one_replayable_team_working_state_entry() {
    let services = services_with_provider();
    services
        .agent_runtime()
        .register_backend(Arc::new(CompletedBackend));
    let projection = services
        .team_runtime()
        .instantiate(request())
        .await
        .expect("team completion");
    assert_eq!(projection.status, "completed");

    let state = services
        .team_runtime()
        .working_state("team:working-state-commit")
        .expect("working state");
    assert_eq!(state.graph_id, projection.graph_id);
    assert_eq!(state.entries.len(), 1);
    assert!(!state.entries[0].producer_instance_id.is_empty());
    assert!(state.entries[0]
        .boundary
        .contains("no raw chain-of-thought"));
    assert_eq!(
        services
            .team_runtime()
            .working_state("team:working-state-commit")
            .unwrap(),
        state,
        "event replay is idempotent"
    );
}
