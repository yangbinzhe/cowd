#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
    DefinitionScope, RevisionSelector,
};
use harness_contract::managed_agent::{
    ManagedAgentDefinition, ManagedAgentHealthPolicy, ManagedAgentOverlapPolicy,
    ManagedAgentRetryPolicy, ManagedAgentTarget, ManagedAgentTrigger,
};
use harness_contract::mission::ScheduleTrigger;
use harness_contract::team::{TeamTemplateDefinitionId, TeamTemplateSelector};
use runtime::{
    AgentBackendCapabilities, AgentBackendKind, AgentModelSelection, AgentRuntimeBackend,
    ManagedAgentDispatcher, ManagedAgentInvocationStatus, RuntimeServices,
};

fn dispatcher() -> ManagedAgentDispatcher {
    ManagedAgentDispatcher::in_memory().expect("dispatcher")
}

fn definition(trigger: ManagedAgentTrigger) -> ManagedAgentDefinition {
    ManagedAgentDefinition {
        managed_agent_id: "workspace/cowd/research-watch".to_string(),
        revision: 1,
        target: ManagedAgentTarget::Agent {
            definition_id: AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/researcher")
                .expect("definition id"),
            selector: RevisionSelector::LatestApprovedStable,
        },
        trigger,
        session_id: "managed-session".to_string(),
        objective: "collect a bounded evidence update".to_string(),
        acceptance: vec!["evidence".to_string()],
        permission_lease: "read_only".to_string(),
        model_lease: "default".to_string(),
        granted_capabilities: vec![AgentCapability::Read],
        allowed_tool_contract_refs: Vec::new(),
        allowed_skill_refs: Vec::new(),
        resource_scopes: vec!["read:crates/runtime".to_string()],
        overlap_policy: ManagedAgentOverlapPolicy::Forbid,
        retry_policy: ManagedAgentRetryPolicy::default(),
        health_policy: ManagedAgentHealthPolicy::default(),
        enabled: true,
    }
}

fn services_with_provider() -> Arc<RuntimeServices> {
    let root = tempfile::tempdir().expect("temporary runtime root").keep();
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
            runtime::ProviderRegistry::new(providers).expect("provider registry"),
        ))
        .build()
        .expect("runtime services")
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
            harness_contract::context::EvidenceRef::new(
                "tool",
                format!("materialized:{}", packet.node_id()),
            ),
            "a".repeat(64),
            1,
            "application/json",
            "artifact://art_managed_agent",
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
                "summary": "managed Agent completed through Runtime binding",
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

#[test]
fn manual_and_schedule_triggers_create_durable_invocations() {
    let dispatcher = dispatcher();
    dispatcher
        .register_definition(definition(ManagedAgentTrigger::Manual), 10)
        .expect("manual definition");
    let manual = dispatcher
        .trigger_manual("workspace/cowd/research-watch", "operator-1", 11)
        .expect("manual invocation");
    assert_eq!(manual.status, ManagedAgentInvocationStatus::Pending);
    assert_eq!(manual.attempt_no, 1);

    let mut scheduled = definition(ManagedAgentTrigger::Schedule {
        trigger: ScheduleTrigger::Interval { every_ms: 100 },
    });
    scheduled.managed_agent_id = "workspace/cowd/scheduled-watch".to_string();
    dispatcher
        .register_definition(scheduled, 0)
        .expect("schedule definition");
    assert!(dispatcher
        .accept_due_schedules(99)
        .expect("before due")
        .accepted
        .is_empty());
    let due = dispatcher.accept_due_schedules(100).expect("due schedule");
    assert_eq!(due.accepted.len(), 1);
    assert!(matches!(
        due.accepted[0].trigger,
        runtime::ManagedAgentInvocationTrigger::Schedule { due_at_ms: 100 }
    ));
}

#[test]
fn terminal_failures_open_the_health_circuit_until_human_reset() {
    let dispatcher = dispatcher();
    let mut managed = definition(ManagedAgentTrigger::Manual);
    managed.health_policy = ManagedAgentHealthPolicy {
        max_consecutive_failures: 1,
        max_run_age_ms: None,
    };
    dispatcher
        .register_definition(managed, 1)
        .expect("definition");
    let invocation = dispatcher
        .trigger_manual("workspace/cowd/research-watch", "first", 2)
        .expect("trigger");
    let claim = dispatcher
        .claim_ready("dispatcher-a", 3, 1)
        .expect("claim")
        .pop()
        .expect("one claim");
    dispatcher
        .start_invocation(
            &claim.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            "run:first".to_string(),
            4,
        )
        .expect("start");
    let failed = dispatcher
        .complete_invocation(
            &invocation.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            false,
            5,
            None,
            Vec::new(),
            Some("model unavailable".to_string()),
        )
        .expect("complete");
    assert_eq!(failed.status, ManagedAgentInvocationStatus::Failed);
    assert!(dispatcher
        .trigger_manual("workspace/cowd/research-watch", "second", 6)
        .is_err());
    let health = dispatcher
        .reset_health("workspace/cowd/research-watch")
        .expect("human reset");
    assert_eq!(health.consecutive_failures, 0);
    assert!(dispatcher
        .trigger_manual("workspace/cowd/research-watch", "second", 7)
        .is_ok());
}

#[tokio::test]
async fn runtime_dispatch_executes_a_bound_definition_without_gateway_scheduler_state() {
    let services = services_with_provider();
    services
        .agent_runtime()
        .register_backend(Arc::new(CompletedBackend));
    let mut managed = definition(ManagedAgentTrigger::Manual);
    managed.managed_agent_id = "workspace/cowd/runtime-dispatch".to_string();
    managed.target = ManagedAgentTarget::Agent {
        definition_id: AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct")
            .expect("builtin agent definition"),
        selector: RevisionSelector::LatestApprovedStable,
    };
    services
        .register_managed_agent(managed)
        .expect("Runtime owns definition registration");
    services
        .trigger_managed_agent_manual("workspace/cowd/runtime-dispatch", "operator-request")
        .expect("Runtime creates durable invocation");

    let admitted = services
        .dispatch_managed_agents("runtime-test-dispatcher", 4)
        .await
        .expect("Runtime dispatch");

    assert_eq!(admitted.claimed.len(), 1);
    assert_eq!(admitted.submitted.len(), 1);
    assert!(admitted.completed.is_empty());
    assert!(admitted.failed.is_empty());
    let graph_id = admitted.submitted[0]
        .execution_ref
        .clone()
        .expect("submitted graph id");
    services
        .execution_supervisor()
        .wait_for_quiescence(&graph_id)
        .await
        .expect("managed graph completes");
    let reconciled = services
        .dispatch_managed_agents("runtime-test-dispatcher", 4)
        .await
        .expect("Runtime reconciliation");
    assert_eq!(reconciled.completed.len(), 1);
    assert_eq!(
        reconciled.completed[0].status,
        ManagedAgentInvocationStatus::Completed
    );
    assert!(reconciled.completed[0]
        .execution_ref
        .as_deref()
        .is_some_and(|reference| reference.starts_with("managed-agent:")));
}

#[tokio::test]
async fn runtime_dispatch_uses_team_instantiation_for_managed_team_targets() {
    let services = services_with_provider();
    services
        .agent_runtime()
        .register_backend(Arc::new(CompletedBackend));
    let mut managed = definition(ManagedAgentTrigger::Manual);
    managed.managed_agent_id = "workspace/cowd/runtime-team-dispatch".to_string();
    managed.target = ManagedAgentTarget::Team {
        template_id: TeamTemplateDefinitionId::new(
            DefinitionScope::Builtin,
            "cowd/direct-executor",
        )
        .expect("builtin Team template"),
        selector: TeamTemplateSelector::LatestStable {
            template_id: TeamTemplateDefinitionId::new(
                DefinitionScope::Builtin,
                "cowd/direct-executor",
            )
            .expect("builtin Team template"),
        },
    };
    // A Team owns its resolved role grants. Allowing the Managed wrapper to
    // carry direct Agent grants would create a second authorization path.
    managed.granted_capabilities.clear();
    managed.allowed_tool_contract_refs.clear();
    managed.allowed_skill_refs.clear();
    services
        .register_managed_agent(managed)
        .expect("Runtime owns Team definition registration");
    services
        .trigger_managed_agent_manual("workspace/cowd/runtime-team-dispatch", "operator-request")
        .expect("Runtime creates Team invocation");

    let admitted = services
        .dispatch_managed_agents("runtime-test-dispatcher", 4)
        .await
        .expect("Runtime Team dispatch");
    assert_eq!(admitted.submitted.len(), 1, "{admitted:?}");
    assert!(admitted.completed.is_empty(), "{admitted:?}");
    assert!(admitted.failed.is_empty(), "{admitted:?}");
    let graph_id = admitted.submitted[0]
        .execution_ref
        .clone()
        .expect("submitted Team graph id");
    services
        .execution_supervisor()
        .wait_for_quiescence(&graph_id)
        .await
        .expect("managed Team graph completes");
    let reconciled = services
        .dispatch_managed_agents("runtime-test-dispatcher", 4)
        .await
        .expect("Runtime Team reconciliation");
    assert_eq!(reconciled.completed.len(), 1, "{reconciled:?}");
    assert!(reconciled.failed.is_empty(), "{reconciled:?}");
    assert_eq!(
        reconciled.completed[0].status,
        ManagedAgentInvocationStatus::Completed
    );
    assert!(reconciled.completed[0]
        .evidence_refs
        .iter()
        .any(|reference| reference.starts_with("team-graph:")));
}
