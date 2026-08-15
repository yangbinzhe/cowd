#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use harness_contract::managed_agent::{
    ManagedAgentDefinition, ManagedAgentHealthPolicy, ManagedAgentOverlapPolicy,
    ManagedAgentRetryPolicy, ManagedAgentTarget, ManagedAgentTrigger,
};
use harness_contract::mission::ScheduleTrigger;
use harness_contract::team::{TeamTemplateDefinitionId, TeamTemplateSelector};
use runtime::{ManagedAgentDispatcher, ManagedAgentInvocationStatus, RuntimeServices};

#[path = "support/canonical_agent_fixture.rs"]
mod canonical_agent_fixture;

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
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "deepseek-v4-flash".to_string(),
        granted_capabilities: vec![AgentCapability::Read],
        allowed_tool_contract_refs: vec!["read_file".to_string()],
        allowed_skill_refs: Vec::new(),
        resource_scopes: vec!["read:crates/runtime".to_string()],
        overlap_policy: ManagedAgentOverlapPolicy::Forbid,
        retry_policy: ManagedAgentRetryPolicy::default(),
        health_policy: ManagedAgentHealthPolicy::default(),
        enabled: true,
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
        .claim_ready("dispatcher-a", 3, 30_000, 1)
        .expect("claim")
        .pop()
        .expect("one claim");
    let claim_token = claim.claim_token.as_deref().expect("claim token");
    dispatcher
        .begin_graph_registration(
            &claim.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            claim_token,
            "run:first".to_string(),
        )
        .expect("graph registration intent");
    dispatcher
        .materialize_invocation(
            &claim.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            claim_token,
            "run:first".to_string(),
            "graph-receipt:run:first".to_string(),
        )
        .expect("graph registration receipt");
    dispatcher
        .start_invocation(
            &claim.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            claim_token,
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
    let (services, _provider) =
        canonical_agent_fixture::services_with_canonical_agent("managed-session").await;
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

    assert_eq!(admitted.claimed.len(), 1, "{admitted:#?}");
    assert_eq!(admitted.submitted.len(), 1, "{admitted:#?}");
    assert!(admitted.completed.is_empty());
    assert!(admitted.failed.is_empty());
    let canonical_task = services
        .task_runtime_port()
        .get(&format!(
            "managed-run:{}:1:fence:1:task",
            admitted.submitted[0].invocation_id
        ))
        .expect("canonical managed Agent Task lookup")
        .expect("canonical managed Agent Task");
    assert!(canonical_task.execution_policy.binding.is_some());
    let graph_id = admitted.submitted[0]
        .execution_ref
        .clone()
        .expect("submitted graph id");
    services
        .execution_supervisor()
        .wait_for_quiescence(&graph_id)
        .await
        .expect("managed graph completes");
    let completed =
        await_terminal_projection(&services, &admitted.submitted[0].invocation_id).await;
    let graph = services
        .graph_state_store()
        .load(&graph_id)
        .expect("managed Agent graph projection");
    assert_eq!(
        completed.status,
        ManagedAgentInvocationStatus::Completed,
        "{completed:#?}\n{graph:#?}"
    );
    assert!(completed
        .execution_ref
        .as_deref()
        .is_some_and(|reference| reference.starts_with("managed-agent:")));
}

#[tokio::test]
async fn runtime_dispatch_uses_team_instantiation_for_managed_team_targets() {
    let (services, _provider) =
        canonical_agent_fixture::services_with_canonical_agent("managed-session").await;
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
    let canonical_root_task = services
        .task_runtime_port()
        .get(&format!(
            "managed-root-task:{}",
            admitted.submitted[0].invocation_id
        ))
        .expect("canonical managed Team root Task lookup")
        .expect("canonical managed Team root Task");
    assert!(canonical_root_task.execution_policy.binding.is_some());
    let graph_id = admitted.submitted[0]
        .execution_ref
        .clone()
        .expect("submitted Team graph id");
    services
        .execution_supervisor()
        .wait_for_quiescence(&graph_id)
        .await
        .expect("managed Team graph completes");
    let completed =
        await_terminal_projection(&services, &admitted.submitted[0].invocation_id).await;
    let graph = services
        .graph_state_store()
        .load(&graph_id)
        .expect("managed Team graph projection");
    assert_eq!(
        completed.status,
        ManagedAgentInvocationStatus::Completed,
        "{completed:#?}\n{graph:#?}"
    );
    assert!(completed
        .evidence_refs
        .iter()
        .any(|reference| reference.starts_with("team-graph:")));
}

async fn await_terminal_projection(
    services: &Arc<RuntimeServices>,
    invocation_id: &str,
) -> runtime::ManagedAgentInvocation {
    for _ in 0..100 {
        let invocation = services
            .managed_agents()
            .invocations()
            .expect("managed projection")
            .into_iter()
            .find(|current| current.invocation_id == invocation_id)
            .expect("submitted invocation remains queryable");
        if !invocation.status.is_active() {
            return invocation;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("graph terminal observer did not project Managed invocation terminal state");
}
