#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use harness_contract::managed_agent::{
    ManagedAgentDefinition, ManagedAgentHealthPolicy, ManagedAgentOverlapPolicy,
    ManagedAgentRetryPolicy, ManagedAgentTarget, ManagedAgentTrigger,
};
use runtime::{FencedEffectStatus, ManagedAgentDispatcher};

fn dispatcher() -> ManagedAgentDispatcher {
    ManagedAgentDispatcher::in_memory().expect("dispatcher")
}

fn definition() -> ManagedAgentDefinition {
    ManagedAgentDefinition {
        managed_agent_id: "workspace/cowd/effect-watch".to_string(),
        revision: 1,
        target: ManagedAgentTarget::Agent {
            definition_id: AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/researcher")
                .expect("definition id"),
            selector: RevisionSelector::LatestApprovedStable,
        },
        trigger: ManagedAgentTrigger::Manual,
        session_id: "managed-session".to_string(),
        objective: "exercise fenced effect".to_string(),
        acceptance: vec!["single write".to_string()],
        permission_lease: "workspace_write".to_string(),
        model_lease: "default".to_string(),
        granted_capabilities: vec![AgentCapability::Read, AgentCapability::Write],
        allowed_tool_contract_refs: vec!["tool/write_file".to_string()],
        allowed_skill_refs: Vec::new(),
        resource_scopes: vec!["workspace".to_string()],
        overlap_policy: ManagedAgentOverlapPolicy::Forbid,
        retry_policy: ManagedAgentRetryPolicy::default(),
        health_policy: ManagedAgentHealthPolicy::default(),
        enabled: true,
    }
}

#[test]
fn completed_effect_is_never_claimed_or_executed_twice() {
    let dispatcher = dispatcher();
    dispatcher
        .register_definition(definition(), 1)
        .expect("definition");
    let invocation = dispatcher
        .trigger_manual("workspace/cowd/effect-watch", "manual-1", 2)
        .expect("invocation");
    let claim = dispatcher
        .claim_ready("dispatcher-a", 3, 1)
        .expect("claim")
        .pop()
        .expect("one claim");
    dispatcher
        .start_invocation(
            &invocation.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            "run".to_string(),
            4,
        )
        .expect("start");
    let queued = dispatcher
        .enqueue_effect(
            &invocation.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            "write-result",
            "workspace_write".to_string(),
            "idempotency:write-result".to_string(),
            "request:write-result".to_string(),
            5,
        )
        .expect("enqueue");
    assert_eq!(queued.status, FencedEffectStatus::Pending);
    dispatcher
        .claim_effect(
            &invocation.invocation_id,
            "write-result",
            claim.fence_generation,
            "dispatcher-a",
        )
        .expect("claim effect");
    let completed = dispatcher
        .complete_effect(
            &invocation.invocation_id,
            "write-result",
            claim.fence_generation,
            "dispatcher-a",
            "receipt:write-result".to_string(),
        )
        .expect("complete effect");
    assert_eq!(completed.status, FencedEffectStatus::Completed);

    let replay = dispatcher
        .enqueue_effect(
            &invocation.invocation_id,
            "dispatcher-a",
            claim.fence_generation,
            "write-result",
            "workspace_write".to_string(),
            "idempotency:write-result".to_string(),
            "request:write-result".to_string(),
            6,
        )
        .expect("same effect replay");
    assert_eq!(replay.status, FencedEffectStatus::Completed);
    assert!(dispatcher
        .claim_effect(
            &invocation.invocation_id,
            "write-result",
            claim.fence_generation,
            "dispatcher-a",
        )
        .is_err());
    assert_eq!(dispatcher.outbox().expect("outbox").len(), 1);
}
