#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use harness_contract::managed_agent::{
    ManagedAgentDefinition, ManagedAgentHealthPolicy, ManagedAgentOverlapPolicy,
    ManagedAgentRetryPolicy, ManagedAgentTarget, ManagedAgentTrigger,
};
use runtime::{ManagedAgentDispatcher, ManagedAgentInvocationStatus};

fn dispatcher() -> ManagedAgentDispatcher {
    ManagedAgentDispatcher::in_memory().expect("dispatcher")
}

fn definition() -> ManagedAgentDefinition {
    ManagedAgentDefinition {
        managed_agent_id: "workspace/cowd/fenced-watch".to_string(),
        revision: 1,
        target: ManagedAgentTarget::Agent {
            definition_id: AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/researcher")
                .expect("definition id"),
            selector: RevisionSelector::LatestApprovedStable,
        },
        trigger: ManagedAgentTrigger::Manual,
        session_id: "managed-session".to_string(),
        objective: "exercise dispatcher fencing".to_string(),
        acceptance: vec!["no duplicate run".to_string()],
        permission_lease: "read_only".to_string(),
        model_lease: "default".to_string(),
        granted_capabilities: vec![AgentCapability::Read],
        allowed_tool_contract_refs: Vec::new(),
        allowed_skill_refs: Vec::new(),
        resource_scopes: vec!["workspace".to_string()],
        overlap_policy: ManagedAgentOverlapPolicy::Forbid,
        retry_policy: ManagedAgentRetryPolicy::default(),
        health_policy: ManagedAgentHealthPolicy::default(),
        enabled: true,
    }
}

#[test]
fn recovered_claim_can_only_be_started_by_newer_dispatcher_fence() {
    let dispatcher = dispatcher();
    dispatcher
        .register_definition(definition(), 1)
        .expect("definition");
    let invocation = dispatcher
        .trigger_manual("workspace/cowd/fenced-watch", "manual-1", 2)
        .expect("invocation");
    let stale_claim = dispatcher
        .claim_ready("dispatcher-a", 3, 30_000, 1)
        .expect("claim")
        .pop()
        .expect("one claim");

    // A process crash before executor start returns only the reservation to
    // pending. It does not create a second invocation.
    let recovered_events = dispatcher.recover(4).expect("recovery");
    assert_eq!(recovered_events.len(), 1);
    assert_eq!(recovered_events[0].invocation_id, invocation.invocation_id);
    let recovered = dispatcher.invocations().expect("projection");
    assert_eq!(recovered[0].status, ManagedAgentInvocationStatus::Pending);
    let fresh_claim = dispatcher
        .claim_ready("dispatcher-b", 5, 30_000, 1)
        .expect("new claim")
        .pop()
        .expect("one new claim");
    assert!(fresh_claim.fence_generation > stale_claim.fence_generation);
    assert!(dispatcher
        .start_invocation(
            &invocation.invocation_id,
            "dispatcher-a",
            stale_claim.fence_generation,
            stale_claim
                .claim_token
                .as_deref()
                .expect("stale claim token"),
            "stale-run".to_string(),
            6,
        )
        .is_err());
    let fresh_claim_token = fresh_claim
        .claim_token
        .as_deref()
        .expect("fresh claim token");
    dispatcher
        .begin_graph_registration(
            &invocation.invocation_id,
            "dispatcher-b",
            fresh_claim.fence_generation,
            fresh_claim_token,
            "fresh-run".to_string(),
        )
        .expect("graph registration intent");
    dispatcher
        .materialize_invocation(
            &invocation.invocation_id,
            "dispatcher-b",
            fresh_claim.fence_generation,
            fresh_claim_token,
            "fresh-run".to_string(),
            "graph-receipt:fresh-run".to_string(),
        )
        .expect("graph registration receipt");
    dispatcher
        .start_invocation(
            &invocation.invocation_id,
            "dispatcher-b",
            fresh_claim.fence_generation,
            fresh_claim_token,
            "fresh-run".to_string(),
            6,
        )
        .expect("fresh start");
    let completed = dispatcher
        .complete_invocation(
            &invocation.invocation_id,
            "dispatcher-b",
            fresh_claim.fence_generation,
            true,
            7,
            None,
            vec!["evidence:fresh".to_string()],
            None,
        )
        .expect("fresh completion");
    assert_eq!(completed.status, ManagedAgentInvocationStatus::Completed);
    assert_eq!(dispatcher.invocations().expect("projection").len(), 1);
}
