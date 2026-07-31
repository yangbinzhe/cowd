#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use harness_contract::managed_agent::{
    ManagedAgentDefinition, ManagedAgentEventOrderPolicy, ManagedAgentEventTrigger,
    ManagedAgentHealthPolicy, ManagedAgentOverlapPolicy, ManagedAgentRetryPolicy,
    ManagedAgentTarget, ManagedAgentTrigger, ManagedAgentTriggerEvent,
};
use runtime::ManagedAgentDispatcher;

fn dispatcher() -> ManagedAgentDispatcher {
    ManagedAgentDispatcher::in_memory().expect("dispatcher")
}

fn definition() -> ManagedAgentDefinition {
    ManagedAgentDefinition {
        managed_agent_id: "workspace/cowd/source-watch".to_string(),
        revision: 1,
        target: ManagedAgentTarget::Agent {
            definition_id: AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/researcher")
                .expect("definition id"),
            selector: RevisionSelector::LatestApprovedStable,
        },
        trigger: ManagedAgentTrigger::Event(ManagedAgentEventTrigger {
            source_id: "feishu".to_string(),
            source_kind: "surface".to_string(),
            event_type: "message.received".to_string(),
            required_source_capabilities: vec!["surface.event.receive".to_string()],
            required_attributes: BTreeMap::from([("channel".to_string(), "ops".to_string())]),
            maximum_age_ms: Some(10_000),
            out_of_order_policy: ManagedAgentEventOrderPolicy::RejectOlderSequence,
        }),
        session_id: "managed-session".to_string(),
        objective: "process accepted source event".to_string(),
        acceptance: vec!["event evidence".to_string()],
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "default".to_string(),
        granted_capabilities: vec![AgentCapability::Read],
        allowed_tool_contract_refs: Vec::new(),
        allowed_skill_refs: Vec::new(),
        resource_scopes: vec!["workspace".to_string()],
        overlap_policy: ManagedAgentOverlapPolicy::AllowParallel { max_concurrent: 4 },
        retry_policy: ManagedAgentRetryPolicy::default(),
        health_policy: ManagedAgentHealthPolicy::default(),
        enabled: true,
    }
}

fn event(id: &str, sequence: u64) -> ManagedAgentTriggerEvent {
    ManagedAgentTriggerEvent {
        event_id: id.to_string(),
        source_id: "feishu".to_string(),
        source_kind: "surface".to_string(),
        event_type: "message.received".to_string(),
        subject: "thread-1".to_string(),
        payload_ref: format!("surface-event:{id}"),
        payload_digest: format!("sha256:{id}"),
        occurred_at_ms: 100,
        source_sequence: Some(sequence),
        idempotency_key: format!("feishu:{id}"),
        source_capabilities: vec!["surface.event.receive".to_string()],
        attributes: BTreeMap::from([("channel".to_string(), "ops".to_string())]),
        trace_refs: vec![format!("trace:{id}")],
    }
}

#[test]
fn event_trigger_requires_source_capability_filter_and_monotonic_sequence() {
    let dispatcher = dispatcher();
    dispatcher
        .register_definition(definition(), 1)
        .expect("definition");

    let first = dispatcher
        .accept_event(event("new", 8), 101)
        .expect("first event");
    assert_eq!(first.accepted.len(), 1);
    let duplicate = dispatcher
        .accept_event(event("new", 8), 102)
        .expect("duplicate event");
    assert_eq!(
        duplicate.accepted[0].invocation_id,
        first.accepted[0].invocation_id
    );

    let stale = dispatcher
        .accept_event(event("old", 7), 103)
        .expect("stale event report");
    assert!(stale.accepted.is_empty());
    assert_eq!(stale.rejected.len(), 1);

    let mut untrusted = event("untrusted", 9);
    untrusted.source_capabilities.clear();
    let ignored = dispatcher
        .accept_event(untrusted, 104)
        .expect("ignored report");
    assert!(ignored.accepted.is_empty());
    assert!(ignored.rejected.is_empty());
    assert_eq!(dispatcher.invocations().expect("invocations").len(), 1);
}
