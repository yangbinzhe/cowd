use super::*;

#[tokio::test]
async fn own791_actual_retirement_rejects_unrelated_malformed_index_without_program_or_goal_append()
{
    for fault in 0..5 {
        let fixture = NativeRetirementFixture::new(&format!("791-retirement-{fault}")).await;
        fixture.cancel_while_holding_settlement().await;
        let services = &fixture.physical.services;
        let packet = &fixture.worker.packet;
        let source = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt,
        );
        let revision = services.event_store().stream_revision(&source).unwrap();
        let mut event = RuntimeTransactionEventInput {
            event: RuntimeEventInput {
                stream_id: source.clone(),
                scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.agent_tool.receipt".into(),
                status: None,
                actor: Some("governed_tool".into()),
                refs: vec![],
                payload: serde_json::json!({
                    "idempotency_key":"791-unrelated-negative-only",
                    "effect_kind":"read",
                    "outcome":{"status":"executed"},
                }),
            },
            idempotency_key: Some("agent-tool-receipt:791-unrelated-negative-only".into()),
            schema_version: 1,
        };
        match fault {
            0 => event.schema_version = 2,
            1 => event.event.scope = RuntimeEventScope::Session,
            2 => event.event.actor = Some("791-foreign".into()),
            3 => event.idempotency_key = Some("agent-tool-receipt:791-borrowed".into()),
            4 => event.event.kind = "execution.agent_tool.unclassified".into(),
            _ => unreachable!(),
        }
        services
            .event_store()
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("791-retirement-negative:{fault}"),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: source,
                    expected_revision: revision,
                }],
                events: vec![event],
            })
            .unwrap();
        assert_rejected_without_program_mutation(
            services,
            &fixture.worker,
            "receipt-index structural",
        );
        services.shutdown_maintenance().await;
        services.shutdown_execution().await;
    }
}

#[tokio::test]
async fn own791_actual_retirement_index_race_cannot_partially_withdraw() {
    let fixture = NativeRetirementFixture::new("791-retirement-index-cas").await;
    fixture.cancel_while_holding_settlement().await;
    let services = &fixture.physical.services;
    let packet = &fixture.worker.packet;
    let source = format!(
        "execution-agent-receipts:{}:{}:{}",
        packet.graph_id(),
        packet.node_id(),
        packet.attempt,
    );
    let before = services
        .agent_action_service()
        .project(&fixture.worker.root_actor.program_id)
        .unwrap();
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let revision = services.event_store().stream_revision(&source).unwrap();
    {
        let mut control = fixture.backend.control.lock().unwrap();
        control.hold_settlement = false;
        control.race_source = Some(source.clone());
    }
    let error = services
        .agent_action_service()
        .apply(&settlement_envelope(&fixture.worker))
        .unwrap_err();
    assert!(
        matches!(error, crate::AgentActionServiceError::Store(
        RuntimeEventStoreError::StaleRevision { ref stream_id, expected, actual }
    ) if stream_id == &source && expected == revision && actual == revision + 1),
        "{error}"
    );
    assert_eq!(
        services
            .agent_action_service()
            .project(&before.program_id)
            .unwrap(),
        before
    );
    assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
    assert_pending(services, &fixture.worker);
    assert!(services
        .event_store()
        .event_by_idempotency_key(
            &format!("agentic-program:{}", before.program_id),
            &format!(
                "agent-action:{}",
                settlement_envelope(&fixture.worker).action_id
            ),
        )
        .unwrap()
        .is_none());
    assert_eq!(fixture.backend.race_hits.load(Ordering::SeqCst), 1);
    services.shutdown_maintenance().await;
    services.shutdown_execution().await;
}
