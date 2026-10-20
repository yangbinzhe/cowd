use super::*;
use crate::execution_core::graph::{ExecutionNodeBinding, NodeExecutionTicket};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
    ExecutionNodeSpec,
};

#[tokio::test]
async fn own865_prepared_reuse_872_sized_canonical_body_advances_with_real_io() {
    assert_eq!(3 * 242788 + 331184 + 1804 - HISTORY_REQUEST_BYTES, 12776);
    assert!(2 * 242788 + 331184 + 1804 + 32 * 1024 < HISTORY_REQUEST_BYTES);
    let services = RuntimeServices::in_memory().unwrap();
    let objective = (0u64..5175)
        .map(|index| format!("{:x}", Sha256::digest(index.to_le_bytes())))
        .collect::<String>();
    let mut graph = ExecutionGraph::new(objective);
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    graph = services.commit_service().register_graph(graph).unwrap().graph;
    let mut restored_bytes = 0;
    let mut initial_complete = false;
    let mut last_diagnostic = serde_json::Value::Null;
    for round in 0..128 {
        let (mut budget, states, _, _) = read(&services, &graph);
        restored_bytes = serde_json::to_vec(&encode_storage_checkpoint(&states[&graph.id]).unwrap()).unwrap().len();
        last_diagnostic = serde_json::json!({"round":round,"position":states[&graph.id].position,
            "through":states[&graph.id].through,"encoded_bytes":restored_bytes,
            "stop":budget.prefix_stop,"consumed":budget.consumed_work()});
        budget.publish(&services).unwrap();
        if !budget.is_incomplete() { initial_complete = true; break; }
    }
    assert!(initial_complete, "initial original canonical prefix did not finish: {last_diagnostic}");
    assert!(restored_bytes >= 242788, "preserve the observed checkpoint scale");
    let previous_cursor = services.event_store().durable_commit_cursor().unwrap();
    let mut next = None;
    for _ in 0..128 {
        graph = services.commit_service().apply_command(&graph,
            &ExecutionGraphCommand::Start { expected_revision: graph.revision }).unwrap().graph;
        let record = services.event_store().latest_for_stream(&graph.id).unwrap().unwrap();
        if record.kind == "execution_graph.checkpoint" {
            next = Some(record);
            break;
        }
    }
    let record = next.unwrap();
    let body_bytes = record.payload["bytes"].as_u64().unwrap() as usize;
    let metadata_bytes = serde_json::to_vec(&record).unwrap().len();
    assert!(body_bytes >= 331184);
    assert!(3 * restored_bytes + body_bytes + metadata_bytes > HISTORY_REQUEST_BYTES);
    let target = services.event_store().durable_commit_cursor().unwrap();
    assert!(target > previous_cursor);
    let mut completed = false;
    for round in 0..128 {
        let (mut budget, states, _, through) = read(&services, &graph);
        let state = states[&graph.id].clone();
        last_diagnostic = serde_json::json!({"round":round,"position":state.position,
            "through":state.through,"target":target,"encoded_bytes":serde_json::to_vec(
                &encode_storage_checkpoint(&state).unwrap()).unwrap().len(),
            "stop":budget.prefix_stop,"consumed":budget.consumed_work(),
            "prefix":state.prefix.as_ref().map(|prefix| prefix.progress(
                state.through, state.position, &state.effect_proofs, state.bindings.len()))});
        let before = services.artifact_store().checkpoint_body_read_count();
        budget.publish(&services).unwrap();
        assert_eq!(services.artifact_store().checkpoint_body_read_count(), before,
            "publication must not reread the body already prepared by this request");
        if !budget.is_incomplete() {
            assert_eq!(through, target);
            assert_eq!(state.through, target);
            completed = true;
            break;
        }
    }
    assert!(completed, "a quiescent original large canonical body must eventually validate: {last_diagnostic}");
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own865_terminal_preio_deferred_preserves_memo_and_original_poison_contract() {
    let (services, graph) = fixture();
    let mut state = None;
    for _ in 0..128 {
        let (mut budget, states, _, _) = read(&services, &graph);
        let current = states[&graph.id].clone();
        budget.publish(&services).unwrap();
        if !budget.is_incomplete() { state = Some(current); break; }
    }
    let mut state = state.unwrap();
    let slot = state.bindings.keys().next().unwrap().clone();
    let ticket = state.start_tickets[&slot].clone();
    let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
    let key = format!("{}:intent", ticket.idempotency_key);
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let content_scope = format!("session:{}", graph.lineage.as_ref().unwrap().session_id);
    let prefix = HistoryPrefix {
        target_cursor: state.through, target_revision: graph.revision,
        base_position: state.position, base_through: state.through,
        history_complete: true, completed_through: 0, audit_index: 0, records: Vec::new(),
    };
    let mut first = RequestBudget::default();
    first.capture_source_fence(&services).unwrap();
    first.begin_terminal(&state, &slot, state.through, COMMIT_BYTES, &projected, &prefix, &content_scope).unwrap();
    let record = first.canonical_record(&services, &stream, &key, state.through).unwrap().unwrap();
    first.resolve_record(&services, record.as_ref().clone()).unwrap();
    let (memo, deferred) = first.finish_terminal().unwrap();
    assert!(!deferred);
    assert!(memo.records.contains_key(&record.event_id));
    state.terminal_pending = Some(memo);
    let encoded = encode_storage_checkpoint(&state).unwrap();
    let state = decode_checkpoint(6, encoded).unwrap();
    let mut next = RequestBudget::default();
    next.capture_source_fence(&services).unwrap();
    next.begin_terminal(&state, &slot, state.through, COMMIT_BYTES, &projected, &prefix, &content_scope).unwrap();
    let before = next.consumed_work();
    assert_eq!(next.canonical_record(&services, &stream, &key, state.through).unwrap().unwrap(), record);
    assert_eq!(next.consumed_work(), before);
    let metadata_bytes = serde_json::to_vec(record.as_ref()).unwrap().len();
    let cached_memo = serde_json::to_value(&next.terminal_work.as_ref().unwrap().memo).unwrap();
    let reserve = next.terminal_work.as_ref().unwrap().reserve;
    let available = next.bytes;
    next.bytes = reserve + metadata_bytes - 1;
    assert!(next.bytes >= metadata_bytes);
    let before = next.consumed_work();
    let error = next.resolve_record(&services, record.as_ref().clone()).unwrap_err();
    assert!(error.to_string().contains(INCOMPLETE));
    assert!(next.terminal_deferred);
    assert!(!next.exhausted);
    assert_eq!(next.consumed_work(), before);
    assert_eq!(serde_json::to_value(&next.terminal_work.as_ref().unwrap().memo).unwrap(), cached_memo);
    next.bytes = available;
    next.terminal_deferred = false;
    let before = next.consumed_work();
    next.resolve_record(&services, record.as_ref().clone()).unwrap();
    assert_eq!(next.consumed_work(), (before.0 + 1, before.1 + metadata_bytes));
    next.bytes = COMMIT_BYTES + 161;
    let before = next.consumed_work();
    assert!(next.canonical_record(&services, &"s".repeat(100), &"k".repeat(71), state.through).is_err());
    let (_, deferred) = next.finish_terminal().unwrap();
    assert!(deferred);
    assert!(!next.exhausted);
    assert_eq!(next.consumed_work(), before);
    let mut foreign = state.clone();
    foreign.source_fence.as_mut().unwrap().mutation_epoch += 1;
    assert!(next.begin_terminal(&foreign, &slot, state.through, COMMIT_BYTES, &projected, &prefix, &content_scope).is_err());
    assert!(next.charge(HISTORY_REQUEST_RECORDS + 1, 0).is_err());
    assert!(next.exhausted);
    assert!(next.publish(&services).is_err());
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own865_terminal_l01_sized_quiescent_bindings_resume_in_original_checkpoint() {
    let services = RuntimeServices::in_memory().unwrap();
    let objective = (0u64..2560).map(|index| format!("{:x}", Sha256::digest(index.to_le_bytes()))).collect::<String>();
    let mut graph = ExecutionGraph::new(objective);
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    for index in 0..16 {
        graph.nodes.push(ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", format!("terminal-{index}")));
    }
    graph = services.commit_service().register_graph(graph).unwrap().graph;
    let mut payload_bytes = 0;
    for (index, node) in graph.nodes.clone().into_iter().enumerate() {
        graph = services.commit_service().transition_node(&graph, &node.id, ExecutionNodeStatus::Ready, None, Vec::new()).unwrap().graph;
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(), node_id: node.id.clone(), executor_kind: node.executor_kind.clone(),
            service_class: graph.service_class, attempt: 1,
            idempotency_key: node.idempotency_key.clone(), payload_ref: node.payload_ref.clone(),
        };
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(), ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1, resource_lease_refs: Vec::new(), scope_lease_ref: None, worktree_lease_ref: None,
        };
        graph = services.commit_service().bind_and_start_nodes(&graph, BTreeMap::from([(node.id.clone(), binding)]), vec![ticket.clone()]).unwrap().graph;
        let summary = (0u64..164).map(|part| format!("{:x}", Sha256::digest(format!("{index}:{part}").as_bytes()))).collect::<String>();
        append_865_receipt(&services, &ticket, summary);
        let receipt = services.event_store().event_by_idempotency_key(
            &ExecutionNodeBinding::effect_stream_id(&ticket), &format!("{}:receipt", ticket.idempotency_key)).unwrap().unwrap();
        payload_bytes += serde_json::to_vec(&receipt.payload).unwrap().len();
        graph = services.commit_service().transition_node(&graph, &node.id, ExecutionNodeStatus::Completed, None, Vec::new()).unwrap().graph;
    }
    assert!(payload_bytes >= 166708);
    let mut canonical = None;
    for _ in 0..128 {
        let (mut budget, states, _, _) = read(&services, &graph);
        let state = states[&graph.id].clone();
        budget.publish(&services).unwrap();
        if !budget.is_incomplete() { canonical = Some(state); break; }
    }
    let mut state = canonical.unwrap();
    assert!(serde_json::to_vec(&encode_storage_checkpoint(&state).unwrap()).unwrap().len() >= 97262);
    let through = state.through;
    let slot = state.bindings.keys().next().unwrap().clone();
    let tickets = state.start_tickets.values().cloned().collect::<Vec<_>>();
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let content_scope = format!("session:{}", graph.lineage.as_ref().unwrap().session_id);
    let prefix = HistoryPrefix {
        target_cursor: through, target_revision: graph.revision,
        base_position: state.position, base_through: through,
        history_complete: true, completed_through: 0, audit_index: 0, records: Vec::new(),
    };
    let mut resumed = false;
    let mut raw_complete = false;
    for batch in 0..8 {
        let mut bounded = RequestBudget::default();
        bounded.capture_source_fence(&services).unwrap();
        if batch == 0 {
            for index in 0..210 {
                bounded.uncached_stream_head(&services, &format!("terminal-payload-preflight-{index}")).unwrap();
            }
        }
        let reserve = commit_reserve(&services, &projected, &state, &prefix, &content_scope).unwrap();
        bounded.begin_terminal(&state, &slot, through, reserve, &projected, &prefix, &content_scope).unwrap();
        let mut complete = true;
        for ticket in &tickets {
            let stream = ExecutionNodeBinding::effect_stream_id(ticket);
            let result = bounded.canonical_record(&services, &stream, &format!("{}:receipt", ticket.idempotency_key), through);
            match result {
                Ok(Some(_)) => (),
                Err(error) if error.to_string().contains(INCOMPLETE) => { complete = false; break; }
                other => panic!("unexpected original terminal payload result: {other:?}"),
            }
        }
        let (memo, deferred) = bounded.finish_terminal().unwrap();
        assert!(!bounded.exhausted);
        assert_eq!(deferred, !complete);
        state.terminal_pending = Some(memo);
        state = decode_checkpoint(6, encode_storage_checkpoint(&state).unwrap()).unwrap();
        assert!(bounded.consumed_work().0 <= HISTORY_REQUEST_RECORDS);
        assert!(bounded.consumed_work().1 <= HISTORY_REQUEST_BYTES);
        if complete {
            assert!(state.terminal_pending.as_ref().unwrap().records.len() >= 16);
            raw_complete = true;
            break;
        }
        resumed = true;
    }
    assert!(resumed && raw_complete);
    state.terminal_pending = None;
    state.terminal_proofs.clear();
    state.prefix = Some(HistoryPrefix {
        target_cursor: through, target_revision: graph.revision,
        base_position: state.position, base_through: through,
        history_complete: true, completed_through: 0, audit_index: 0, records: Vec::new(),
    });
    let projection_id = format!("projection-history:{}", graph.id);
    let revision = services.event_store().projection_checkpoint(&projection_id).unwrap().unwrap().revision;
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let content_scope = format!("session:{}", graph.lineage.as_ref().unwrap().session_id);
    let mut seed = RequestBudget::default();
    stage(&services, &projected, &mut state, through, revision, &content_scope, &mut seed).unwrap();
    seed.publish(&services).unwrap();
    let mut first = RequestBudget::default();
    first.enable_incremental();
    first.capture_source_fence(&services).unwrap();
    let scope = ExecutionProjectionScope::load_budgeted(&services, &graph.id, &projected, true, through, &mut first).unwrap();
    for index in 0..210 {
        first.uncached_stream_head(&services, &format!("terminal-preflight-{index}")).unwrap();
    }
    let states = super::super::load_scope_budgeted(&services, &scope, through, false, &mut first).unwrap();
    assert!(first.is_incomplete());
    assert!(!states[&graph.id].terminal_proofs.is_empty());
    assert!(states[&graph.id].terminal_proofs.len() < 16);
    let before = first.consumed_work();
    first.publish(&services).unwrap();
    assert_eq!(first.consumed_work(), before);
    let mut finished = false;
    for _ in 0..32 {
        let (mut budget, states, _, cut) = read(&services, &graph);
        if !budget.is_incomplete() {
            assert_eq!(states[&graph.id].terminal_proofs.len(), 16);
            let before = budget.consumed_work();
            let closure = settlement::read(&services, &states, cut, &mut budget).unwrap();
            assert_eq!(budget.consumed_work(), before);
            assert!(!closure.complete);
            assert!(settlement::unchanged(&services, &closure, &mut budget).unwrap());
            assert_eq!(budget.consumed_work(), before);
            finished = true;
        }
        let before = budget.consumed_work();
        budget.publish(&services).unwrap();
        assert_eq!(budget.consumed_work(), before);
        if finished { break; }
    }
    assert!(finished);
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

fn append_865_receipt(services: &RuntimeServices, ticket: &NodeExecutionTicket, payload: String) {
    let stream = ExecutionNodeBinding::effect_stream_id(ticket);
    let outcome = crate::execution_core::graph::NodeExecutionOutcome::new(
        harness_contract::execution_graph::ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed, result_ref: None, wait_class: None,
            summary: Some(payload), evidence_refs: Vec::new(), failure: None,
            usage: Default::default(), finished_at_ms: 1,
        });
    services.event_store().append_transaction(crate::AppendTransactionRequest {
        transaction_id: format!("865-receipt-{}", uuid::Uuid::new_v4()),
        expected_streams: vec![crate::ExpectedStreamRevision {
            stream_id: stream.clone(),
            expected_revision: services.event_store().stream_revision(&stream).unwrap(),
        }],
        events: vec![crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: stream, scope: RuntimeEventScope::ExecutionNode,
                kind: "execution.effect.receipt".into(), status: Some("completed".into()),
                actor: Some(ticket.executor_kind.clone()),
                refs: vec![crate::RuntimeEventRef { kind: "execution_node".into(), id: ticket.node_id.clone() }],
                payload: serde_json::to_value(outcome).unwrap(),
            },
            schema_version: 1, idempotency_key: Some(format!("{}:receipt", ticket.idempotency_key)),
        }],
    }).unwrap();
}

#[tokio::test]
async fn own865_bounded_lookup_deferral_is_not_absence_or_poison_and_future_cut_stays_absent() {
    let (services, graph) = fixture();
    let through = services.event_store().durable_commit_cursor().unwrap();
    let node = &graph.nodes[0];
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(), node_id: node.id.clone(), executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class, attempt: 1,
        idempotency_key: node.idempotency_key.clone(), payload_ref: node.payload_ref.clone(),
    };
    let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
    let key = format!("{}:intent", ticket.idempotency_key);
    let raw = services.event_store().event_by_idempotency_key(&stream, &key).unwrap().unwrap();
    let mut budget = RequestBudget::default();
    let reserve = budget.bytes - stream.len() - key.len() - 9;
    assert!(!budget.admit_canonical_record(&services, &stream, &key, through, reserve).unwrap());
    assert!(!budget.exhausted);
    assert!(!budget.records_by_key.contains_key(&(stream.clone(), key.clone(), through)));
    assert!(budget.admit_canonical_record(&services, &stream, &key, raw.commit_cursor - 1, 0).unwrap());
    assert!(budget.records_by_key[&(stream.clone(), key.clone(), raw.commit_cursor - 1)].is_none());
    assert!(budget.admit_canonical_record(&services, &stream, &key, through, 0).unwrap());
    assert_eq!(budget.records_by_key[&(stream, key, through)].as_deref(), Some(&raw));
    let mut strict = RequestBudget::default();
    let stream = ExecutionNodeBinding::effect_stream_id(&ticket);
    let key = format!("{}:intent", ticket.idempotency_key);
    strict.bytes = stream.len() + key.len() + 9;
    assert!(matches!(strict.canonical_record(&services, &stream, &key, through),
        Err(RuntimeServicesError::EventStore(crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)))));
    assert!(strict.exhausted);
    assert!(strict.records_by_key.is_empty());
    assert!(strict.publish(&services).is_err());
    let original = services.event_store().latest_for_stream(&graph.id).unwrap().unwrap();
    let witness = fingerprint(&original).unwrap();
    let fingerprint_before = serde_json::to_value(&witness).unwrap();
    let mut audit = RequestBudget::default();
    audit.bytes = COMMIT_BYTES + witness.stream.len() + witness.key.as_ref().unwrap().len() + 9;
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    assert!(matches!(verify_source(&services, &projected, &witness, &mut audit),
        Err(RuntimeServicesError::EventStore(crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)))));
    assert_eq!(serde_json::to_value(&witness).unwrap(), fingerprint_before);
    assert!(!audit.exhausted);
    assert!(audit.checkpoints.is_empty());
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own865_l01_sized_effects_resume_at_fixed_cut_without_unpaid_publication() {
    let services = RuntimeServices::in_memory().unwrap();
    let objective = (0u64..2560).map(|index| format!("{:x}", Sha256::digest(index.to_le_bytes()))).collect::<String>();
    let mut graph = ExecutionGraph::new(objective);
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    for index in 0..16 {
        graph.nodes.push(ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", format!("865-node-{index}")));
    }
    graph = services.commit_service().register_graph(graph).unwrap().graph;
    let mut payload_bytes = 0;
    for (index, node) in graph.nodes.clone().into_iter().enumerate() {
        graph = services.commit_service().transition_node(&graph, &node.id, ExecutionNodeStatus::Ready, None, Vec::new()).unwrap().graph;
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(), node_id: node.id.clone(), executor_kind: node.executor_kind.clone(),
            service_class: graph.service_class, attempt: 1,
            idempotency_key: node.idempotency_key.clone(), payload_ref: node.payload_ref.clone(),
        };
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(), ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1, resource_lease_refs: Vec::new(), scope_lease_ref: None, worktree_lease_ref: None,
        };
        graph = services.commit_service().bind_and_start_nodes(&graph, BTreeMap::from([(node.id, binding)]), vec![ticket.clone()]).unwrap().graph;
        let summary = (0u64..164).map(|part| format!("{:x}", Sha256::digest(format!("{index}:{part}").as_bytes()))).collect::<String>();
        append_865_receipt(&services, &ticket, summary);
        let receipt = services.event_store().event_by_idempotency_key(
            &ExecutionNodeBinding::effect_stream_id(&ticket), &format!("{}:receipt", ticket.idempotency_key)).unwrap().unwrap();
        payload_bytes += serde_json::to_vec(&receipt.payload).unwrap().len();
    }
    assert!(payload_bytes >= 166708);
    let mut canonical = None;
    for _ in 0..128 {
        let (mut budget, states, _, _) = read(&services, &graph);
        let state = states[&graph.id].clone();
        budget.publish(&services).unwrap();
        if !budget.is_incomplete() { canonical = Some(state); break; }
    }
    let mut state = canonical.expect("canonical source must finish through bounded original reads");
    assert_eq!(state.bindings.len(), 16);
    assert!(serde_json::to_vec(&encode_storage_checkpoint(&state).unwrap()).unwrap().len() >= 97262);
    let fixed = state.through;
    state.effect_proofs.clear();
    state.prefix = Some(HistoryPrefix {
        target_cursor: fixed, target_revision: graph.revision,
        base_position: state.position, base_through: fixed,
        history_complete: true, completed_through: 0, audit_index: 0, records: Vec::new(),
    });
    let projection_id = format!("projection-history:{}", graph.id);
    let saved = services.event_store().projection_checkpoint(&projection_id).unwrap().unwrap();
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let scope_name = format!("session:{}", graph.lineage.as_ref().unwrap().session_id);
    let mut seed = RequestBudget::default();
    stage(&services, &projected, &mut state, fixed, saved.revision, &scope_name, &mut seed).unwrap();
    seed.publish(&services).unwrap();
    let mut budget = RequestBudget::default();
    budget.enable_incremental();
    budget.capture_source_fence(&services).unwrap();
    let scope = ExecutionProjectionScope::load_budgeted(&services, &graph.id, &projected, true, fixed, &mut budget).unwrap();
    for index in 0..180 {
        budget.uncached_stream_head(&services, &format!("865-real-preflight-head-{index}")).unwrap();
    }
    let first = super::super::load_scope_budgeted(&services, &scope, fixed, false, &mut budget).unwrap();
    assert!(budget.is_incomplete());
    let progress = &first[&graph.id];
    assert!(progress.effect_proofs.len() > 0 && progress.effect_proofs.len() < 16);
    assert_eq!(progress.prefix.as_ref().unwrap().target_cursor, fixed);
    let unchanged = progress.effect_proofs.iter().map(|(slot, proof)| (slot.clone(), proof.identity.clone())).collect::<BTreeMap<_, _>>();
    let before_publish = budget.consumed_work();
    budget.publish(&services).unwrap();
    assert_eq!(budget.consumed_work(), before_publish);
    services.event_store().append(crate::RuntimeEventInput {
        stream_id: "865-unrelated-global".into(), scope: RuntimeEventScope::Session,
        kind: "865.unrelated".into(), status: None, actor: None, refs: Vec::new(), payload: serde_json::json!({}),
    }).unwrap();
    let mut complete = false;
    for _ in 0..16 {
        let (mut next, states, _, through) = read(&services, &graph);
        let state = &states[&graph.id];
        for (slot, identity) in &unchanged { assert_eq!(&state.effect_proofs[slot].identity, identity); }
        next.publish(&services).unwrap();
        if !next.is_incomplete() {
            assert_eq!(state.effects_checked_through, Some(through));
            assert_eq!(state.effect_proofs.len(), 16);
            assert!(!state.effect_missing_receipt);
            complete = true;
            break;
        }
    }
    assert!(complete);
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own865_old_cut_missing_receipt_is_not_reused_as_new_cut_absence() {
    let (services, graph) = fixture();
    let mut previous = None;
    for _ in 0..32 {
        let (mut budget, states, _, _) = read(&services, &graph);
        let state = states[&graph.id].clone();
        budget.publish(&services).unwrap();
        if !budget.is_incomplete() { previous = Some(state); break; }
    }
    let previous = previous.unwrap();
    let slot = previous.effect_proofs.keys().next().unwrap().clone();
    assert!(previous.effect_proofs[&slot].missing_receipt);
    let ticket = &previous.start_tickets[&slot];
    append_865_receipt(&services, ticket, "865 late real receipt".into());
    let (mut budget, states, _, through) = read(&services, &graph);
    assert!(!budget.is_incomplete());
    let proof = &states[&graph.id].effect_proofs[&slot];
    assert_eq!(proof.through, through);
    assert!(proof.receipt_present);
    assert!(!proof.missing_receipt);
    budget.publish(&services).unwrap();
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[test]
fn own843_record_exhaustion_preserves_typed_error_and_reports_charge_source() {
    let mut budget = RequestBudget::default();
    budget.charge(HISTORY_REQUEST_RECORDS, 0).unwrap();
    let failure = budget.charge(1, 0).unwrap_err();
    let RuntimeServicesError::EventStore(crate::RuntimeEventStoreError::HistoryBudgetExceeded(message)) = failure else {
        panic!("record exhaustion lost its typed contract");
    };
    assert!(message.starts_with("projection request work: requested_records=1, remaining_records=0,"));
    assert!(message.contains("caller="));
    assert!(message.contains("history_prefix_tests.rs:"));
    assert!(budget.exhausted);
    assert!(budget.checkpoints.is_empty());
    assert!(budget.prepaid_checkpoints.is_empty());
    assert!(budget.prepaid_heads.is_empty());
    assert_eq!(budget.bytes, HISTORY_REQUEST_BYTES);
}

#[tokio::test]
async fn own843_final_source_recheck_does_not_trust_persisted_audit() {
    let services = RuntimeServices::in_memory().unwrap();
    let mut graph = ExecutionGraph::new("843 final source recheck");
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    let graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let raw = services
        .event_store()
        .latest_for_stream(&graph.id)
        .unwrap()
        .unwrap();
    let witness = fingerprint(&raw).unwrap();
    let mut budget = RequestBudget::default();
    verify_source(&services, &projected, &witness, &mut budget).unwrap();
    let mut stale = witness.clone();
    stale.digest = "sha256:stale".into();
    assert!(verify_source(&services, &projected, &stale, &mut budget).is_err());
    let source = budget.read_source_fence(&services).unwrap();
    budget.require_source_fence(&services, &source).unwrap();
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own843_pending_publication_is_prepaid_across_roots_for_inline_and_artifact() {
    for large in [false, true] {
        let services = RuntimeServices::in_memory().unwrap();
        let objective = if large {
            (0u64..2048)
                .map(|index| format!("{:x}", Sha256::digest(index.to_le_bytes())))
                .collect()
        } else {
            "843 inline prepaid checkpoint".to_string()
        };
        let mut graphs = Vec::new();
        for _ in 0..2 {
            let mut graph = ExecutionGraph::new(objective.clone());
            crate::test_support::attach_execution_graph_lineage(&mut graph);
            graphs.push(
                services
                    .commit_service()
                    .register_graph(graph)
                    .unwrap()
                    .graph,
            );
        }
        let through = services.event_store().durable_commit_cursor().unwrap();
        let first = &graphs[0];
        let projected = services.graph_state_store().projection(&first.id).unwrap();
        let mut state = HistoryState {
            source_fence: Some(services.event_store().source_mutation_fence().unwrap()),
            identity: "843-cumulative-prepaid".into(),
            through,
            graph_state: Some(first.clone()),
            ..Default::default()
        };
        let bytes = serde_json::to_vec(&encode_storage_checkpoint(&state).unwrap())
            .unwrap()
            .len();
        let content_scope = format!("session:{}", first.lineage.as_ref().unwrap().session_id);
        let pending = pending_checkpoint(&state, &first.id, through, 0, &content_scope).unwrap();
        let reserve = RequestBudget::publication_reserve(&services, &pending, &first.id).unwrap();
        let allowance = if large {
            reserve + reserve / 2
        } else {
            2 * reserve
        };
        assert!(allowance < HISTORY_REQUEST_BYTES);
        if large {
            assert!(bytes > 64 * 1024);
        }
        let mut budget = RequestBudget::default();
        budget.charge(0, HISTORY_REQUEST_BYTES - allowance).unwrap();
        let before = budget.bytes;
        stage(
            &services,
            &projected,
            &mut state,
            through,
            0,
            &content_scope,
            &mut budget,
        )
        .unwrap();
        let projection_id = format!("projection-history:{}", first.id);
        let payload = &budget.prepaid_checkpoints[&projection_id].payload;
        assert_eq!(
            payload["body"]["body"]["kind"].as_str().unwrap(),
            if large { "artifact_ref" } else { "inline" }
        );
        let payload_bytes = serde_json::to_vec(payload).unwrap().len();
        let (_, validation_bytes) = crate::runtime_event_store::content::history_checkpoint_metadata_reserve(&pending.encoded).unwrap();
        let stage_head_bytes = first.id.len() + projected.revision.to_string().len();
        let prepaid_head_bytes = first.id.len() + 20;
        let source = state.source_fence.as_ref().unwrap();
        let source_bytes = serde_json::to_vec(source).unwrap().len();
        let source_probes = source_bytes * 2;
        assert_eq!(
            before - budget.bytes,
            bytes + payload_bytes + validation_bytes + stage_head_bytes + prepaid_head_bytes + source_probes
        );
        let second = &graphs[1];
        let projected_second = services.graph_state_store().projection(&second.id).unwrap();
        state.graph_state = Some(second.clone());
        if large {
            assert!(!room(&budget, reserve));
            assert!(stage(
                &services,
                &projected_second,
                &mut state,
                through,
                0,
                &content_scope,
                &mut budget
            )
            .is_err());
            assert!(!budget.exhausted);
            assert_eq!(budget.checkpoints.len(), 1);
            assert_eq!(budget.prepaid_checkpoints.len(), 1);
        } else {
            stage(
                &services,
                &projected_second,
                &mut state,
                through,
                0,
                &content_scope,
                &mut budget,
            )
            .unwrap();
        }
        let accounted = budget.consumed_work();
        budget.publish(&services).unwrap();
        assert_eq!(budget.consumed_work(), accounted);
        assert!(services
            .event_store()
            .projection_checkpoint(&projection_id)
            .unwrap()
            .is_some());
        assert_eq!(
            services
                .event_store()
                .projection_checkpoint(&format!("projection-history:{}", second.id))
                .unwrap()
                .is_some(),
            !large
        );
        assert!(accounted.0 <= HISTORY_REQUEST_RECORDS && accounted.1 <= HISTORY_REQUEST_BYTES);
        let mut poisoned = RequestBudget::default();
        state.graph_state = Some(first.clone());
        let saved_revision = services
            .event_store()
            .projection_checkpoint(&projection_id)
            .unwrap()
            .unwrap()
            .revision;
        stage(
            &services,
            &projected,
            &mut state,
            through,
            saved_revision,
            &content_scope,
            &mut poisoned,
        )
        .unwrap();
        assert!(poisoned.charge(0, HISTORY_REQUEST_BYTES + 1).is_err());
        assert!(poisoned.prepaid_checkpoints.is_empty());
        assert!(poisoned.prepaid_heads.is_empty());
        assert!(poisoned.publish(&services).is_err());
        assert_eq!(
            services
                .event_store()
                .projection_checkpoint(&projection_id)
                .unwrap()
                .unwrap()
                .revision,
            saved_revision
        );
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }
}

#[tokio::test]
async fn own843_first_goal_rebuilds_same_checkpoint_without_accepting_foreign_scope() {
    let services = RuntimeServices::in_memory().unwrap();
    let mut graph = ExecutionGraph::new("843 first Goal discovery");
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    let graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    let (mut first, states, _, _) = read(&services, &graph);
    assert!(!first.is_incomplete());
    let old_identity = states[&graph.id].identity.clone();
    first.publish(&services).unwrap();
    let goal_id = format!("goal:{}", graph.id);
    let goal = serde_json::from_value(serde_json::json!({
        "id":goal_id,"session_id":graph.lineage.as_ref().unwrap().session_id,
        "objective":"843 externally attached Goal",
        "criteria":[{"id":"843-goal-criterion","statement":"visible after checkpoint","status":"open"}],
        "phase":"execution","scope":"internal","completion":"open",
        "revision":1,"user_sequence":1,"spec_revision":1,"spec_digest":"843-first-goal"
    })).unwrap();
    crate::execution_core::GoalStore::new(services.event_store().clone())
        .create(goal)
        .unwrap();
    let (mut second, states, scope, through) = read(&services, &graph);
    assert!(!second.is_incomplete());
    assert_ne!(states[&graph.id].identity, old_identity);
    assert_eq!(scope.goals.first().unwrap().id, goal_id);
    assert_eq!(states[&graph.id].through, through);
    assert_eq!(
        states[&graph.id].graph_state.as_ref().unwrap().revision,
        graph.revision
    );
    second.publish(&services).unwrap();
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let mut foreign = scope;
    foreign.turn_id = Some("843-foreign-turn".into());
    let mut budget = RequestBudget::default();
    budget.enable_incremental();
    assert!(load(&services, &foreign, &projected, through, &mut budget).is_err());
    assert!(budget.checkpoints.is_empty());
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own843_two_roots_prebody_stop_publishes_first_then_finishes_second() {
    let services = RuntimeServices::in_memory().unwrap();
    let mut graphs = Vec::new();
    for _ in 0..2 {
        let mut graph = ExecutionGraph::new("x".repeat(600 * 1024));
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graphs.push(
            services
                .commit_service()
                .register_graph(graph)
                .unwrap()
                .graph,
        );
    }
    let through = services.event_store().durable_commit_cursor().unwrap();
    let projected = services
        .graph_state_store()
        .projection(&graphs[0].id)
        .unwrap();
    let mut discovery = RequestBudget::default();
    let mut scope = ExecutionProjectionScope::load_budgeted(
        &services,
        &graphs[0].id,
        &projected,
        true,
        through,
        &mut discovery,
    )
    .unwrap();
    scope.execution_ids = graphs.iter().map(|graph| graph.id.clone()).collect();
    let ordered: Vec<_> = scope.execution_ids.iter().cloned().collect();
    let mut complete = false;
    for round in 0..4 {
        let mut budget = RequestBudget::default();
        budget.enable_incremental();
        let states =
            super::super::load_scope_budgeted(&services, &scope, through, false, &mut budget)
                .unwrap();
        if round == 0 {
            assert!(budget.is_incomplete());
            assert!(states[&ordered[0]].prefix.is_none());
            assert!(states[&ordered[1]].incomplete);
            assert!(states[&ordered[1]].graph_state.is_none());
            assert!(!budget
                .checkpoints
                .contains_key(&format!("projection-history:{}", ordered[1])));
        }
        complete = !budget.is_incomplete();
        budget.publish(&services).unwrap();
        assert!(budget.consumed_work().0 <= HISTORY_REQUEST_RECORDS);
        assert!(budget.consumed_work().1 <= HISTORY_REQUEST_BYTES);
        if complete {
            break;
        }
    }
    assert!(complete);
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

fn fixture() -> (std::sync::Arc<RuntimeServices>, ExecutionGraph) {
    let services = RuntimeServices::in_memory().unwrap();
    let mut graph = ExecutionGraph::new("canonical blob objective ".repeat(16 * 1024))
        .with_lineage(ExecutionGraphLineage {
            session_id: "843-session".into(),
            turn_id: "843-turn".into(),
            root_task_id: "843-task".into(),
            task_id: "843-task".into(),
            generation: 1,
        });
    for index in 0..4 {
        graph.nodes.push(ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            format!("{{\"node\":{index}}}"),
        ));
    }
    graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    for node in graph.nodes[..2].to_vec() {
        graph = services
            .commit_service()
            .transition_node(
                &graph,
                &node.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: node.executor_kind.clone(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        let binding = ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: Vec::new(),
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        graph = services
            .commit_service()
            .bind_and_start_nodes(
                &graph,
                BTreeMap::from([(node.id.clone(), binding)]),
                vec![ticket],
            )
            .unwrap()
            .graph;
        let mut checkpoint = false;
        for _ in 0..128 {
            if services
                .event_store()
                .latest_for_stream(&graph.id)
                .unwrap()
                .unwrap()
                .kind
                == "execution_graph.checkpoint"
            {
                checkpoint = true;
                break;
            }
            graph = services
                .commit_service()
                .apply_command(
                    &graph,
                    &ExecutionGraphCommand::Start {
                        expected_revision: graph.revision,
                    },
                )
                .unwrap()
                .graph;
        }
        assert!(checkpoint, "original large body checkpoint must exist");
    }
    for _ in 0..5 {
        graph = services
            .commit_service()
            .apply_command(
                &graph,
                &ExecutionGraphCommand::Start {
                    expected_revision: graph.revision,
                },
            )
            .unwrap()
            .graph;
    }
    (services, graph)
}

fn read(
    services: &RuntimeServices,
    graph: &ExecutionGraph,
) -> (
    RequestBudget,
    BTreeMap<String, HistoryState>,
    ExecutionProjectionScope,
    u64,
) {
    let mut budget = RequestBudget::default();
    budget.enable_incremental();
    budget.capture_source_fence(services).unwrap();
    let through = services.event_store().durable_commit_cursor().unwrap();
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let scope = ExecutionProjectionScope::load_budgeted(
        services,
        &graph.id,
        &projected,
        true,
        through,
        &mut budget,
    )
    .unwrap();
    let states =
        super::super::load_scope_budgeted(services, &scope, through, false, &mut budget).unwrap();
    assert!(budget.consumed_work().0 <= HISTORY_REQUEST_RECORDS);
    assert!(budget.consumed_work().1 <= HISTORY_REQUEST_BYTES);
    (budget, states, scope, through)
}

#[tokio::test]
async fn own843_original_large_bodies_finish_across_bounded_requests_with_unrelated_appends() {
    let (services, graph) = fixture();
    let original_source = services.event_store().source_mutation_fence().unwrap();
    let mut incomplete_seen = false;
    let mut previous = None;
    let mut complete = false;
    for _ in 0..16 {
        let (mut budget, states, scope, through) = read(&services, &graph);
        let state = &states[&graph.id];
        assert_eq!(state.source_fence.as_ref(), Some(&original_source));
        assert!(previous.is_none_or(|position| state.position >= Some(position)));
        previous = state.position;
        if budget.is_incomplete() {
            incomplete_seen = true;
            let prefix = state.prefix.as_ref().unwrap();
            assert_eq!(prefix.audit_index, prefix.records.len());
            let projected = services.graph_state_store().projection(&graph.id).unwrap();
            let axes = reducer_support::execution_terminal_axes(
                &services,
                &projected,
                &scope,
                &states,
                through,
                &mut budget,
            )
            .unwrap();
            assert!(!axes.execution.coverage_complete);
            assert_eq!(
                axes.execution.phase,
                harness_contract::projection::TerminalExecutionPhase::Unknown
            );
            assert_eq!(
                axes.execution.settlement,
                harness_contract::projection::TerminalEffectSettlement::Unknown
            );
            assert!(state.through <= state.prefix.as_ref().unwrap().target_cursor);
        } else {
            assert!(state.prefix.is_none());
            assert_eq!(state.bindings.len(), 2);
            assert_eq!(state.start_specs.len(), 2);
            assert_eq!(state.start_tickets.len(), 2);
            assert_eq!(state.start_positions.len(), 2);
            assert_eq!(state.effects_checked_through, Some(through));
            assert_eq!(state.graph_state.as_ref().unwrap().revision, graph.revision);
            complete = true;
        }
        budget.publish(&services).unwrap();
        let saved = services.event_store().projection_checkpoint(
            &format!("projection-history:{}", graph.id),
        ).unwrap().unwrap();
        assert_eq!(saved.payload["source_fence"], serde_json::to_value(&original_source).unwrap());
        assert_eq!(saved.payload["incomplete"], budget.is_incomplete());
        assert!(budget.consumed_work().0 <= HISTORY_REQUEST_RECORDS);
        assert!(budget.consumed_work().1 <= HISTORY_REQUEST_BYTES);
        if complete {
            break;
        }
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "843-unrelated-global-cursor".into(),
                scope: RuntimeEventScope::Session,
                kind: "843.unrelated".into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({}),
            })
            .unwrap();
    }
    assert!(incomplete_seen && complete);
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own843_prefix_is_not_a_strict_fact_delta_or_activity_detail() {
    let (services, graph) = fixture();
    let (mut budget, _, scope, through) = read(&services, &graph);
    assert!(budget.is_incomplete());
    budget.publish(&services).unwrap();
    let error = super::super::load_scope_budgeted(
        &services,
        &scope,
        through,
        false,
        &mut RequestBudget::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("incomplete"));
    let context = harness_contract::projection::ProjectionQueryContext {
        principal: "843-test".into(),
        workspace_id: services.workspace_key().into(),
        session_scopes: Vec::new(),
        mission_scopes: Vec::new(),
        visibility_grants: vec![
            format!("workspace:{}", services.workspace_key()),
            "resource:*".into(),
        ],
        detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
        authorization_revision: 1,
    };
    let delta =
        crate::execution_projection::delta(&services, &graph.id, graph.revision, through, &context)
            .unwrap();
    assert_eq!(
        delta.resync_reason,
        Some(harness_contract::projection::ProjectionResyncReason::UnsafeMaterialization)
    );
    assert!(delta.operations.is_empty());
    let detail = crate::execution_projection::activity_detail(
        &services,
        &graph.id,
        "843-not-yet-covered",
        &context,
    )
    .await
    .unwrap_err();
    assert!(detail.to_string().contains("incomplete"));
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own843_poison_cannot_publish_and_same_slot_cas_conflict_is_not_swallowed() {
    let (services, graph) = fixture();
    let (mut poisoned, _, _, _) = read(&services, &graph);
    assert!(poisoned.is_incomplete());
    assert!(poisoned.charge(0, HISTORY_REQUEST_BYTES + 1).is_err());
    assert!(poisoned.publish(&services).is_err());
    assert!(services
        .event_store()
        .projection_checkpoint(&format!("projection-history:{}", graph.id))
        .unwrap()
        .is_none());
    let (mut first, _, _, _) = read(&services, &graph);
    let (mut second, _, _, _) = read(&services, &graph);
    first.publish(&services).unwrap();
    assert!(matches!(
        second.publish(&services),
        Err(RuntimeServicesError::EventStore(
            crate::RuntimeEventStoreError::StaleRevision { .. }
        ))
    ));
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own843_declared_extent_and_graph_race_are_not_safe_yield() {
    let (services, mut graph) = fixture();
    let mut raw = services
        .event_store()
        .list_stream_after(&graph.id, 0, 1, 1, HISTORY_REQUEST_BYTES)
        .unwrap()
        .remove(0);
    assert!(body_bytes(&raw).unwrap() > 256 * 1024);
    raw.payload["bytes"] = serde_json::json!(HISTORY_REQUEST_BYTES + 1);
    assert!(body_bytes(&raw).is_err());
    raw.payload.as_object_mut().unwrap().remove("bytes");
    assert!(body_bytes(&raw).is_err());
    let (mut pending, _, _, _) = read(&services, &graph);
    graph = services
        .commit_service()
        .apply_command(
            &graph,
            &ExecutionGraphCommand::Start {
                expected_revision: graph.revision,
            },
        )
        .unwrap()
        .graph;
    assert!(graph.revision > 1);
    assert!(pending.publish(&services).is_err());
    assert!(services
        .event_store()
        .projection_checkpoint(&format!("projection-history:{}", graph.id))
        .unwrap()
        .is_none());
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own843_malformed_or_foreign_generation_prefix_is_rejected_not_rebuilt() {
    let (services, graph) = fixture();
    let (mut pending, _, scope, through) = read(&services, &graph);
    assert!(pending.is_incomplete());
    pending.publish(&services).unwrap();
    let projection_id = format!("projection-history:{}", graph.id);
    let saved = services
        .event_store()
        .projection_checkpoint(&projection_id)
        .unwrap()
        .unwrap();
    let content_scope = saved.payload["scope"].as_str().unwrap();
    let origin = crate::PreparedArtifactOrigin::CheckpointBody {
        projection_id: projection_id.clone(),
        source_cursor: saved.source_cursor,
    };
    let value = services
        .event_store()
        .resolve_content(content_scope, &saved.payload["body"], &origin)
        .unwrap();
    let baseline = decode_checkpoint(6, value).unwrap();
    for fault in ["audit", "position", "generation", "source", "missing", "effect_owner", "effect_cut", "terminal_owner", "terminal_cut", "terminal_pending"] {
        let mut changed = baseline.clone();
        match fault {
            "audit" => changed.prefix.as_mut().unwrap().audit_index = usize::MAX,
            "position" => changed.position.as_mut().unwrap().0 += 1,
            "source" => changed.source_fence.as_mut().unwrap().mutation_epoch += 1,
            "missing" => changed.source_fence = None,
            "effect_owner" => {
                changed.effect_proofs.insert("foreign-effect-slot".into(), HistoryEffectProof {
                    identity: "foreign".into(), through: saved.source_cursor, references: Vec::new(),
                    missing_receipt: false, receipt_present: true,
                });
            }
            "effect_cut" => changed.prefix.as_mut().unwrap().completed_through = saved.source_cursor + 1,
            "terminal_owner" | "terminal_cut" => {
                let slot = changed.bindings.keys().next().unwrap().clone();
                let identity = if fault == "terminal_owner" { "foreign-terminal-owner".into() }
                    else { effect_proof_identity(&changed, &slot).unwrap() };
                changed.terminal_proofs.insert(slot, TerminalBindingProof {
                    identity, through: saved.source_cursor + 1, read: settlement::ClosureRead::default(),
                });
            }
            "terminal_pending" => changed.terminal_pending = Some(TerminalMemo {
                slot: "foreign-terminal-slot".into(), identity: "foreign".into(),
                through: saved.source_cursor, ..Default::default()
            }),
            _ => {
                changed
                    .graph_state
                    .as_mut()
                    .unwrap()
                    .lineage
                    .as_mut()
                    .unwrap()
                    .generation += 1
            }
        }
        let encoded = encode_storage_checkpoint(&changed).unwrap();
        let body = services
            .event_store()
            .prepare_content(
                content_scope,
                &format!("843-fault-{fault}"),
                &encoded,
                &origin,
            )
            .unwrap();
        let payload = serde_json::json!({"schema":6, "scope":content_scope, "body":body,
            "target_cursor":saved.source_cursor, "verified_through":changed.through, "incomplete":true,
            "source_fence":changed.source_fence});
        let expected = services
            .event_store()
            .projection_checkpoint(&projection_id)
            .unwrap()
            .unwrap()
            .revision;
        let write = services
            .event_store()
            .compare_and_put_projection_checkpoint(
                &projection_id,
                saved.source_cursor,
                expected,
                &payload,
                0,
            );
        if fault == "missing" {
            assert!(write.is_err());
            continue;
        }
        write.unwrap();
        let mut budget = RequestBudget::default();
        budget.enable_incremental();
        assert!(
            super::super::load_scope_budgeted(&services, &scope, through, false, &mut budget)
                .is_err(),
            "{fault}"
        );
        assert!(budget.checkpoints.is_empty());
    }
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn own843_hot_checkpoint_above_28k_uses_actual_publication_budget() {
    let services = RuntimeServices::in_memory().unwrap();
    let objective = (0u64..1024)
        .map(|index| format!("{:x}", Sha256::digest(index.to_le_bytes())))
        .collect::<String>();
    let mut graph = ExecutionGraph::new(objective);
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    let graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    let projected = services.graph_state_store().projection(&graph.id).unwrap();
    let through = services.event_store().durable_commit_cursor().unwrap();
    let mut state = HistoryState {
        source_fence: Some(services.event_store().source_mutation_fence().unwrap()),
        identity: "843-hot-reserve".into(),
        through,
        graph_state: Some(graph.clone()),
        ..Default::default()
    };
    let encoded = encode_storage_checkpoint(&state).unwrap();
    let bytes = serde_json::to_vec(&encoded).unwrap().len();
    assert!(bytes > (COMMIT_BYTES - 8 * 1024) / 2);
    let session = graph.lineage.as_ref().unwrap().session_id.clone();
    let content_scope = format!("session:{session}");
    let pending = pending_checkpoint(&state, &graph.id, through, 0, &content_scope).unwrap();
    let reserve = RequestBudget::publication_reserve(&services, &pending, &graph.id).unwrap();
    assert!(reserve > COMMIT_BYTES && reserve < HISTORY_REQUEST_BYTES);
    let mut too_small = RequestBudget::default();
    too_small
        .charge(0, HISTORY_REQUEST_BYTES - reserve + 1)
        .unwrap();
    assert!(stage(
        &services,
        &projected,
        &mut state,
        through,
        0,
        &content_scope,
        &mut too_small
    )
    .is_err());
    assert!(too_small.checkpoints.is_empty());
    assert!(!too_small.exhausted);
    let mut budget = RequestBudget::default();
    stage(
        &services,
        &projected,
        &mut state,
        through,
        0,
        &content_scope,
        &mut budget,
    )
    .unwrap();
    budget.publish(&services).unwrap();
    assert!(budget.consumed_work().0 <= HISTORY_REQUEST_RECORDS);
    assert!(budget.consumed_work().1 <= HISTORY_REQUEST_BYTES);
    let saved = services
        .event_store()
        .projection_checkpoint(&format!("projection-history:{}", graph.id))
        .unwrap()
        .unwrap();
    assert_eq!(saved.payload["incomplete"], false);
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}
