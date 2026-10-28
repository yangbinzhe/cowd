use super::*;

const COMMIT_BYTES: usize = 64 * 1024;
pub(super) const COMMIT_RECORDS: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryPrefix {
    target_cursor: u64,
    target_revision: u64,
    base_position: Option<(u64, u32)>,
    base_through: u64,
    history_complete: bool,
    #[serde(default)]
    completed_through: u64,
    audit_index: usize,
    records: Vec<SourceRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRecord {
    position: (u64, u32),
    stream: String,
    key: Option<String>,
    digest: String,
    metadata_bytes: usize,
}

impl HistoryPrefix {
    pub(super) fn progress(&self, through: u64, position: Option<(u64, u32)>,
        effects: &BTreeMap<String, HistoryEffectProof>, total: usize) -> serde_json::Value {
        serde_json::json!({
            "status": "incomplete", "verified_through": through, "position": position,
            "target_cursor": self.target_cursor, "target_graph_revision": self.target_revision,
            "phase": if !self.history_complete { "canonical_prefix" } else { "effects_pending" },
            "source_records": self.records.len(), "verified_source_records": self.audit_index,
            "effect_cut": self.target_cursor, "effect_bindings_total": total,
            "completed_effect_cut": self.completed_through,
            "effect_bindings_verified": effects.values().filter(|proof| proof.through == self.target_cursor).count(),
        })
    }
}

fn invalid(message: &str) -> RuntimeServicesError {
    RuntimeServicesError::Invariant(format!("history prefix: {message}"))
}

fn fingerprint(record: &DurableRuntimeEvent) -> Result<SourceRecord, RuntimeServicesError> {
    let encoded = serde_json::to_vec(record).map_err(|_| invalid("record encoding"))?;
    Ok(SourceRecord {
        position: (record.commit_cursor, record.transaction_index),
        stream: record.stream_id.clone(),
        key: record.idempotency_key.clone(),
        digest: format!("sha256:{:x}", Sha256::digest(&encoded)),
        metadata_bytes: encoded.len(),
    })
}

fn pending_checkpoint(
    state: &HistoryState,
    graph_id: &str,
    target: u64,
    expected_revision: u64,
    content_scope: &str,
) -> Result<PendingCheckpoint, RuntimeServicesError> {
    Ok(PendingCheckpoint {
        schema: 6,
        source_fence: state.source_fence.clone(),
        projection_id: format!("projection-history:{graph_id}"),
        content_scope: content_scope.into(),
        identity: state.identity.clone(),
        through: target,
        expected_revision,
        encoded: encode_storage_checkpoint(state)?,
    })
}

pub(super) fn commit_reserve(
    services: &RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    state: &HistoryState,
    prefix: &HistoryPrefix,
    content_scope: &str,
) -> Result<usize, RuntimeServicesError> {
    let mut staged = state.clone();
    staged.prefix = Some(prefix.clone());
    let pending = pending_checkpoint(&staged, &graph.graph_id, prefix.target_cursor, 0, content_scope)?;
    Ok(COMMIT_BYTES.max(RequestBudget::publication_reserve(services, &pending, &graph.graph_id)?))
}

pub(super) fn commit_reserve_delta(
    services: &RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    state: &HistoryState,
    prefix: &HistoryPrefix,
    content_scope: &str,
    budget: &mut RequestBudget,
) -> Result<usize, RuntimeServicesError> {
    let mut staged = state.clone();
    staged.prefix = Some(prefix.clone());
    let pending = pending_checkpoint(&staged, &graph.graph_id, prefix.target_cursor, 0, content_scope)?;
    let records = RequestBudget::publication_records(&pending)?;
    let bytes = RequestBudget::publication_reserve(services, &pending, &graph.graph_id)?;
    budget.checkpoint_records = records;
    Ok(COMMIT_BYTES.max(bytes.saturating_sub(budget.checkpoint_base_bytes)))
}

pub(super) fn commit_record_reserve(
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    state: &HistoryState,
    prefix: &HistoryPrefix,
    content_scope: &str,
) -> Result<usize, RuntimeServicesError> {
    let mut staged = state.clone();
    staged.prefix = Some(prefix.clone());
    RequestBudget::publication_records(&pending_checkpoint(
        &staged, &graph.graph_id, prefix.target_cursor, 0, content_scope,
    )?)
}

fn publication_fits(
    services: &RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    state: &HistoryState,
    prefix: &HistoryPrefix,
    content_scope: &str,
    budget: &mut RequestBudget,
) -> Result<bool, RuntimeServicesError> {
    let bytes = commit_reserve_delta(services, graph, state, prefix, content_scope, budget)?;
    Ok(bytes <= budget.bytes && budget.checkpoint_records <= budget.records)
}

fn room(budget: &RequestBudget, reserve: usize) -> bool {
    budget.records > budget.checkpoint_records + 2 && budget.bytes > reserve.saturating_add(16 * 1024)
}

fn body_bytes(record: &DurableRuntimeEvent) -> Result<usize, RuntimeServicesError> {
    let envelope = if record.scope == RuntimeEventScope::ExecutionGraph {
        &record.payload
    } else if record.kind == "goal.observation" {
        &record.payload["observation_body"]
    } else {
        return Ok(0);
    };
    let bytes = envelope["bytes"]
        .as_u64()
        .ok_or_else(|| invalid("unknown body extent"))?;
    let bytes = usize::try_from(bytes).map_err(|_| invalid("body extent overflow"))?;
    if bytes > HISTORY_REQUEST_BYTES - COMMIT_BYTES {
        return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            "history prefix indivisible body".into(),
        )
        .into());
    }
    match envelope["body"]["kind"].as_str() {
        Some("inline") => Ok(0),
        Some("artifact_ref") => Ok(bytes),
        _ => Err(invalid("unknown body representation")),
    }
}

fn verify_source(
    services: &RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    witness: &SourceRecord,
    budget: &mut RequestBudget,
) -> Result<(), RuntimeServicesError> {
    budget.charge(
        1,
        witness.stream.len() + witness.key.as_deref().map_or(0, str::len),
    )?;
    let record = if let Some(key) = witness.key.as_deref() {
        budget.charge(0, 8)?;
        let limit = budget.bytes.saturating_sub(COMMIT_BYTES);
        match services.event_store().bounded_event_by_idempotency_key(
            &witness.stream, key, witness.position.0, limit)? {
            crate::RuntimeBoundedEventLookup::Absent => {
                return Err(invalid("verified original record disappeared"));
            }
            crate::RuntimeBoundedEventLookup::Deferred { required_bytes } => {
                return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
                    format!("history witness deferred: requested_bytes={required_bytes}, available_bytes={limit}"),
                ).into());
            }
            crate::RuntimeBoundedEventLookup::Record { record, encoded_bytes } => {
                budget.charge(1, 0)?;
                if serde_json::to_vec(&record).map_err(|_| invalid("witness encoding"))?.len() != encoded_bytes
                    || encoded_bytes > limit {
                    return Err(invalid("bounded witness extent mismatch"));
                }
                *record
            }
        }
    } else {
        let before = if witness.position.1 > 0 {
            (witness.position.0, witness.position.1 - 1)
        } else {
            (
                witness
                    .position
                    .0
                    .checked_sub(1)
                    .ok_or_else(|| invalid("zero original cursor"))?,
                u32::MAX,
            )
        };
        let mut page = services
            .event_store()
            .position_page(
                RuntimeEventScope::ExecutionGraph,
                &graph.graph_id,
                None,
                Some(before),
                witness.position.0,
                1,
                budget.bytes.saturating_sub(COMMIT_BYTES),
            )
            .map_err(error)?;
        if page.len() != 1 {
            return Err(invalid("verified original position disappeared"));
        }
        page.remove(0)
    };
    budget.charge(1, 0)?;
    budget.charge_value(&record)?;
    let current = fingerprint(&record)?;
    if current.position != witness.position
        || current.stream != witness.stream
        || current.key != witness.key
        || current.digest != witness.digest
    {
        return Err(invalid("verified original record changed"));
    }
    Ok(())
}

#[track_caller]
fn stage(
    services: &RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    state: &mut HistoryState,
    source_cursor: u64,
    expected_revision: u64,
    content_scope: &str,
    budget: &mut RequestBudget,
) -> Result<(), RuntimeServicesError> {
    budget.charge(0, 0)?;
    if state.graph_state.is_none() {
        return Err(invalid("cannot persist a prefix without original planned Graph"));
    }
    let pending = pending_checkpoint(state, &graph.graph_id, source_cursor, expected_revision, content_scope)?;
    let reserve = RequestBudget::publication_reserve(services, &pending, &graph.graph_id)?
        .saturating_sub(budget.checkpoint_base_bytes);
    let records = RequestBudget::publication_records(&pending)?;
    if reserve > budget.bytes || records > budget.records {
        return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            format!("history prefix commit publication budget: required_records={records}, remaining_records={}, required_bytes={reserve}, remaining_bytes={}, caller={}",
                    budget.records, budget.bytes, std::panic::Location::caller()),
        ).into());
    }
    budget.checkpoint_records = records;
    budget.require_source_fence(services, pending.source_fence.as_ref().ok_or_else(|| invalid("source proof missing"))?)?;
    let bytes = serde_json::to_vec(&pending.encoded).map_err(|_| invalid("checkpoint encoding"))?.len();
    budget.charge(1, bytes.saturating_sub(budget.checkpoint_base_bytes))?;
    if budget.uncached_stream_head(services, &graph.graph_id)? != graph.revision {
        return Err(invalid("graph source changed"));
    }
    budget.prefix_fences.insert(graph.graph_id.clone(), graph.revision);
    budget.prepay_checkpoint_publication(services, &pending, &graph.graph_id)?;
    budget.checkpoints.insert(pending.projection_id.clone(), pending);
    Ok(())
}

pub(super) fn load(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    through: u64,
    budget: &mut RequestBudget,
) -> Result<HistoryState, RuntimeServicesError> {
    if budget.scope_progress && !room(budget, COMMIT_BYTES) {
        budget.incomplete = true;
        return Ok(HistoryState { incomplete: true, ..Default::default() });
    }
    let source_fence = budget.capture_source_fence(services)?;
    budget.require_source_fence(services, &source_fence)?;
    budget.charge(1, 0)?;
    if graph.commit_cursor > through {
        return Err(invalid("graph ahead of requested cut"));
    }
    let root_scope = activity::execution_scope(services, scope, graph);
    let identity = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&root_scope).map_err(|_| invalid("scope encoding"))?)
    );
    let mut without_goal = root_scope.clone();
    without_goal.goal_id = None;
    let before_goal_identity = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&without_goal).map_err(|_| invalid("scope encoding"))?)
    );
    let projection_id = format!("projection-history:{}", graph.graph_id);
    let content_scope = format!(
        "session:{}",
        scope
            .session_id
            .as_deref()
            .ok_or(RuntimeServicesError::ProjectionAccessDenied)?
    );
    let checkpoint = services
        .event_store()
        .projection_checkpoint(&projection_id)?;
    let expected_revision = checkpoint.as_ref().map_or(0, |saved| saved.revision);
    let mut state = HistoryState {
        source_fence: Some(source_fence.clone()),
        identity: identity.clone(),
        ..Default::default()
    };
    let mut first_goal_attached = false;
    budget.checkpoint_base_bytes = 0;
    if let Some(saved) = checkpoint.as_ref() {
        let schema = saved.payload["schema"]
            .as_u64()
            .ok_or_else(|| invalid("checkpoint schema missing"))?;
        if !matches!(schema, 3 | 4 | 5 | 6)
            || saved.projection_id != projection_id
            || saved.source_cursor > through
            || saved.payload["scope"] != content_scope
        {
            return Err(invalid("checkpoint owner/source mismatch"));
        }
        let metadata = budget.charge_value(saved)?;
        if budget.scope_progress {
            let declared = saved.payload["body"]["bytes"].as_u64()
                .and_then(|bytes| usize::try_from(bytes).ok()).ok_or_else(|| invalid("checkpoint extent missing"))?;
            if budget.bytes < declared.saturating_mul(2).saturating_add(COMMIT_BYTES) {
                #[cfg(test)]
                { budget.prefix_stop = Some(serde_json::json!({"phase":"restore_admission",
                    "declared":declared,"available":budget.bytes,"reserve":COMMIT_BYTES})); }
                budget.incomplete = true;
                return Ok(HistoryState { incomplete: true, ..Default::default() });
            }
        }
        let prepaid = metadata + budget.charge_envelope(&saved.payload["body"])?;
        let value = services
            .event_store()
            .resolve_content(
                &content_scope,
                &saved.payload["body"],
                &crate::PreparedArtifactOrigin::CheckpointBody {
                    projection_id: projection_id.clone(),
                    source_cursor: saved.source_cursor,
                },
            )
            .map_err(error)?;
        let actual = serde_json::to_vec(&value)
            .map_err(|_| invalid("checkpoint extent"))?
            .len();
        budget.checkpoint_base_bytes = actual;
        budget.charge(0, actual.saturating_sub(prepaid))?;
        state = decode_checkpoint(schema, value)?;
        first_goal_attached = root_scope.goal_id.is_some()
            && state.identity != identity
            && state.identity == before_goal_identity;
        if (state.identity != identity && !first_goal_attached)
            || state.through > saved.source_cursor
            || state
                .position
                .is_some_and(|position| position.0 > saved.source_cursor)
            || state.graph_state.as_ref().is_none_or(|current| {
                current.id != graph.graph_id
                    || current.revision > graph.revision
                    || current.lineage != graph.lineage
                    || current.parent_execution != graph.parent_execution
            })
            || state.start_tickets.len() != state.bindings.len()
            || state.start_specs.len() != state.bindings.len()
            || state.start_positions.len() != state.bindings.len()
        {
            return Err(invalid("checkpoint state identity mismatch"));
        }
        for (slot, proof) in &state.effect_proofs {
            if proof.identity != effect_proof_identity(&state, slot)?
                || proof.through > saved.source_cursor
                || (proof.through > state.through && !state.prefix.as_ref().is_some_and(|prefix| {
                    prefix.history_complete && proof.through == prefix.target_cursor
                }))
                || (proof.receipt_present && proof.missing_receipt)
            {
                return Err(invalid("effect proof identity or cut mismatch"));
            }
        }
        for (slot, proof) in &state.terminal_proofs {
            if proof.identity != effect_proof_identity(&state, slot)?
                || proof.through > saved.source_cursor
                || !proof.read.valid_binding_cut(proof.through)
                || (proof.through > state.through && !state.prefix.as_ref().is_some_and(|prefix| {
                    prefix.history_complete && proof.through == prefix.target_cursor
                }))
            {
                return Err(invalid("terminal proof identity or cut mismatch"));
            }
        }
        if let Some(memo) = &state.terminal_pending {
            if memo.identity != effect_proof_identity(&state, &memo.slot)?
                || memo.through > saved.source_cursor
                || !state.prefix.as_ref().is_some_and(|prefix| prefix.history_complete && memo.through == prefix.target_cursor)
            {
                return Err(invalid("terminal memo identity or cut mismatch"));
            }
        }
        if schema == 6 {
            if saved.payload["target_cursor"] != saved.source_cursor
                || saved.payload["verified_through"] != state.through
                || saved.payload["incomplete"].as_bool() != Some(state.prefix.is_some())
            {
                return Err(invalid("checkpoint coverage mismatch"));
            }
            if let Some(prefix) = &state.prefix {
                if prefix.target_cursor != saved.source_cursor
                    || prefix.target_revision > graph.revision
                    || prefix.completed_through > prefix.target_cursor
                    || (prefix.completed_through > 0 && prefix.completed_through < prefix.base_through)
                    || state.bindings.keys().any(|slot| {
                        state.start_positions.get(slot).is_some_and(|position| position.0 <= prefix.completed_through)
                            && state.effect_proofs.get(slot).is_none_or(|proof| proof.through < prefix.completed_through)
                    })
                    || (prefix.history_complete
                        && state
                            .graph_state
                            .as_ref()
                            .is_none_or(|current| current.revision != prefix.target_revision))
                    || prefix.audit_index != prefix.records.len()
                    || prefix
                        .records
                        .windows(2)
                        .any(|records| records[0].position >= records[1].position)
                    || prefix.base_through > prefix.target_cursor
                    || prefix.base_position > state.position
                    || prefix.records.first().is_some_and(|record| {
                        prefix
                            .base_position
                            .is_some_and(|base| record.position <= base)
                    })
                    || prefix
                        .records
                        .last()
                        .map(|record| record.position)
                        .or(prefix.base_position)
                        != state.position
                    || state.through
                        != prefix
                            .base_through
                            .max(prefix.completed_through)
                            .max(state.position.map_or(0, |position| position.0))
                {
                    return Err(invalid("invalid prefix position or target"));
                }
            } else if state.through != saved.source_cursor {
                return Err(invalid("complete checkpoint has incomplete cut"));
            }
        } else if state.prefix.is_some() || state.through != saved.source_cursor {
            return Err(invalid("legacy checkpoint cannot carry a prefix"));
        }
        if state.prefix.is_none()
            && state.through == through
            && state
                .graph_state
                .as_ref()
                .is_none_or(|current| current.revision != graph.revision)
        {
            return Err(invalid("complete checkpoint Graph revision mismatch"));
        }
        if saved.payload["schema"] == 6 {
            if state.source_fence.as_ref() != Some(&source_fence)
                || saved.payload["source_fence"] != serde_json::to_value(
                    state.source_fence.as_ref().ok_or_else(|| invalid("checkpoint source fence missing"))?
                ).map_err(|_| invalid("source fence encoding"))?
            {
                if let Some(prefix) = &state.prefix {
                    for witness in &prefix.records {
                        if !room(budget, COMMIT_BYTES) { break; }
                        verify_source(services, graph, witness, budget)?;
                    }
                }
                return Err(invalid("checkpoint source mutation fence changed"));
            }
        }
    }
    if first_goal_attached || checkpoint.as_ref().is_some_and(|saved| saved.payload["schema"] != 6) {
        state = HistoryState {
            source_fence: Some(source_fence.clone()),
            identity: identity.clone(),
            ..Default::default()
        };
    }
    let mut prefix = state.prefix.take().unwrap_or(HistoryPrefix {
        target_cursor: through,
        target_revision: graph.revision,
        base_position: state.position,
        base_through: state.through,
        history_complete: state.through == through,
        completed_through: 0,
        audit_index: 0,
        records: Vec::new(),
    });
    let initial_position = state.position;
    let initial_audit = prefix.audit_index;
    let mut changed = false;
    let mut safe_stop = false;
    loop {
        let reserve = commit_reserve_delta(services, graph, &state, &prefix, &content_scope, budget)?;
        if !prefix.history_complete {
            if !room(budget, reserve) {
                #[cfg(test)]
                { budget.prefix_stop = Some(serde_json::json!({"phase":"canonical_room",
                    "position":state.position,"available":budget.bytes,"records":budget.records,"reserve":reserve})); }
                safe_stop = true;
                break;
            }
            budget.charge(1, 0)?;
            let page = services
                .event_store()
                .position_page(
                    RuntimeEventScope::ExecutionGraph,
                    &graph.graph_id,
                    None,
                    state.position,
                    prefix.target_cursor,
                    1,
                    budget.bytes.saturating_sub(reserve),
                )
                .map_err(error)?;
            if page.len() > 1 {
                return Err(invalid("unbounded position page"));
            }
            let Some(record) = page.into_iter().next() else {
                if state.graph_state.as_ref().is_none_or(|current| {
                    current.id != graph.graph_id || current.revision != prefix.target_revision
                }) {
                    return Err(invalid("canonical history incomplete at target"));
                }
                prefix.history_complete = true;
                changed = true;
                continue;
            };
            let position = (record.commit_cursor, record.transaction_index);
            if state.position.is_some_and(|previous| position <= previous)
                || position.0 > prefix.target_cursor
                || position.0 <= prefix.base_through
            {
                return Err(invalid("original position/scope mismatch"));
            }
            let included =
                activity::event_belongs_to_graph(&record, graph) && scope.contains_event(&record);
            if !included && record.scope == RuntimeEventScope::ExecutionGraph {
                return Err(invalid("authoritative Graph history scope mismatch"));
            }
            let metadata = serde_json::to_vec(&record)
                .map_err(|_| invalid("metadata encoding"))?
                .len();
            let body = body_bytes(&record)?;
            // The original reader also calls charge_record. Register prepayment
            // with that same owner so resolving this record cannot bill its body
            // a second time. A previously read original must still match exactly.
            let prepaid = if budget.charged_records.contains_key(&record.event_id) {
                Some(budget.charge_record(&record)?)
            } else {
                None
            };
            if prepaid.is_none()
                && metadata.saturating_add(body) > budget.bytes.saturating_sub(reserve)
            {
                // The page metadata was read even when its body must wait for
                // the next bounded request.
                budget.charge(1, metadata)?;
                if metadata.saturating_add(body).saturating_add(COMMIT_BYTES)
                    > HISTORY_REQUEST_BYTES
                {
                    return Err(invalid(
                        "original record cannot fit a fresh bounded request",
                    ));
                }
                safe_stop = true;
                #[cfg(test)]
                { budget.prefix_stop = Some(serde_json::json!({"phase":"canonical_body_admission",
                    "position":position,"body":body,"metadata":metadata,"available":budget.bytes,
                    "records":budget.records,"reserve":reserve})); }
                break;
            }
            let previous_state = state.clone();
            let previous_records = prefix.records.len();
            let prepaid = match prepaid {
                Some(prepaid) => prepaid,
                None => budget.charge_record(&record)?,
            };
            if included {
                let event = budget.resolve_prepaid_record(services, &record, prepaid)?;
                budget.remember_source_record(&record, prefix.target_cursor)?;
                budget.remember_record(&event, prefix.target_cursor)?;
                if event.kind == "session.cancellation_committed" {
                    let mut receipt: harness_contract::turn::CancellationReceipt =
                        serde_json::from_value(event.payload.clone())
                            .map_err(|_| invalid("invalid cancellation"))?;
                    if receipt.execution_id == graph.graph_id
                        && Some(receipt.session_id.as_str()) == scope.session_id.as_deref()
                        && Some(receipt.turn_id.as_str()) == scope.turn_id.as_deref()
                    {
                        receipt.journal_sequence = event.commit_cursor;
                        receipt.projection_revision = event.sequence;
                        state.cancellation = Some(receipt);
                    }
                }
                state.fold(&event, graph, &root_scope).map_err(error)?;
            }
            prefix.records.push(fingerprint(&record)?);
            state.position = Some(position);
            state.through = position.0;
            if !publication_fits(services, graph, &state, &prefix, &content_scope, budget)? {
                #[cfg(test)]
                { budget.prefix_stop = Some(serde_json::json!({"phase":"post_fold_publication",
                    "position":position,"body":body,"available":budget.bytes,"records":budget.records,
                    "reserve":commit_reserve_delta(services, graph, &state, &prefix, &content_scope, budget)?})); }
                state = previous_state;
                prefix.records.truncate(previous_records);
                commit_reserve_delta(services, graph, &state, &prefix, &content_scope, budget)?;
                safe_stop = true;
                break;
            }
            prefix.audit_index = prefix.records.len();
            changed = true;
            if serde_json::to_vec(&state)
                .map_err(|_| invalid("state encoding"))?
                .len()
                > HISTORY_STATE_BYTES
            {
                return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
                    "projection semantic state".into(),
                )
                .into());
            }
            continue;
        }
        let mut next = None;
        for (slot, (node_id, binding)) in &state.bindings {
            let identity = effect_proof_identity(&state, slot)?;
            if state.effect_proofs.get(slot).is_some_and(|proof| {
                proof.identity == identity && proof.through == prefix.target_cursor
            }) {
                continue;
            }
            next = Some((slot.clone(), node_id.clone(), binding.clone(), identity));
            break;
        }
        if let Some((slot, node_id, binding, identity)) = next {
            if budget.records < budget.checkpoint_records + 7 {
                safe_stop = true;
                break;
            }
            let reusable = state.effect_proofs.get(&slot).is_some_and(|proof| {
                proof.identity == identity && proof.through <= prefix.target_cursor
                    && proof.receipt_present && !proof.missing_receipt
            });
            let ticket = state.start_tickets.get(&slot).ok_or_else(|| invalid("effect ticket missing"))?;
            let stream = crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(ticket);
            let receipt_key = format!("{}:receipt", binding.ticket_idempotency_key);
            if !reusable {
                let intent_key = format!("{}:intent", binding.ticket_idempotency_key);
                if !budget.admit_canonical_record(services, &stream, &intent_key, prefix.target_cursor, reserve)?
                    || !budget.admit_canonical_record(services, &stream, &receipt_key, prefix.target_cursor, reserve)?
                {
                    safe_stop = true;
                    break;
                }
            }
            let delegated = format!("execution-agent-receipts:{}:{}:{}", ticket.graph_id, node_id, binding.attempt);
            if !budget.admit_effect_head(services, &delegated, prefix.target_cursor, reserve)? {
                safe_stop = true;
                break;
            }
            let (references, missing_receipt) = reducer_support::validate_history_effect_binding(
                services, &state, &slot, &node_id, &binding, prefix.target_cursor, budget)?;
            let receipt_present = reusable || budget.records_by_key
                .get(&(stream, receipt_key, prefix.target_cursor)).is_some_and(Option::is_some);
            let previous = state.effect_proofs.insert(slot.clone(), HistoryEffectProof {
                identity, through: prefix.target_cursor, references, missing_receipt, receipt_present,
            });
            if !publication_fits(services, graph, &state, &prefix, &content_scope, budget)? {
                if let Some(previous) = previous {
                    state.effect_proofs.insert(slot, previous);
                } else {
                    state.effect_proofs.remove(&slot);
                }
                commit_reserve_delta(services, graph, &state, &prefix, &content_scope, budget)?;
                safe_stop = true;
                break;
            }
            changed = true;
            continue;
        }
        let (running, draining) = reducer_support::execution_activity(services, graph, scope);
        if (running || draining) && state.terminal_pending.take().is_some() {
            changed = true;
        }
        if !running && !draining {
            let next = state.bindings.keys().find(|slot| {
                state.terminal_proofs.get(*slot).is_none_or(|proof| {
                    proof.through != prefix.target_cursor
                        || effect_proof_identity(&state, slot).ok().as_ref() != Some(&proof.identity)
                })
            }).cloned();
            if let Some(slot) = next {
                if budget.records < budget.checkpoint_records + 8 {
                    safe_stop = true;
                    break;
                }
                budget.begin_terminal(&state, &slot, prefix.target_cursor, reserve, graph, &prefix, &content_scope)?;
                let result = (|| {
                    if let Some(proof) = state.terminal_proofs.get(&slot) {
                        if proof.identity == effect_proof_identity(&state, &slot)? {
                            if let Some(read) = settlement::refresh_binding(services, proof, prefix.target_cursor, budget)? {
                                return Ok(read);
                            }
                        }
                    }
                    settlement::read_binding(services, &state, &slot, prefix.target_cursor, budget)
                })();
                if budget.exhausted {
                    return Err(result.err().unwrap_or_else(checkpoint_error));
                }
                let (memo, deferred) = budget.finish_terminal()?;
                match result {
                    Err(error) if deferred && error.to_string().contains(INCOMPLETE) => {
                        let previous = state.terminal_pending.replace(memo);
                        if !publication_fits(services, graph, &state, &prefix, &content_scope, budget)? {
                            state.terminal_pending = previous;
                            commit_reserve_delta(services, graph, &state, &prefix, &content_scope, budget)?;
                            safe_stop = true;
                            break;
                        }
                        changed |= serde_json::to_value(&state.terminal_pending).map_err(|_| invalid("terminal memo encoding"))?
                            != serde_json::to_value(&previous).map_err(|_| invalid("terminal memo encoding"))?;
                        safe_stop = true;
                        break;
                    }
                    Err(error) => return Err(error),
                    Ok(read) => {
                        if deferred {
                            return Err(invalid("deferred terminal validator returned a proof"));
                        }
                        let previous_memo = state.terminal_pending.take();
                        let proof_identity = effect_proof_identity(&state, &slot)?;
                        let previous = state.terminal_proofs.insert(slot.clone(), TerminalBindingProof {
                            identity: proof_identity,
                            through: prefix.target_cursor,
                            read,
                        });
                        if !publication_fits(services, graph, &state, &prefix, &content_scope, budget)? {
                            if let Some(previous) = previous { state.terminal_proofs.insert(slot, previous); }
                            else { state.terminal_proofs.remove(&slot); }
                            state.terminal_pending = previous_memo;
                            commit_reserve_delta(services, graph, &state, &prefix, &content_scope, budget)?;
                            safe_stop = true;
                            break;
                        }
                        changed = true;
                        continue;
                    }
                }
            }
        }
        if prefix.target_cursor < through {
            prefix.completed_through = prefix.target_cursor;
            state.through = state.through.max(prefix.completed_through);
            prefix.target_cursor = through;
            prefix.target_revision = graph.revision;
            prefix.history_complete = false;
            changed = true;
            continue;
        }
        if !running && !draining {
            let read = settlement::verified_read(&state, prefix.target_cursor)?
                .ok_or_else(|| invalid("terminal proof coverage missing"))?;
            let (_, records, bytes) = read.recheck_work()?.into_iter().fold(
                (0usize, 0usize, 0usize), |(_, records, bytes), (_, count, extent)| {
                    (0, records.saturating_add(count), bytes.saturating_add(extent))
                });
            if budget.records < records.saturating_add(budget.checkpoint_records)
                || budget.bytes < reserve.saturating_add(bytes)
                || !budget.prepay_terminal_recheck(&read)?
            {
                #[cfg(test)]
                { budget.prefix_stop = Some(serde_json::json!({"phase":"terminal_final_recheck",
                    "available":budget.bytes,"records":budget.records,"reserve":reserve,
                    "recheck_records":records,"recheck_bytes":bytes})); }
                safe_stop = true;
                break;
            }
        }
        state.through = prefix.target_cursor;
        state.prefix = None;
        state.effect_evidence_refs = state.effect_proofs.values()
            .flat_map(|proof| proof.references.iter().cloned()).collect();
        state.effect_missing_receipt = state.effect_proofs.values().any(|proof| proof.missing_receipt);
        state.effects_checked_through = Some(prefix.target_cursor);
        state.incomplete = false;
        stage(
            services,
            graph,
            &mut state,
            through,
            expected_revision,
            &content_scope,
            budget,
        )?;
        return Ok(state);
    }
    if !changed && state.position == initial_position && prefix.audit_index == initial_audit {
        if safe_stop && (state.graph_state.is_some() || budget.scope_progress) {
            state.prefix = state.graph_state.is_some().then_some(prefix);
            budget.require_source_fence(services, &source_fence)?;
            state.incomplete = true;
            budget.incomplete = true;
            return Ok(state);
        }
        return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            "history prefix made no progress".into(),
        )
        .into());
    }
    let target = prefix.target_cursor;
    state.prefix = Some(prefix);
    state.incomplete = true;
    budget.incomplete = true;
    stage(
        services,
        graph,
        &mut state,
        target,
        expected_revision,
        &content_scope,
        budget,
    )?;
    Ok(state)
}

#[cfg(test)]
#[path = "history_prefix_tests.rs"]
mod tests;
