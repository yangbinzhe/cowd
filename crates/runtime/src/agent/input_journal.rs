//! Agent input facts belong to the original EventStore, not the worker inbox.
use super::*;
use crate::runtime_event_store::RuntimeTransactionEventInput;

const MAX_PENDING_INPUTS: usize = 256;
const MAX_INPUT_BYTES: usize = 64 * 1024;

#[derive(Clone, Serialize, Deserialize)]
struct DurableAgentInput {
    run_id: String,
    request: AgentCommandRequest,
    receipt: AgentCommandReceipt,
}

fn run_prefix(agent_id: &str, run_id: &str) -> String {
    let identity = serde_json::to_vec(&(agent_id, run_id)).expect("input identity");
    format!("agent-input:v1:{:x}:", Sha256::digest(identity))
}

fn input_stream(agent_id: &str, run_id: &str, command_id: &str) -> String {
    format!(
        "{}{:x}",
        run_prefix(agent_id, run_id),
        Sha256::digest(command_id.as_bytes())
    )
}

fn input_event(input: &DurableAgentInput, kind: &str) -> RuntimeTransactionEventInput {
    RuntimeEventInput {
        stream_id: input_stream(
            &input.request.agent_id,
            &input.run_id,
            &input.request.command_id,
        ),
        scope: RuntimeEventScope::SessionInput,
        kind: kind.into(),
        status: Some(
            if input.receipt.accepted {
                "queued"
            } else {
                "delivery_pending"
            }
            .into(),
        ),
        actor: Some("agent_runtime".into()),
        refs: vec![RuntimeEventRef {
            kind: "agent_run".into(),
            id: input.run_id.clone(),
        }],
        payload: serde_json::to_value(input).expect("durable input"),
    }
    .into()
}

fn rejected(
    request: &AgentCommandRequest,
    status: AgentStatus,
    reason: AgentCommandRejectReason,
    message: impl Into<String>,
) -> AgentCommandReceipt {
    AgentCommandReceipt {
        command_id: request.command_id.clone(),
        agent_id: request.agent_id.clone(),
        accepted_revision: request.expected_revision,
        status,
        accepted: false,
        reject_reason: Some(reason),
        message: message.into(),
    }
}

impl AgentRuntime {
    pub(super) fn reject_reserved_input_command_id(
        &self,
        request: &AgentCommandRequest,
    ) -> Option<AgentCommandReceipt> {
        let snapshot = self.get(&request.agent_id)?;
        let stream = input_stream(&request.agent_id, &snapshot.run_id, &request.command_id);
        match self
            .event_store
            .latest_for_stream_kind(&stream, "agent.input_admitted")
        {
            Ok(None) => None,
            Ok(Some(_)) => Some(rejected(
                request,
                snapshot.status,
                AgentCommandRejectReason::InvalidInput,
                "command_id is reserved by a different durable input command",
            )),
            Err(error) => Some(rejected(
                request,
                snapshot.status,
                AgentCommandRejectReason::InvalidInput,
                error.to_string(),
            )),
        }
    }
    pub(crate) fn input_text(input: &harness_contract::agent::AgentInput) -> String {
        use harness_contract::agent::AgentInput;
        match input {
            AgentInput::UserSupplement(text) => text.clone(),
            AgentInput::PeerMessage {
                from_agent_id,
                message,
            } => format!("Peer message from {from_agent_id}: {message}"),
            AgentInput::ControlContext(value) => format!("Control context: {value}"),
            AgentInput::ApprovalResult {
                approval_id,
                approved,
            } => format!(
                "Approval {approval_id}: {}",
                if *approved { "approved" } else { "denied" }
            ),
        }
    }
    pub(super) async fn command_durable_input(
        &self,
        request: AgentCommandRequest,
    ) -> AgentCommandReceipt {
        let Some(snapshot) = self.get(&request.agent_id) else {
            return rejected(
                &request,
                AgentStatus::Blocked,
                AgentCommandRejectReason::NotFound,
                "agent not found",
            );
        };
        let stream = input_stream(&request.agent_id, &snapshot.run_id, &request.command_id);
        let existing = match self
            .event_store
            .latest_for_stream_kind(&stream, "agent.input_admitted")
        {
            Ok(existing) => existing,
            Err(error) => {
                return rejected(
                    &request,
                    snapshot.status,
                    AgentCommandRejectReason::InvalidInput,
                    error.to_string(),
                )
            }
        };
        if let Some(existing) = existing {
            let Ok(input) = serde_json::from_value::<DurableAgentInput>(existing.payload) else {
                return rejected(
                    &request,
                    snapshot.status,
                    AgentCommandRejectReason::InvalidInput,
                    "invalid durable input record",
                );
            };
            if input.request != request || input.run_id != snapshot.run_id {
                return rejected(
                    &request,
                    snapshot.status,
                    AgentCommandRejectReason::InvalidInput,
                    "command_id conflicts with its durable input payload",
                );
            }
            if input.receipt.accepted {
                return input.receipt;
            }
            return match self.event_store.latest_for_stream_kind(&stream, "agent.input_delivered") {
                Ok(Some(event)) => serde_json::from_value::<DurableAgentInput>(event.payload)
                    .map(|input| input.receipt)
                    .unwrap_or_else(|_| rejected(&request, snapshot.status, AgentCommandRejectReason::InvalidInput, "invalid delivery receipt")),
                _ => rejected(&request, snapshot.status, AgentCommandRejectReason::UnsupportedByBackend,
                    "input delivery is unresolved; reconcile the original process command, do not resend"),
            };
        }
        if snapshot.revision != request.expected_revision {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::StaleRevision,
                "agent revision does not match",
            );
        }
        if self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&request.agent_id)
            .is_some_and(|record| record.receipts.contains_key(&request.command_id))
        {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::InvalidInput,
                "command_id is already bound to another command",
            );
        }
        if snapshot.status.is_terminal() {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::Terminal,
                "agent is terminal",
            );
        }
        if request.input.is_none()
            || request.command_id.trim().is_empty()
            || serde_json::to_vec(&request).map_or(true, |bytes| bytes.len() > MAX_INPUT_BYTES)
        {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::InvalidInput,
                "send_input requires an input, nonempty command_id and at most 64 KiB",
            );
        }
        match self.pending_agent_inputs(&snapshot.agent_id, &snapshot.run_id) {
            Ok(inputs) if inputs.len() < MAX_PENDING_INPUTS => {}
            Ok(_) => {
                return rejected(
                    &request,
                    snapshot.status,
                    AgentCommandRejectReason::InvalidInput,
                    "agent durable input backlog is full",
                )
            }
            Err(error) => {
                return rejected(
                    &request,
                    snapshot.status,
                    AgentCommandRejectReason::InvalidInput,
                    error,
                )
            }
        }
        let backend = self
            .backends
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&snapshot.backend)
            .cloned();
        let Some(backend) = backend else {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::UnsupportedByBackend,
                "agent backend is unavailable",
            );
        };
        if !backend.backend.capabilities().supports_input {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::UnsupportedByBackend,
                "backend does not support Agent inputs",
            );
        }
        let native = snapshot.backend == AgentBackendKind::InProcess;
        let mut input = DurableAgentInput {
            run_id: snapshot.run_id.clone(),
            request: request.clone(),
            receipt: AgentCommandReceipt {
                command_id: request.command_id.clone(),
                agent_id: request.agent_id.clone(),
                accepted_revision: snapshot.revision.saturating_add(1),
                status: snapshot.status,
                accepted: native,
                reject_reason: None,
                message: if native {
                    "input durably queued; model consumption is a separate committed fact"
                } else {
                    "input delivery pending transport acknowledgement"
                }
                .into(),
            },
        };
        if let Err(error) = self.persist_snapshot_with_input(
            snapshot.clone(),
            "agent.input_queued",
            "input intent persisted",
            native.then(|| input.receipt.clone()),
            None,
            None,
            Some((input_event(&input, "agent.input_admitted"), 0)),
        ) {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::StaleRevision,
                error,
            );
        }
        // No identity/lifecycle lock crosses this await. If this caller is lost,
        // Native replay owns the queued fact; Process remains explicitly unknown.
        let delivery = backend.backend.command(&snapshot.handle(), &request).await;
        if native {
            return input.receipt;
        }
        if let Err(reason) = delivery {
            return rejected(
                &request,
                snapshot.status,
                reason,
                "durable input delivery unresolved; process reconciliation required",
            );
        }
        let Some(current) = self
            .get(&request.agent_id)
            .filter(|current| current.run_id == snapshot.run_id)
        else {
            return rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::StaleRevision,
                "input acknowledgement belongs to a superseded run",
            );
        };
        input.receipt.accepted = true;
        input.receipt.accepted_revision = current.revision.saturating_add(1);
        input.receipt.message =
            "input durably acknowledged by the current process; consumption is not attested".into();
        match self.persist_snapshot_with_input(
            current,
            "agent.command",
            "input transport acknowledged",
            Some(input.receipt.clone()),
            None,
            None,
            Some((input_event(&input, "agent.input_delivered"), 1)),
        ) {
            Ok(receipt) => receipt,
            Err(error) => rejected(
                &request,
                snapshot.status,
                AgentCommandRejectReason::InvalidInput,
                format!(
                    "input acknowledgement persistence failed; reconciliation required: {error}"
                ),
            ),
        }
    }

    /// Bounded page replay; terminal/consumed history does not become a cache.
    pub(crate) fn pending_agent_inputs(
        &self,
        agent_id: &str,
        run_id: &str,
    ) -> Result<Vec<AgentCommandRequest>, String> {
        let prefix = run_prefix(agent_id, run_id);
        let mut after = None;
        let mut pending = Vec::new();
        loop {
            let events = self.event_store.list_scope_stream_prefix_page_asc(
                RuntimeEventScope::SessionInput,
                &prefix,
                after,
                128,
            )?;
            if events.is_empty() {
                break;
            }
            after = events
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            for event in events {
                if event.kind != "agent.input_admitted" {
                    continue;
                }
                let input: DurableAgentInput =
                    serde_json::from_value(event.payload).map_err(|error| error.to_string())?;
                if input.request.agent_id != agent_id || input.run_id != run_id {
                    return Err("agent input identity mismatch".into());
                }
                let consumed = self
                    .event_store
                    .latest_for_stream_kind(&event.stream_id, "agent.input_consumed")
                    .map_err(|error| error.to_string())?
                    .is_some();
                if !consumed {
                    pending.push(input.request);
                    if pending.len() >= MAX_PENDING_INPUTS {
                        return Ok(pending);
                    }
                }
            }
        }
        Ok(pending)
    }

    pub(crate) fn agent_input_consumption_events(
        &self,
        packet: &AgentTaskPacket,
        records: &[crate::session_input::SessionInputRecord],
    ) -> Result<Vec<RuntimeTransactionEventInput>, String> {
        let mut events = Vec::new();
        for record in records {
            if record.envelope.source_ref.as_deref()
                != Some(format!("agent-input:{}", packet.run_id()).as_str())
            {
                continue;
            }
            let command_id = record
                .envelope
                .source_message_id
                .as_deref()
                .ok_or("agent input has no command identity")?;
            if record.envelope.session_id.as_str() != packet.session_id() {
                return Err("agent input session mismatch".into());
            }
            let stream = input_stream(packet.agent_id(), packet.run_id(), command_id);
            let admitted = self
                .event_store
                .latest_for_stream_kind(&stream, "agent.input_admitted")
                .map_err(|error| error.to_string())?
                .ok_or("agent input has no durable intent")?;
            let input: DurableAgentInput =
                serde_json::from_value(admitted.payload).map_err(|error| error.to_string())?;
            if record.envelope.source_kind != harness_contract::turn::InputSourceKind::Agent
                || input
                    .request
                    .input
                    .as_ref()
                    .map(Self::input_text)
                    .as_deref()
                    != Some(record.envelope.content.as_str())
            {
                return Err("agent input content differs from its durable intent".into());
            }
            if input.run_id != packet.run_id()
                || input.request.agent_id != packet.agent_id()
                || !input.receipt.accepted
            {
                return Err("agent input consumption authority mismatch".into());
            }
            let mut event = input_event(&input, "agent.input_consumed");
            event.event.status = Some("consumed".into());
            event.idempotency_key = Some(format!("{stream}:consumed"));
            events.push(event);
        }
        Ok(events)
    }
}
