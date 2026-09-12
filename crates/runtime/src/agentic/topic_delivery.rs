//! The existing Topic cursor is acknowledged at the actual consumer boundary.
//! Preparing or selecting a page is not proof of successful delivery.
use super::{AgentActionService, AgentActionServiceError, AgenticTopicObservationAck};
use harness_contract::agent::AgentTaskPacket;

pub(crate) struct TopicDelivery {
    pub item: crate::ContextItem,
    pub ack: AgenticTopicObservationAck,
}

pub(crate) fn prepare(
    actions: &AgentActionService,
    packet: &AgentTaskPacket,
) -> Result<Option<TopicDelivery>, AgentActionServiceError> {
    let Some(binding) = packet.agentic_binding.as_ref() else {
        return Ok(None);
    };
    let Some(page) = actions.topic_observations(
        &binding.program_id,
        &binding.agent_id,
        &binding.team_id,
        packet.graph_id(),
        16,
        48 * 1024,
    )?
    else {
        return Ok(None);
    };
    let entries = page.entries.iter().map(|observation| serde_json::json!({
        "topic_ref": observation.topic_ref,
        "entry": observation.entry,
        "read_request": {"name":"state_inspect","input":{"entry_ref":observation.entry.entry_id}}
    })).collect::<Vec<_>>();
    let content = serde_json::to_string(&serde_json::json!({
        "kind":"agentic_topic_delta", "program_id":binding.program_id,
        "from_revision":page.from_revision, "through_revision":page.to_revision,
        "entries":entries,
        "coverage":{"kind":"unread_authorized_metadata","exact_content":"follow read_request"}
    }))?;
    let mut item = crate::ContextItem::new(
        format!(
            "agentic-topic:{}:{}:{}",
            binding.program_id,
            packet.graph_id(),
            page.to_revision
        ),
        crate::ContextSourceKind::AgentPeer,
        crate::ContextRole::Evidence,
        content,
    );
    item.authority = crate::ContextAuthority::Tool;
    item.visibility = crate::ContextVisibility::Private;
    item.evidence = page
        .entries
        .iter()
        .map(|entry| entry.entry.entry_id.clone())
        .collect();
    Ok(Some(TopicDelivery {
        item,
        ack: AgenticTopicObservationAck {
            program_id: binding.program_id.clone(),
            execution_id: packet.graph_id().to_string(),
            through_revision: page.to_revision,
            expected_cursor_revision: page.cursor_revision,
            observation_kind: super::TopicObservationKind::ProviderModel,
        },
    }))
}

impl TopicDelivery {
    pub(crate) fn selected_in(&self, envelope: Option<&crate::ContextEnvelope>) -> bool {
        envelope.is_some_and(|envelope| {
            envelope
                .selected
                .iter()
                .any(|item| item.id == self.item.id && item.content == self.item.content)
        })
    }
}

/// One outstanding transport page. New events wait for the next page rather
/// than replacing an unacknowledged delivery during pipelined tool requests.
#[derive(Default)]
pub(crate) struct TopicTransport {
    pending: Option<TopicDelivery>,
    last_acknowledged: Option<String>,
}
impl TopicDelivery {
    fn delivery_id(&self) -> String {
        use sha2::{Digest, Sha256};
        format!(
            "topic-delivery:{:x}",
            Sha256::digest(format!("{}\n{}", self.item.id, self.item.content).as_bytes())
        )
    }
}
impl TopicTransport {
    pub(crate) fn issue(
        &mut self,
        actions: &AgentActionService,
        packet: &AgentTaskPacket,
    ) -> Result<Option<serde_json::Value>, String> {
        let fresh = prepare(actions, packet).map_err(|e| e.to_string())?;
        // Recheck live visibility at every transport boundary. Cached tool
        // responses must never replay a revoked private Topic page.
        let keep_pending = self
            .pending
            .as_ref()
            .zip(fresh.as_ref())
            .is_some_and(|(old, new)| {
                old.item
                    .evidence
                    .iter()
                    .all(|reference| new.item.evidence.contains(reference))
            });
        if !keep_pending {
            self.pending = fresh;
        }
        self.pending.as_ref().map(|delivery| {
            Ok(serde_json::json!({"delivery_id":delivery.delivery_id(),
                "context":serde_json::from_str::<serde_json::Value>(&delivery.item.content).map_err(|e|e.to_string())?,
                "acknowledgement":{"kind":"context_ack","delivery_id":delivery.delivery_id()},
                "acknowledgement_meaning":"worker_transport_received_not_semantic_verification"}))
        }).transpose()
    }

    pub(crate) fn acknowledge(
        &mut self,
        actions: &AgentActionService,
        delivery_id: &str,
    ) -> Result<(), String> {
        if self.last_acknowledged.as_deref() == Some(delivery_id) {
            return Ok(());
        }
        let pending = self
            .pending
            .as_ref()
            .filter(|pending| pending.delivery_id() == delivery_id)
            .ok_or_else(|| {
                "Topic acknowledgement does not match this worker's outstanding delivery"
                    .to_string()
            })?;
        let mut ack = pending.ack.clone();
        ack.observation_kind = super::TopicObservationKind::WorkerTransport;
        actions
            .acknowledge_topic_observations(ack)
            .map_err(|e| e.to_string())?;
        self.last_acknowledged = Some(delivery_id.to_string());
        self.pending = None;
        Ok(())
    }
}
