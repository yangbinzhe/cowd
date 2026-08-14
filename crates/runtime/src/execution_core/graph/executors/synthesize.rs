use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use harness_contract::execution_graph::ExecutionNodeSpec;
use harness_contract::outcome::{AnswerOrigin, TerminalPresentationState};

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};

#[async_trait]
pub trait SynthesizeBackend: Send + Sync {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String>;
    async fn after_commit(&self, _ticket: &NodeExecutionTicket) -> Result<(), String> {
        Ok(())
    }
    async fn after_abort(
        &self,
        _ticket: &NodeExecutionTicket,
        _reason: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}

pub trait SynthesizeBackendResolver: Send + Sync {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>>;
}

/// The sole publisher of a graph terminal candidate, resolved from its ticket.
pub struct SynthesizeNodeExecutor {
    resolvers: RwLock<Vec<Arc<dyn SynthesizeBackendResolver>>>,
}

impl SynthesizeNodeExecutor {
    pub const KIND: &'static str = "synthesize";
    #[must_use]
    pub fn new() -> Self {
        Self {
            resolvers: RwLock::new(Vec::new()),
        }
    }
    pub fn install_resolver(&self, resolver: Arc<dyn SynthesizeBackendResolver>) {
        self.resolvers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(resolver);
    }

    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        self.resolvers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find_map(|resolver| resolver.resolve(ticket))
    }
}
impl Default for SynthesizeNodeExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeExecutor for SynthesizeNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }
    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind == Self::KIND {
            Ok(())
        } else {
            Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Synthesize must use canonical synthesize executor".into(),
            })
        }
    }
    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: Self::KIND.into(),
            service_class: context.graph.service_class,
            attempt: context.attempt,
            idempotency_key: context.node.idempotency_key,
            payload_ref: context.node.payload_ref,
        })
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        let outcome =
            backend
                .synthesize(ticket)
                .await
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
        validate_terminal_candidate(ticket, &outcome).map_err(|reason| {
            NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            }
        })?;
        Ok(outcome)
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        backend
            .after_commit(ticket)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }
    async fn after_abort(
        &self,
        ticket: &NodeExecutionTicket,
        reason: &str,
    ) -> Result<(), NodeExecutorError> {
        let backend = self
            .resolve(ticket)
            .ok_or_else(|| NodeExecutorError::Unavailable {
                executor_kind: Self::KIND.into(),
                node_id: ticket.node_id.clone(),
            })?;
        backend
            .after_abort(ticket, reason)
            .await
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }
}

fn validate_terminal_candidate(
    ticket: &NodeExecutionTicket,
    outcome: &NodeExecutionOutcome,
) -> Result<(), String> {
    let Some(presentation) = outcome.terminal_presentation.as_ref() else {
        return Ok(());
    };
    if presentation.answer_origin != AnswerOrigin::TeamSynthesizer {
        return Ok(());
    }
    let envelope = outcome.delivery_envelope.as_ref().ok_or_else(|| {
        "TeamSynthesizer presentation has no Runtime-authored delivery envelope".to_string()
    })?;
    if !ticket.payload_ref.starts_with("team:")
        || presentation.envelope_id != envelope.envelope_id
        || presentation.envelope_revision != envelope.revision
        || presentation.state != TerminalPresentationState::Validating
        || presentation
            .source_execution_id
            .as_deref()
            .is_none_or(str::is_empty)
        || presentation.committed_at_ms.is_some()
        || outcome
            .result
            .result_ref
            .as_deref()
            .is_none_or(|reference| !reference.starts_with("assistant_json:"))
        || outcome
            .result
            .summary
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
    {
        return Err(
            "TeamSynthesizer presentation is not bound to a complete envelope-consuming terminal Agent candidate"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{
        ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
    };
    use harness_contract::outcome::{
        AnswerValidation, DeliveryCoverage, DeliveryEnvelope, DeliveryStatus, PipelineStatus,
        TerminalPresentation, UserAnswerContract,
    };

    use super::*;

    fn ticket() -> NodeExecutionTicket {
        NodeExecutionTicket {
            graph_id: "graph".to_string(),
            node_id: "synthesize".to_string(),
            executor_kind: SynthesizeNodeExecutor::KIND.to_string(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "synthesize:1".to_string(),
            payload_ref: "team:team-1".to_string(),
        }
    }

    fn envelope() -> DeliveryEnvelope {
        DeliveryEnvelope {
            envelope_id: "delivery:graph:4".to_string(),
            revision: 4,
            objective_id: "graph".to_string(),
            pipeline_status: PipelineStatus::Completed,
            delivery_status: DeliveryStatus::Partial,
            branch_terminals: Vec::new(),
            verified_receipts: Vec::new(),
            verified_artifacts: Vec::new(),
            verified_effects: Vec::new(),
            coverage: DeliveryCoverage::default(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
            cancellation: None,
            user_answer_contract: UserAnswerContract::default(),
            created_at_ms: 1,
        }
    }

    fn outcome() -> NodeExecutionOutcome {
        let envelope = envelope();
        let mut outcome = NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some("assistant_json:\"answer\"".to_string()),
            summary: Some("answer".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: ExecutionUsage::default(),
            finished_at_ms: 1,
        });
        outcome.terminal_presentation = Some(TerminalPresentation {
            presentation_id: "presentation".to_string(),
            attempt_id: "candidate".to_string(),
            envelope_id: envelope.envelope_id.clone(),
            envelope_revision: envelope.revision,
            state: TerminalPresentationState::Validating,
            answer_origin: AnswerOrigin::TeamSynthesizer,
            source_execution_id: Some("agent-run".to_string()),
            narrator_model: Some("model".to_string()),
            narrator_provider: Some("provider".to_string()),
            models_attempted: Vec::new(),
            validation: AnswerValidation::default(),
            fallback_reason: None,
            generated_at_ms: 1,
            committed_at_ms: None,
        });
        outcome.delivery_envelope = Some(envelope);
        outcome
    }

    #[test]
    fn team_synthesizer_requires_matching_delivery_envelope() {
        let mut outcome = outcome();
        assert!(validate_terminal_candidate(&ticket(), &outcome).is_ok());

        outcome.delivery_envelope = None;
        assert!(validate_terminal_candidate(&ticket(), &outcome).is_err());
    }

    #[test]
    fn mechanical_reduction_without_presentation_is_not_promoted() {
        let mut outcome = outcome();
        outcome.result.result_ref = Some("delivery-envelope: delivery:graph:4".to_string());
        outcome.terminal_presentation = None;
        assert!(validate_terminal_candidate(&ticket(), &outcome).is_ok());
    }
}
