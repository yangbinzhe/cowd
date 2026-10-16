use super::{
    receipt_payload_matches, ExecutionCommitError, ExecutionCommitService, ToolReceiptReplay,
};
use crate::step_snapshot::StepCallBinding;
use harness_contract::execution_graph::ExecutionParentBinding;
use serde_json::json;

pub(crate) struct ToolReceiptQuery<'a> {
    pub(crate) step_binding: &'a StepCallBinding,
    pub(crate) parent_execution: &'a ExecutionParentBinding,
    pub(crate) session_id: &'a str,
    pub(crate) policy_revision: u64,
    pub(crate) idempotency_key: &'a str,
    pub(crate) tool_use_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) input: &'a str,
}

impl ExecutionCommitService {
    pub(crate) fn query_completed_tool_effect(
        &self,
        query: &ToolReceiptQuery<'_>,
    ) -> Result<Option<ToolReceiptReplay>, ExecutionCommitError> {
        self.query_completed_tool_effect_scoped(query, false)
    }

    pub(crate) fn query_completed_agent_tool_effect(
        &self,
        query: &ToolReceiptQuery<'_>,
    ) -> Result<Option<ToolReceiptReplay>, ExecutionCommitError> {
        self.query_completed_tool_effect_scoped(query, true)
    }

    fn query_completed_tool_effect_scoped(
        &self,
        query: &ToolReceiptQuery<'_>,
        exact_parent_node: bool,
    ) -> Result<Option<ToolReceiptReplay>, ExecutionCommitError> {
        let binding = query.step_binding;
        let parent = query.parent_execution;
        let stream = format!("execution-effect:{}", query.idempotency_key);
        for suffix in ["receipt", "read-receipt"] {
            let Some(receipt) = self.event_store.event_by_idempotency_key(
                &stream,
                &format!("{}:{suffix}", query.idempotency_key),
            )?
            else {
                continue;
            };
            let Some(replay) = receipt
                .payload
                .get("replay")
                .filter(|value| !value.is_null())
            else {
                return Ok(None);
            };
            let graph = self
                .effect_authority_graphs
                .load(&parent.execution_id)
                .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            if graph.lineage.as_ref().is_none_or(|lineage| {
                lineage.session_id != query.session_id
                    || lineage.turn_id != binding.snapshot.identity.turn_id
            }) || !graph.nodes.iter().any(|node| node.id == parent.node_id)
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "receipt reader parent lineage mismatch".into(),
                ));
            }
            if query.policy_revision == 0
                || query.session_id != binding.snapshot.identity.session_id
                || replay["session_id"] != json!(query.session_id)
                || replay["parent_execution"]["execution_id"] != json!(parent.execution_id)
                || (exact_parent_node
                    && replay["parent_execution"]["node_id"] != json!(parent.node_id))
                || replay["call_id"] != query.tool_use_id
                || replay["origin"] != json!(binding.origin)
                || receipt.payload["tool_name"] != query.tool_name
                || receipt_payload_matches(query.input, &receipt.payload)? != Some(true)
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "tool receipt read identity or canonical payload conflict".into(),
                ));
            }
            let mut invocation: harness_contract::tool::GovernedToolInvocation =
                serde_json::from_value(replay["invocation"].clone())?;
            if invocation.origin.as_ref() != Some(&binding.origin)
                || invocation.invocation_id != query.tool_use_id
            {
                return Err(ExecutionCommitError::InvalidCommand(
                    "stored receipt invocation identity conflict".into(),
                ));
            }
            invocation.intent.normalized_input =
                harness_contract::step::parse_strict_json(query.input)
                    .map_err(|error| ExecutionCommitError::InvalidCommand(error.to_string()))?;
            let mut outcome: crate::RuntimeToolExecutionOutcome =
                serde_json::from_value(receipt.payload["outcome"].clone())?;
            for evidence in &mut outcome.observed_evidence {
                evidence.provenance =
                    harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;
            }
            return Ok(Some(ToolReceiptReplay {
                invocation,
                outcome,
                completed_at_ms: receipt.created_at_ms,
            }));
        }
        Ok(None)
    }
}
