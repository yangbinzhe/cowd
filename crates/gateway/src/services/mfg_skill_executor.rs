use std::path::PathBuf;

use app_mfg::{MfgSkillExecutionContext, MfgSkillRun, MfgSkillToolCall, MfgSkillToolResult};
use async_trait::async_trait;
use harness_contract::{
    context::EvidenceAccessRef,
    core::{EvidenceRef, KernelRef},
    execution_graph::{ExecutionFailure, ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage},
};
use runtime::execution_core::{
    NodeExecutionOutcome, NodeExecutionTicket, NodeExecutorError, ScopedNodeBackend,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::MatrixService;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct MfgSkillExecutionPayload {
    pub(crate) execution_id: String,
    pub(crate) skill_id: String,
    pub(crate) expected_incident_revision: u64,
    pub(crate) planned_run: MfgSkillRun,
    pub(crate) evidence_confidence: f32,
}

pub(crate) struct GatewayMfgSkillExecutor {
    matrix: MatrixService,
    cross_plane: std::sync::Arc<runtime::CrossPlaneRuntimeService>,
    config_home: PathBuf,
}

impl GatewayMfgSkillExecutor {
    pub(crate) fn new(
        matrix: MatrixService,
        cross_plane: std::sync::Arc<runtime::CrossPlaneRuntimeService>,
        config_home: PathBuf,
    ) -> Self {
        Self {
            matrix,
            cross_plane,
            config_home,
        }
    }

    fn execute_tool(
        &self,
        call: &MfgSkillToolCall,
        context: &MfgSkillExecutionContext,
    ) -> MfgSkillToolResult {
        match call.tool_name.as_str() {
            "mfg.metric_lineage" => {
                let result = context
                    .metric_keys
                    .iter()
                    .map(|metric_id| {
                        self.matrix
                            .metric_lineage(&self.config_home, metric_id, 4)
                            .map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>();
                tool_result(
                    &call.tool_name,
                    result.map(|lineage| serde_json::json!({"lineage": lineage})),
                    context
                        .metric_keys
                        .iter()
                        .map(|metric_id| format!("matrix://metric/{metric_id}/lineage"))
                        .collect(),
                )
            }
            "mfg.entity_impact_trace" => {
                let entity_ids = context
                    .entity_refs
                    .iter()
                    .filter_map(|reference| canonical_entity_id(reference))
                    .collect::<std::collections::BTreeSet<_>>();
                let result = if entity_ids.is_empty() {
                    Err("skill execution has no canonical entity scope".to_string())
                } else {
                    entity_ids
                        .iter()
                        .map(|entity_id| {
                            match self.matrix.impact_trace(&self.config_home, entity_id, 4) {
                                Ok(trace) => Ok(serde_json::json!({
                                    "entity_id": entity_id,
                                    "status": "resolved",
                                    "trace": trace,
                                })),
                                Err(super::GatewayMatrixRepositoryError::NotFound(_)) => {
                                    Ok(serde_json::json!({
                                        "entity_id": entity_id,
                                        "status": "not_found",
                                    }))
                                }
                                Err(error) => Err(error.to_string()),
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()
                };
                tool_result(
                    &call.tool_name,
                    result.map(|traces| serde_json::json!({"impact_traces": traces})),
                    entity_ids
                        .iter()
                        .map(|entity_id| format!("matrix://entity/{entity_id}/impact-path"))
                        .collect(),
                )
            }
            "mfg.evidence_packet" => {
                let result = context
                    .evidence_packet_id
                    .as_deref()
                    .ok_or_else(|| "skill execution has no canonical evidence packet".to_string())
                    .and_then(|packet_id| {
                        self.matrix
                            .get_evidence_packet(&self.config_home, packet_id)
                            .map_err(|error| error.to_string())?
                            .ok_or_else(|| {
                                format!("canonical evidence packet {packet_id} was not found")
                            })
                    });
                let evidence_refs = context
                    .evidence_packet_id
                    .iter()
                    .map(|packet_id| format!("evidence://matrix/{packet_id}"))
                    .collect();
                tool_result(
                    &call.tool_name,
                    result.map(|packet| serde_json::json!({"packet": packet})),
                    evidence_refs,
                )
            }
            "mfg.cross_plane_preflight" => {
                let mut action = runtime::CrossPlaneAction::new(
                    "runtime:mfg-skill",
                    "mfg.skill.cross_plane.preflight",
                );
                action.session_id = Some(context.incident_id.clone());
                action.resource_ref = context
                    .evidence_packet_id
                    .as_ref()
                    .map(|packet_id| format!("evidence://matrix/{packet_id}"));
                action.identity_trust = runtime::IdentityTrust::Verified;
                let (action, decision, evidence) = self.cross_plane.decide_with_connector_context(
                    action,
                    None,
                    chrono::Utc::now(),
                );
                tool_result(
                    &call.tool_name,
                    Ok(serde_json::json!({
                        "action": action,
                        "decision": decision,
                        "evidence": evidence,
                    })),
                    Vec::new(),
                )
            }
            _ => tool_result(
                &call.tool_name,
                Err(format!(
                    "MFG skill tool {} has no Runtime executor",
                    call.tool_name
                )),
                Vec::new(),
            ),
        }
    }
}

#[async_trait]
impl ScopedNodeBackend for GatewayMfgSkillExecutor {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let payload = serde_json::from_str::<MfgSkillExecutionPayload>(&ticket.payload_ref)
            .map_err(|error| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: format!("invalid MFG skill execution payload: {error}"),
            })?;
        let started_at = chrono::Utc::now();
        let context = payload
            .planned_run
            .execution_context
            .as_ref()
            .ok_or_else(|| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: "MFG skill payload has no canonical execution context".to_string(),
            })?;
        let tool_results = payload
            .planned_run
            .tool_plan
            .iter()
            .map(|call| self.execute_tool(call, context))
            .collect::<Vec<_>>();
        let failed = tool_results
            .iter()
            .filter(|result| result.status != "completed")
            .map(|result| result.tool_name.clone())
            .collect::<Vec<_>>();
        let status = if failed.is_empty() {
            ExecutionNodeStatus::Completed
        } else {
            ExecutionNodeStatus::Failed
        };
        let result_value = serde_json::json!({
            "execution_id": payload.execution_id,
            "status": if failed.is_empty() { "completed" } else { "failed" },
            "started_at": started_at,
            "completed_at": chrono::Utc::now(),
            "tool_results": tool_results,
        });
        let result_bytes =
            serde_json::to_vec(&result_value).map_err(|error| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let evidence = EvidenceAccessRef::durable(
            EvidenceRef(KernelRef::new(
                "mfg_skill_execution",
                payload.execution_id.clone(),
            )),
            format!("{:x}", Sha256::digest(&result_bytes)),
            result_bytes.len() as u64,
            "application/json",
            format!(
                "runtime-execution://{}/nodes/{}",
                ticket.graph_id, ticket.node_id
            ),
            "workspace",
        );
        let finished_at = chrono::Utc::now();
        let result = ExecutionNodeResult {
            status,
            result_ref: Some(String::from_utf8(result_bytes).map_err(|error| {
                NodeExecutorError::Start {
                    node_id: ticket.node_id.clone(),
                    reason: error.to_string(),
                }
            })?),
            summary: Some(if failed.is_empty() {
                format!(
                    "MFG skill executed {} Runtime tool calls",
                    payload.planned_run.tool_plan.len()
                )
            } else {
                format!("MFG skill tools failed: {}", failed.join(", "))
            }),
            evidence_refs: vec![evidence.clone()],
            failure: (!failed.is_empty()).then(|| ExecutionFailure {
                kind: "mfg_skill_tool_failure".to_string(),
                message: format!("failed tools: {}", failed.join(", ")),
                retryable: true,
                evidence_refs: vec![evidence],
            }),
            usage: ExecutionUsage {
                duration_ms: finished_at
                    .signed_duration_since(started_at)
                    .num_milliseconds()
                    .max(0) as u64,
                tool_calls: payload.planned_run.tool_plan.len() as u64,
                ..ExecutionUsage::default()
            },
            finished_at_ms: finished_at.timestamp_millis().max(0) as u64,
        };
        Ok(NodeExecutionOutcome::new(result))
    }
}

fn tool_result(
    tool_name: &str,
    result: Result<serde_json::Value, String>,
    evidence_refs: Vec<String>,
) -> MfgSkillToolResult {
    match result {
        Ok(result) => MfgSkillToolResult {
            tool_name: tool_name.to_string(),
            status: "completed".to_string(),
            summary: format!("{tool_name} completed against canonical services"),
            result,
            evidence_refs,
        },
        Err(error) => MfgSkillToolResult {
            tool_name: tool_name.to_string(),
            status: "failed".to_string(),
            summary: error.clone(),
            result: serde_json::json!({"error": error}),
            evidence_refs,
        },
    }
}

fn canonical_entity_id(reference: &str) -> Option<String> {
    let value = reference
        .trim()
        .strip_prefix("matrix:entity:")
        .or_else(|| reference.trim().strip_prefix("entity:"))
        .unwrap_or(reference.trim())
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .trim();
    (!value.is_empty()).then(|| value.to_string())
}
