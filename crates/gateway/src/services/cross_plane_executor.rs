use async_trait::async_trait;
use harness_contract::execution_graph::{ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage};
use runtime::execution_core::{
    NodeExecutionOutcome, NodeExecutionTicket, NodeExecutorError, ScopedNodeBackend,
};
use runtime::{CrossPlaneDispatchOutcome, CrossPlaneDispatchTarget};
use surface::{SurfaceOperationResult, SurfaceSendRequest};

use super::SurfaceService;
use connector::{builtin_service_connector_registry, ServiceToolRequest};

pub(crate) struct GatewayCrossPlaneExecutor {
    surface: std::sync::Arc<dyn SurfaceSender>,
    target: CrossPlaneDispatchTarget,
    control: std::sync::Arc<runtime::CrossPlaneRuntimeService>,
}

#[async_trait]
trait SurfaceSender: Send + Sync {
    async fn send(&self, request: SurfaceSendRequest) -> Result<SurfaceOperationResult, String>;
}

#[async_trait]
impl SurfaceSender for SurfaceService {
    async fn send(&self, request: SurfaceSendRequest) -> Result<SurfaceOperationResult, String> {
        SurfaceService::send(self, request).await
    }
}

pub(crate) struct GatewayConnectorServiceExecutor {
    service_id: String,
    request: ServiceToolRequest,
}

impl GatewayConnectorServiceExecutor {
    pub(crate) fn new(service_id: String, request: ServiceToolRequest) -> Self {
        Self {
            service_id,
            request,
        }
    }
}

#[async_trait]
impl ScopedNodeBackend for GatewayConnectorServiceExecutor {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        let registry = builtin_service_connector_registry();
        let connector =
            registry
                .connector(&self.service_id)
                .ok_or_else(|| NodeExecutorError::Start {
                    node_id: ticket.node_id.clone(),
                    reason: "connector service is unavailable".to_string(),
                })?;
        let result = connector.execute_tool(self.request.clone());
        let status = if matches!(result.status.as_str(), "ok" | "executed") {
            ExecutionNodeStatus::Completed
        } else {
            ExecutionNodeStatus::Failed
        };
        let node_result = ExecutionNodeResult {
            status,
            result_ref: Some(serde_json::to_string(&result).map_err(|error| {
                NodeExecutorError::Start {
                    node_id: ticket.node_id.clone(),
                    reason: error.to_string(),
                }
            })?),
            evidence_refs: Vec::new(),
            failure: None,
            usage: ExecutionUsage::default(),
            finished_at_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
        };
        Ok(NodeExecutionOutcome::new(node_result))
    }
}

impl GatewayCrossPlaneExecutor {
    pub(crate) fn new(
        surface: SurfaceService,
        target: CrossPlaneDispatchTarget,
        control: std::sync::Arc<runtime::CrossPlaneRuntimeService>,
    ) -> Self {
        Self {
            surface: std::sync::Arc::new(surface),
            target,
            control,
        }
    }
}

#[async_trait]
impl ScopedNodeBackend for GatewayCrossPlaneExecutor {
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        if let Some(receipt) = self
            .control
            .dispatch_receipt(&ticket.idempotency_key)
            .map_err(|error| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?
        {
            return Ok(NodeExecutionOutcome::new(node_result_from_dispatch(
                receipt,
            )?));
        }
        if let Some(intent) = self
            .control
            .dispatch_intent(&ticket.idempotency_key)
            .map_err(|error| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?
        {
            if intent != self.target {
                return Err(NodeExecutorError::Start {
                    node_id: ticket.node_id.clone(),
                    reason: "durable dispatch intent does not match the requested target".into(),
                });
            }
            let outbound =
                intent
                    .outbound_message
                    .as_ref()
                    .ok_or_else(|| NodeExecutorError::Start {
                        node_id: ticket.node_id.clone(),
                        reason: "durable dispatch intent has no outbound message".into(),
                    })?;
            let outcome = CrossPlaneDispatchOutcome::delivery_uncertain(
                intent.platform.as_deref().unwrap_or("unknown"),
                intent.operation.as_deref().unwrap_or("send_text"),
                &outbound.session_key,
            );
            return Ok(NodeExecutionOutcome::new(node_result_from_dispatch(
                outcome,
            )?));
        }
        if !self.target.ready {
            return Err(NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: "cross-plane dispatch target is not ready".to_string(),
            });
        }
        let outbound =
            self.target
                .outbound_message
                .as_ref()
                .ok_or_else(|| NodeExecutorError::Start {
                    node_id: ticket.node_id.clone(),
                    reason: "outbound message is missing".to_string(),
                })?;
        let platform = self
            .target
            .platform
            .clone()
            .ok_or_else(|| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: "platform is missing".to_string(),
            })?;
        let operation = self
            .target
            .operation
            .clone()
            .unwrap_or_else(|| "send_text".to_string());
        self.control
            .begin_dispatch(&ticket.idempotency_key, &self.target)
            .map_err(|error| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let sent = self.surface.send(SurfaceSendRequest {
            surface: platform.clone(), recipient: outbound.session_key.clone(), thread: outbound.reply_to.clone(),
            text: outbound.caption.clone().unwrap_or_else(|| outbound.payload_ref.clone()),
            idempotency_key: Some(ticket.idempotency_key.clone()),
            metadata: serde_json::json!({"payload_kind": outbound.payload_kind, "payload_ref": outbound.payload_ref, "file_name": outbound.file_name, "operation": operation, "source": "execution_graph", "idempotency_key": ticket.idempotency_key}),
        }).await;
        let outcome = match sent {
            Ok(result) if matches!(result.status.as_str(), "sent" | "ok") => {
                CrossPlaneDispatchOutcome::sent(
                    &platform,
                    &operation,
                    &outbound.session_key,
                    result.message_id,
                )
            }
            Ok(result) => {
                let error = result
                    .error
                    .map(|value| value.message)
                    .unwrap_or_else(|| "surface send failed".to_string());
                CrossPlaneDispatchOutcome::failed(
                    &platform,
                    &operation,
                    &outbound.session_key,
                    error,
                )
            }
            Err(error) => CrossPlaneDispatchOutcome::failed(
                &platform,
                &operation,
                &outbound.session_key,
                error.to_string(),
            ),
        };
        let outcome = self
            .control
            .complete_dispatch(&ticket.idempotency_key, &outcome)
            .map_err(|error| NodeExecutorError::Start {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let result = node_result_from_dispatch(outcome)?;
        Ok(NodeExecutionOutcome::new(result))
    }
}

fn node_result_from_dispatch(
    outcome: CrossPlaneDispatchOutcome,
) -> Result<ExecutionNodeResult, NodeExecutorError> {
    let status = match outcome.status.as_str() {
        "sent" => ExecutionNodeStatus::Completed,
        "delivery_uncertain" => ExecutionNodeStatus::Blocked,
        _ => ExecutionNodeStatus::Failed,
    };
    Ok(ExecutionNodeResult {
        status,
        result_ref: Some(serde_json::to_string(&outcome).map_err(|error| {
            NodeExecutorError::Start {
                node_id: "cross_plane_connector".to_string(),
                reason: error.to_string(),
            }
        })?),
        evidence_refs: Vec::new(),
        failure: None,
        usage: ExecutionUsage::default(),
        finished_at_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::{CrossPlaneOutboundMessagePlan, RuntimeEventStore};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingSender {
        sends: AtomicUsize,
    }

    #[async_trait]
    impl SurfaceSender for CountingSender {
        async fn send(
            &self,
            request: SurfaceSendRequest,
        ) -> Result<SurfaceOperationResult, String> {
            self.sends.fetch_add(1, Ordering::SeqCst);
            Ok(SurfaceOperationResult::ok(
                &request.surface,
                serde_json::json!({"status": "sent", "message_id": "provider-1"}),
            ))
        }
    }

    fn target() -> CrossPlaneDispatchTarget {
        CrossPlaneDispatchTarget {
            platform: Some("feishu".into()),
            operation: Some("send_text".into()),
            ready: true,
            outbound_message: Some(CrossPlaneOutboundMessagePlan {
                session_key: "user:1".into(),
                text: "hello".into(),
                payload_kind: "text".into(),
                payload_ref: "hello".into(),
                caption: Some("hello".into()),
                file_name: None,
                reply_to: None,
                metadata: serde_json::Value::Null,
            }),
            ..CrossPlaneDispatchTarget::default()
        }
    }

    #[tokio::test]
    async fn crash_after_external_send_blocks_replay_without_second_send() {
        let control = std::sync::Arc::new(
            runtime::CrossPlaneRuntimeService::open(std::sync::Arc::new(
                RuntimeEventStore::try_open_in_memory().unwrap(),
            ))
            .unwrap(),
        );
        let sender = std::sync::Arc::new(CountingSender {
            sends: AtomicUsize::new(0),
        });
        let key = "stable-key:tool";
        let target = target();
        control.begin_dispatch(key, &target).unwrap();
        SurfaceSender::send(
            sender.as_ref(),
            SurfaceSendRequest {
                surface: "feishu".into(),
                recipient: "user:1".into(),
                thread: None,
                text: "hello".into(),
                idempotency_key: Some(key.into()),
                metadata: serde_json::Value::Null,
            },
        )
        .await
        .unwrap();
        assert_eq!(sender.sends.load(Ordering::SeqCst), 1);

        let executor = GatewayCrossPlaneExecutor {
            surface: sender.clone(),
            target,
            control,
        };
        let outcome = executor
            .execute(&NodeExecutionTicket {
                graph_id: "graph-1".into(),
                node_id: "send-1".into(),
                executor_kind: "cross_plane_connector".into(),
                attempt: 1,
                idempotency_key: key.into(),
                payload_ref: "payload".into(),
            })
            .await
            .unwrap();

        assert_eq!(sender.sends.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.result.status, ExecutionNodeStatus::Blocked);
        assert!(outcome
            .result
            .result_ref
            .as_deref()
            .is_some_and(|value| value.contains("delivery_uncertain")));
    }
}
