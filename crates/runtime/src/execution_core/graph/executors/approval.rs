use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use harness_contract::core::TaskRisk;
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
};
use serde::Deserialize;

use crate::execution_core::graph::{
    NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket, NodeExecutor,
    NodeExecutorError,
};
use crate::{
    ApprovalQueue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, GlobalApprovalStatus,
    SessionExecutionPolicy, SubmitGlobalApprovalRequest,
};

type SessionPolicyLookup =
    Arc<dyn Fn(&str) -> Option<SessionExecutionPolicy> + Send + Sync + 'static>;

const DEFAULT_BLOCKING_APPROVAL_TIMEOUT_MS: u64 = 15 * 60 * 1_000;

#[derive(Debug, Deserialize)]
struct ApprovalPayload {
    action: String,
    summary: String,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    team_id: Option<String>,
    #[serde(default)]
    mission_id: Option<String>,
    #[serde(default)]
    evidence_refs: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    timeout_ms: Option<u64>,
}

pub struct ApprovalNodeExecutor {
    queue: Arc<ApprovalQueue>,
    session_policy_lookup: Option<SessionPolicyLookup>,
}

impl ApprovalNodeExecutor {
    pub const KIND: &'static str = "approval";
    #[must_use]
    pub fn new(queue: Arc<ApprovalQueue>) -> Self {
        Self {
            queue,
            session_policy_lookup: None,
        }
    }

    #[must_use]
    pub fn with_session_policy_lookup(
        queue: Arc<ApprovalQueue>,
        session_policy_lookup: SessionPolicyLookup,
    ) -> Self {
        Self {
            queue,
            session_policy_lookup: Some(session_policy_lookup),
        }
    }
}

/// Stable, unambiguous approval identity for one graph node. Graph and node
/// IDs themselves contain `:` in the canonical Runtime format, so raw string
/// concatenation cannot be parsed safely at Gateway boundaries.
#[must_use]
pub fn graph_approval_id(graph_id: &str, node_id: &str) -> String {
    format!(
        "approval:v1:{}:{}",
        URL_SAFE_NO_PAD.encode(graph_id),
        URL_SAFE_NO_PAD.encode(node_id)
    )
}

/// Decode a Runtime graph approval identity. Legacy colon-concatenated IDs
/// are deliberately rejected: they were ambiguous and could target the wrong
/// graph or node when either identifier contained a colon.
#[must_use]
pub fn parse_graph_approval_id(approval_id: &str) -> Option<(String, String)> {
    let encoded = approval_id.strip_prefix("approval:v1:")?;
    let (graph, node) = encoded.split_once(':')?;
    let graph = String::from_utf8(URL_SAFE_NO_PAD.decode(graph).ok()?).ok()?;
    let node = String::from_utf8(URL_SAFE_NO_PAD.decode(node).ok()?).ok()?;
    (!graph.trim().is_empty() && !node.trim().is_empty()).then_some((graph, node))
}

#[async_trait]
impl NodeExecutor for ApprovalNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }
    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind != Self::KIND {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Approval must use the canonical approval executor".into(),
            });
        }
        serde_json::from_str::<ApprovalPayload>(&node.payload_ref).map_err(|error| {
            NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: format!("invalid approval payload: {error}"),
            }
        })?;
        Ok(())
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
            // Waiting for approval is a completed observation, not the
            // irreversible approved transition. A resumed node must receive
            // a new effect identity so the Runner does not replay the old
            // `WaitingApproval` receipt instead of re-reading the durable
            // approval decision.
            idempotency_key: format!(
                "{}:approval-attempt:{}",
                context.node.idempotency_key, context.attempt
            ),
            payload_ref: context.node.payload_ref,
        })
    }
    async fn poll_or_await(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        // A graph approval decision is committed atomically with graph state
        // by the Runner. Refresh before inspecting the idempotent request so
        // this executor never reopens an approval that another durable commit
        // has already resolved.
        self.queue.refresh();
        let payload: ApprovalPayload =
            serde_json::from_str(&ticket.payload_ref).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let approval_id = graph_approval_id(&ticket.graph_id, &ticket.node_id);
        let source = ApprovalSource {
            kind: if payload.agent_id.is_some() {
                ApprovalSourceKind::Agent
            } else if payload.team_id.is_some() {
                ApprovalSourceKind::Team
            } else if payload.mission_id.is_some() {
                ApprovalSourceKind::Mission
            } else {
                ApprovalSourceKind::Session
            },
            session_id: payload.session_id,
            agent_id: payload.agent_id,
            team_id: payload.team_id,
            mission_id: payload.mission_id,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        let action = payload.action;
        let mut auto_grant = false;
        let mut approval_context = harness_contract::policy::ApprovalContext::owned(
            &source,
            action.clone(),
            &ticket.graph_id,
        );
        if let (Some(session_id), Some(lookup)) =
            (source.session_id.as_deref(), &self.session_policy_lookup)
        {
            if let Some(policy) = lookup(session_id) {
                // Only the explicit trust-all approval profile may bypass a
                // graph wait. DangerFullAccess is a permission ceiling, not
                // approval authority; Autonomous still waits for critical
                // human decisions.
                auto_grant =
                    policy.approval_profile == harness_contract::policy::ApprovalProfile::TrustAll;
                approval_context = approval_context.with_execution_policy(&policy);
            }
        }
        let request = self
            .queue
            .submit_scoped_with_policy(
                approval_id.clone(),
                SubmitGlobalApprovalRequest {
                    context: approval_context,
                    source,
                    action: action.clone(),
                    summary: payload.summary,
                    risk: TaskRisk::High,
                    domain: harness_contract::policy::ApprovalDomain::Execution,
                    blocks_execution: true,
                    evidence_refs: payload.evidence_refs,
                    timeout_policy: ApprovalTimeoutPolicy::ContinueAlternative,
                },
                Some(
                    now_ms().saturating_add(
                        payload
                            .timeout_ms
                            .unwrap_or(DEFAULT_BLOCKING_APPROVAL_TIMEOUT_MS),
                    ),
                ),
                payload.read_only,
                vec![
                    harness_contract::policy::ApprovalGrantScope::Once,
                    harness_contract::policy::ApprovalGrantScope::Turn,
                    harness_contract::policy::ApprovalGrantScope::Task,
                    harness_contract::policy::ApprovalGrantScope::Session,
                ],
            )
            .map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?;
        let mut approval_status = request.status;
        if auto_grant && approval_status == GlobalApprovalStatus::Pending {
            let receipt = self
                .queue
                .decide_internal(crate::ApprovalDecisionCommand {
                    approval_id: approval_id.clone(),
                    approved: true,
                    skip: false,
                    reason: "trust-all session policy auto-approves graph approval".to_string(),
                    scope: harness_contract::policy::ApprovalGrantScope::Once,
                    actor: harness_contract::policy::ApprovalDecisionActor {
                        kind: harness_contract::policy::ApprovalDecisionActorKind::Policy,
                        actor_id: "autonomy-session-policy".to_string(),
                    },
                    evidence_refs: vec!["approval.autonomy_auto_grant_graph".to_string()],
                })
                .map_err(|reason| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason,
                })?;
            approval_status = receipt.status;
        }
        // A single queue-owned active set reconciles deadlines. No approval
        // request may spawn a detached timer that survives terminal cleanup.
        self.queue.reconcile_deadlines_at(now_ms());
        approval_status = self
            .queue
            .get(&approval_id)
            .map_or(approval_status, |approval| approval.status);
        let skip_allowed = approval_status == GlobalApprovalStatus::Skipped && payload.read_only;
        let status = match approval_status {
            GlobalApprovalStatus::Pending => ExecutionNodeStatus::WaitingApproval,
            GlobalApprovalStatus::Approved => ExecutionNodeStatus::Completed,
            GlobalApprovalStatus::Skipped if skip_allowed => ExecutionNodeStatus::Completed,
            GlobalApprovalStatus::Skipped => ExecutionNodeStatus::Failed,
            GlobalApprovalStatus::Denied | GlobalApprovalStatus::TimedOut => {
                ExecutionNodeStatus::WaitingExternal
            }
            GlobalApprovalStatus::Cancelled | GlobalApprovalStatus::Superseded => {
                ExecutionNodeStatus::Cancelled
            }
        };
        let failure = (matches!(
            status,
            ExecutionNodeStatus::Blocked
                | ExecutionNodeStatus::Failed
                | ExecutionNodeStatus::WaitingExternal
        ))
        .then(|| {
            if approval_status == GlobalApprovalStatus::Skipped {
                ExecutionFailure {
                    kind: "approval_skip_not_allowed_for_write".into(),
                    message: format!(
                        "approval `{approval_id}` was skipped for write-capable action `{}`; skip is only allowed for read-only/reversible actions",
                        action
                    ),
                    retryable: false,
                    evidence_refs: Vec::new(),
                }
            } else {
                ExecutionFailure {
                    kind: if status == ExecutionNodeStatus::WaitingExternal {
                        "approval_waiting_external_decision".into()
                    } else {
                        "approval_denied".into()
                    },
                    message: format!(
                        "approval `{approval_id}` was {}",
                        if status == ExecutionNodeStatus::WaitingExternal {
                            "waiting for external decision"
                        } else {
                            "not granted"
                        }
                    ),
                    retryable: false,
                    evidence_refs: Vec::new(),
                }
            }
        });
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref: Some(approval_id),
            summary: failure
                .as_ref()
                .map(|failure| failure.message.clone())
                .or_else(|| {
                    Some(if approval_status == GlobalApprovalStatus::Skipped {
                        "Approval skipped by user; read-only/reversible node may continue"
                            .to_string()
                    } else {
                        "Approval decision committed".to_string()
                    })
                }),
            evidence_refs: Vec::new(),
            failure,
            usage: ExecutionUsage::default(),
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }

    async fn cancel(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        let approval_id = graph_approval_id(&ticket.graph_id, &ticket.node_id);
        self.queue
            .cancel(
                &approval_id,
                "parent execution terminated before approval was decided",
                false,
            )
            .map(|_| ())
            .map_err(|reason| NodeExecutorError::Cancel {
                node_id: ticket.node_id.clone(),
                reason,
            })
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ApprovalDecisionCommand, RuntimeEventStore};
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind};

    #[tokio::test]
    async fn approval_waits_and_only_completes_after_queue_decision() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let executor = ApprovalNodeExecutor::new(Arc::clone(&queue));
        let mut graph = ExecutionGraph::new("approval");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({"action":"write","summary":"write workspace","session_id":"session-1"}).to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();
        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(waiting.status, ExecutionNodeStatus::WaitingApproval);
        let approval_id = waiting.result_ref.unwrap();
        assert!(queue
            .get(&approval_id)
            .and_then(|request| request.expires_at_ms)
            .is_some_and(|deadline| deadline > now_ms()));
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                ApprovalDecisionCommand {
                    approval_id,
                    approved: true,
                    skip: false,
                    reason: "reviewed".into(),
                    scope: crate::ApprovalGrantScope::Once,
                    actor: harness_contract::policy::ApprovalDecisionActor {
                        kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                        actor_id: "test-human".to_string(),
                    },
                    evidence_refs: vec!["test.graph.approval".to_string()],
                },
            )
            .unwrap();
        let completed = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(completed.status, ExecutionNodeStatus::Completed);
    }

    #[tokio::test]
    async fn skipped_approval_completes_node_without_grant() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let executor = ApprovalNodeExecutor::new(Arc::clone(&queue));
        let mut graph = ExecutionGraph::new("skip approval");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({"action":"read","summary":"read evidence","read_only":true,"session_id":"session-1"})
                .to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();
        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(waiting.status, ExecutionNodeStatus::WaitingApproval);
        let approval_id = waiting.result_ref.unwrap();
        queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                ApprovalDecisionCommand {
                    approval_id,
                    approved: false,
                    skip: true,
                    reason: "user skipped the read-only node".into(),
                    scope: crate::ApprovalGrantScope::Once,
                    actor: harness_contract::policy::ApprovalDecisionActor {
                        kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                        actor_id: "test-human".to_string(),
                    },
                    evidence_refs: vec!["test.graph.approval.skip".to_string()],
                },
            )
            .unwrap();
        let completed = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(completed.status, ExecutionNodeStatus::Completed);
        assert!(completed
            .summary
            .as_deref()
            .is_some_and(|summary| summary.contains("skipped")));
    }

    #[tokio::test]
    async fn parent_terminal_cancels_correlated_pending_approval() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let executor = ApprovalNodeExecutor::new(Arc::clone(&queue));
        let mut graph = ExecutionGraph::new("cancel pending approval");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({
                "action": "write",
                "summary": "write workspace",
                "session_id": "session-cancel"
            })
            .to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();
        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        let approval_id = waiting.result_ref.expect("pending approval id");

        executor.cancel(&ticket).await.expect("parent cancellation");

        assert!(queue.pending().is_empty());
        assert_eq!(
            queue
                .get(&approval_id)
                .expect("durable terminal approval")
                .status,
            GlobalApprovalStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn skipped_approval_blocks_write_capable_node() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let executor = ApprovalNodeExecutor::new(Arc::clone(&queue));
        let mut graph = ExecutionGraph::new("skip write approval");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({"action":"write workspace","summary":"write workspace","session_id":"session-1"})
                .to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();
        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(waiting.status, ExecutionNodeStatus::WaitingApproval);
        let approval_id = waiting.result_ref.unwrap();
        let error = queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                ApprovalDecisionCommand {
                    approval_id,
                    approved: false,
                    skip: true,
                    reason: "user skipped the write node".into(),
                    scope: crate::ApprovalGrantScope::Once,
                    actor: harness_contract::policy::ApprovalDecisionActor {
                        kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                        actor_id: "test-human".to_string(),
                    },
                    evidence_refs: vec!["test.graph.approval.skip.write".to_string()],
                },
            )
            .expect_err("write-capable graph approvals cannot be skipped");
        assert_eq!(error, "approval_skip_not_allowed_for_subject");
        let blocked = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(blocked.status, ExecutionNodeStatus::WaitingApproval);
    }

    #[tokio::test]
    async fn skipped_approval_blocks_prefix_bypass_capability() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let executor = ApprovalNodeExecutor::new(Arc::clone(&queue));
        let mut graph = ExecutionGraph::new("skip prefix bypass");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({
                "action": "approval.respond",
                "summary": "approve another approval",
                "session_id": "session-1",
            })
            .to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();
        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(waiting.status, ExecutionNodeStatus::WaitingApproval);
        let approval_id = waiting.result_ref.unwrap();
        let error = queue
            .decide(
                &crate::security::test_human_interactive_principal(),
                ApprovalDecisionCommand {
                    approval_id,
                    approved: false,
                    skip: true,
                    reason: "user skipped approval.respond".into(),
                    scope: crate::ApprovalGrantScope::Once,
                    actor: harness_contract::policy::ApprovalDecisionActor {
                        kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                        actor_id: "test-human".to_string(),
                    },
                    evidence_refs: vec!["test.graph.approval.skip.bypass".to_string()],
                },
            )
            .expect_err("prefix-like action cannot bypass the typed skip contract");
        assert_eq!(error, "approval_skip_not_allowed_for_subject");
        let blocked = executor.poll_or_await(&ticket).await.unwrap().result;
        assert_eq!(blocked.status, ExecutionNodeStatus::WaitingApproval);
    }

    #[tokio::test]
    async fn session_graph_approval_is_fenced_by_the_live_policy_revision() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let policy = SessionExecutionPolicy::from_profile(
            crate::AutonomyProfileId::Yolo,
            9,
            crate::SessionExecutionPolicyOrigin::SessionExplicit,
        );
        let executor = ApprovalNodeExecutor::with_session_policy_lookup(
            Arc::clone(&queue),
            Arc::new(move |session_id| (session_id == "session-9").then_some(policy.clone())),
        );
        let mut graph = ExecutionGraph::new("revision fenced approval");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({
                "action": "write",
                "summary": "write workspace",
                "session_id": "session-9"
            })
            .to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();

        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        let request = queue
            .get(waiting.result_ref.as_deref().unwrap())
            .expect("approval request");
        // Decision B: YOLO (DangerFullAccess) graph approvals are granted by
        // policy without a human, while the full audit pair is retained.
        assert_eq!(waiting.status, ExecutionNodeStatus::Completed);
        assert_eq!(request.status, GlobalApprovalStatus::Approved);
        assert_eq!(request.context.policy_revision, 9);
        assert_eq!(request.context.profile_id, "yolo");
        assert_eq!(
            request.context.approval_profile,
            Some(crate::ApprovalProfile::TrustAll)
        );
    }

    #[tokio::test]
    async fn autonomous_full_access_is_not_approval_authority() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let queue = Arc::new(ApprovalQueue::new(store));
        let policy = SessionExecutionPolicy::from_profile(
            crate::AutonomyProfileId::Autonomous,
            10,
            crate::SessionExecutionPolicyOrigin::SessionExplicit,
        );
        let executor = ApprovalNodeExecutor::with_session_policy_lookup(
            Arc::clone(&queue),
            Arc::new(move |_| Some(policy.clone())),
        );
        let mut graph = ExecutionGraph::new("autonomous approval boundary");
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            ApprovalNodeExecutor::KIND,
            serde_json::json!({
                "action": "critical mutation",
                "summary": "critical mutation",
                "session_id": "session-autonomous"
            })
            .to_string(),
        );
        graph.nodes.push(node.clone());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node,
                attempt: 1,
            })
            .await
            .unwrap();

        let waiting = executor.poll_or_await(&ticket).await.unwrap().result;
        let request = queue
            .get(waiting.result_ref.as_deref().unwrap())
            .expect("durable approval request");
        assert_eq!(waiting.status, ExecutionNodeStatus::WaitingApproval);
        assert_eq!(request.status, GlobalApprovalStatus::Pending);
        assert_eq!(request.context.policy_revision, 10);
        assert_eq!(
            request.context.approval_profile,
            Some(crate::ApprovalProfile::Autonomous)
        );
    }
}
