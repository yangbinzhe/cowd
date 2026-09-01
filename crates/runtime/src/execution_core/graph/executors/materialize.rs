use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use harness_contract::context::{EvidenceAccessRef, EvidenceRef};
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionMaterializationContent, ExecutionMaterializationRequest,
    ExecutionNodeResult, ExecutionNodeSpec, ExecutionNodeStatus, ExecutionUsage,
};
use harness_contract::outcome::WorkspaceMaterializationReceipt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionContext, NodeExecutionOutcome, NodeExecutionTicket,
    NodeExecutor, NodeExecutorError,
};

/// Deterministic workspace effect executor. It never invokes a model and only
/// consumes a committed predecessor result from the same graph.
pub struct MaterializeNodeExecutor {
    state_store: ExecutionGraphStateStore,
    workspace_root: PathBuf,
}

impl MaterializeNodeExecutor {
    pub const KIND: &'static str = "materialize";

    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore, workspace_root: PathBuf) -> Self {
        Self {
            state_store,
            workspace_root,
        }
    }

    fn request(
        node_id: &str,
        payload: &str,
    ) -> Result<ExecutionMaterializationRequest, NodeExecutorError> {
        let request =
            serde_json::from_str::<ExecutionMaterializationRequest>(payload).map_err(|error| {
                NodeExecutorError::Invalid {
                    node_id: node_id.to_string(),
                    reason: format!("invalid materialization request: {error}"),
                }
            })?;
        request
            .validate()
            .map_err(|reason| NodeExecutorError::Invalid {
                node_id: node_id.to_string(),
                reason,
            })?;
        Ok(request)
    }

    fn target(&self, node_id: &str, relative: &str) -> Result<PathBuf, NodeExecutorError> {
        let path = Path::new(relative);
        if path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(NodeExecutorError::Poll {
                node_id: node_id.to_string(),
                reason: "materialization target escapes the workspace".to_string(),
            });
        }
        Ok(self.workspace_root.join(path))
    }
}

#[async_trait]
impl NodeExecutor for MaterializeNodeExecutor {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn validate(&self, node: &ExecutionNodeSpec) -> Result<(), NodeExecutorError> {
        if node.executor_kind != Self::KIND {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Materialize must use the canonical materialize executor".to_string(),
            });
        }
        let request = Self::request(&node.id, &node.payload_ref)?;
        let expected_scope = format!("write:{}", request.target_path);
        if node
            .resource_scopes
            .iter()
            .filter(|scope| *scope == &expected_scope)
            .count()
            != 1
        {
            return Err(NodeExecutorError::Invalid {
                node_id: node.id.clone(),
                reason: "Materialize requires one exact target write scope".to_string(),
            });
        }
        Ok(())
    }

    async fn start(
        &self,
        context: NodeExecutionContext,
    ) -> Result<NodeExecutionTicket, NodeExecutorError> {
        Self::request(&context.node.id, &context.node.payload_ref).map_err(|error| {
            NodeExecutorError::Start {
                node_id: context.node.id.clone(),
                reason: error.to_string(),
            }
        })?;
        Ok(NodeExecutionTicket {
            graph_id: context.graph.id.clone(),
            node_id: context.node.id,
            executor_kind: Self::KIND.to_string(),
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
        let request = Self::request(&ticket.node_id, &ticket.payload_ref).map_err(|error| {
            NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            }
        })?;
        let graph = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let Some(source) = graph.node_results.get(&request.source_node_id) else {
            return Ok(blocked(
                ticket,
                &request,
                "materialization_source_missing",
                "source node has no committed result",
                None,
            ));
        };
        if source.status != ExecutionNodeStatus::Completed {
            return Ok(blocked(
                ticket,
                &request,
                "materialization_source_incomplete",
                "source node did not complete",
                source.result_ref.clone(),
            ));
        }
        let source_ref = source
            .result_ref
            .clone()
            .unwrap_or_else(|| format!("execution-node:{}:result", request.source_node_id));
        let target = self.target(&ticket.node_id, &request.target_path)?;
        let existing = match tokio::fs::read(&target).await {
            Ok(bytes) if !bytes.is_empty() => Some(bytes),
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Ok(blocked(
                    ticket,
                    &request,
                    "materialization_read_failed",
                    &error.to_string(),
                    Some(source_ref),
                ));
            }
        };
        let source_attests_target = source
            .usage
            .runtime_write_attempt_paths
            .iter()
            .any(|path| path == &request.target_path)
            || source.evidence_refs.iter().any(|reference| {
                reference.evidence_ref.ref_type == "runtime_change"
                    && serde_json::from_str::<harness_contract::agent::AgentChangeReceipt>(
                        &reference.evidence_ref.id,
                    )
                    .is_ok_and(|receipt| receipt.path == request.target_path)
            });
        let content = match (&request.content, existing) {
            (ExecutionMaterializationContent::ExistingOrSourceSummary, Some(bytes))
                if source_attests_target =>
            {
                bytes
            }
            (ExecutionMaterializationContent::ExistingOrSourceSummary, None)
            | (ExecutionMaterializationContent::ExistingOrSourceSummary, Some(_))
            | (ExecutionMaterializationContent::SourceSummary, _) => source
                .summary
                .as_deref()
                .filter(|summary| !summary.trim().is_empty())
                .map(str::as_bytes)
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
            (ExecutionMaterializationContent::SourceJsonField { field }, _) => source
                .summary
                .as_deref()
                .and_then(|summary| serde_json::from_str::<serde_json::Value>(summary).ok())
                .and_then(|value| value.get(field).cloned())
                .map(|value| match value {
                    serde_json::Value::String(text) => text.into_bytes(),
                    value => serde_json::to_vec_pretty(&value).unwrap_or_default(),
                })
                .unwrap_or_default(),
        };
        if content.is_empty() {
            return Ok(blocked(
                ticket,
                &request,
                "materialization_content_missing",
                "committed source result does not contain the requested content",
                Some(source_ref),
            ));
        }
        let content_sha256 = sha256(&content);
        if request
            .expected_sha256
            .as_deref()
            .is_some_and(|expected| expected != content_sha256)
        {
            return Ok(blocked(
                ticket,
                &request,
                "materialization_digest_mismatch",
                "source content does not match the requested digest",
                Some(source_ref),
            ));
        }
        let before_sha256 = tokio::fs::read(&target)
            .await
            .ok()
            .map(|bytes| sha256(&bytes));
        let Some(parent) = target.parent() else {
            return Err(NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: "materialization target has no parent".to_string(),
            });
        };
        if let Err(error) = tokio::fs::create_dir_all(parent).await {
            return Ok(blocked(
                ticket,
                &request,
                "materialization_write_failed",
                &error.to_string(),
                Some(source_ref),
            ));
        }
        let canonical_root = tokio::fs::canonicalize(&self.workspace_root)
            .await
            .map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: format!("canonicalize workspace root: {error}"),
            })?;
        let canonical_parent =
            tokio::fs::canonicalize(parent)
                .await
                .map_err(|error| NodeExecutorError::Poll {
                    node_id: ticket.node_id.clone(),
                    reason: format!("canonicalize materialization parent: {error}"),
                })?;
        if !canonical_parent.starts_with(&canonical_root) {
            return Ok(blocked(
                ticket,
                &request,
                "materialization_scope_escape",
                "target parent resolves outside workspace",
                Some(source_ref),
            ));
        }
        let temporary = parent.join(format!(
            ".cowd-materialize-{}-{}.tmp",
            ticket.attempt,
            uuid::Uuid::new_v4().simple()
        ));
        let write_result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .await?;
            file.write_all(&content).await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&temporary, &target).await?;
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(error) = write_result {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Ok(blocked(
                ticket,
                &request,
                "materialization_write_failed",
                &error.to_string(),
                Some(source_ref),
            ));
        }
        let reread = match tokio::fs::read(&target).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Ok(blocked(
                    ticket,
                    &request,
                    "materialization_reread_failed",
                    &error.to_string(),
                    Some(source_ref),
                ));
            }
        };
        let reread_sha256 = sha256(&reread);
        if reread_sha256 != content_sha256 {
            return Ok(blocked(
                ticket,
                &request,
                "materialization_reread_mismatch",
                "written bytes failed digest verification",
                Some(source_ref),
            ));
        }
        let receipt = WorkspaceMaterializationReceipt {
            receipt_id: format!("materialization:{}:{}", ticket.graph_id, ticket.node_id),
            source_execution_id: ticket.graph_id.clone(),
            source_node_id: request.source_node_id.clone(),
            source_result_ref: source_ref,
            target_path: request.target_path.clone(),
            artifact_kind: request.artifact_kind.clone(),
            before_sha256: before_sha256.clone(),
            sha256: reread_sha256.clone(),
            bytes: reread.len() as u64,
            write_effect_id: ticket.idempotency_key.clone(),
            reread_verified: true,
            materialized_at_ms: crate::tool_invocation::now_ms(),
        };
        let change = harness_contract::agent::AgentChangeReceipt {
            path: request.target_path.clone(),
            before_sha256,
            after_sha256: reread_sha256.clone(),
            write_sequence: u64::from(ticket.attempt),
            bytes: Some(reread.len() as u64),
            reread_sequence: Some(u64::from(ticket.attempt)),
            reread_evidence_ref: Some(receipt.receipt_id.clone()),
        };
        let change_json = serde_json::to_string(&change).unwrap_or_default();
        let receipt_json =
            serde_json::to_string(&receipt).map_err(|error| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason: error.to_string(),
            })?;
        let evidence = vec![
            EvidenceAccessRef::durable(
                EvidenceRef::observed("runtime_change", change_json.clone()),
                sha256(change_json.as_bytes()),
                change_json.len() as u64,
                "application/json",
                format!(
                    "execution-graph://{}/node/{}",
                    ticket.graph_id, ticket.node_id
                ),
                "workspace",
            ),
            EvidenceAccessRef::durable(
                EvidenceRef::observed(&request.artifact_kind, request.target_path.clone()),
                reread_sha256,
                reread.len() as u64,
                "application/octet-stream",
                format!("workspace://{}", request.target_path),
                "workspace",
            ),
        ];
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(format!("materialization-receipt:{receipt_json}")),
            summary: Some(receipt_json),
            evidence_refs: evidence,
            failure: None,
            usage: ExecutionUsage {
                runtime_write_attempt_paths: vec![request.target_path],
                tool_calls: 1,
                ..ExecutionUsage::default()
            },
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}

fn blocked(
    _ticket: &NodeExecutionTicket,
    request: &ExecutionMaterializationRequest,
    kind: &str,
    message: &str,
    source_ref: Option<String>,
) -> NodeExecutionOutcome {
    NodeExecutionOutcome::new(ExecutionNodeResult {
        status: ExecutionNodeStatus::Blocked,
        result_ref: source_ref,
        summary: Some(format!(
            "materialization of `{}` to `{}` was not applied: {message}",
            request.artifact_kind, request.target_path
        )),
        evidence_refs: Vec::new(),
        failure: Some(ExecutionFailure {
            kind: kind.to_string(),
            message: message.to_string(),
            retryable: true,
            evidence_refs: Vec::new(),
        }),
        usage: ExecutionUsage {
            runtime_write_attempt_paths: vec![request.target_path.clone()],
            ..ExecutionUsage::default()
        },
        finished_at_ms: crate::tool_invocation::now_ms(),
    })
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionNodeKind, ExecutionNodeStatus,
    };

    use super::*;
    use crate::execution_core::graph::ExecutionCommitService;
    use crate::runtime_event_store::RuntimeEventStore;

    fn request(target_path: &str) -> ExecutionMaterializationRequest {
        ExecutionMaterializationRequest {
            source_node_id: "source".to_string(),
            target_path: target_path.to_string(),
            artifact_kind: target_path.to_string(),
            content: ExecutionMaterializationContent::SourceSummary,
            expected_sha256: None,
        }
    }

    fn node(target_path: &str) -> ExecutionNodeSpec {
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::Materialize,
            MaterializeNodeExecutor::KIND,
            serde_json::to_string(&request(target_path)).unwrap(),
        );
        node.id = "materialize".to_string();
        node.idempotency_key = "materialize:1".to_string();
        node.resource_scopes = vec![format!("write:{target_path}")];
        node
    }

    fn completed_source() -> ExecutionNodeResult {
        ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some("assistant_json:{\"report\":\"hello materializer\"}".to_string()),
            summary: Some("hello materializer\n".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: ExecutionUsage::default(),
            finished_at_ms: 1,
        }
    }

    async fn graph_with_source(
        state: &ExecutionGraphStateStore,
        commits: &ExecutionCommitService,
        materialize: ExecutionNodeSpec,
    ) -> ExecutionGraph {
        let mut graph = ExecutionGraph::new("materialize a required file");
        graph.id = format!("materialize-test:{}", uuid::Uuid::new_v4());
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut source = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "fixture", "source");
        source.id = "source".to_string();
        graph.nodes = vec![source, materialize];
        let graph = commits.register_graph(graph).unwrap().graph;
        let graph = commits
            .transition_node(
                &graph,
                "source",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let graph = commits
            .transition_node(
                &graph,
                "source",
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        commits
            .transition_node(
                &graph,
                "source",
                ExecutionNodeStatus::Completed,
                Some(completed_source()),
                Vec::new(),
            )
            .unwrap();
        state.load_async(&graph.id).await.unwrap()
    }

    #[tokio::test]
    async fn writes_rereads_and_attests_exact_digest() {
        let workspace = tempfile::tempdir().unwrap();
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let state = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let commits = ExecutionCommitService::new(event_store);
        let materialize = node("reports/final.md");
        let graph = graph_with_source(&state, &commits, materialize.clone()).await;
        let executor = MaterializeNodeExecutor::new(state, workspace.path().to_path_buf());
        executor.validate(&materialize).unwrap();
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node: materialize,
                attempt: 1,
            })
            .await
            .unwrap();
        let outcome = executor.poll_or_await(&ticket).await.unwrap();

        assert_eq!(outcome.result.status, ExecutionNodeStatus::Completed);
        let bytes = tokio::fs::read(workspace.path().join("reports/final.md"))
            .await
            .unwrap();
        assert_eq!(bytes, b"hello materializer\n");
        let receipt: WorkspaceMaterializationReceipt =
            serde_json::from_str(outcome.result.summary.as_deref().unwrap()).unwrap();
        assert!(receipt.reread_verified);
        assert_eq!(receipt.sha256, sha256(&bytes));
        assert!(outcome
            .result
            .evidence_refs
            .iter()
            .any(|reference| reference.evidence_ref.ref_type == "runtime_change"));
    }

    #[tokio::test]
    async fn write_failure_blocks_but_retains_source_result_reference() {
        let workspace = tempfile::tempdir().unwrap();
        tokio::fs::write(workspace.path().join("blocked"), b"not a directory")
            .await
            .unwrap();
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let state = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let commits = ExecutionCommitService::new(event_store);
        let materialize = node("blocked/final.md");
        let graph = graph_with_source(&state, &commits, materialize.clone()).await;
        let executor = MaterializeNodeExecutor::new(state, workspace.path().to_path_buf());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node: materialize,
                attempt: 1,
            })
            .await
            .unwrap();
        let outcome = executor.poll_or_await(&ticket).await.unwrap();

        assert_eq!(outcome.result.status, ExecutionNodeStatus::Blocked);
        assert_eq!(
            outcome.result.result_ref.as_deref(),
            Some("assistant_json:{\"report\":\"hello materializer\"}")
        );
        assert!(outcome
            .result
            .failure
            .unwrap()
            .kind
            .starts_with("materialization_"));
    }

    #[tokio::test]
    async fn preexisting_unattested_file_cannot_satisfy_materialization() {
        let workspace = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(workspace.path().join("reports"))
            .await
            .unwrap();
        tokio::fs::write(
            workspace.path().join("reports/final.md"),
            b"stale preexisting bytes",
        )
        .await
        .unwrap();
        let event_store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let state = ExecutionGraphStateStore::new(Arc::clone(&event_store));
        let commits = ExecutionCommitService::new(event_store);
        let materialize = node("reports/final.md");
        let graph = graph_with_source(&state, &commits, materialize.clone()).await;
        let executor = MaterializeNodeExecutor::new(state, workspace.path().to_path_buf());
        let ticket = executor
            .start(NodeExecutionContext {
                graph: Arc::new(graph),
                node: materialize,
                attempt: 1,
            })
            .await
            .unwrap();

        let outcome = executor.poll_or_await(&ticket).await.unwrap();

        assert_eq!(outcome.result.status, ExecutionNodeStatus::Completed);
        assert_eq!(
            tokio::fs::read_to_string(workspace.path().join("reports/final.md"))
                .await
                .unwrap(),
            "hello materializer\n"
        );
    }
}
