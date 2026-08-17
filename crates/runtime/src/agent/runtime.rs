use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentBindingSnapshot, AgentCommand, AgentCommandReceipt, AgentCommandRejectReason,
    AgentCommandRequest, AgentInput, AgentLifecycleEvent, AgentReturnPacket, AgentStatus,
    AgentTaskPacket, AgentTerminalStatus, RevisionSelector,
};
use harness_contract::execution::ExecutionIdentity;
use harness_contract::execution_graph::{
    ExecutionEdgeKind, ExecutionNodeKind, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};

use crate::execution_core::graph::executors::{AgentTaskBackend, AgentTaskBackendResolver};
use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope,
};
use crate::{
    project_self_models, AgentLifecyclePhase, AgentRunEvaluation, AgentSelfModel, CowdEvent,
    CowdExecutionContext, CowdExecutionLineage, ProviderRegistry, RuntimeEventStore,
    RuntimeServices,
};
use sha2::{Digest, Sha256};

use crate::agent_catalog::AgentCatalog;
use crate::agent_model_selector::{AgentModelSelection, AgentModelSelector};
use crate::agent_result_validator::validate_agent_return;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunSnapshot {
    pub execution_identity: ExecutionIdentity,
    pub run_id: String,
    pub agent_id: String,
    pub task_id: String,
    pub root_task_id: String,
    pub session_id: String,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub expected_graph_revision: u64,
    pub backend: AgentBackendKind,
    pub status: AgentStatus,
    pub revision: u64,
    pub model: Option<String>,
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<AgentBindingSnapshot>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub failure: Option<String>,
}

/// A verified Agent record exported by the pre-V4 upgrade coordinator.
///
/// Legacy files alone are intentionally not accepted: they did not carry the
/// complete workspace/session/graph/node binding required by the canonical
/// runtime. The coordinator must provide that binding and, for a terminal
/// record, its canonical result before this importer will write lifecycle
/// truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAgentStateRecord {
    pub source_ref: String,
    pub snapshot: AgentRunSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returned: Option<AgentReturnPacket>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAgentImportReport {
    pub source_id: String,
    pub duplicate: bool,
    pub imported_agent_ids: Vec<String>,
    pub blocked_agent_ids: Vec<String>,
}

impl AgentRunSnapshot {
    #[must_use]
    pub fn handle(&self) -> AgentRunHandle {
        AgentRunHandle {
            run_id: self.run_id.clone(),
            agent_id: self.agent_id.clone(),
            backend: self.backend,
            revision: self.revision,
            status: self.status,
        }
    }
}

#[async_trait]
pub trait AgentRuntimeBackend: Send + Sync {
    fn kind(&self) -> AgentBackendKind;
    fn capabilities(&self) -> AgentBackendCapabilities;
    async fn execute(
        &self,
        packet: AgentTaskPacket,
        selection: AgentModelSelection,
    ) -> Result<AgentReturnPacket, String>;
    async fn command(
        &self,
        _handle: &AgentRunHandle,
        _request: &AgentCommandRequest,
    ) -> Result<(), AgentCommandRejectReason> {
        Err(AgentCommandRejectReason::UnsupportedByBackend)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAgentEvent {
    snapshot: AgentRunSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<AgentCommandReceipt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    returned: Option<AgentReturnPacket>,
}

#[derive(Default)]
struct AgentRunRecord {
    snapshot: Option<AgentRunSnapshot>,
    receipts: BTreeMap<String, AgentCommandReceipt>,
    inputs: Vec<AgentInput>,
    returned: Option<AgentReturnPacket>,
}

#[derive(Clone)]
struct RegisteredAgentBackend {
    backend: Arc<dyn AgentRuntimeBackend>,
    observation_authority: bool,
}

/// Single workspace-scoped owner of agent lifecycle state. The event store is
/// canonical; the in-memory map is only a replayable command/cache projection.
pub struct AgentRuntime {
    event_store: Arc<RuntimeEventStore>,
    selector: AgentModelSelector,
    catalog: Arc<AgentCatalog>,
    records: RwLock<BTreeMap<String, AgentRunRecord>>,
    graph_agent_ids: RwLock<BTreeMap<String, BTreeSet<String>>>,
    backends: RwLock<BTreeMap<AgentBackendKind, RegisteredAgentBackend>>,
    services: RwLock<Option<Weak<RuntimeServices>>>,
    pending_cancellations: Mutex<BTreeSet<String>>,
    run_locks: Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>,
    lifecycle_locks: Mutex<BTreeMap<String, Weak<Mutex<()>>>>,
}

impl AgentRuntime {
    #[must_use]
    pub fn new(
        event_store: Arc<RuntimeEventStore>,
        provider_registry: Arc<ProviderRegistry>,
    ) -> Self {
        let runtime = Self {
            event_store,
            selector: AgentModelSelector::new(provider_registry),
            catalog: Arc::new(AgentCatalog::new()),
            records: RwLock::new(BTreeMap::new()),
            graph_agent_ids: RwLock::new(BTreeMap::new()),
            backends: RwLock::new(BTreeMap::new()),
            services: RwLock::new(None),
            pending_cancellations: Mutex::new(BTreeSet::new()),
            run_locks: Mutex::new(BTreeMap::new()),
            lifecycle_locks: Mutex::new(BTreeMap::new()),
        };
        runtime.restore_projection();
        runtime
    }

    async fn acquire_run_lock(&self, agent_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self
                .run_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(agent_id).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(agent_id.to_string(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    fn lifecycle_lock(&self, agent_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self
            .lifecycle_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(agent_id).and_then(Weak::upgrade) {
            lock
        } else {
            let lock = Arc::new(Mutex::new(()));
            locks.insert(agent_id.to_string(), Arc::downgrade(&lock));
            lock
        }
    }

    #[cfg(test)]
    fn retained_lock_counts(&self) -> (usize, usize) {
        let run_locks = {
            let mut locks = self
                .run_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            locks.len()
        };
        let lifecycle_locks = {
            let mut locks = self
                .lifecycle_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            locks.len()
        };
        (run_locks, lifecycle_locks)
    }

    pub fn bind_services(&self, services: Arc<RuntimeServices>) {
        *self
            .services
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(&services));
    }

    #[must_use]
    pub fn services(&self) -> Option<Arc<RuntimeServices>> {
        self.services
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    #[must_use]
    pub fn catalog(&self) -> &Arc<AgentCatalog> {
        &self.catalog
    }

    pub fn register_backend(&self, backend: Arc<dyn AgentRuntimeBackend>) {
        let kind = backend.kind();
        self.backends
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                kind,
                RegisteredAgentBackend {
                    backend,
                    observation_authority: false,
                },
            );
    }

    /// Register a Runtime-owned backend whose observations originate from
    /// the canonical ToolHost receipt chain. Kept crate-private so external
    /// backend plugins cannot self-promote model-authored acceptance.
    pub(crate) fn register_observation_authority_backend(
        &self,
        backend: Arc<dyn AgentRuntimeBackend>,
    ) {
        let kind = backend.kind();
        self.backends
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                kind,
                RegisteredAgentBackend {
                    backend,
                    observation_authority: true,
                },
            );
    }

    #[must_use]
    pub fn list(&self) -> Vec<AgentRunSnapshot> {
        let mut runs = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter_map(|record| record.snapshot.clone())
            .collect::<Vec<_>>();
        runs.sort_by(|left, right| {
            left.updated_at_ms
                .cmp(&right.updated_at_ms)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
        });
        runs
    }

    #[must_use]
    pub fn list_for_graphs(&self, graph_ids: &BTreeSet<String>) -> Vec<AgentRunSnapshot> {
        let agent_ids = {
            let index = self
                .graph_agent_ids
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            graph_ids
                .iter()
                .filter_map(|graph_id| index.get(graph_id))
                .flat_map(|ids| ids.iter().cloned())
                .collect::<BTreeSet<_>>()
        };
        let mut runs = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(agent_id, _)| agent_ids.contains(*agent_id))
            .filter_map(|(_, record)| record.snapshot.as_ref())
            .cloned()
            .collect::<Vec<_>>();
        runs.sort_by(|left, right| {
            left.updated_at_ms
                .cmp(&right.updated_at_ms)
                .then_with(|| left.agent_id.cmp(&right.agent_id))
        });
        runs
    }

    #[must_use]
    pub fn get(&self, agent_id: &str) -> Option<AgentRunSnapshot> {
        self.records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .and_then(|record| record.snapshot.clone())
            .or_else(|| {
                self.latest_durable_record(agent_id)
                    .and_then(|record| record.snapshot)
            })
    }

    /// Return the canonical terminal packet retained with an Agent lifecycle
    /// event. Consumers may project it, but cannot mutate graph state through
    /// this read API.
    #[must_use]
    pub fn terminal_return(&self, agent_id: &str) -> Option<AgentReturnPacket> {
        self.records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .and_then(|record| record.returned.clone())
            .or_else(|| {
                self.event_store
                    .list_stream(&agent_stream_id(agent_id))
                    .ok()?
                    .into_iter()
                    .rev()
                    .filter_map(|event| {
                        serde_json::from_value::<PersistedAgentEvent>(event.payload).ok()
                    })
                    .find_map(|payload| payload.returned)
            })
    }

    fn latest_durable_record(&self, agent_id: &str) -> Option<AgentRunRecord> {
        let event = self
            .event_store
            .latest_for_stream(&agent_stream_id(agent_id))
            .ok()
            .flatten()?;
        let payload = serde_json::from_value::<PersistedAgentEvent>(event.payload).ok()?;
        let mut receipts = BTreeMap::new();
        if let Some(receipt) = payload.receipt {
            receipts.insert(receipt.command_id.clone(), receipt);
        }
        Some(AgentRunRecord {
            snapshot: Some(payload.snapshot),
            returned: payload.returned,
            receipts,
            inputs: Vec::new(),
        })
    }

    /// Restore a verified durable run projection during Runtime recovery.
    ///
    /// This is intentionally a projection recovery API, not an execution API:
    /// callers cannot attach a backend or mutate graph state through it.
    pub fn restore_verified_run(&self, snapshot: AgentRunSnapshot) -> Result<(), String> {
        if [
            snapshot.run_id.as_str(),
            snapshot.agent_id.as_str(),
            snapshot.task_id.as_str(),
            snapshot.session_id.as_str(),
            snapshot.graph_id.as_str(),
            snapshot.node_id.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        {
            return Err("recovered AgentRuntime snapshot has an empty binding".into());
        }
        self.persist_snapshot(snapshot, "agent.recovered", "recovered", None, None)
            .map(|_| ())
    }

    /// Convert replayed active runs without a reattached backend handle into
    /// a durable blocked state. Restart recovery must never pretend a child
    /// process or in-process turn is still controllable merely because its
    /// last persisted event was `running`.
    pub fn block_unrecoverable_replayed_runs(&self) -> Result<Vec<String>, String> {
        let snapshots = self.list();
        let mut blocked = Vec::new();
        for mut snapshot in snapshots
            .into_iter()
            .filter(|snapshot| !snapshot.status.is_terminal())
        {
            snapshot.status = AgentStatus::Blocked;
            snapshot.updated_at_ms = now_ms();
            snapshot.failure = Some("backend handle is unavailable after runtime restart".into());
            let agent_id = snapshot.agent_id.clone();
            self.persist_snapshot(
                snapshot,
                "agent.blocked_recovery",
                "backend handle is unavailable after runtime restart",
                None,
                None,
            )?;
            blocked.push(agent_id);
        }
        Ok(blocked)
    }

    /// Import a coordinator-verified legacy Agent snapshot set exactly once.
    ///
    /// This transaction owns both the import marker and every imported Agent
    /// stream, so a crash cannot leave a marker without an Agent projection or
    /// import only a subset of the supplied records. Active legacy records are
    /// deliberately blocked because an old process/session handle cannot be
    /// proved recoverable by a new runtime.
    pub fn import_legacy_state_records(
        &self,
        source_id: impl Into<String>,
        records: Vec<LegacyAgentStateRecord>,
    ) -> Result<LegacyAgentImportReport, String> {
        let source_id = source_id.into();
        if source_id.trim().is_empty() {
            return Err("legacy Agent import source_id must not be empty".into());
        }
        let marker_stream = legacy_import_stream_id(&source_id);
        let existing_marker = self
            .event_store
            .list_stream(&marker_stream)
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|event| event.kind == "agent.legacy_imported");
        if let Some(marker) = existing_marker {
            return Ok(LegacyAgentImportReport {
                source_id,
                duplicate: true,
                imported_agent_ids: marker.payload["imported_agent_ids"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
                blocked_agent_ids: marker.payload["blocked_agent_ids"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
            });
        }

        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: marker_stream.clone(),
            expected_revision: self
                .event_store
                .stream_revision(&marker_stream)
                .map_err(|error| error.to_string())?,
        }];
        let mut events = Vec::with_capacity(records.len().saturating_add(1));
        let mut imported = Vec::with_capacity(records.len());
        let mut blocked = Vec::new();
        let mut snapshots = Vec::with_capacity(records.len());
        let mut seen_agents = std::collections::BTreeSet::new();

        for record in records {
            validate_legacy_record(&record)?;
            if !seen_agents.insert(record.snapshot.agent_id.clone()) {
                return Err(format!(
                    "legacy Agent import contains duplicate agent_id {}",
                    record.snapshot.agent_id
                ));
            }

            let stream_id = agent_stream_id(&record.snapshot.agent_id);
            let mut snapshot = record.snapshot;
            if !snapshot.status.is_terminal() {
                snapshot.status = AgentStatus::Blocked;
                snapshot.failure =
                    Some("legacy active backend handle is not recoverable by AgentRuntime".into());
                blocked.push(snapshot.agent_id.clone());
            }
            snapshot.updated_at_ms = now_ms();
            snapshot.revision = self
                .event_store
                .stream_revision(&stream_id)
                .map_err(|error| error.to_string())?
                .saturating_add(1);
            expected_streams.push(ExpectedStreamRevision {
                stream_id: stream_id.clone(),
                expected_revision: snapshot.revision.saturating_sub(1),
            });
            let payload = PersistedAgentEvent {
                snapshot: snapshot.clone(),
                receipt: None,
                returned: record.returned,
            };
            events.push(
                RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::Agent,
                    kind: "agent.legacy_imported".into(),
                    status: Some("legacy Agent state imported".into()),
                    actor: Some("upgrade_coordinator".into()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "run".into(),
                            id: snapshot.run_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "legacy_source".into(),
                            id: record.source_ref,
                        },
                    ],
                    payload: serde_json::to_value(payload).map_err(|error| error.to_string())?,
                }
                .into(),
            );
            imported.push(snapshot.agent_id.clone());
            snapshots.push(snapshot);
        }
        events.push(
            RuntimeEventInput {
                stream_id: marker_stream.clone(),
                scope: RuntimeEventScope::Recovery,
                kind: "agent.legacy_imported".into(),
                status: Some("legacy Agent import complete".into()),
                actor: Some("upgrade_coordinator".into()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "source_id": source_id.clone(),
                    "imported_agent_ids": imported.clone(),
                    "blocked_agent_ids": blocked.clone(),
                }),
            }
            .into(),
        );
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("legacy-agent-import:{source_id}"),
                expected_streams,
                events,
            })
            .map_err(|error| error.to_string())?;
        self.restore_projection();
        Ok(LegacyAgentImportReport {
            source_id,
            duplicate: false,
            imported_agent_ids: imported,
            blocked_agent_ids: blocked,
        })
    }

    #[must_use]
    pub fn events(&self, agent_id: &str) -> Vec<AgentLifecycleEvent> {
        let stream_id = agent_stream_id(agent_id);
        self.event_store
            .list_stream(&stream_id)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|event| {
                let payload = serde_json::from_value::<PersistedAgentEvent>(event.payload).ok()?;
                Some(AgentLifecycleEvent {
                    event_id: event.event_id,
                    agent_id: payload.snapshot.agent_id,
                    revision: payload.snapshot.revision,
                    status: payload.snapshot.status,
                    kind: event.kind,
                    message: event.status.unwrap_or_default(),
                    created_at_ms: event.created_at_ms,
                })
            })
            .collect()
    }

    pub async fn execute_task(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String> {
        let _run_guard = self.acquire_run_lock(packet.agent_id()).await;
        if let Some(existing) = self.get(packet.agent_id()) {
            if existing.run_id == packet.run_id() && existing.status.is_terminal() {
                return self.terminal_return(packet.agent_id()).ok_or_else(|| {
                    "terminal AgentRuntime state lacks a canonical return packet".to_string()
                });
            }
            if existing.run_id != packet.run_id() && !existing.status.is_terminal() {
                return Err(format!(
                    "agent {} already owns an active run",
                    packet.agent_id()
                ));
            }
        }
        let packet = self.attach_predecessor_context(packet).await?;
        let packet = self.ensure_runtime_binding(packet)?;
        let backend_kind = backend_from_packet(&packet);
        ensure_team_backend_trusted(&packet, backend_kind)?;
        if self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(packet.agent_id())
        {
            let returned = cancelled_return(
                &packet,
                "agent cancelled before provider/backend admission".to_string(),
            );
            self.persist_snapshot(
                AgentRunSnapshot {
                    execution_identity: packet.assignment.execution_identity.clone(),
                    run_id: packet.run_id().to_string(),
                    agent_id: packet.agent_id().to_string(),
                    task_id: packet.task_id().to_string(),
                    root_task_id: packet.assignment.root_task_id.clone(),
                    session_id: packet.session_id().to_string(),
                    graph_id: packet.graph_id().to_string(),
                    node_id: packet.node_id().to_string(),
                    attempt: packet.attempt,
                    expected_graph_revision: packet.expected_graph_revision,
                    backend: backend_kind,
                    status: AgentStatus::Cancelled,
                    revision: 0,
                    model: None,
                    provider: None,
                    binding: packet.binding.clone(),
                    started_at_ms: now_ms(),
                    updated_at_ms: now_ms(),
                    failure: returned.failure.clone(),
                },
                "agent.cancelled",
                "cancelled before backend admission",
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        let selection = match self.selector.select(nonempty(&packet.model_lease)) {
            Ok(selection) => selection,
            Err(error) => {
                let failure = error.to_string();
                let returned = blocked_return(&packet, failure.clone());
                self.persist_snapshot(
                    AgentRunSnapshot {
                        execution_identity: packet.assignment.execution_identity.clone(),
                        run_id: packet.run_id().to_string(),
                        agent_id: packet.agent_id().to_string(),
                        task_id: packet.task_id().to_string(),
                        root_task_id: packet.assignment.root_task_id.clone(),
                        session_id: packet.session_id().to_string(),
                        graph_id: packet.graph_id().to_string(),
                        node_id: packet.node_id().to_string(),
                        attempt: packet.attempt,
                        expected_graph_revision: packet.expected_graph_revision,
                        backend: backend_kind,
                        status: AgentStatus::Blocked,
                        revision: 0,
                        model: None,
                        provider: None,
                        binding: packet.binding.clone(),
                        started_at_ms: now_ms(),
                        updated_at_ms: now_ms(),
                        failure: Some(failure.clone()),
                    },
                    "agent.blocked",
                    &failure,
                    None,
                    Some(returned.clone()),
                )?;
                return Ok(returned);
            }
        };
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "Runtime failed to materialize Agent Binding".to_string())?;
        if !binding.model_policy.allowed_models.is_empty()
            && !binding
                .model_policy
                .allowed_models
                .contains(&selection.model)
        {
            let failure = format!(
                "selected model `{}` is outside Binding model policy for {}@{}",
                selection.model,
                binding.definition_ref.definition_id.as_str(),
                binding.definition_ref.revision
            );
            let returned = blocked_return(&packet, failure.clone());
            self.persist_snapshot(
                AgentRunSnapshot {
                    execution_identity: packet.assignment.execution_identity.clone(),
                    run_id: packet.run_id().to_string(),
                    agent_id: packet.agent_id().to_string(),
                    task_id: packet.task_id().to_string(),
                    root_task_id: packet.assignment.root_task_id.clone(),
                    session_id: packet.session_id().to_string(),
                    graph_id: packet.graph_id().to_string(),
                    node_id: packet.node_id().to_string(),
                    attempt: packet.attempt,
                    expected_graph_revision: packet.expected_graph_revision,
                    backend: backend_kind,
                    status: AgentStatus::Blocked,
                    revision: 0,
                    model: Some(selection.model),
                    provider: Some(selection.provider),
                    binding: packet.binding.clone(),
                    started_at_ms: now_ms(),
                    updated_at_ms: now_ms(),
                    failure: Some(failure.clone()),
                },
                "agent.blocked",
                &failure,
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        let snapshot = AgentRunSnapshot {
            execution_identity: packet.assignment.execution_identity.clone(),
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            root_task_id: packet.assignment.root_task_id.clone(),
            session_id: packet.session_id().to_string(),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            backend: backend_kind,
            status: AgentStatus::Prepared,
            revision: 0,
            model: Some(selection.model.clone()),
            provider: Some(selection.provider.clone()),
            binding: packet.binding.clone(),
            started_at_ms: now_ms(),
            updated_at_ms: now_ms(),
            failure: None,
        };
        self.persist_snapshot(snapshot, "agent.prepared", "prepared", None, None)?;
        if self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(packet.agent_id())
        {
            let returned = cancelled_return(
                &packet,
                "agent cancelled after prepare and before backend admission".to_string(),
            );
            let mut cancelled = self.get(packet.agent_id()).ok_or_else(|| {
                format!(
                    "prepared agent projection `{}` is missing",
                    packet.agent_id()
                )
            })?;
            cancelled.status = AgentStatus::Cancelled;
            cancelled.updated_at_ms = now_ms();
            cancelled.failure = returned.failure.clone();
            self.persist_snapshot(
                cancelled,
                "agent.cancelled",
                "cancelled before backend admission",
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        let mut running = self.get(packet.agent_id()).ok_or_else(|| {
            format!(
                "prepared agent projection `{}` is missing",
                packet.agent_id()
            )
        })?;
        if running.status == AgentStatus::Cancelled {
            let returned = cancelled_return(
                &packet,
                "agent cancelled during backend admission".to_string(),
            );
            running.failure = returned.failure.clone();
            running.updated_at_ms = now_ms();
            self.persist_snapshot(
                running,
                "agent.terminal",
                "cancelled during backend admission",
                None,
                Some(returned.clone()),
            )?;
            return Ok(returned);
        }
        running.status = AgentStatus::Running;
        running.updated_at_ms = now_ms();
        self.persist_snapshot(running.clone(), "agent.running", "running", None, None)?;

        let backend = self
            .backends
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&backend_kind)
            .cloned();
        let observation_authority = backend
            .as_ref()
            .is_some_and(|registered| registered.observation_authority);
        let mut returned = match backend {
            Some(registered) => match registered.backend.execute(packet.clone(), selection).await {
                Ok(returned) => returned,
                Err(error) => failed_return(&packet, error),
            },
            None => blocked_return(
                &packet,
                format!("agent backend {backend_kind:?} is not installed for this RuntimeServices instance"),
            ),
        };
        if !observation_authority {
            // Extension/process backends may return business output, but they
            // cannot mint Runtime observation truth. Only the crate-private
            // canonical ToolHost-backed registration path has that authority.
            returned.observed_acceptance = crate::path_identity::evaluate_observed_acceptance(
                &packet.required_acceptance,
                Vec::new(),
                Vec::new(),
            );
            returned.runtime_observed_resource_scopes.clear();
        }
        // A cancel/shutdown command is durable lifecycle truth. Backends may
        // observe the interruption as a transport/process error, but they may
        // not overwrite a committed cancellation with `failed` or `completed`.
        let cancellation_requested = self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(packet.agent_id());
        if cancellation_requested
            || self
                .get(packet.agent_id())
                .is_some_and(|snapshot| snapshot.status == AgentStatus::Cancelled)
        {
            returned.status = AgentTerminalStatus::Cancelled;
            returned.outcome.clear();
            returned.failure = Some("agent cancelled by command".into());
        }
        if let Err(error) = validate_agent_return(&packet, &returned) {
            let missing_acceptance = packet
                .acceptance
                .iter()
                .filter(|criterion| {
                    !returned
                        .observed_acceptance
                        .satisfied_criteria
                        .contains(criterion)
                })
                .cloned()
                .collect::<Vec<_>>();
            // A backend result that fails the Runtime contract is a terminal
            // failure, but committed effects, observed evidence, artifacts
            // and the Runtime acceptance evaluation are facts that must
            // survive the verdict. They are never rewritten to empty.
            returned.status = AgentTerminalStatus::Failed;
            returned.outcome.clear();
            returned.failure = Some(format!(
                "Runtime rejected Agent terminal result: {error}; missing_acceptance={missing_acceptance:?}; runtime_change_receipts={}; observed_evidence_count={}; unresolved_obligations={:?}",
                returned.runtime_change_receipts.len(),
                returned.observed_acceptance.observed_evidence.len(),
                returned.observed_acceptance.unresolved_obligation_ids,
            ));
        }
        let mut terminal = self.get(packet.agent_id()).ok_or_else(|| {
            format!(
                "running agent projection `{}` is missing",
                packet.agent_id()
            )
        })?;
        terminal.status = terminal_status(returned.status);
        terminal.updated_at_ms = now_ms();
        terminal.failure = returned.failure.clone();
        let services = self.services();
        let path_resolver = services
            .as_ref()
            .map(|services| services.path_identity_resolver().as_ref());
        let evaluation = AgentRunEvaluation::from_terminal(
            &packet,
            &returned,
            path_resolver,
            terminal.updated_at_ms,
        );
        let refresh_canary_observation = evaluation.as_ref().is_some_and(|evaluation| {
            evaluation.release_channel == Some(harness_contract::agent::ReleaseChannel::Canary)
        });
        let started_at_ms = terminal.started_at_ms;
        let completed_at_ms = terminal.updated_at_ms;
        self.persist_snapshot_with_evaluation(
            terminal,
            "agent.terminal",
            "terminal",
            None,
            Some(returned.clone()),
            evaluation,
        )?;
        self.record_agent_outcome(&packet, &returned, started_at_ms, completed_at_ms)?;
        if refresh_canary_observation {
            if let Some(services) = self.services() {
                if let Err(error) = services.refresh_evolution_canary_observations() {
                    // The terminal run is already durably committed. Canary
                    // observation is a replayable projection and will be
                    // rebuilt before any Stable-review request, so do not
                    // turn a completed user task into a false failure here.
                    tracing::warn!(
                        agent_id = %packet.agent_id(),
                        run_id = %packet.run_id(),
                        error = %error,
                        "failed to refresh replayable Canary observation"
                    );
                }
            }
        }
        Ok(returned)
    }

    fn record_agent_outcome(
        &self,
        packet: &AgentTaskPacket,
        returned: &AgentReturnPacket,
        started_at_ms: u64,
        completed_at_ms: u64,
    ) -> Result<(), String> {
        let Some(services) = self.services() else {
            return Ok(());
        };
        let identity = &packet.assignment.execution_identity;
        let turn_id = identity
            .turn_id()
            .ok_or_else(|| "Agent outcome has no canonical turn identity".to_string())?;
        let terminal = match returned.status {
            AgentTerminalStatus::Completed => {
                harness_contract::outcome::OutcomeTerminalClass::Succeeded(returned.outcome.clone())
            }
            AgentTerminalStatus::Failed => harness_contract::outcome::OutcomeTerminalClass::Failed(
                returned
                    .failure
                    .clone()
                    .unwrap_or_else(|| "failed".to_string()),
            ),
            AgentTerminalStatus::Cancelled => {
                harness_contract::outcome::OutcomeTerminalClass::Cancelled(
                    returned
                        .failure
                        .clone()
                        .unwrap_or_else(|| "cancelled".to_string()),
                )
            }
            AgentTerminalStatus::Blocked => {
                harness_contract::outcome::OutcomeTerminalClass::PartialFailure(
                    returned.failure.clone().unwrap_or_else(|| {
                        "agent blocked; committed evidence retained".to_string()
                    }),
                )
            }
        };
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| "Agent outcome has no immutable binding".to_string())?;
        let evidence_refs = returned
            .evidence_refs
            .iter()
            .map(|reference| reference.evidence_ref.clone())
            .collect::<Vec<_>>();
        let evidence_completeness = if evidence_refs.is_empty() {
            harness_contract::reality::EvidenceCompleteness::None
        } else {
            harness_contract::reality::EvidenceCompleteness::Partial
        };
        let outcome = harness_contract::outcome::ExecutionOutcome {
            identity: harness_contract::outcome::OutcomeIdentity {
                execution_id: returned.run_id.clone(),
                session_id: returned.session_id.clone(),
                turn_id: turn_id.to_string(),
                terminal_generation: u64::from(returned.attempt).saturating_add(1),
                paired_sample_id: None,
                task_id: Some(returned.task_id.clone()),
                mission_id: Some(returned.mission_id.clone()),
                agent_id: Some(returned.agent_id.clone()),
                team_id: returned.team_id.clone(),
                execution_graph_ref: Some(returned.graph_id.clone()),
            },
            runtime: harness_contract::outcome::RuntimeIdentity {
                workspace_key: identity.workspace_id().to_string(),
                runtime_revision: env!("CARGO_PKG_VERSION").to_string(),
                config_revision: format!("agent-binding:{}", binding.binding_digest),
                build: Default::default(),
            },
            provider: (!returned.provider.is_empty() || !returned.model.is_empty()).then(|| {
                harness_contract::outcome::ProviderIdentity {
                    registry_revision: None,
                    provider_name: returned.provider.clone(),
                    model: returned.model.clone(),
                    profile: None,
                    protocol: None,
                    capabilities: std::collections::BTreeMap::new(),
                }
            }),
            strategy: harness_contract::outcome::StrategyIdentity {
                decision_id: binding.binding_digest.clone(),
                policy_revision: format!(
                    "agent-definition:{}@{}",
                    binding.definition_ref.definition_id.as_str(),
                    binding.definition_ref.revision
                ),
                decision_source: "runtime.agent_binding".to_string(),
                selected_candidate: if returned.team_id.is_some() {
                    harness_contract::strategy::ExecutionCandidateKind::Team
                } else {
                    harness_contract::strategy::ExecutionCandidateKind::Direct
                },
                selected_pattern: "agent".to_string(),
            },
            timing: harness_contract::outcome::OutcomeTiming {
                started_at_ms,
                completed_at_ms,
                duration_ms: completed_at_ms.saturating_sub(started_at_ms),
            },
            usage: harness_contract::outcome::OutcomeUsage {
                input_tokens: Some(returned.input_tokens),
                output_tokens: Some(returned.output_tokens),
                cached_tokens: Some(returned.cached_tokens),
                evaluation_tokens: None,
                tool_calls: returned.tool_calls,
                duplicate_tool_calls: returned.duplicate_tool_calls,
                retries: u64::from(returned.attempt),
                max_observed_concurrency: returned.max_tool_concurrency_observed.max(1),
            },
            terminal,
            quality: harness_contract::outcome::OutcomeQuality::Unknown,
            observation: harness_contract::outcome::OutcomeObservation {
                source: "runtime.agent_terminal".to_string(),
                observed_at_ms: completed_at_ms,
                freshness_ms: 0,
            },
            strategy_feedback: harness_contract::outcome::OutcomeStrategyFeedback {
                evaluation_environment: if returned.session_id.starts_with("evolution-eval:") {
                    "evolution_evaluation".to_string()
                } else {
                    "production".to_string()
                },
                ..Default::default()
            },
            evidence_refs,
            evidence_completeness,
            schema_revision: harness_contract::outcome::OUTCOME_SCHEMA_REVISION,
        };
        services.outcome_service().record_terminal(&outcome)?;
        Ok(())
    }

    /// Immutable per-run evidence, written only with a terminal lifecycle
    /// event. This projection deliberately groups by Definition revision and
    /// environment instead of mutable instance reputation.
    #[must_use]
    pub fn evaluations(&self) -> Vec<AgentRunEvaluation> {
        let Ok(events) = self
            .event_store
            .replay_scope_kind(RuntimeEventScope::Evolution, "agent.run_evaluated")
        else {
            return Vec::new();
        };
        let mut evaluations = events
            .into_iter()
            .filter_map(|event| event.payload.get("evaluation").cloned())
            .filter_map(|value| serde_json::from_value::<AgentRunEvaluation>(value).ok())
            .collect::<Vec<_>>();
        evaluations.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.evaluation_id.cmp(&right.evaluation_id))
        });
        evaluations
    }

    #[must_use]
    pub fn self_models(&self) -> Vec<AgentSelfModel> {
        project_self_models(self.evaluations())
    }

    /// Persist a bounded lifecycle progress marker for an active Agent.
    ///
    /// Provider deltas are intentionally not written one-by-one. Backends use
    /// this for state transitions such as execution admission and first model
    /// output, which makes a live team observable without turning telemetry
    /// into transcript or event-store noise.
    pub fn record_progress(&self, agent_id: &str, kind: &str, message: &str) -> Result<(), String> {
        let Some(mut snapshot) = self.get(agent_id) else {
            return Err(format!("agent {agent_id} does not exist"));
        };
        if snapshot.status.is_terminal() {
            return Ok(());
        }
        snapshot.updated_at_ms = now_ms();
        self.persist_snapshot(snapshot, kind, message, None, None)
            .map(|_| ())
    }

    /// Verify the immutable Runtime Binding persisted with an AgentTask node.
    /// New executable packets are compiled before graph registration; Runtime
    /// intentionally rejects an unbound payload rather than selecting a
    /// default Definition during execution or recovery.
    fn ensure_runtime_binding(&self, packet: AgentTaskPacket) -> Result<AgentTaskPacket, String> {
        packet
            .assignment
            .validate()
            .map_err(|error| error.to_string())?;
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "unbound AgentTaskPacket is not executable; compile AgentTaskIntent before graph registration"
                .to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        if packet.agent_id() != binding.instance.instance_id {
            return Err(
                "AgentTaskPacket agent_id must equal its Binding instance identity".to_string(),
            );
        }
        if binding.data_lease.session_id != packet.session_id()
            || binding.data_lease.task_id != packet.task_id()
            || binding.data_lease.team_id.as_deref() != packet.team_id()
        {
            return Err("AgentTaskPacket binding data lease does not match task identity".into());
        }
        if let Some(services) = self.services() {
            let resolved = if binding.evaluation.is_some() {
                services
                    .validate_agent_evaluation_binding(binding)
                    .map_err(|error| {
                        format!("AgentTaskPacket evaluation Binding is not runnable: {error}")
                    })?;
                services
                    .definition_registry()
                    .resolve_agent_canary(&binding.definition_ref)
                    .map_err(|error| {
                        format!(
                            "AgentTaskPacket evaluation Binding cannot resolve its candidate revision: {error}"
                        )
                    })?
            } else if binding.release.as_ref().is_some_and(|release| {
                release.channel == harness_contract::agent::ReleaseChannel::Canary
            }) {
                services
                    .validate_agent_binding_release(binding)
                    .map_err(|error| {
                        format!("AgentTaskPacket Canary Binding is not runnable: {error}")
                    })?;
                services
                    .definition_registry()
                    .resolve_agent_canary(&binding.definition_ref)
                    .map_err(|error| {
                        format!(
                            "AgentTaskPacket Canary Binding cannot resolve its revision: {error}"
                        )
                    })?
            } else {
                services
                    .definition_registry()
                    .resolve_agent(
                        &binding.definition_ref.definition_id,
                        RevisionSelector::ExactApprovedRevision {
                            revision: binding.definition_ref.revision,
                        },
                    )
                    .map_err(|error| format!("AgentTaskPacket Binding is not runnable: {error}"))?
            };
            if resolved.revision.content_digest != binding.definition_digest
                || resolved.agent_markdown != binding.instructions
            {
                return Err(
                    "AgentTaskPacket Binding content does not match the approved Definition"
                        .to_string(),
                );
            }
            verify_binding_against_definition(binding, &resolved.revision.manifest)?;
        }
        Ok(packet)
    }

    /// Derive bounded peer context from the canonical graph projection before a
    /// dependent AgentTask starts. Graph edges remain the scheduling truth;
    /// this method only makes already-committed predecessor results visible to
    /// the next Agent. It has no graph mutation authority and is shared by
    /// protocol and generic Team graphs.
    async fn attach_predecessor_context(
        &self,
        mut packet: AgentTaskPacket,
    ) -> Result<AgentTaskPacket, String> {
        let Some(services) = self.services() else {
            return Ok(packet);
        };
        let Ok(graph) = services
            .graph_state_store()
            .load_async(packet.graph_id().to_string())
            .await
        else {
            // Isolated AgentRuntime tests and external adapters may execute
            // standalone packets. They have no graph peer context to attach.
            return Ok(packet);
        };
        let mut predecessor_ids = BTreeSet::new();
        let predecessors = graph
            .edges
            .iter()
            .filter(|edge| {
                edge.to == packet.node_id()
                    && matches!(
                        edge.kind,
                        ExecutionEdgeKind::DependsOn
                            | ExecutionEdgeKind::Produces
                            | ExecutionEdgeKind::Verifies
                    )
            })
            .filter_map(|edge| {
                graph
                    .nodes
                    .iter()
                    .find(|node| node.id == edge.from && node.kind == ExecutionNodeKind::AgentTask)
            })
            .filter(|node| predecessor_ids.insert(node.id.clone()))
            .collect::<Vec<_>>();
        if predecessors.is_empty() {
            return Ok(packet);
        }

        let max_chars = predecessor_context_limit(packet.budget_lease.max_tokens);
        let mut remaining = max_chars;
        let mut sections = Vec::new();
        for predecessor in predecessors {
            let predecessor_packet: AgentTaskPacket =
                serde_json::from_str(&predecessor.payload_ref).map_err(|error| {
                    format!(
                        "predecessor AgentTask packet {} is invalid: {error}",
                        predecessor.id
                    )
                })?;
            let result = graph.node_results.get(&predecessor.id).ok_or_else(|| {
                format!(
                    "completed predecessor {} has no durable graph result",
                    predecessor.id
                )
            })?;
            let role = predecessor_packet
                .constraints
                .iter()
                .find_map(|constraint| {
                    constraint
                        .strip_prefix("team_role:")
                        .or_else(|| constraint.strip_prefix("protocol_role:"))
                })
                .unwrap_or(predecessor_packet.agent_id());
            let available = remaining.saturating_sub(96);
            if available == 0 {
                break;
            }
            let upstream_outcome = if result.status == ExecutionNodeStatus::Completed {
                result.summary.clone().unwrap_or_else(|| {
                    format!("completed upstream result {}", predecessor_packet.run_id())
                })
            } else {
                format!(
                    "UNRESOLVED: upstream role did not complete: {}",
                    result
                        .failure
                        .as_ref()
                        .map(|failure| failure.message.clone())
                        .unwrap_or_else(|| "no durable terminal summary".to_string())
                )
            };
            let outcome = truncate_context_text(&upstream_outcome, available);
            remaining = remaining.saturating_sub(outcome.len().saturating_add(96));
            sections.push(format!("### Upstream {role}\n{outcome}"));
        }
        if !sections.is_empty() {
            packet.objective.push_str(
                "\n\n## Canonical upstream results\nUse completed peer results as evidence. Entries marked UNRESOLVED are failed or blocked peer lanes, not evidence; preserve them as explicit gaps rather than treating them as facts. Reconcile contradictions explicitly.\n\n",
            );
            packet.objective.push_str(&sections.join("\n\n"));
        }
        Ok(packet)
    }

    pub async fn command(&self, request: AgentCommandRequest) -> AgentCommandReceipt {
        if let Some(receipt) = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&request.agent_id)
            .and_then(|record| record.receipts.get(&request.command_id).cloned())
        {
            return receipt;
        }
        let Some(snapshot) = self.get(&request.agent_id) else {
            return self.reject_command(
                request,
                AgentStatus::Blocked,
                AgentCommandRejectReason::NotFound,
                "agent not found",
            );
        };
        if snapshot.revision != request.expected_revision {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::StaleRevision,
                "agent revision does not match",
            );
        }
        if snapshot.status.is_terminal() {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::Terminal,
                "agent is terminal",
            );
        }
        if matches!(request.command, AgentCommand::SendInput) && request.input.is_none() {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::InvalidInput,
                "send_input requires input",
            );
        }
        let backend = self
            .backends
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&snapshot.backend)
            .cloned();
        let Some(backend) = backend else {
            return self.reject_command(
                request,
                snapshot.status,
                AgentCommandRejectReason::UnsupportedByBackend,
                "agent backend is unavailable",
            );
        };
        if let Err(reason) = backend.backend.command(&snapshot.handle(), &request).await {
            return self.reject_command(
                request,
                snapshot.status,
                reason,
                "agent backend rejected command",
            );
        }
        let mut updated = snapshot;
        if let Some(input) = request.input.clone() {
            self.records
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(updated.agent_id.clone())
                .or_default()
                .inputs
                .push(input);
        }
        updated.status = match request.command {
            AgentCommand::Pause => AgentStatus::Paused,
            AgentCommand::Resume => AgentStatus::Running,
            AgentCommand::Cancel | AgentCommand::Shutdown => AgentStatus::Cancelled,
            AgentCommand::SendInput | AgentCommand::Interrupt => updated.status,
        };
        updated.updated_at_ms = now_ms();
        let receipt = AgentCommandReceipt {
            command_id: request.command_id,
            agent_id: updated.agent_id.clone(),
            accepted_revision: updated.revision.saturating_add(1),
            status: updated.status,
            accepted: true,
            reject_reason: None,
            message: "command accepted".into(),
        };
        self.persist_snapshot(
            updated,
            "agent.command",
            "command accepted",
            Some(receipt.clone()),
            None,
        )
        .unwrap_or_else(|error| AgentCommandReceipt {
            accepted: false,
            reject_reason: Some(AgentCommandRejectReason::InvalidInput),
            message: error,
            ..receipt
        })
    }

    fn reject_command(
        &self,
        request: AgentCommandRequest,
        status: AgentStatus,
        reason: AgentCommandRejectReason,
        message: &str,
    ) -> AgentCommandReceipt {
        let receipt = AgentCommandReceipt {
            command_id: request.command_id,
            agent_id: request.agent_id.clone(),
            accepted_revision: request.expected_revision,
            status,
            accepted: false,
            reject_reason: Some(reason),
            message: message.into(),
        };
        if let Some(snapshot) = self.get(&request.agent_id) {
            let _ = self.persist_snapshot(
                snapshot,
                "agent.command_rejected",
                message,
                Some(receipt.clone()),
                None,
            );
        }
        receipt
    }

    fn persist_snapshot(
        &self,
        snapshot: AgentRunSnapshot,
        kind: &str,
        message: &str,
        receipt: Option<AgentCommandReceipt>,
        returned: Option<AgentReturnPacket>,
    ) -> Result<AgentCommandReceipt, String> {
        self.persist_snapshot_with_evaluation(snapshot, kind, message, receipt, returned, None)
    }

    fn persist_snapshot_with_evaluation(
        &self,
        mut snapshot: AgentRunSnapshot,
        kind: &str,
        message: &str,
        receipt: Option<AgentCommandReceipt>,
        returned: Option<AgentReturnPacket>,
        evaluation: Option<AgentRunEvaluation>,
    ) -> Result<AgentCommandReceipt, String> {
        validate_snapshot_identity(&snapshot)?;
        let lifecycle_lock = self.lifecycle_lock(&snapshot.agent_id);
        let _lifecycle_guard = lifecycle_lock
            .lock()
            .map_err(|_| "AgentRuntime lifecycle lock poisoned".to_string())?;
        let current = self
            .records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&snapshot.agent_id)
            .and_then(|record| record.snapshot.clone());
        match current {
            Some(current) if current.run_id == snapshot.run_id => {
                if current.execution_identity != snapshot.execution_identity {
                    return Err(format!(
                        "Agent {} attempted to change immutable execution identity",
                        snapshot.agent_id
                    ));
                }
                if current.status.is_terminal() && !snapshot.status.is_terminal() {
                    return Err(format!(
                        "Agent lifecycle cannot regress terminal {:?} to {:?}",
                        current.status, snapshot.status
                    ));
                }
                if snapshot.revision != current.revision {
                    return Err(format!(
                        "Agent lifecycle transition is stale for {}: expected revision {}, actual {}",
                        snapshot.agent_id, snapshot.revision, current.revision
                    ));
                }
            }
            Some(current) if !current.status.is_terminal() => {
                return Err(format!(
                    "Agent {} already owns active run {}",
                    snapshot.agent_id, current.run_id
                ));
            }
            Some(_) | None if snapshot.revision != 0 => {
                return Err(format!(
                    "Agent lifecycle initial transition for {} must start at revision 0",
                    snapshot.agent_id
                ));
            }
            Some(_) | None => {}
        }
        let stream_id = agent_stream_id(&snapshot.agent_id);
        snapshot.revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?
            .saturating_add(1);
        let payload = PersistedAgentEvent {
            snapshot: snapshot.clone(),
            receipt: receipt.clone(),
            returned: returned.clone(),
        };
        let candidate_event = returned
            .as_ref()
            .and_then(|returned| {
                crate::knowledge_candidate_projector::agent_terminal_candidate(&snapshot, returned)
            })
            .map(crate::knowledge_candidate_projector::candidate_proposal_event)
            .transpose()?;
        let activity_generation = self
            .services
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
            .and_then(|services| {
                services
                    .graph_state_store()
                    .projection(&snapshot.graph_id)
                    .ok()
            })
            .and_then(|projection| projection.lineage)
            .map_or_else(
                || u64::from(snapshot.attempt.max(1)),
                |lineage| lineage.generation,
            );
        let agent_event = RuntimeEventInput {
            stream_id: stream_id.clone(),
            scope: RuntimeEventScope::Agent,
            kind: kind.into(),
            status: Some(message.into()),
            actor: Some("agent_runtime".into()),
            refs: snapshot_identity_refs(&snapshot),
            payload: serde_json::to_value(payload).map_err(|error| error.to_string())?,
        }
        .with_activity_binding(harness_contract::projection::RuntimeActivityBinding {
            root_execution_id: snapshot.graph_id.clone(),
            session_id: snapshot.session_id.clone(),
            turn_id: snapshot
                .execution_identity
                .turn_id()
                .unwrap_or(snapshot.run_id.as_str())
                .to_string(),
            root_task_id: snapshot.root_task_id.clone(),
            task_id: snapshot.task_id.clone(),
            activity_id: format!(
                "activity:execution:{}:node:{}",
                snapshot.graph_id, snapshot.node_id
            ),
            node_id: Some(snapshot.node_id.clone()),
            parent_activity_id: Some(format!("activity:execution:{}", snapshot.graph_id)),
            initiator_activity_id: Some(format!("activity:execution:{}", snapshot.graph_id)),
            team_run_id: snapshot.execution_identity.team_run_id().map(str::to_owned),
            agent_instance_id: Some(snapshot.agent_id.clone()),
            agent_run_id: Some(snapshot.run_id.clone()),
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: None,
            tool_call_id: None,
            approval_id: None,
            parallel_group_id: None,
            revision: snapshot.revision.max(1),
            fence: snapshot.expected_graph_revision.max(1),
            generation: activity_generation,
        })
        .map_err(|error| error.to_string())?;
        if let Some(evaluation) = evaluation {
            let evaluation_stream = agent_evaluation_stream(&evaluation.run_id);
            let evaluation_revision = self
                .event_store
                .stream_revision(&evaluation_stream)
                .map_err(|error| error.to_string())?;
            let mut expected_streams = vec![
                ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: snapshot.revision.saturating_sub(1),
                },
                ExpectedStreamRevision {
                    stream_id: evaluation_stream.clone(),
                    expected_revision: evaluation_revision,
                },
            ];
            let mut events = vec![
                agent_event.into(),
                RuntimeEventInput {
                    stream_id: evaluation_stream,
                    scope: RuntimeEventScope::Evolution,
                    kind: "agent.run_evaluated".to_string(),
                    status: Some(if evaluation.is_success() {
                        "completed".to_string()
                    } else {
                        "failed".to_string()
                    }),
                    actor: Some("runtime.agent_evaluation".to_string()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "run".to_string(),
                            id: evaluation.run_id.clone(),
                        },
                        RuntimeEventRef {
                            kind: "agent_definition".to_string(),
                            id: format!(
                                "{}@{}",
                                evaluation.definition_id, evaluation.definition_revision
                            ),
                        },
                        RuntimeEventRef {
                            kind: "binding".to_string(),
                            id: evaluation.binding_digest.clone(),
                        },
                    ],
                    payload: serde_json::json!({"evaluation": evaluation}),
                }
                .into(),
            ];
            if let Some(candidate_event) = candidate_event {
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: candidate_event.event.stream_id.clone(),
                    expected_revision: self
                        .event_store
                        .stream_revision(&candidate_event.event.stream_id)
                        .map_err(|error| error.to_string())?,
                });
                events.push(candidate_event);
            }
            self.event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!(
                        "agent-terminal-evaluation:{}:{}",
                        snapshot.run_id, evaluation.evaluation_id
                    ),
                    expected_streams,
                    events,
                })
                .map_err(|error| error.to_string())?;
        } else if let Some(candidate_event) = candidate_event {
            let candidate_stream = candidate_event.event.stream_id.clone();
            self.event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!(
                        "agent-terminal-knowledge:{}:{}",
                        snapshot.run_id, snapshot.revision
                    ),
                    expected_streams: vec![
                        ExpectedStreamRevision {
                            stream_id: stream_id.clone(),
                            expected_revision: snapshot.revision.saturating_sub(1),
                        },
                        ExpectedStreamRevision {
                            stream_id: candidate_stream.clone(),
                            expected_revision: self
                                .event_store
                                .stream_revision(&candidate_stream)
                                .map_err(|error| error.to_string())?,
                        },
                    ],
                    events: vec![agent_event.into(), candidate_event],
                })
                .map_err(|error| error.to_string())?;
        } else {
            self.event_store
                .append(agent_event)
                .map_err(|error| error.to_string())?;
        }
        self.publish_live_lifecycle(&snapshot, kind, message, returned.as_ref());
        let mut records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = records.entry(snapshot.agent_id.clone()).or_default();
        let previous_graph_id = record
            .snapshot
            .as_ref()
            .map(|previous| previous.graph_id.clone());
        record.snapshot = Some(snapshot.clone());
        if returned.is_some() {
            record.returned = returned;
        }
        if let Some(receipt) = receipt {
            record
                .receipts
                .insert(receipt.command_id.clone(), receipt.clone());
            drop(records);
            self.update_graph_index(
                &snapshot.agent_id,
                previous_graph_id.as_deref(),
                &snapshot.graph_id,
            );
            return Ok(receipt);
        }
        drop(records);
        self.update_graph_index(
            &snapshot.agent_id,
            previous_graph_id.as_deref(),
            &snapshot.graph_id,
        );
        Ok(AgentCommandReceipt {
            command_id: format!("event:{}:{}", snapshot.agent_id, snapshot.revision),
            agent_id: snapshot.agent_id,
            accepted_revision: snapshot.revision,
            status: snapshot.status,
            accepted: true,
            reject_reason: None,
            message: message.into(),
        })
    }

    fn publish_live_lifecycle(
        &self,
        snapshot: &AgentRunSnapshot,
        kind: &str,
        message: &str,
        returned: Option<&AgentReturnPacket>,
    ) {
        let Some((phase, status)) = live_lifecycle_phase(kind, snapshot.status) else {
            return;
        };
        let Some(services) = self.services() else {
            return;
        };
        let Some((root_execution_id, parent_bus)) =
            services.resolve_active_execution_bus(&snapshot.graph_id)
        else {
            return;
        };
        let identity = &snapshot.execution_identity;
        let summary = returned
            .and_then(|returned| {
                returned
                    .failure
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| {
                        (!returned.outcome.trim().is_empty()).then_some(returned.outcome.as_str())
                    })
            })
            .or_else(|| (!message.trim().is_empty() && message != status).then_some(message))
            .map(|value| bounded_lifecycle_summary(value, 240));
        parent_bus.emit(CowdEvent::RelatedExecution {
            lineage: CowdExecutionLineage {
                parent_execution_id: root_execution_id,
                graph_id: snapshot.graph_id.clone(),
                node_id: snapshot.node_id.clone(),
                team_id: identity.team_run_id().map(str::to_owned),
                agent_id: Some(snapshot.agent_id.clone()),
            },
            event: Box::new(CowdEvent::ExecutionScoped {
                context: CowdExecutionContext {
                    execution_id: snapshot.run_id.clone(),
                    session_id: snapshot.session_id.clone(),
                    turn_id: identity.turn_id().unwrap_or(&snapshot.run_id).to_string(),
                },
                activity_binding: None,
                event: Box::new(CowdEvent::AgentLifecycle {
                    run_id: snapshot.run_id.clone(),
                    agent_id: snapshot.agent_id.clone(),
                    role: snapshot
                        .binding
                        .as_ref()
                        .and_then(|binding| binding.instance.role_slot_id.clone()),
                    phase,
                    status: status.to_string(),
                    summary,
                }),
            }),
        });
    }

    fn restore_projection(&self) {
        const RESTORE_PAGE_SIZE: usize = 512;
        let mut after_position = None;
        let mut records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            let Ok(events) = self.event_store.list_scope_page_asc(
                RuntimeEventScope::Agent,
                after_position,
                RESTORE_PAGE_SIZE,
            ) else {
                return;
            };
            if events.is_empty() {
                break;
            }
            let page_is_complete = events.len() < RESTORE_PAGE_SIZE;
            after_position = events
                .last()
                .map(|event| (event.commit_cursor, event.transaction_index));
            for event in events {
                let Ok(payload) = serde_json::from_value::<PersistedAgentEvent>(event.payload)
                else {
                    continue;
                };
                if payload.snapshot.status.is_terminal() {
                    // Terminal history remains in the event store and is loaded
                    // only by exact lookup. Keeping it in the active projection
                    // makes restart memory proportional to all historical Agents.
                    records.remove(&payload.snapshot.agent_id);
                    continue;
                }
                let record = records
                    .entry(payload.snapshot.agent_id.clone())
                    .or_default();
                record.snapshot = Some(payload.snapshot);
                if let Some(receipt) = payload.receipt {
                    record.receipts.insert(receipt.command_id.clone(), receipt);
                }
                if payload.returned.is_some() {
                    record.returned = payload.returned;
                }
            }
            if page_is_complete {
                break;
            }
        }
        let index = records
            .iter()
            .filter_map(|(agent_id, record)| {
                record
                    .snapshot
                    .as_ref()
                    .map(|snapshot| (agent_id.clone(), snapshot.graph_id.clone()))
            })
            .fold(
                BTreeMap::<String, BTreeSet<String>>::new(),
                |mut index, (agent_id, graph_id)| {
                    index.entry(graph_id).or_default().insert(agent_id);
                    index
                },
            );
        drop(records);
        *self
            .graph_agent_ids
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = index;
    }

    fn update_graph_index(&self, agent_id: &str, previous_graph_id: Option<&str>, graph_id: &str) {
        let mut index = self
            .graph_agent_ids
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(previous_graph_id) = previous_graph_id.filter(|previous| *previous != graph_id)
        {
            if let Some(agent_ids) = index.get_mut(previous_graph_id) {
                agent_ids.remove(agent_id);
                if agent_ids.is_empty() {
                    index.remove(previous_graph_id);
                }
            }
        }
        index
            .entry(graph_id.to_string())
            .or_default()
            .insert(agent_id.to_string());
    }
}

#[async_trait]
impl AgentTaskBackend for AgentRuntime {
    async fn execute(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String> {
        self.execute_task(packet).await
    }

    async fn cancel(&self, packet: &AgentTaskPacket) -> Result<(), String> {
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(packet.agent_id().to_string());
        let Some(snapshot) = self.get(packet.agent_id()) else {
            return Ok(());
        };
        if snapshot.status.is_terminal() {
            self.pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(packet.agent_id());
            return Ok(());
        }
        let receipt = self
            .command(AgentCommandRequest {
                command_id: format!("graph-cancel:{}:{}", packet.graph_id(), packet.node_id()),
                agent_id: packet.agent_id().to_string(),
                expected_revision: snapshot.revision,
                command: AgentCommand::Cancel,
                input: None,
            })
            .await;
        if receipt.accepted
            || receipt.reject_reason == Some(AgentCommandRejectReason::Terminal)
            || self
                .get(packet.agent_id())
                .is_some_and(|snapshot| snapshot.status == AgentStatus::Cancelled)
        {
            self.pending_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(packet.agent_id());
            Ok(())
        } else {
            Err(format!(
                "AgentRuntime cancel rejected for {}: {}",
                packet.agent_id(),
                receipt.message
            ))
        }
    }

    fn terminal_committed(&self, packet: &AgentTaskPacket) {
        let removed = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(packet.agent_id());
        if removed.is_none() {
            return;
        }
        let mut index = self
            .graph_agent_ids
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(agent_ids) = index.get_mut(packet.graph_id()) {
            agent_ids.remove(packet.agent_id());
            if agent_ids.is_empty() {
                index.remove(packet.graph_id());
            }
        }
    }

    fn cancellation_finalized(&self, packet: &AgentTaskPacket) {
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(packet.agent_id());
    }
}

pub struct AgentRuntimeResolver {
    runtime: Arc<AgentRuntime>,
}

impl AgentRuntimeResolver {
    #[must_use]
    pub fn new(runtime: Arc<AgentRuntime>) -> Self {
        Self { runtime }
    }
}

impl AgentTaskBackendResolver for AgentRuntimeResolver {
    fn resolve(&self, _packet: &AgentTaskPacket) -> Option<Arc<dyn AgentTaskBackend>> {
        Some(Arc::clone(&self.runtime) as Arc<dyn AgentTaskBackend>)
    }
}

fn agent_stream_id(agent_id: &str) -> String {
    format!("agent:{agent_id}")
}

fn agent_evaluation_stream(run_id: &str) -> String {
    format!("agent-evaluation:{run_id}")
}

fn legacy_import_stream_id(source_id: &str) -> String {
    format!("agent-migration:{source_id}")
}

fn validate_snapshot_identity(snapshot: &AgentRunSnapshot) -> Result<(), String> {
    snapshot
        .execution_identity
        .validate()
        .map_err(|error| error.to_string())?;
    if snapshot.execution_identity.kind()
        != harness_contract::execution::ExecutionIdentityKind::AgentNode
        || snapshot.execution_identity.task_id() != Some(snapshot.task_id.as_str())
        || snapshot.execution_identity.session_id() != Some(snapshot.session_id.as_str())
        || snapshot.execution_identity.graph_id() != Some(snapshot.graph_id.as_str())
        || snapshot.execution_identity.agent_run_id() != Some(snapshot.run_id.as_str())
        || snapshot.execution_identity.node_id() != Some(snapshot.node_id.as_str())
    {
        return Err(format!(
            "Agent {} snapshot conflicts with its canonical execution identity",
            snapshot.agent_id
        ));
    }
    Ok(())
}

fn snapshot_identity_refs(snapshot: &AgentRunSnapshot) -> Vec<RuntimeEventRef> {
    let identity = &snapshot.execution_identity;
    let mut refs = vec![
        RuntimeEventRef {
            kind: "principal".to_string(),
            id: identity.principal_id().to_string(),
        },
        RuntimeEventRef {
            kind: "workspace".to_string(),
            id: identity.workspace_id().to_string(),
        },
        RuntimeEventRef {
            kind: "agent_instance".to_string(),
            id: snapshot.agent_id.clone(),
        },
        RuntimeEventRef {
            kind: "agent_run".to_string(),
            id: snapshot.run_id.clone(),
        },
    ];
    for (kind, id) in [
        ("mission", identity.mission_id()),
        ("task", identity.task_id()),
        ("session", identity.session_id()),
        ("turn", identity.turn_id()),
        ("execution_graph", identity.graph_id()),
        ("team_run", identity.team_run_id()),
        ("node", identity.node_id()),
    ] {
        if let Some(id) = id {
            refs.push(RuntimeEventRef {
                kind: kind.to_string(),
                id: id.to_string(),
            });
        }
    }
    refs
}

fn validate_legacy_record(record: &LegacyAgentStateRecord) -> Result<(), String> {
    let snapshot = &record.snapshot;
    validate_snapshot_identity(snapshot)?;
    if record.source_ref.trim().is_empty()
        || [
            snapshot.run_id.as_str(),
            snapshot.agent_id.as_str(),
            snapshot.task_id.as_str(),
            snapshot.session_id.as_str(),
            snapshot.graph_id.as_str(),
            snapshot.node_id.as_str(),
        ]
        .iter()
        .any(|value| value.trim().is_empty())
    {
        return Err("legacy Agent record lacks a verified canonical binding".into());
    }
    if snapshot.status.is_terminal() {
        let Some(returned) = record.returned.as_ref() else {
            return Err(format!(
                "legacy terminal Agent {} lacks its canonical return packet",
                snapshot.agent_id
            ));
        };
        if returned.run_id != snapshot.run_id
            || returned.agent_id != snapshot.agent_id
            || returned.task_id != snapshot.task_id
            || returned.session_id != snapshot.session_id
            || snapshot.execution_identity.mission_id() != Some(returned.mission_id.as_str())
            || returned.graph_id != snapshot.graph_id
            || returned.node_id != snapshot.node_id
            || returned.attempt != snapshot.attempt
            || returned.expected_graph_revision != snapshot.expected_graph_revision
            || terminal_status(returned.status) != snapshot.status
        {
            return Err(format!(
                "legacy terminal Agent {} return binding does not match its snapshot",
                snapshot.agent_id
            ));
        }
    } else if record.returned.is_some() {
        return Err(format!(
            "legacy active Agent {} must not carry a terminal return packet",
            snapshot.agent_id
        ));
    }
    Ok(())
}

fn backend_from_packet(packet: &AgentTaskPacket) -> AgentBackendKind {
    match packet.binding.as_ref().map(|binding| &binding.executor) {
        Some(harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }) => {
            AgentBackendKind::ProcessJsonl
        }
        _ => AgentBackendKind::InProcess,
    }
}

fn ensure_team_backend_trusted(
    packet: &AgentTaskPacket,
    backend_kind: AgentBackendKind,
) -> Result<(), String> {
    if packet.team_id().is_some() && backend_kind != AgentBackendKind::InProcess {
        Err(
            "Team acceptance/evidence/change receipts require the Cowd-native in-process Runtime backend"
                .to_string(),
        )
    } else {
        Ok(())
    }
}

fn verify_binding_against_definition(
    binding: &AgentBindingSnapshot,
    manifest: &harness_contract::agent::AgentDefinitionManifest,
) -> Result<(), String> {
    if binding.executor != manifest.executor {
        return Err("AgentTaskPacket Binding executor differs from the Definition".to_string());
    }
    if binding.model_policy != manifest.model_policy {
        return Err("AgentTaskPacket Binding model policy differs from the Definition".to_string());
    }
    if binding.effective_capabilities.iter().any(|capability| {
        !manifest
            .capability_contract
            .capability_ceiling
            .contains(capability)
    }) {
        return Err(
            "AgentTaskPacket Binding expands the Definition capability ceiling".to_string(),
        );
    }
    if binding
        .skill_refs
        .iter()
        .any(|skill| !manifest.capability_contract.skill_refs.contains(skill))
    {
        return Err("AgentTaskPacket Binding exposes an undeclared Skill".to_string());
    }
    if binding.tool_contract_refs.iter().any(|tool_ref| {
        !binding
            .effective_capabilities
            .contains(&crate::agent::binding::capability_required_by_tool_contract(tool_ref))
    }) {
        return Err(
            "AgentTaskPacket Binding exposes a Tool outside its effective capability grant"
                .to_string(),
        );
    }
    if binding
        .data_lease
        .read_scopes
        .iter()
        .any(|scope| !manifest.cognitive_policy.read_scopes.contains(scope))
        || (binding.data_lease.team_working_state_visible
            && !manifest.cognitive_policy.team_working_state_visible)
        || binding.data_lease.write_mode
            == harness_contract::agent::CognitiveWriteMode::CandidateOnly
            && manifest.cognitive_policy.write_mode
                == harness_contract::agent::CognitiveWriteMode::None
    {
        return Err("AgentTaskPacket Binding expands the Definition cognitive policy".to_string());
    }
    let expected_digest = binding_digest(binding)?;
    if binding.binding_digest != expected_digest {
        return Err("AgentTaskPacket Binding digest is invalid".to_string());
    }
    Ok(())
}

fn binding_digest(binding: &AgentBindingSnapshot) -> Result<String, String> {
    let mut unsigned = binding.clone();
    unsigned.binding_digest.clear();
    serde_json::to_string(&unsigned)
        .map(|encoded| format!("{:x}", Sha256::digest(encoded.as_bytes())))
        .map_err(|error| format!("failed to encode Agent Binding: {error}"))
}

fn predecessor_context_limit(budget_tokens: u64) -> usize {
    let derived = budget_tokens.saturating_div(5).saturating_mul(4) as usize;
    if derived == 0 {
        12_000
    } else {
        derived.clamp(1_024, 16_384)
    }
}

fn truncate_context_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let retained = max_chars.saturating_sub(48);
    let mut output = value.chars().take(retained).collect::<String>();
    output.push_str("\n[upstream result truncated; canonical result remains in graph evidence]");
    output
}

fn live_lifecycle_phase(
    kind: &str,
    status: AgentStatus,
) -> Option<(AgentLifecyclePhase, &'static str)> {
    match kind {
        "agent.running" => Some((AgentLifecyclePhase::Started, "running")),
        "agent.provider.first_output" => Some((AgentLifecyclePhase::FirstOutput, "running")),
        "agent.acceptance.evaluated" => Some((AgentLifecyclePhase::Evaluating, "running")),
        "agent.cancelled" => Some((AgentLifecyclePhase::Cancelled, "cancelled")),
        "agent.blocked" => Some((AgentLifecyclePhase::Blocked, "blocked")),
        "agent.terminal" => match status {
            AgentStatus::Completed => Some((AgentLifecyclePhase::Completed, "completed")),
            AgentStatus::Failed => Some((AgentLifecyclePhase::Failed, "failed")),
            AgentStatus::Cancelled => Some((AgentLifecyclePhase::Cancelled, "cancelled")),
            AgentStatus::Blocked => Some((AgentLifecyclePhase::Blocked, "blocked")),
            _ => None,
        },
        _ => None,
    }
}

fn bounded_lifecycle_summary(value: &str, max_chars: usize) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        compact
    } else {
        compact
            .chars()
            .take(max_chars.saturating_sub(1))
            .chain(std::iter::once('…'))
            .collect()
    }
}

fn terminal_status(status: AgentTerminalStatus) -> AgentStatus {
    match status {
        AgentTerminalStatus::Completed => AgentStatus::Completed,
        AgentTerminalStatus::Failed => AgentStatus::Failed,
        AgentTerminalStatus::Cancelled => AgentStatus::Cancelled,
        AgentTerminalStatus::Blocked => AgentStatus::Blocked,
    }
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.trim().is_empty() && value != "default" && value != "auto").then_some(value)
}

fn blocked_return(packet: &AgentTaskPacket, failure: String) -> AgentReturnPacket {
    return_packet(
        packet,
        AgentTerminalStatus::Blocked,
        String::new(),
        Some(failure),
    )
}

fn failed_return(packet: &AgentTaskPacket, failure: String) -> AgentReturnPacket {
    return_packet(
        packet,
        AgentTerminalStatus::Failed,
        String::new(),
        Some(failure),
    )
}

fn cancelled_return(packet: &AgentTaskPacket, failure: String) -> AgentReturnPacket {
    return_packet(
        packet,
        AgentTerminalStatus::Cancelled,
        String::new(),
        Some(failure),
    )
}

fn return_packet(
    packet: &AgentTaskPacket,
    status: AgentTerminalStatus,
    outcome: String,
    failure: Option<String>,
) -> AgentReturnPacket {
    AgentReturnPacket {
        run_id: packet.run_id().to_string(),
        agent_id: packet.agent_id().to_string(),
        task_id: packet.task_id().to_string(),
        session_id: packet.session_id().to_string(),
        mission_id: packet.mission_id().to_string(),
        team_id: packet.team_id().map(str::to_owned),
        graph_id: packet.graph_id().to_string(),
        node_id: packet.node_id().to_string(),
        attempt: packet.attempt,
        expected_graph_revision: packet.expected_graph_revision,
        status,
        outcome,
        answer_candidate: None,
        observed_acceptance: Default::default(),
        acceptance: Vec::new(),
        evidence_refs: Vec::new(),
        changes: Vec::new(),
        runtime_change_receipts: Vec::new(),
        conflicts: Vec::new(),
        unresolved: Vec::new(),
        input_tokens: 0,
        output_tokens: 0,
        cached_tokens: 0,
        model: String::new(),
        provider: String::new(),
        tool_calls: 0,
        duplicate_tool_calls: 0,
        max_tool_concurrency_observed: 0,
        parallel_tool_batches: 0,
        runtime_write_attempt_paths: Vec::new(),
        runtime_observed_resource_scopes: Vec::new(),
        failure,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{ProviderConfig, ProvidersConfig};
    use harness_contract::agent::{AgentCapability, AgentDefinitionId, DefinitionScope};
    use harness_contract::context::ChildExecutionBudgetReservation;

    struct CompletedBackend;

    #[async_trait]
    impl AgentRuntimeBackend for CompletedBackend {
        fn kind(&self) -> AgentBackendKind {
            AgentBackendKind::InProcess
        }

        fn capabilities(&self) -> AgentBackendCapabilities {
            AgentBackendCapabilities::in_process()
        }

        async fn execute(
            &self,
            packet: AgentTaskPacket,
            selection: AgentModelSelection,
        ) -> Result<AgentReturnPacket, String> {
            Ok(AgentReturnPacket {
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                session_id: packet.session_id().to_string(),
                mission_id: packet.mission_id().to_string(),
                team_id: packet.team_id().map(ToString::to_string),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: "completed".into(),
                answer_candidate: None,
                observed_acceptance: harness_contract::context::ObservedAcceptance {
                    satisfied_criteria: vec!["verified".into()],
                    observed_evidence: Vec::new(),
                    unresolved_obligation_ids: Vec::new(),
                },
                acceptance: vec!["verified".into()],
                evidence_refs: Vec::new(),
                changes: Vec::new(),
                runtime_change_receipts: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 3,
                output_tokens: 2,
                cached_tokens: 0,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 0,
                duplicate_tool_calls: 0,
                max_tool_concurrency_observed: 0,
                parallel_tool_batches: 0,
                runtime_write_attempt_paths: Vec::new(),
                runtime_observed_resource_scopes: Vec::new(),
                failure: None,
            })
        }

        async fn command(
            &self,
            _handle: &AgentRunHandle,
            _request: &AgentCommandRequest,
        ) -> Result<(), AgentCommandRejectReason> {
            Ok(())
        }
    }

    fn configured_registry() -> Arc<ProviderRegistry> {
        Arc::new(
            ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "test".into(),
                    ProviderConfig {
                        name: "test".into(),
                        base_url: "https://example.test/v1".into(),
                        api_key: "test".into(),
                        models: vec!["fast".into()],
                        protocol: Some("responses".into()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            })
            .expect("valid provider registry"),
        )
    }

    fn test_binding(agent_id: &str) -> AgentBindingSnapshot {
        AgentBindingSnapshot {
            binding_id: format!("binding:{agent_id}"),
            definition_ref: harness_contract::agent::AgentDefinitionRevisionRef {
                definition_id: AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct")
                    .expect("builtin id"),
                revision: 1,
            },
            definition_digest: "a".repeat(64),
            instructions: "# Test\n\nComplete the test task.\n".to_string(),
            instance: harness_contract::agent::AgentInstanceRef {
                instance_id: format!("instance:{agent_id}"),
                role_slot_id: None,
            },
            executor: harness_contract::agent::AgentExecutorPolicy::CowdNative,
            model_policy: harness_contract::agent::AgentModelPolicy {
                profile: "test".to_string(),
                allowed_models: vec!["fast".to_string()],
                fallback_allowed: true,
            },
            effective_capabilities: vec![AgentCapability::Read],
            skill_refs: Vec::new(),
            tool_contract_refs: Vec::new(),
            data_lease: harness_contract::agent::AgentDataLease {
                session_id: "session-1".to_string(),
                task_id: "task-1".to_string(),
                team_id: None,
                read_scopes: vec![harness_contract::agent::CognitiveReadScope::Session],
                write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
                fact_boundaries: Vec::new(),
                fact_refs: Vec::new(),
                matrix_snapshot_refs: Vec::new(),
            },
            release: None,
            evaluation: None,
            display: None,
            binding_digest: "b".repeat(64),
        }
    }

    fn task(agent_id: &str) -> AgentTaskPacket {
        let binding = test_binding(agent_id);
        let instance_id = binding.instance.instance_id.clone();
        let definition_ref = binding.definition_ref.clone();
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                Some(definition_ref),
                &instance_id,
                &format!("run-{agent_id}"),
                "task-1",
                "session-1",
                "mission-1",
                None,
                "graph-1",
                "node-1",
            ),
            attempt: 1,
            expected_graph_revision: 1,
            policy_revision: 1,
            objective: "verify lifecycle".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: vec!["verified".into()],
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "fast".into(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget-1",
                instance_id,
                "agent",
                1_000,
                75_000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: Some(binding),
            managed_invocation: None,
            idempotency_key: format!("idempotency-{agent_id}"),
        }
    }

    fn team_task(agent_id: &str, team_run_id: &str) -> AgentTaskPacket {
        let mut packet = task(agent_id);
        let definition_ref = packet.assignment.definition_ref.clone();
        packet.assignment = crate::test_support::agent_assignment(
            Some(definition_ref),
            packet.agent_id(),
            packet.run_id(),
            packet.task_id(),
            packet.session_id(),
            packet.mission_id(),
            Some(team_run_id),
            packet.graph_id(),
            packet.node_id(),
        );
        if let Some(binding) = packet.binding.as_mut() {
            binding.data_lease.team_id = Some(team_run_id.to_string());
            binding.data_lease.team_working_state_visible = true;
        }
        packet
    }

    #[test]
    fn process_backend_cannot_mint_team_acceptance_or_change_receipts() {
        let mut packet = team_task("external-team", "team-1");
        packet.binding.as_mut().expect("binding").executor =
            harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                command_ref: "external/worker".to_string(),
            };

        let backend = backend_from_packet(&packet);
        assert_eq!(backend, AgentBackendKind::ProcessJsonl);
        assert!(ensure_team_backend_trusted(&packet, backend).is_err());
    }

    #[tokio::test]
    async fn graph_cancel_before_agent_poll_persists_terminal_cancelled_without_backend_work() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(store, configured_registry());
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend));
        let packet = task("cancel-before-poll");

        AgentTaskBackend::cancel(&runtime, &packet)
            .await
            .expect("queue graph cancellation");
        let returned = runtime
            .execute_task(packet.clone())
            .await
            .expect("cancelled return");

        assert_eq!(returned.status, AgentTerminalStatus::Cancelled);
        assert_eq!(
            runtime
                .get(packet.agent_id())
                .expect("cancelled projection")
                .status,
            AgentStatus::Cancelled
        );
        assert!(runtime
            .events(packet.agent_id())
            .iter()
            .any(|event| event.status == AgentStatus::Cancelled));
    }

    #[tokio::test]
    async fn public_backend_registration_cannot_self_promote_observation_truth() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(store, configured_registry());
        runtime.register_backend(Arc::new(CompletedBackend));

        let returned = runtime
            .execute_task(task("untrusted-observation"))
            .await
            .expect("terminal packet");

        assert_eq!(returned.status, AgentTerminalStatus::Failed);
        assert!(returned.observed_acceptance.observed_evidence.is_empty());
        assert!(returned
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("omitted acceptance evaluation")));
    }

    #[tokio::test]
    async fn rejected_backend_result_persists_a_terminal_failed_projection() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(store, configured_registry());
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend));
        let mut packet = team_task("invalid-acceptance", "team-1");
        packet.acceptance = vec!["evidence".to_string()];
        packet.constraints.push(format!(
            "team_acceptance_contract:{}",
            serde_json::to_string(&vec![harness_contract::team::TeamAcceptanceRequirement {
                criterion: "evidence".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                    scopes: vec!["read:src".to_string()],
                },
            },])
            .expect("acceptance contract")
        ));

        let returned = runtime
            .execute_task(packet.clone())
            .await
            .expect("validation failure is a terminal Agent result");

        assert_eq!(returned.status, AgentTerminalStatus::Failed);
        assert!(returned
            .failure
            .as_deref()
            .is_some_and(|failure| failure.contains("Runtime rejected Agent terminal result")));
        assert_eq!(returned.input_tokens, 3);
        assert_eq!(returned.output_tokens, 2);
        assert_eq!(
            runtime
                .get(packet.agent_id())
                .expect("terminal projection")
                .status,
            AgentStatus::Failed
        );
        assert!(runtime
            .events(packet.agent_id())
            .iter()
            .any(|event| event.status == AgentStatus::Failed));
    }

    #[test]
    fn stale_prepare_or_running_snapshot_cannot_overwrite_terminal_cancellation() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(store, configured_registry());
        let packet = task("cancel-cas");
        let prepared = legacy_snapshot(&packet, AgentStatus::Prepared);
        runtime
            .persist_snapshot(prepared, "agent.prepared", "prepared", None, None)
            .expect("prepare");
        let stale_prepared = runtime.get(packet.agent_id()).expect("prepared projection");
        let mut cancelled = stale_prepared.clone();
        cancelled.status = AgentStatus::Cancelled;
        runtime
            .persist_snapshot(cancelled, "agent.command", "cancelled", None, None)
            .expect("cancel");

        let mut stale_running = stale_prepared;
        stale_running.status = AgentStatus::Running;
        assert!(runtime
            .persist_snapshot(stale_running, "agent.running", "running", None, None)
            .is_err());
        assert_eq!(
            runtime.get(packet.agent_id()).expect("terminal").status,
            AgentStatus::Cancelled
        );
    }

    fn legacy_snapshot(packet: &AgentTaskPacket, status: AgentStatus) -> AgentRunSnapshot {
        AgentRunSnapshot {
            execution_identity: packet.assignment.execution_identity.clone(),
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            root_task_id: packet.assignment.root_task_id.clone(),
            session_id: packet.session_id().to_string(),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            backend: AgentBackendKind::InProcess,
            status,
            revision: 0,
            model: Some("fast".into()),
            provider: Some("test".into()),
            binding: None,
            started_at_ms: 1,
            updated_at_ms: 1,
            failure: None,
        }
    }

    #[test]
    fn graph_scoped_agent_lookup_uses_replayable_index() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(store, configured_registry());
        let packet = task("graph-indexed");
        runtime
            .restore_verified_run(legacy_snapshot(&packet, AgentStatus::Completed))
            .expect("restore indexed Agent");

        assert_eq!(
            runtime
                .list_for_graphs(&BTreeSet::from(["graph-1".to_string()]))
                .iter()
                .map(|snapshot| snapshot.agent_id.as_str())
                .collect::<Vec<_>>(),
            vec![packet.agent_id()],
        );
        assert!(runtime
            .list_for_graphs(&BTreeSet::from(["graph-other".to_string()]))
            .is_empty());
    }

    #[tokio::test]
    async fn legacy_import_is_atomic_idempotent_and_blocks_unrecoverable_active_runs() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(Arc::clone(&store), configured_registry());
        let active = task("legacy-active");
        let completed = task("legacy-completed");
        let returned = return_packet(
            &completed,
            AgentTerminalStatus::Completed,
            "completed before upgrade".into(),
            None,
        );
        let report = runtime
            .import_legacy_state_records(
                "upgrade-manifest-sha256",
                vec![
                    LegacyAgentStateRecord {
                        source_ref: "legacy/active.json".into(),
                        snapshot: legacy_snapshot(&active, AgentStatus::Running),
                        returned: None,
                    },
                    LegacyAgentStateRecord {
                        source_ref: "legacy/completed.json".into(),
                        snapshot: legacy_snapshot(&completed, AgentStatus::Completed),
                        returned: Some(returned.clone()),
                    },
                ],
            )
            .expect("import succeeds");
        assert!(!report.duplicate);
        assert_eq!(report.imported_agent_ids.len(), 2);
        assert_eq!(
            report.blocked_agent_ids,
            vec![active.agent_id().to_string()]
        );
        assert_eq!(
            runtime
                .get(active.agent_id())
                .expect("active projection")
                .status,
            AgentStatus::Blocked
        );
        assert_eq!(
            runtime
                .execute_task(completed.clone())
                .await
                .expect("replayed terminal result"),
            returned
        );

        let replayed = AgentRuntime::new(Arc::clone(&store), configured_registry());
        assert_eq!(
            replayed
                .get(completed.agent_id())
                .expect("terminal replay")
                .status,
            AgentStatus::Completed
        );
        let duplicate = replayed
            .import_legacy_state_records("upgrade-manifest-sha256", Vec::new())
            .expect("duplicate marker");
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.imported_agent_ids.len(), 2);
    }

    #[test]
    fn legacy_import_rejects_unbound_records_without_partial_writes() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(Arc::clone(&store), configured_registry());
        let valid = task("legacy-valid");
        let mut invalid_snapshot = legacy_snapshot(&task("legacy-invalid"), AgentStatus::Running);
        invalid_snapshot.graph_id.clear();
        runtime
            .import_legacy_state_records(
                "bad-upgrade-manifest",
                vec![
                    LegacyAgentStateRecord {
                        source_ref: "legacy/valid.json".into(),
                        snapshot: legacy_snapshot(&valid, AgentStatus::Running),
                        returned: None,
                    },
                    LegacyAgentStateRecord {
                        source_ref: "legacy/invalid.json".into(),
                        snapshot: invalid_snapshot,
                        returned: None,
                    },
                ],
            )
            .expect_err("unbound records are rejected");
        assert!(runtime.get(valid.agent_id()).is_none());
        assert!(store
            .list_stream(&legacy_import_stream_id("bad-upgrade-manifest"))
            .expect("marker stream")
            .is_empty());
    }

    #[tokio::test]
    async fn terminal_lifecycle_replays_from_the_event_store() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(Arc::clone(&store), configured_registry());
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend));
        let packet = task("agent-replay");

        let returned = runtime.execute_task(packet.clone()).await.expect("run");
        assert_eq!(returned.status, AgentTerminalStatus::Completed);
        assert_eq!(
            runtime.get(packet.agent_id()).unwrap().status,
            AgentStatus::Completed
        );
        assert_eq!(runtime.events(packet.agent_id()).len(), 3);
        let terminal = store
            .list_stream(&agent_stream_id(packet.agent_id()))
            .expect("agent event stream")
            .into_iter()
            .last()
            .expect("terminal agent event");
        for (kind, id) in [
            ("principal", "test.principal"),
            ("workspace", "test-workspace"),
            ("mission", packet.mission_id()),
            ("task", packet.task_id()),
            ("session", packet.session_id()),
            ("turn", "test-turn"),
            ("execution_graph", packet.graph_id()),
            ("agent_instance", packet.agent_id()),
            ("agent_run", packet.run_id()),
            ("node", packet.node_id()),
        ] {
            assert!(
                terminal
                    .refs
                    .iter()
                    .any(|reference| reference.kind == kind && reference.id == id),
                "terminal Agent event must retain {kind}:{id}"
            );
        }
        let evaluations = runtime.evaluations();
        assert_eq!(evaluations.len(), 1);
        assert_eq!(evaluations[0].definition_revision, 1);
        assert_eq!(evaluations[0].binding_digest, "b".repeat(64));
        assert_eq!(runtime.self_models().len(), 1);
        AgentTaskBackend::terminal_committed(&runtime, &packet);
        assert!(
            !runtime
                .records
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(packet.agent_id()),
            "a graph-committed terminal Agent must leave the active hot projection"
        );
        assert_eq!(
            runtime
                .get(packet.agent_id())
                .map(|snapshot| snapshot.status),
            Some(AgentStatus::Completed),
            "exact history lookup must rehydrate from the durable stream"
        );
        let replayed_return = runtime
            .execute_task(packet.clone())
            .await
            .expect("replay result");
        assert_eq!(replayed_return, returned);
        assert_eq!(runtime.events(packet.agent_id()).len(), 3);

        let restored = AgentRuntime::new(store, configured_registry());
        assert_eq!(restored.evaluations().len(), 1);
        assert_eq!(restored.self_models()[0].run_count, 1);
        let snapshot = restored.get(packet.agent_id()).expect("replayed snapshot");
        assert_eq!(snapshot.status, AgentStatus::Completed);
        assert_eq!(snapshot.graph_id, packet.graph_id());
        assert_eq!(snapshot.node_id, packet.node_id());
        assert_eq!(
            restored
                .execute_task(packet)
                .await
                .expect("restored return"),
            returned
        );
    }

    #[test]
    fn team_agent_snapshot_refs_preserve_the_team_lineage() {
        let packet = team_task("team-lineage", "team-run-1");
        let snapshot = legacy_snapshot(&packet, AgentStatus::Running);
        let refs = snapshot_identity_refs(&snapshot);
        assert!(refs
            .iter()
            .any(|reference| reference.kind == "team_run" && reference.id == "team-run-1"));
        assert!(refs.iter().any(|reference| {
            reference.kind == "agent_instance" && reference.id == packet.agent_id()
        }));
    }

    #[test]
    fn public_lifecycle_projection_is_derived_from_canonical_agent_events() {
        assert_eq!(
            live_lifecycle_phase("agent.running", AgentStatus::Running),
            Some((AgentLifecyclePhase::Started, "running"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.provider.first_output", AgentStatus::Running),
            Some((AgentLifecyclePhase::FirstOutput, "running"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.acceptance.evaluated", AgentStatus::Running),
            Some((AgentLifecyclePhase::Evaluating, "running"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.terminal", AgentStatus::Completed),
            Some((AgentLifecyclePhase::Completed, "completed"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.terminal", AgentStatus::Failed),
            Some((AgentLifecyclePhase::Failed, "failed"))
        );
        assert_eq!(
            live_lifecycle_phase("agent.prepared", AgentStatus::Prepared),
            None,
            "prepared is durable internal truth, not duplicate public start noise"
        );
    }

    #[tokio::test]
    async fn command_receipt_is_revisioned_and_idempotent() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("store")),
            configured_registry(),
        );
        runtime.register_observation_authority_backend(Arc::new(CompletedBackend));
        let packet = task("agent-command");
        runtime
            .restore_verified_run(AgentRunSnapshot {
                execution_identity: packet.assignment.execution_identity.clone(),
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                session_id: packet.session_id().to_string(),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                backend: AgentBackendKind::InProcess,
                status: AgentStatus::Running,
                revision: 0,
                model: Some("fast".into()),
                provider: Some("test".into()),
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore");
        let revision = runtime.get(packet.agent_id()).unwrap().revision;
        let command = AgentCommandRequest {
            command_id: "command-1".into(),
            agent_id: packet.agent_id().to_string(),
            expected_revision: revision,
            command: AgentCommand::Interrupt,
            input: None,
        };
        let first = runtime.command(command.clone()).await;
        let duplicate = runtime.command(command).await;
        assert!(first.accepted);
        assert_eq!(first, duplicate);
        assert_eq!(runtime.events(packet.agent_id()).len(), 2);
    }

    #[test]
    fn progress_markers_preserve_running_lifecycle_without_becoming_terminal() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("store")),
            configured_registry(),
        );
        let packet = task("agent-progress");
        runtime
            .restore_verified_run(legacy_snapshot(&packet, AgentStatus::Running))
            .expect("restore running agent");

        runtime
            .record_progress(
                packet.agent_id(),
                "agent.provider.first_output",
                "provider emitted the first output",
            )
            .expect("progress is durable");

        assert_eq!(
            runtime
                .get(packet.agent_id())
                .expect("agent projection")
                .status,
            AgentStatus::Running
        );
        let events = runtime.events(packet.agent_id());
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].kind, "agent.provider.first_output");
    }

    #[tokio::test]
    async fn unavailable_model_is_recorded_as_blocked_not_running() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("store")),
            Arc::new(ProviderRegistry::empty()),
        );
        let packet = task("agent-blocked");
        let returned = runtime
            .execute_task(packet.clone())
            .await
            .expect("blocked return");
        assert_eq!(returned.status, AgentTerminalStatus::Blocked);
        let snapshot = runtime.get(packet.agent_id()).expect("blocked snapshot");
        assert_eq!(snapshot.status, AgentStatus::Blocked);
        assert!(snapshot.failure.is_some());
        assert_eq!(runtime.events(packet.agent_id()).len(), 1);
    }

    #[tokio::test]
    async fn agent_locks_are_keyed_and_reclaimed_after_the_last_holder() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("store")),
            configured_registry(),
        );

        let first_run = runtime.acquire_run_lock("agent-a").await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                runtime.acquire_run_lock("agent-a")
            )
            .await
            .is_err(),
            "the same agent key must remain serialized"
        );
        let other_run = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            runtime.acquire_run_lock("agent-b"),
        )
        .await
        .expect("different agent keys must not share a global run lock");

        let lifecycle_a = runtime.lifecycle_lock("agent-a");
        let lifecycle_a_again = runtime.lifecycle_lock("agent-a");
        let lifecycle_b = runtime.lifecycle_lock("agent-b");
        assert!(Arc::ptr_eq(&lifecycle_a, &lifecycle_a_again));
        assert!(!Arc::ptr_eq(&lifecycle_a, &lifecycle_b));

        drop(lifecycle_a);
        drop(lifecycle_a_again);
        drop(lifecycle_b);
        drop(other_run);
        drop(first_run);
        assert_eq!(runtime.retained_lock_counts(), (0, 0));
    }

    #[test]
    fn restart_recovery_blocks_runs_without_a_recovered_handle() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("store")),
            configured_registry(),
        );
        let packet = task("agent-recovery");
        runtime
            .restore_verified_run(AgentRunSnapshot {
                execution_identity: packet.assignment.execution_identity.clone(),
                run_id: packet.run_id().to_string(),
                agent_id: packet.agent_id().to_string(),
                task_id: packet.task_id().to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                session_id: packet.session_id().to_string(),
                graph_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                backend: AgentBackendKind::ProcessJsonl,
                status: AgentStatus::Running,
                revision: 0,
                model: Some("fast".into()),
                provider: Some("test".into()),
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore");

        assert_eq!(
            runtime.block_unrecoverable_replayed_runs().expect("block"),
            vec![packet.agent_id().to_string()]
        );
        let snapshot = runtime.get(packet.agent_id()).expect("blocked run");
        assert_eq!(snapshot.status, AgentStatus::Blocked);
        assert!(snapshot
            .failure
            .as_deref()
            .unwrap_or_default()
            .contains("backend handle"));
    }
}
