use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentBindingSnapshot, AgentCommand, AgentCommandReceipt, AgentCommandRejectReason,
    AgentCommandRequest, AgentInput, AgentLifecycleEvent, AgentReturnPacket, AgentStatus,
    AgentTaskPacket, AgentTerminalStatus, RevisionSelector,
};
use harness_contract::execution_graph::{ExecutionEdgeKind, ExecutionNodeKind};
use serde::{Deserialize, Serialize};

use crate::execution_core::graph::executors::{AgentTaskBackend, AgentTaskBackendResolver};
use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope,
};
use crate::{
    project_self_models, AgentRunEvaluation, AgentSelfModel, ProviderRegistry, RuntimeEventStore,
    RuntimeServices,
};
use sha2::{Digest, Sha256};

use crate::agent_catalog::AgentCatalog;
use crate::agent_model_selector::{AgentModelSelection, AgentModelSelector};
use crate::agent_result_validator::validate_agent_return;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunSnapshot {
    pub run_id: String,
    pub agent_id: String,
    pub task_id: String,
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

/// Single workspace-scoped owner of agent lifecycle state. The event store is
/// canonical; the in-memory map is only a replayable command/cache projection.
pub struct AgentRuntime {
    event_store: Arc<RuntimeEventStore>,
    selector: AgentModelSelector,
    catalog: Arc<AgentCatalog>,
    records: RwLock<BTreeMap<String, AgentRunRecord>>,
    backends: RwLock<BTreeMap<AgentBackendKind, Arc<dyn AgentRuntimeBackend>>>,
    services: RwLock<Option<Weak<RuntimeServices>>>,
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
            backends: RwLock::new(BTreeMap::new()),
            services: RwLock::new(None),
        };
        runtime.restore_projection();
        runtime
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
        self.backends
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(backend.kind(), backend);
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
    pub fn get(&self, agent_id: &str) -> Option<AgentRunSnapshot> {
        self.records
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .and_then(|record| record.snapshot.clone())
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
        if let Some(existing) = self.get(&packet.agent_id) {
            if existing.run_id == packet.run_id && existing.status.is_terminal() {
                return self
                    .records
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&packet.agent_id)
                    .and_then(|record| record.returned.clone())
                    .ok_or_else(|| {
                        "terminal AgentRuntime state lacks a canonical return packet".to_string()
                    });
            }
            if existing.run_id != packet.run_id && !existing.status.is_terminal() {
                return Err(format!(
                    "agent {} already owns an active run",
                    packet.agent_id
                ));
            }
        }
        let packet = self.attach_predecessor_context(packet).await?;
        let packet = self.ensure_runtime_binding(packet)?;
        let backend_kind = backend_from_packet(&packet);
        let selection = match self.selector.select(nonempty(&packet.model_lease)) {
            Ok(selection) => selection,
            Err(error) => {
                let failure = error.to_string();
                let returned = blocked_return(&packet, failure.clone());
                self.persist_snapshot(
                    AgentRunSnapshot {
                        run_id: packet.run_id.clone(),
                        agent_id: packet.agent_id.clone(),
                        task_id: packet.task_id.clone(),
                        session_id: packet.session_id.clone(),
                        graph_id: packet.graph_id.clone(),
                        node_id: packet.node_id.clone(),
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
                    run_id: packet.run_id.clone(),
                    agent_id: packet.agent_id.clone(),
                    task_id: packet.task_id.clone(),
                    session_id: packet.session_id.clone(),
                    graph_id: packet.graph_id.clone(),
                    node_id: packet.node_id.clone(),
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
            run_id: packet.run_id.clone(),
            agent_id: packet.agent_id.clone(),
            task_id: packet.task_id.clone(),
            session_id: packet.session_id.clone(),
            graph_id: packet.graph_id.clone(),
            node_id: packet.node_id.clone(),
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
        let mut running = self
            .get(&packet.agent_id)
            .ok_or_else(|| format!("prepared agent projection `{}` is missing", packet.agent_id))?;
        running.status = AgentStatus::Running;
        running.updated_at_ms = now_ms();
        self.persist_snapshot(running.clone(), "agent.running", "running", None, None)?;

        let backend = self
            .backends
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&backend_kind)
            .cloned();
        let mut returned = match backend {
            Some(backend) => match backend.execute(packet.clone(), selection).await {
                Ok(returned) => returned,
                Err(error) => failed_return(&packet, error),
            },
            None => blocked_return(
                &packet,
                format!("agent backend {backend_kind:?} is not installed for this RuntimeServices instance"),
            ),
        };
        // A cancel/shutdown command is durable lifecycle truth. Backends may
        // observe the interruption as a transport/process error, but they may
        // not overwrite a committed cancellation with `failed` or `completed`.
        if self
            .get(&packet.agent_id)
            .is_some_and(|snapshot| snapshot.status == AgentStatus::Cancelled)
        {
            returned.status = AgentTerminalStatus::Cancelled;
            returned.outcome.clear();
            returned.failure = Some("agent cancelled by command".into());
        }
        validate_agent_return(&packet, &returned).map_err(|error| error.to_string())?;
        let mut terminal = self
            .get(&packet.agent_id)
            .ok_or_else(|| format!("running agent projection `{}` is missing", packet.agent_id))?;
        terminal.status = terminal_status(returned.status);
        terminal.updated_at_ms = now_ms();
        terminal.failure = returned.failure.clone();
        let evaluation =
            AgentRunEvaluation::from_terminal(&packet, &returned, terminal.updated_at_ms);
        let refresh_canary_observation = evaluation.as_ref().is_some_and(|evaluation| {
            evaluation.release_channel == Some(harness_contract::agent::ReleaseChannel::Canary)
        });
        self.persist_snapshot_with_evaluation(
            terminal,
            "agent.terminal",
            "terminal",
            None,
            Some(returned.clone()),
            evaluation,
        )?;
        if refresh_canary_observation {
            if let Some(services) = self.services() {
                if let Err(error) = services.refresh_evolution_canary_observations() {
                    // The terminal run is already durably committed. Canary
                    // observation is a replayable projection and will be
                    // rebuilt before any Stable-review request, so do not
                    // turn a completed user task into a false failure here.
                    tracing::warn!(
                        agent_id = %packet.agent_id,
                        run_id = %packet.run_id,
                        error = %error,
                        "failed to refresh replayable Canary observation"
                    );
                }
            }
        }
        Ok(returned)
    }

    /// Immutable per-run evidence, written only with a terminal lifecycle
    /// event. This projection deliberately groups by Definition revision and
    /// environment instead of mutable instance reputation.
    #[must_use]
    pub fn evaluations(&self) -> Vec<AgentRunEvaluation> {
        let Ok(events) = self
            .event_store
            .list_scope(RuntimeEventScope::Evolution, 100_000)
        else {
            return Vec::new();
        };
        let mut evaluations = events
            .into_iter()
            .filter(|event| event.kind == "agent.run_evaluated")
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
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "unbound AgentTaskPacket is not executable; compile AgentTaskIntent before graph registration"
                .to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        if packet.agent_id != binding.instance.instance_id {
            return Err(
                "AgentTaskPacket agent_id must equal its Binding instance identity".to_string(),
            );
        }
        if binding.data_lease.session_id != packet.session_id
            || binding.data_lease.task_id != packet.task_id
            || binding.data_lease.team_id != packet.team_id
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
            .load_async(packet.graph_id.clone())
            .await
        else {
            // Isolated AgentRuntime tests and external adapters may execute
            // standalone packets. They have no graph peer context to attach.
            return Ok(packet);
        };
        let predecessors = graph
            .edges
            .iter()
            .filter(|edge| {
                edge.to == packet.node_id
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
            let returned = self
                .terminal_return(&predecessor_packet.agent_id)
                .ok_or_else(|| {
                    format!(
                        "completed predecessor {} has no AgentRuntime terminal return",
                        predecessor.id
                    )
                })?;
            if returned.run_id != predecessor_packet.run_id
                || returned.graph_id != graph.id
                || returned.node_id != predecessor.id
                || returned.attempt != predecessor_packet.attempt
                || returned.expected_graph_revision != predecessor_packet.expected_graph_revision
            {
                return Err(format!(
                    "predecessor AgentRuntime binding mismatch for {}",
                    predecessor_packet.agent_id
                ));
            }
            let role = predecessor_packet
                .constraints
                .iter()
                .find_map(|constraint| {
                    constraint
                        .strip_prefix("team_role:")
                        .or_else(|| constraint.strip_prefix("protocol_role:"))
                })
                .unwrap_or(predecessor_packet.agent_id.as_str());
            let available = remaining.saturating_sub(96);
            if available == 0 {
                break;
            }
            let upstream_outcome = if returned.status == AgentTerminalStatus::Completed {
                returned.outcome.clone()
            } else {
                format!(
                    "UNRESOLVED: upstream role did not complete: {}",
                    returned
                        .failure
                        .clone()
                        .unwrap_or_else(|| "no terminal outcome".to_string())
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
        if let Err(reason) = backend.command(&snapshot.handle(), &request).await {
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
        let agent_event = RuntimeEventInput {
            stream_id: stream_id.clone(),
            scope: RuntimeEventScope::Agent,
            kind: kind.into(),
            status: Some(message.into()),
            actor: Some("agent_runtime".into()),
            refs: vec![
                RuntimeEventRef {
                    kind: "run".into(),
                    id: snapshot.run_id.clone(),
                },
                RuntimeEventRef {
                    kind: "graph".into(),
                    id: snapshot.graph_id.clone(),
                },
                RuntimeEventRef {
                    kind: "node".into(),
                    id: snapshot.node_id.clone(),
                },
            ],
            payload: serde_json::to_value(payload).map_err(|error| error.to_string())?,
        };
        if let Some(evaluation) = evaluation {
            let evaluation_stream = agent_evaluation_stream(&evaluation.run_id);
            let evaluation_revision = self
                .event_store
                .stream_revision(&evaluation_stream)
                .map_err(|error| error.to_string())?;
            self.event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!(
                        "agent-terminal-evaluation:{}:{}",
                        snapshot.run_id, evaluation.evaluation_id
                    ),
                    expected_streams: vec![
                        ExpectedStreamRevision {
                            stream_id: stream_id.clone(),
                            expected_revision: snapshot.revision.saturating_sub(1),
                        },
                        ExpectedStreamRevision {
                            stream_id: evaluation_stream.clone(),
                            expected_revision: evaluation_revision,
                        },
                    ],
                    events: vec![
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
                    ],
                })
                .map_err(|error| error.to_string())?;
        } else {
            self.event_store
                .append(agent_event)
                .map_err(|error| error.to_string())?;
        }
        let mut records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = records.entry(snapshot.agent_id.clone()).or_default();
        record.snapshot = Some(snapshot.clone());
        if returned.is_some() {
            record.returned = returned;
        }
        if let Some(receipt) = receipt {
            record
                .receipts
                .insert(receipt.command_id.clone(), receipt.clone());
            return Ok(receipt);
        }
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

    fn restore_projection(&self) {
        let Ok(stream_ids) = self
            .event_store
            .stream_ids_for_scope(RuntimeEventScope::Agent)
        else {
            return;
        };
        let mut records = self
            .records
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for stream_id in stream_ids {
            for event in self.event_store.list_stream(&stream_id).unwrap_or_default() {
                let Ok(payload) = serde_json::from_value::<PersistedAgentEvent>(event.payload)
                else {
                    continue;
                };
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
        }
    }
}

#[async_trait]
impl AgentTaskBackend for AgentRuntime {
    async fn execute(&self, packet: AgentTaskPacket) -> Result<AgentReturnPacket, String> {
        self.execute_task(packet).await
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

fn validate_legacy_record(record: &LegacyAgentStateRecord) -> Result<(), String> {
    let snapshot = &record.snapshot;
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

fn return_packet(
    packet: &AgentTaskPacket,
    status: AgentTerminalStatus,
    outcome: String,
    failure: Option<String>,
) -> AgentReturnPacket {
    AgentReturnPacket {
        run_id: packet.run_id.clone(),
        agent_id: packet.agent_id.clone(),
        task_id: packet.task_id.clone(),
        session_id: packet.session_id.clone(),
        mission_id: packet.mission_id.clone(),
        team_id: packet.team_id.clone(),
        graph_id: packet.graph_id.clone(),
        node_id: packet.node_id.clone(),
        attempt: packet.attempt,
        expected_graph_revision: packet.expected_graph_revision,
        status,
        outcome,
        acceptance: Vec::new(),
        evidence_refs: Vec::new(),
        changes: Vec::new(),
        conflicts: Vec::new(),
        unresolved: Vec::new(),
        input_tokens: 0,
        output_tokens: 0,
        model: String::new(),
        provider: String::new(),
        tool_calls: 0,
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
    use harness_contract::context::ContextBudgetLeaseRef;

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
                run_id: packet.run_id,
                agent_id: packet.agent_id,
                task_id: packet.task_id,
                session_id: packet.session_id,
                mission_id: packet.mission_id,
                team_id: packet.team_id,
                graph_id: packet.graph_id,
                node_id: packet.node_id,
                attempt: packet.attempt,
                expected_graph_revision: packet.expected_graph_revision,
                status: AgentTerminalStatus::Completed,
                outcome: "completed".into(),
                acceptance: vec!["verified".into()],
                evidence_refs: Vec::new(),
                changes: Vec::new(),
                conflicts: Vec::new(),
                unresolved: Vec::new(),
                input_tokens: 3,
                output_tokens: 2,
                model: selection.model,
                provider: selection.provider,
                tool_calls: 0,
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
            binding_digest: "b".repeat(64),
        }
    }

    fn task(agent_id: &str) -> AgentTaskPacket {
        let binding = test_binding(agent_id);
        let instance_id = binding.instance.instance_id.clone();
        AgentTaskPacket {
            run_id: format!("run-{agent_id}"),
            agent_id: instance_id.clone(),
            task_id: "task-1".into(),
            session_id: "session-1".into(),
            mission_id: None,
            team_id: None,
            graph_id: "graph-1".into(),
            node_id: "node-1".into(),
            attempt: 1,
            expected_graph_revision: 1,
            objective: "verify lifecycle".into(),
            acceptance: vec!["verified".into()],
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".into(),
            model_lease: "fast".into(),
            budget_lease: ContextBudgetLeaseRef::new("budget-1", instance_id, "agent", 1000, 1),
            binding: Some(binding),
            managed_invocation: None,
            idempotency_key: format!("idempotency-{agent_id}"),
        }
    }

    fn legacy_snapshot(packet: &AgentTaskPacket, status: AgentStatus) -> AgentRunSnapshot {
        AgentRunSnapshot {
            run_id: packet.run_id.clone(),
            agent_id: packet.agent_id.clone(),
            task_id: packet.task_id.clone(),
            session_id: packet.session_id.clone(),
            graph_id: packet.graph_id.clone(),
            node_id: packet.node_id.clone(),
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
        assert_eq!(report.blocked_agent_ids, vec![active.agent_id.clone()]);
        assert_eq!(
            runtime
                .get(&active.agent_id)
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
                .get(&completed.agent_id)
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
        let error = runtime
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
        assert!(error.contains("canonical binding"));
        assert!(runtime.get(&valid.agent_id).is_none());
        assert!(store
            .list_stream(&legacy_import_stream_id("bad-upgrade-manifest"))
            .expect("marker stream")
            .is_empty());
    }

    #[tokio::test]
    async fn terminal_lifecycle_replays_from_the_event_store() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
        let runtime = AgentRuntime::new(Arc::clone(&store), configured_registry());
        runtime.register_backend(Arc::new(CompletedBackend));
        let packet = task("agent-replay");

        let returned = runtime.execute_task(packet.clone()).await.expect("run");
        assert_eq!(returned.status, AgentTerminalStatus::Completed);
        assert_eq!(
            runtime.get(&packet.agent_id).unwrap().status,
            AgentStatus::Completed
        );
        assert_eq!(runtime.events(&packet.agent_id).len(), 3);
        let evaluations = runtime.evaluations();
        assert_eq!(evaluations.len(), 1);
        assert_eq!(evaluations[0].definition_revision, 1);
        assert_eq!(evaluations[0].binding_digest, "b".repeat(64));
        assert_eq!(runtime.self_models().len(), 1);
        let replayed_return = runtime
            .execute_task(packet.clone())
            .await
            .expect("replay result");
        assert_eq!(replayed_return, returned);
        assert_eq!(runtime.events(&packet.agent_id).len(), 3);

        let restored = AgentRuntime::new(store, configured_registry());
        assert_eq!(restored.evaluations().len(), 1);
        assert_eq!(restored.self_models()[0].run_count, 1);
        let snapshot = restored.get(&packet.agent_id).expect("replayed snapshot");
        assert_eq!(snapshot.status, AgentStatus::Completed);
        assert_eq!(snapshot.graph_id, packet.graph_id);
        assert_eq!(snapshot.node_id, packet.node_id);
        assert_eq!(
            restored
                .execute_task(packet)
                .await
                .expect("restored return"),
            returned
        );
    }

    #[tokio::test]
    async fn command_receipt_is_revisioned_and_idempotent() {
        let runtime = AgentRuntime::new(
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("store")),
            configured_registry(),
        );
        runtime.register_backend(Arc::new(CompletedBackend));
        let packet = task("agent-command");
        runtime
            .restore_verified_run(AgentRunSnapshot {
                run_id: packet.run_id.clone(),
                agent_id: packet.agent_id.clone(),
                task_id: packet.task_id.clone(),
                session_id: packet.session_id.clone(),
                graph_id: packet.graph_id.clone(),
                node_id: packet.node_id.clone(),
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
        let revision = runtime.get(&packet.agent_id).unwrap().revision;
        let command = AgentCommandRequest {
            command_id: "command-1".into(),
            agent_id: packet.agent_id.clone(),
            expected_revision: revision,
            command: AgentCommand::Interrupt,
            input: None,
        };
        let first = runtime.command(command.clone()).await;
        let duplicate = runtime.command(command).await;
        assert!(first.accepted);
        assert_eq!(first, duplicate);
        assert_eq!(runtime.events(&packet.agent_id).len(), 2);
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
                &packet.agent_id,
                "agent.provider.first_output",
                "provider emitted the first output",
            )
            .expect("progress is durable");

        assert_eq!(
            runtime
                .get(&packet.agent_id)
                .expect("agent projection")
                .status,
            AgentStatus::Running
        );
        let events = runtime.events(&packet.agent_id);
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
        let snapshot = runtime.get(&packet.agent_id).expect("blocked snapshot");
        assert_eq!(snapshot.status, AgentStatus::Blocked);
        assert!(snapshot.failure.is_some());
        assert_eq!(runtime.events(&packet.agent_id).len(), 1);
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
                run_id: packet.run_id.clone(),
                agent_id: packet.agent_id.clone(),
                task_id: packet.task_id.clone(),
                session_id: packet.session_id.clone(),
                graph_id: packet.graph_id.clone(),
                node_id: packet.node_id.clone(),
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
            vec![packet.agent_id.clone()]
        );
        let snapshot = runtime.get(&packet.agent_id).expect("blocked run");
        assert_eq!(snapshot.status, AgentStatus::Blocked);
        assert!(snapshot
            .failure
            .as_deref()
            .unwrap_or_default()
            .contains("backend handle"));
    }
}
