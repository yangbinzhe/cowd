use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use chrono::DateTime;
use harness_contract::{
    mission::{
        MissionCommand, MissionCommandAction, MissionCommandTarget, MissionControlEventLine,
        MissionControlSessionNode, MissionMaterializedSnapshot, MissionProjectionDelta,
        MISSION_CONTROL_SCHEMA_VERSION,
    },
    turn::{InputSourceKind, SessionInputEnvelope},
};
use serde::Deserialize;
use session::SessionListOptions;

use super::session_service::EnsureSessionOutcome;
use super::{
    service_envelope, EnsureSessionRequest, MissionService, RuntimeEventService, ServiceEnvelope,
    SessionService, SessionSource,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartMissionSessionHttpRequest {
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateMissionScheduleHttpRequest {
    pub(crate) mission_id: String,
    pub(crate) target_session_id: String,
    pub(crate) objective: String,
    pub(crate) trigger: harness_contract::mission::ScheduleTrigger,
    #[serde(default = "default_schedule_autonomy_profile")]
    pub(crate) autonomy_profile: String,
    #[serde(default = "default_schedule_permission_ceiling")]
    pub(crate) permission_ceiling: harness_contract::policy::PermissionMode,
    #[serde(default = "default_schedule_priority")]
    pub(crate) priority: u8,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateMissionScheduleHttpRequest {
    pub(crate) expected_revision: u64,
    #[serde(default)]
    pub(crate) objective: Option<String>,
    #[serde(default)]
    pub(crate) trigger: Option<harness_contract::mission::ScheduleTrigger>,
    #[serde(default)]
    pub(crate) autonomy_profile: Option<String>,
    #[serde(default)]
    pub(crate) permission_ceiling: Option<harness_contract::policy::PermissionMode>,
    #[serde(default)]
    pub(crate) priority: Option<u8>,
}

fn default_schedule_autonomy_profile() -> String {
    "assisted".to_string()
}

fn default_schedule_permission_ceiling() -> harness_contract::policy::PermissionMode {
    harness_contract::policy::PermissionMode::ReadOnly
}

const fn default_schedule_priority() -> u8 {
    64
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SubmitMissionApprovalHttpRequest {
    pub(crate) source: runtime::ApprovalSource,
    pub(crate) action: String,
    pub(crate) summary: String,
    pub(crate) risk: harness_contract::core::TaskRisk,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) timeout_policy: runtime::ApprovalTimeoutPolicy,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DecideMissionApprovalHttpRequest {
    pub(crate) approved: bool,
    pub(crate) scope: runtime::ApprovalGrantScope,
    #[serde(default)]
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpsertMissionProxyHttpRequest {
    pub(crate) session_id: String,
    pub(crate) summary: String,
    #[serde(default)]
    pub(crate) evidence_refs: Vec<String>,
    #[serde(default)]
    pub(crate) decisions: Vec<String>,
    #[serde(default)]
    pub(crate) open_questions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InterpretMissionCommandHttpRequest {
    pub(crate) current_session_id: String,
    pub(crate) command_text: String,
    #[serde(default)]
    pub(crate) target_ref: Option<String>,
    #[serde(default)]
    pub(crate) dispatch_mode: Option<runtime::SessionDispatchMode>,
    #[serde(default)]
    pub(crate) allow_background: Option<bool>,
    #[serde(default)]
    pub(crate) execute: bool,
}

impl MissionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "mission",
            owner: "Gateway MissionApplicationService",
            runtime_port: None,
            session_service: None,
            runtime_events: None,
            projection_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn with_dependencies(
        mut self,
        runtime_port: runtime::MissionRuntimePort,
        session_service: Arc<SessionService>,
        runtime_events: RuntimeEventService,
    ) -> Self {
        self.runtime_port = Some(runtime_port);
        self.session_service = Some(session_service);
        self.runtime_events = Some(runtime_events);
        self
    }

    #[allow(
        clippy::expect_used,
        reason = "MissionService methods are installed only in runtime-bound GatewayServices; baseline services do not expose mission operations"
    )]
    fn runtime(&self) -> &runtime::MissionRuntimePort {
        self.runtime_port
            .as_ref()
            .expect("MissionService requires MissionRuntimePort")
    }

    #[allow(
        clippy::expect_used,
        reason = "MissionApplicationService is installed with the canonical SessionService"
    )]
    fn sessions(&self) -> &Arc<SessionService> {
        self.session_service
            .as_ref()
            .expect("MissionApplicationService requires SessionService")
    }

    #[allow(
        clippy::expect_used,
        reason = "MissionApplicationService is installed with the Runtime event reader"
    )]
    fn events(&self) -> &RuntimeEventService {
        self.runtime_events
            .as_ref()
            .expect("MissionApplicationService requires RuntimeEventService")
    }

    pub(crate) fn subscribe_projection_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.events().subscribe_commits()
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn projection_contract(&self) -> ServiceEnvelope {
        self.envelope("projection")
    }

    pub(crate) fn session_control_contract(&self) -> ServiceEnvelope {
        self.envelope("session_control")
    }

    pub(crate) fn approval_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("approval_projection")
    }

    pub(crate) fn relation_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("relation_projection")
    }

    pub(crate) fn conflict_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("conflict_projection")
    }

    pub(crate) fn approval_command_contract(&self) -> ServiceEnvelope {
        self.envelope("approval_command")
    }

    pub(crate) fn relation_command_contract(&self) -> ServiceEnvelope {
        self.envelope("relation_command")
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.projection_contract(),
            self.session_control_contract(),
            self.approval_projection_contract(),
            self.relation_projection_contract(),
            self.conflict_projection_contract(),
            self.approval_command_contract(),
            self.relation_command_contract(),
        ]
    }

    pub(crate) fn projection(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.projection_contract(),
            "mission": self.runtime().projection(),
        })
    }

    pub(crate) async fn mission_control(
        &self,
        selected_mission_id: Option<&str>,
        detail: &str,
    ) -> Result<serde_json::Value, String> {
        if !matches!(detail, "summary" | "graph") {
            return Err(format!(
                "unsupported mission detail `{detail}`; legal values: summary, graph"
            ));
        }
        let snapshot = self.materialized_snapshot_for(selected_mission_id).await?;
        let snapshot_value = serde_json::to_value(&snapshot)
            .map_err(|error| format!("serialize mission snapshot: {error}"))?;
        // M-01: `snapshot` is always the full typed MissionMaterializedSnapshot.
        // Summary consumers must use mission_control_summary() (small payload)
        // instead of reading a replaced mission_graph from the typed snapshot.
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "snapshot": snapshot_value,
        }))
    }

    /// M-01/M-04: bounded summary contract for lightweight first-panel
    /// consumers. Never contains the full mission graph; the graph facts are
    /// a digest plus counts so expansion can request `detail=graph`.
    pub(crate) async fn mission_control_summary(
        &self,
        selected_mission_id: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let snapshot = self.materialized_snapshot_for(selected_mission_id).await?;
        let graph = &snapshot.projection.mission_graph;
        let digest = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(
                serde_json::to_string(graph).unwrap_or_default(),
            );
            let digest = hasher.finalize();
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let projection_value = serde_json::to_value(&snapshot.projection)
            .map_err(|error| format!("serialize mission projection: {error}"))?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "summary": {
                "mission_id": graph.mission_id,
                "cursor": snapshot.cursor,
                "revision": snapshot.revision,
                "graph": {
                    "available": true,
                    "node_count": graph.nodes.len(),
                    "edge_count": graph.edges.len(),
                    "hash": digest,
                },
                "projection": summarize_mission_projection(&projection_value),
            },
        }))
    }

    pub(crate) async fn execute_mission_control_command(
        &self,
        command: MissionCommand,
    ) -> Result<serde_json::Value, String> {
        let command_id = command.command_id.clone();
        let reserved = self.runtime().reserve_command(command.clone())?;
        let final_record = match reserved.phase {
            harness_contract::mission::MissionCommandSagaPhase::Reserved => {
                match self.execute_reserved_effect(&command).await {
                    Ok((result, evidence_refs)) => {
                        self.runtime()
                            .commit_command_effect(&command_id, result, evidence_refs)?;
                        self.runtime().commit_command_receipt(&command_id)?;
                        self.runtime().finalize_command(&command_id)?
                    }
                    Err(error) => self.runtime().reject_command(&command_id, error)?,
                }
            }
            harness_contract::mission::MissionCommandSagaPhase::EffectCommitted => {
                self.runtime().commit_command_receipt(&command_id)?;
                self.runtime().finalize_command(&command_id)?
            }
            harness_contract::mission::MissionCommandSagaPhase::ReceiptCommitted => {
                self.runtime().finalize_command(&command_id)?
            }
            harness_contract::mission::MissionCommandSagaPhase::Finalized
            | harness_contract::mission::MissionCommandSagaPhase::Rejected => reserved,
            harness_contract::mission::MissionCommandSagaPhase::ReconciliationRequired => {
                return Err(format!(
                    "mission command {command_id} requires reconciliation before replay"
                ));
            }
        };
        let receipt = final_record.receipt.clone().ok_or_else(|| {
            format!(
                "mission command {} reached {:?} without a receipt",
                final_record.command.command_id, final_record.phase
            )
        })?;
        let snapshot = self.materialized_snapshot().await?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.command_result",
            "ok": receipt.status == "accepted",
            "receipt": receipt,
            "saga": final_record,
            "snapshot": snapshot,
        }))
    }

    pub(crate) async fn interpret_mission_command(
        &self,
        request: InterpretMissionCommandHttpRequest,
    ) -> serde_json::Value {
        let current_session_id = request.current_session_id.clone();
        let interpretation = runtime::MissionCommandInterpreter::interpret(
            runtime::MissionCommandInterpretRequest {
                current_session_id: request.current_session_id,
                command_text: request.command_text,
                target_ref: request.target_ref,
                dispatch_mode: request.dispatch_mode,
                allow_background: request.allow_background,
            },
        );
        let execution = if request.execute {
            Some(
                match self
                    .sessions()
                    .session_input_admission(&current_session_id)
                    .await
                {
                    Ok(Some(admission)) => match self
                        .runtime()
                        .bind_task_lineage(
                            interpretation.clone(),
                            &current_session_id,
                            admission.generation,
                            harness_contract::task::TaskOrigin::Mission,
                            None,
                        )
                        .await
                    {
                        Ok(bound) => self.runtime().submit_interpretation(bound).await,
                        Err(error) => serde_json::json!({
                            "ok": false,
                            "kind": "runtime.mission_command_lineage_error",
                            "error": error,
                        }),
                    },
                    Ok(None) => serde_json::json!({
                        "ok": false,
                        "kind": "runtime.mission_command_lineage_error",
                        "error": format!("source Session `{current_session_id}` has no admission authority"),
                    }),
                    Err(error) => serde_json::json!({
                        "ok": false,
                        "kind": "runtime.mission_command_lineage_error",
                        "error": error.to_string(),
                    }),
                },
            )
        } else {
            None
        };
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.command_interpretation",
            "ok": interpretation.status == "interpreted"
                && execution
                    .as_ref()
                    .and_then(|result| result.get("ok"))
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true),
            "interpretation": interpretation,
            "execution": execution,
            "snapshot": self.materialized_snapshot().await.ok(),
        })
    }

    pub(crate) async fn bridge_session_handoff(
        &self,
        handoff: runtime::SessionHandoff,
    ) -> serde_json::Value {
        let source_session_id = handoff.source_session_id.clone();
        let mut route_hint = handoff.task_route_hint.clone().unwrap_or_default();
        route_hint.handoff_id = Some(handoff.correlation_id.clone());
        let admission = match self
            .sessions()
            .session_input_admission(&source_session_id)
            .await
        {
            Ok(Some(admission)) => admission,
            Ok(None) => {
                return serde_json::json!({
                    "ok": false,
                    "kind": "mission_control.session_bridge_submission",
                    "error": format!("source Session `{source_session_id}` has no admission authority"),
                });
            }
            Err(error) => {
                return serde_json::json!({
                    "ok": false,
                    "kind": "mission_control.session_bridge_submission",
                    "error": error.to_string(),
                });
            }
        };
        let interpretation = runtime::MissionCommandInterpreter::interpret_session_handoff(handoff);
        let bound = match self
            .runtime()
            .bind_task_lineage(
                interpretation,
                &source_session_id,
                admission.generation,
                harness_contract::task::TaskOrigin::Mission,
                Some(route_hint),
            )
            .await
        {
            Ok(bound) => bound,
            Err(error) => {
                return serde_json::json!({
                    "ok": false,
                    "kind": "mission_control.session_bridge_submission",
                    "error": error,
                });
            }
        };
        let execution = self.runtime().submit_interpretation(bound).await;
        serde_json::json!({
            "ok": execution.get("ok").and_then(serde_json::Value::as_bool).unwrap_or(false),
            "kind": "mission_control.session_bridge_submission",
            "execution": execution,
        })
    }

    async fn execute_reserved_effect(
        &self,
        command: &MissionCommand,
    ) -> Result<
        (
            serde_json::Value,
            Vec<harness_contract::reality::EvidenceRef>,
        ),
        String,
    > {
        match (&command.target, command.action) {
            (MissionCommandTarget::Session { session_id }, MissionCommandAction::Create) => {
                let title = payload_text(&command.payload, "title")
                    .unwrap_or("Mission session")
                    .to_string();
                let model = payload_text(&command.payload, "model").map(str::to_string);
                let mut request =
                    EnsureSessionRequest::new(session_id, model, SessionSource::MissionControl);
                request.title = Some(title);
                request.owner_principal_id =
                    (!command.actor.trim().is_empty()).then(|| command.actor.clone());
                request.metadata = serde_json::json!({
                    "source": "mission_control",
                    "command_id": command.command_id,
                    "correlation_id": command.correlation_id,
                });
                let outcome = self.sessions().ensure_surface_session(request).await?;
                Ok((
                    ensure_session_value(&outcome),
                    command.evidence_refs.clone(),
                ))
            }
            (
                MissionCommandTarget::Session { session_id },
                MissionCommandAction::Activate | MissionCommandAction::Resume,
            ) => {
                let outcome = self
                    .sessions()
                    .activate_existing_session(EnsureSessionRequest::new(
                        session_id,
                        None,
                        SessionSource::Internal,
                    ))
                    .await?;
                Ok((
                    ensure_session_value(&outcome),
                    command.evidence_refs.clone(),
                ))
            }
            (
                MissionCommandTarget::Session { session_id },
                MissionCommandAction::Background | MissionCommandAction::Pause,
            ) => {
                let unloaded = self.sessions().unload_runtime(session_id).await?;
                Ok((
                    serde_json::json!({"session_id": session_id, "unloaded": unloaded}),
                    command.evidence_refs.clone(),
                ))
            }
            (MissionCommandTarget::Session { session_id }, MissionCommandAction::Cancel) => {
                let cancelled = self
                    .sessions()
                    .cancel_active_turns(session_id, "Mission control cancellation")?;
                Ok((
                    serde_json::json!({"session_id": session_id, "cancelled_turns": cancelled}),
                    command.evidence_refs.clone(),
                ))
            }
            (MissionCommandTarget::Session { session_id }, MissionCommandAction::Close) => {
                let archived = self.sessions().archive_session(session_id).await?;
                Ok((
                    serde_json::json!({"session_id": session_id, "archived": archived}),
                    command.evidence_refs.clone(),
                ))
            }
            (
                MissionCommandTarget::Session { session_id },
                MissionCommandAction::Input
                | MissionCommandAction::Continue
                | MissionCommandAction::Replan,
            ) => {
                let content = payload_text(&command.payload, "content")
                    .ok_or_else(|| "Session input requires payload.content".to_string())?;
                let envelope =
                    SessionInputEnvelope::text(session_id, InputSourceKind::Api, content)
                        .with_idempotency_key(command.command_id.clone())
                        .with_metadata(serde_json::json!({
                            "mission_command_action": command.action,
                            "correlation_id": command.correlation_id,
                        }));
                let admission = self.sessions().admit_input(envelope).await?;
                Ok((
                    serde_json::json!({
                        "receipt": admission.receipt,
                        "materialized": admission.materialized,
                        "execution_graph_id": admission.execution_graph_id,
                        "terminal_id": admission.terminal_id,
                        "turn_id": admission.turn_id,
                    }),
                    command.evidence_refs.clone(),
                ))
            }
            (MissionCommandTarget::Session { session_id }, MissionCommandAction::Branch) => {
                let target_session_id = payload_text(&command.payload, "target_session_id")
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("branch-{}", uuid::Uuid::new_v4()));
                let title = payload_text(&command.payload, "title")
                    .unwrap_or("Mission branch")
                    .to_string();
                let model = payload_text(&command.payload, "model").map(str::to_string);
                let outcome = self
                    .sessions()
                    .branch_session(
                        session_id,
                        &target_session_id,
                        command.command_id.clone(),
                        title,
                        model,
                        command.actor.clone(),
                    )
                    .await?;
                Ok((
                    serde_json::json!({
                        "session": ensure_session_value(&outcome.session),
                        "operation_id": outcome.operation_id,
                        "replayed": outcome.replayed,
                        "copied_message_count": outcome.copied_message_count,
                        "source_message_count": outcome.source_message_count,
                    }),
                    command.evidence_refs.clone(),
                ))
            }
            (MissionCommandTarget::Team { team_id }, MissionCommandAction::Create) => {
                let mut request: harness_contract::team::TeamInstantiationRequest =
                    serde_json::from_value(command.payload.clone())
                        .map_err(|error| format!("Team create payload is invalid: {error}"))?;
                request.request_id = command.command_id.clone();
                request.validate().map_err(|error| error.to_string())?;
                if request.team_id != *team_id {
                    return Err(format!(
                        "Team command target {team_id} does not match payload {}",
                        request.team_id
                    ));
                }
                if !self
                    .sessions()
                    .session_exists(&request.lineage.session_id)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    return Err(format!("session {} not found", request.lineage.session_id));
                }
                let team = self.runtime().instantiate_team(request).await?;
                Ok((
                    serde_json::to_value(team).map_err(|error| error.to_string())?,
                    command.evidence_refs.clone(),
                ))
            }
            (MissionCommandTarget::Team { team_id }, MissionCommandAction::Cancel) => {
                let receipt = self.runtime().cancel_team(team_id).await?;
                Ok((receipt, command.evidence_refs.clone()))
            }
            (MissionCommandTarget::Approval { .. }, _) => Err(
                "Approval commands require the authenticated approval decision endpoint"
                    .to_string(),
            ),
            _ => {
                let record = self
                    .runtime()
                    .execute_reserved_runtime_effect(&command.command_id)
                    .await?;
                Ok((
                    record.effect_result.unwrap_or_default(),
                    record.command.evidence_refs,
                ))
            }
        }
    }

    pub(crate) async fn materialized_snapshot(
        &self,
    ) -> Result<MissionMaterializedSnapshot, String> {
        self.materialized_snapshot_for(None).await
    }

    /// P5/T7: eagerly build the first mission summary projection in the
    /// background so the first user request after cold start is served from
    /// the cache. Never blocks readiness; failures only log a warning.
    pub(crate) async fn warm_projection_cache(&self) -> Result<(), String> {
        if self.runtime_port.is_none() {
            return Ok(());
        }
        // Never create the default Mission during warm-up: that would commit
        // an event before readiness and make /readyz transiently report a
        // projector lag. Only warm the cache for missions that already exist.
        if !self.runtime().has_default_mission() {
            return Ok(());
        }
        self.materialized_snapshot().await.map(|_| ())
    }

    pub(crate) async fn materialized_snapshot_for(
        &self,
        selected_mission_id: Option<&str>,
    ) -> Result<MissionMaterializedSnapshot, String> {
        // The default Mission is a durable aggregate. Ensure it before reading
        // the event cursor so the returned snapshot never trails the commit
        // performed while constructing its own projection.
        self.runtime().ensure_default_mission()?;
        let sessions = self.canonical_session_nodes().await?;
        let active_session_id = sessions
            .iter()
            .filter(|session| session.active)
            .max_by_key(|session| session.updated_at_ms)
            .map(|session| session.session_id.clone());
        let latest_cursor = *self.events().subscribe_commits().borrow();
        let cache_key = selected_mission_id.unwrap_or_default().to_string();
        let mut cache = self.projection_cache.lock().await;
        if let Some(entry) = cache.get(&cache_key) {
            if entry.snapshot.cursor == latest_cursor
                && entry.canonical_sessions == sessions
                && entry.snapshot.projection.workspace.active_session_id == active_session_id
            {
                return Ok(entry.snapshot.clone());
            }
        }
        let revision = cache
            .get(&cache_key)
            .map_or(1, |entry| entry.snapshot.revision.saturating_add(1));
        let projection = self.runtime().control_projection(
            sessions.clone(),
            active_session_id,
            selected_mission_id.map(str::to_owned),
        )?;
        let snapshot = MissionMaterializedSnapshot {
            schema_version: MISSION_CONTROL_SCHEMA_VERSION,
            kind: "mission_control.materialized_snapshot".to_string(),
            cursor: latest_cursor,
            revision,
            needs_resync: false,
            projection,
        };
        cache.insert(
            cache_key,
            super::MissionProjectionCacheEntry {
                snapshot: snapshot.clone(),
                canonical_sessions: sessions,
            },
        );
        Ok(snapshot)
    }

    pub(crate) async fn materialized_delta(
        &self,
        from_cursor: u64,
        from_revision: Option<u64>,
    ) -> Result<MissionProjectionDelta, String> {
        self.materialized_delta_for(from_cursor, from_revision, None)
            .await
    }

    pub(crate) async fn materialized_delta_for(
        &self,
        from_cursor: u64,
        from_revision: Option<u64>,
        selected_mission_id: Option<&str>,
    ) -> Result<MissionProjectionDelta, String> {
        const MAX_COMMITS: usize = 256;
        let snapshot = self.materialized_snapshot_for(selected_mission_id).await?;
        if from_cursor == snapshot.cursor && from_revision == Some(snapshot.revision) {
            return Ok(MissionProjectionDelta {
                schema_version: MISSION_CONTROL_SCHEMA_VERSION,
                kind: "mission_control.projection_delta".to_string(),
                from_cursor,
                from_revision,
                to_cursor: snapshot.cursor,
                revision: snapshot.revision,
                needs_resync: false,
                changed_domains: Vec::new(),
                events: Vec::new(),
                patch: serde_json::json!({}),
            });
        }
        if from_cursor > snapshot.cursor {
            return Ok(resync_delta(from_cursor, from_revision, &snapshot));
        }
        let batches = self
            .events()
            .events_after_cursor(from_cursor, MAX_COMMITS)?;
        let reached_cursor = batches
            .last()
            .map_or(from_cursor, |batch| batch.commit_cursor);
        if batches.len() == MAX_COMMITS && reached_cursor < snapshot.cursor {
            return Ok(resync_delta(from_cursor, from_revision, &snapshot));
        }
        let mut domains = BTreeSet::new();
        let mut events = Vec::new();
        for batch in batches {
            for event in batch.events {
                domains.insert(event_domain(&event));
                events.push(event_line(event));
            }
        }
        if !events.is_empty() {
            domains.insert("event_digest".to_string());
        }
        if domains.is_empty() {
            domains.insert("sessions".to_string());
        }
        let changed_domains = domains.into_iter().collect::<Vec<_>>();
        let patch = projection_patch(&snapshot.projection, &changed_domains);
        Ok(MissionProjectionDelta {
            schema_version: MISSION_CONTROL_SCHEMA_VERSION,
            kind: "mission_control.projection_delta".to_string(),
            from_cursor,
            from_revision,
            to_cursor: snapshot.cursor,
            revision: snapshot.revision,
            needs_resync: false,
            changed_domains,
            events,
            patch,
        })
    }

    async fn canonical_session_nodes(&self) -> Result<Vec<MissionControlSessionNode>, String> {
        let presence = self
            .sessions()
            .presence_snapshots()
            .await
            .into_iter()
            .map(|snapshot| (snapshot.session_id.clone(), snapshot))
            .collect::<HashMap<_, _>>();
        let working_set = self.sessions().working_set_projection().await?;
        let hydration = working_set
            .entries
            .into_iter()
            .map(|entry| {
                (
                    entry.session_id,
                    (
                        format!("{:?}", entry.status).to_lowercase(),
                        entry.last_error,
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let teams = self.runtime().team_projection_json();
        let mut team_counts = HashMap::<String, usize>::new();
        let mut agent_counts = HashMap::<String, usize>::new();
        if let Some(items) = teams.get("teams").and_then(serde_json::Value::as_array) {
            for team in items {
                if let Some(session_id) = team.get("session_id").and_then(serde_json::Value::as_str)
                {
                    *team_counts.entry(session_id.to_string()).or_default() += 1;
                    *agent_counts.entry(session_id.to_string()).or_default() += team
                        .get("tasks")
                        .and_then(serde_json::Value::as_array)
                        .map_or(0, Vec::len);
                }
            }
        }
        let active = self
            .sessions()
            .list_active_session_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut relevant_session_ids = active.clone();
        relevant_session_ids.extend(presence.keys().cloned());
        relevant_session_ids.extend(hydration.keys().cloned());
        relevant_session_ids.extend(team_counts.keys().cloned());
        relevant_session_ids.extend(self.runtime().referenced_session_ids());
        let relevant_session_ids = relevant_session_ids.into_iter().collect::<Vec<_>>();
        let task_contributions = self.runtime().session_task_contributions();
        let mut stored = Vec::new();
        let mut offset = 0;
        loop {
            let Some(page) = self
                .sessions()
                .list_stored_sessions_page(&SessionListOptions {
                    owner_principal_id: None,
                    visible_session_ids: &relevant_session_ids,
                    unrestricted: false,
                    include_deleted: false,
                    sort: "last_activity",
                    order: "desc",
                    limit: 500,
                    offset,
                    ..SessionListOptions::default()
                })
                .await
                .map_err(|error| error.to_string())?
            else {
                break;
            };
            let fetched = page.records.len();
            stored.extend(page.records);
            offset = offset.saturating_add(fetched);
            if fetched == 0 || offset >= page.total {
                break;
            }
        }
        let mut nodes = stored
            .into_iter()
            .map(|record| {
                let lifecycle = presence.get(&record.session_id);
                let (hydration, last_error) = hydration
                    .get(&record.session_id)
                    .cloned()
                    .unwrap_or_else(|| ("unloaded".to_string(), None));
                let metadata = record
                    .metadata_json
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
                let title = metadata
                    .as_ref()
                    .and_then(|value| value.get("title"))
                    .and_then(serde_json::Value::as_str)
                    .filter(|title| !title.trim().is_empty())
                    .unwrap_or(record.session_id.as_str())
                    .to_string();
                MissionControlSessionNode {
                    session_id: record.session_id.clone(),
                    title,
                    status: record.status,
                    lifecycle: lifecycle
                        .map(|snapshot| snapshot.state.as_str().to_string())
                        .unwrap_or_else(|| "detached".to_string()),
                    hydration,
                    active: active.contains(&record.session_id),
                    attachment_count: lifecycle.map_or(0, |snapshot| snapshot.attachments.len()),
                    team_count: team_counts.get(&record.session_id).copied().unwrap_or(0),
                    agent_count: agent_counts.get(&record.session_id).copied().unwrap_or(0),
                    contributing_task_count: task_contributions
                        .get(&record.session_id)
                        .map_or(0, Vec::len),
                    contributing_task_ids: task_contributions
                        .get(&record.session_id)
                        .cloned()
                        .unwrap_or_default(),
                    created_at_ms: parse_timestamp_ms(&record.created_at),
                    updated_at_ms: parse_timestamp_ms(&record.last_activity),
                    last_error,
                }
            })
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| {
            (
                std::cmp::Reverse(node.updated_at_ms),
                node.session_id.clone(),
            )
        });
        Ok(nodes)
    }

    pub(crate) fn team_execution_plan(&self, team_id: &str) -> Result<serde_json::Value, String> {
        let team = self.runtime().team_projection(team_id)?;
        let graph = self.runtime().team_graph(team_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_execution_plan",
            "ok": true,
            "team": team,
            "graph": graph,
        }))
    }

    pub(crate) fn collaboration_runs(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.collaboration_runs",
            "ok": true,
            "projection": self.runtime().team_projection_json(),
        })
    }

    pub(crate) fn collaboration_run(&self, team_id: &str) -> Result<serde_json::Value, String> {
        let run = self.runtime().team_projection(team_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.collaboration_run",
            "ok": true,
            "run": run,
        }))
    }

    pub(crate) async fn cancel_team_runtime(
        &self,
        team_id: &str,
    ) -> Result<serde_json::Value, String> {
        self.execute_mission_control_command(MissionCommand {
            command_id: format!("mission-team-cancel-{}", uuid::Uuid::new_v4()),
            action: MissionCommandAction::Cancel,
            target: MissionCommandTarget::Team {
                team_id: team_id.to_string(),
            },
            actor: "gateway_mission_team_route".to_string(),
            expected_revision: None,
            correlation_id: String::new(),
            payload: serde_json::Value::Null,
            evidence_refs: Vec::new(),
        })
        .await
    }

    pub(crate) fn agent_mission_events(&self, agent_id: &str) -> serde_json::Value {
        let data = self.runtime().agent_events(agent_id);
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.agent_events",
            "ok": true,
            "agent_id": agent_id,
            "events": data["events"],
            "run": data["run"],
        })
    }

    pub(crate) fn team_mission_evidence(&self, team_id: &str) -> serde_json::Value {
        let data = self.runtime().team_evidence(team_id);
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission_control.team_evidence",
            "ok": true,
            "team_id": team_id,
            "events": data["events"],
            "tasks": data["tasks"],
            "team": data["team"],
            "evidence": data["evidence"],
        })
    }

    pub(crate) fn approvals(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.approval_projection_contract(),
            "approvals": self.runtime().approvals_projection(),
        })
    }

    pub(crate) fn relations(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.relation_projection_contract(),
            "relations": self.runtime().relations_projection(),
        })
    }

    pub(crate) fn conflicts(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.conflict_projection_contract(),
            "conflicts": self.runtime().conflicts_projection(),
        })
    }

    pub(crate) async fn start_session(
        &self,
        request: StartMissionSessionHttpRequest,
        actor: String,
    ) -> Result<serde_json::Value, String> {
        let session_id = request
            .session_id
            .unwrap_or_else(|| format!("mission-{}", uuid::Uuid::new_v4()));
        self.execute_mission_control_command(MissionCommand {
            command_id: format!("mission-session-create-{}", uuid::Uuid::new_v4()),
            action: MissionCommandAction::Create,
            target: MissionCommandTarget::Session { session_id },
            actor,
            expected_revision: Some(0),
            correlation_id: String::new(),
            payload: serde_json::json!({"title": request.title}),
            evidence_refs: Vec::new(),
        })
        .await
    }

    pub(crate) fn schedules(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedules": self.runtime().schedule_projection(),
            "policy": self.runtime().schedule_policy(),
        })
    }

    pub(crate) async fn create_schedule(
        &self,
        request: CreateMissionScheduleHttpRequest,
    ) -> Result<serde_json::Value, String> {
        if !self
            .sessions()
            .session_exists(&request.target_session_id)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err(format!(
                "mission target session not found: {}",
                request.target_session_id
            ));
        }
        let schedule = self
            .runtime()
            .create_schedule(runtime::CreateMissionScheduleRequest {
                mission_id: request.mission_id,
                target_session_id: request.target_session_id,
                objective: request.objective,
                trigger: request.trigger,
                autonomy_profile: request.autonomy_profile,
                permission_ceiling: request.permission_ceiling,
                priority: request.priority,
            })?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
            "schedules": self.runtime().schedule_projection(),
            "policy": self.runtime().schedule_policy(),
        }))
    }

    pub(crate) async fn tick_schedules(&self) -> Result<serde_json::Value, String> {
        let report = self.runtime().dispatch_due_schedules().await?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": report.failed.is_empty(),
            "report": report,
            "schedules": self.runtime().schedule_projection(),
            "policy": self.runtime().schedule_policy(),
        }))
    }

    pub(crate) fn pause_schedule(&self, schedule_id: &str) -> Result<serde_json::Value, String> {
        let schedule = self.runtime().pause_schedule(schedule_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
        }))
    }

    pub(crate) fn resume_schedule(&self, schedule_id: &str) -> Result<serde_json::Value, String> {
        let schedule = self.runtime().resume_schedule(schedule_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
        }))
    }

    pub(crate) async fn run_schedule_now(
        &self,
        schedule_id: &str,
    ) -> Result<serde_json::Value, String> {
        let report = self.runtime().run_schedule_now(schedule_id).await?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": report.failed.is_empty(),
            "report": report,
            "schedules": self.runtime().schedule_projection(),
        }))
    }

    pub(crate) fn delete_schedule(&self, schedule_id: &str) -> Result<serde_json::Value, String> {
        let schedule = self.runtime().delete_schedule(schedule_id)?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "deleted": schedule,
            "schedules": self.runtime().schedule_projection(),
        }))
    }

    pub(crate) fn update_schedule(
        &self,
        schedule_id: &str,
        request: UpdateMissionScheduleHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let schedule = self.runtime().update_schedule(
            schedule_id,
            runtime::UpdateMissionScheduleRequest {
                expected_revision: request.expected_revision,
                objective: request.objective,
                trigger: request.trigger,
                autonomy_profile: request.autonomy_profile,
                permission_ceiling: request.permission_ceiling,
                priority: request.priority,
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "schedule": schedule,
        }))
    }

    pub(crate) async fn session_detail(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, String> {
        let session = self
            .sessions()
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("mission session not found: {session_id}"))?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "kind": "mission.session",
            "session": session,
            "mission": self.runtime().projection(),
        }))
    }

    pub(crate) async fn switch_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(session_id, MissionCommandAction::Activate)
            .await
    }

    pub(crate) async fn background_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(session_id, MissionCommandAction::Background)
            .await
    }

    pub(crate) async fn pause_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(session_id, MissionCommandAction::Pause)
            .await
    }

    pub(crate) async fn close_session(&self, session_id: &str) -> serde_json::Value {
        self.session_transition_command(session_id, MissionCommandAction::Close)
            .await
    }

    pub(crate) fn submit_approval(
        &self,
        request: SubmitMissionApprovalHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let context = harness_contract::policy::ApprovalContext::owned(
            &request.source,
            &request.action,
            "mission",
        );
        let approval = self
            .runtime()
            .submit_approval(runtime::SubmitGlobalApprovalRequest {
                source: request.source,
                context,
                action: request.action,
                summary: request.summary,
                risk: request.risk,
                domain: harness_contract::policy::ApprovalDomain::Execution,
                blocks_execution: true,
                evidence_refs: request.evidence_refs,
                timeout_policy: request.timeout_policy,
            })?;
        Ok(serde_json::json!({
            "envelope": self.approval_command_contract(),
            "ok": true,
            "approval": approval,
            "approvals": self.runtime().approvals_projection(),
        }))
    }

    pub(crate) fn decide_approval(
        &self,
        approval_id: &str,
        request: DecideMissionApprovalHttpRequest,
        principal: &runtime::VerifiedPrincipal,
    ) -> Result<serde_json::Value, String> {
        if !principal.is_human_interactive() || !principal.has_capability("approval.respond") {
            return Err("approval_human_interactive_capability_required".to_string());
        }
        let receipt = self.runtime().decide_approval(
            principal,
            runtime::ApprovalDecisionCommand {
                approval_id: approval_id.to_string(),
                approved: request.approved,
                skip: false,
                reason: request.reason,
                scope: request.scope,
                actor: harness_contract::policy::ApprovalDecisionActor {
                    kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                    actor_id: principal.claims().principal_id.clone(),
                },
                evidence_refs: vec!["gateway.mission.approval".to_string()],
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.approval_command_contract(),
            "ok": true,
            "receipt": receipt,
            "approvals": self.runtime().approvals_projection(),
        }))
    }

    pub(crate) fn upsert_proxy(
        &self,
        request: UpsertMissionProxyHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let proxy = self.runtime().upsert_proxy(runtime::SessionProxy {
            session_id: request.session_id,
            summary: request.summary,
            evidence_refs: request.evidence_refs,
            decisions: request.decisions,
            open_questions: request.open_questions,
            updated_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        })?;
        Ok(serde_json::json!({
            "envelope": self.relation_command_contract(),
            "ok": true,
            "proxy": proxy,
            "relations": self.runtime().relations_projection(),
        }))
    }

    async fn session_transition_command(
        &self,
        session_id: &str,
        action: MissionCommandAction,
    ) -> serde_json::Value {
        self.execute_mission_control_command(MissionCommand {
            command_id: format!("mission-session-command-{}", uuid::Uuid::new_v4()),
            target: MissionCommandTarget::Session {
                session_id: session_id.to_string(),
            },
            action,
            actor: "gateway_mission_session_route".to_string(),
            expected_revision: None,
            correlation_id: String::new(),
            payload: serde_json::Value::Null,
            evidence_refs: Vec::new(),
        })
        .await
        .unwrap_or_else(|error| {
            serde_json::json!({
                "envelope": self.session_control_contract(),
                "kind": "mission_control.command_result",
                "ok": false,
                "error": error,
            })
        })
    }
}

/// Bounded summary rendering for `mission_control?detail=summary` (P5).
/// Long prose is truncated so list/table surfaces stay complete while the
/// payload stays small; `detail=graph` returns the full projection.
fn summarize_mission_projection(value: &serde_json::Value) -> serde_json::Value {
    const MAX_TEXT: usize = 80;
    const MAX_ELEMENT_TEXT: usize = 40;
    match value {
        serde_json::Value::String(text) if text.chars().count() > MAX_TEXT => {
            serde_json::Value::String(format!(
                "{}…",
                text.chars().take(MAX_TEXT).collect::<String>()
            ))
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(summarize_mission_projection).collect())
        }
        serde_json::Value::Object(map) => {
            if let Some(allow) = element_field_allowlist(&map) {
                let mut out = serde_json::Map::new();
                for (key, item) in map {
                    if allow.iter().any(|field| *field == key) {
                        let summarized = if key == "detail" {
                            summarize_element_text(item, MAX_ELEMENT_TEXT)
                        } else {
                            summarize_mission_projection(item)
                        };
                        out.insert(key.clone(), summarized);
                    } else if item.is_object() || item.is_array() {
                        out.insert(
                            key.clone(),
                            serde_json::json!({
                                "count": if item.is_array() {
                                    item.as_array().map_or(0, Vec::len)
                                } else {
                                    item.as_object().map_or(0, serde_json::Map::len)
                                }
                            }),
                        );
                    } else {
                        out.insert(key.clone(), item.clone());
                    }
                }
                serde_json::Value::Object(out)
            } else {
                serde_json::Value::Object(
                    map.iter()
                        .map(|(key, item)| (key.clone(), summarize_mission_projection(item)))
                        .collect(),
                )
            }
        }
        other => other.clone(),
    }
}

fn summarize_element_text(value: &serde_json::Value, limit: usize) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) if text.chars().count() > limit => {
            serde_json::Value::String(format!("{}…", text.chars().take(limit).collect::<String>()))
        }
        serde_json::Value::Object(map) => {
            serde_json::json!({ "count": map.len() })
        }
        serde_json::Value::Array(items) => {
            serde_json::json!({ "count": items.len() })
        }
        other => other.clone(),
    }
}

fn element_field_allowlist(
    map: &serde_json::Map<String, serde_json::Value>,
) -> Option<&'static [&'static str]> {
    if map.contains_key("team_id") && map.contains_key("status") {
        Some(&[
            "team_id",
            "status",
            "task_id",
            "mission_id",
            "session_id",
            "graph_id",
            "agent_count",
            "detail",
        ])
    } else if map.contains_key("agent_id") && map.contains_key("status") {
        Some(&[
            "agent_id",
            "status",
            "backend",
            "execution_id",
            "mission_id",
            "session_id",
            "task_id",
            "team_id",
            "detail",
        ])
    } else if map.contains_key("execution_id")
        && map.contains_key("status")
        && map.contains_key("graph_id")
    {
        Some(&["execution_id", "graph_id", "status", "kind"])
    } else {
        None
    }
}

fn ensure_session_value(outcome: &EnsureSessionOutcome) -> serde_json::Value {
    serde_json::json!({
        "session_id": outcome.session_id,
        "model": outcome.model,
        "created": outcome.created,
        "restored": outcome.restored,
        "record": outcome.record,
    })
}

fn payload_text<'a>(payload: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_timestamp_ms(value: &str) -> u64 {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis().max(0) as u64)
        .unwrap_or_default()
}

fn event_domain(event: &runtime::DurableRuntimeEvent) -> String {
    let kind = event.kind.as_str();
    if kind.starts_with("mission.") {
        "mission"
    } else if kind.starts_with("task.") {
        "tasks"
    } else if kind.starts_with("team.") {
        "teams"
    } else if kind.starts_with("agent.") {
        "agents"
    } else if kind.starts_with("approval.") {
        "approvals"
    } else if kind.starts_with("relation.")
        || kind.starts_with("session_relation.")
        || kind.starts_with("session.relation.")
        || kind.starts_with("session.proxy.")
    {
        "relations"
    } else if kind.starts_with("conflict.") {
        "conflicts"
    } else if kind.starts_with("evidence.") {
        "evidence"
    } else if kind.starts_with("execution.") || kind.starts_with("graph.") {
        "execution_graphs"
    } else if matches!(event.scope, runtime::RuntimeEventScope::Session) {
        "sessions"
    } else {
        "event_digest"
    }
    .to_string()
}

fn event_line(event: runtime::DurableRuntimeEvent) -> MissionControlEventLine {
    MissionControlEventLine {
        event_id: event.event_id,
        stream_id: event.stream_id,
        cursor: event.commit_cursor,
        transaction_index: event.transaction_index,
        scope: format!("{:?}", event.scope).to_lowercase(),
        kind: event.kind,
        status: event.status,
        actor: event.actor,
        created_at_ms: event.created_at_ms,
    }
}

fn projection_patch(
    projection: &runtime::MissionControlProjection,
    changed_domains: &[String],
) -> serde_json::Value {
    let mut patch = serde_json::Map::new();
    for domain in changed_domains {
        let value = match domain.as_str() {
            "mission" => projection.mission.clone(),
            "sessions" => serde_json::to_value(&projection.sessions).unwrap_or_default(),
            "tasks" => serde_json::to_value(&projection.tasks).unwrap_or_default(),
            "teams" => serde_json::to_value(&projection.teams).unwrap_or_default(),
            "agents" => serde_json::to_value(&projection.agents).unwrap_or_default(),
            "approvals" => serde_json::to_value(&projection.approvals).unwrap_or_default(),
            "relations" => projection.relations.clone(),
            "conflicts" => projection.conflicts.clone(),
            "evidence" => projection.evidence.clone(),
            "execution_graphs" => projection.execution_graphs.clone(),
            "event_digest" => serde_json::to_value(&projection.event_digest).unwrap_or_default(),
            _ => continue,
        };
        patch.insert(domain.clone(), value);
    }
    patch.insert(
        "workspace".to_string(),
        serde_json::to_value(&projection.workspace).unwrap_or_default(),
    );
    patch.insert(
        "summary".to_string(),
        serde_json::to_value(&projection.summary).unwrap_or_default(),
    );
    patch.insert(
        "control_readiness".to_string(),
        serde_json::to_value(&projection.control_readiness).unwrap_or_default(),
    );
    patch.insert(
        "mission_graph".to_string(),
        serde_json::to_value(&projection.mission_graph).unwrap_or_default(),
    );
    serde_json::Value::Object(patch)
}

fn resync_delta(
    from_cursor: u64,
    from_revision: Option<u64>,
    snapshot: &MissionMaterializedSnapshot,
) -> MissionProjectionDelta {
    MissionProjectionDelta {
        schema_version: MISSION_CONTROL_SCHEMA_VERSION,
        kind: "mission_control.projection_delta".to_string(),
        from_cursor,
        from_revision,
        to_cursor: snapshot.cursor,
        revision: snapshot.revision,
        needs_resync: true,
        changed_domains: Vec::new(),
        events: Vec::new(),
        patch: serde_json::json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scoped_mission_service() -> MissionService {
        scoped_mission_service_with_runtime().0
    }

    fn scoped_mission_service_with_runtime() -> (MissionService, Arc<runtime::RuntimeServices>) {
        let runtime_services =
            runtime::RuntimeServices::in_memory().expect("workspace-scoped runtime services");
        let store =
            Arc::new(session::UnifiedSessionStore::open_in_memory().expect("Session store"));
        let repository = Arc::new(
            crate::services::session_service::repository::SessionRepository::new(
                Arc::new(crate::gateway::HotSessionPool::new()),
                Some(store),
                crate::event_bus::SessionProjectionHub::new(),
            ),
        );
        let sessions = Arc::new(SessionService::for_tests(
            repository,
            Arc::new(crate::services::session_service::presence::SessionPresenceLedger::new()),
        ));
        (
            MissionService::new().with_dependencies(
                runtime::MissionRuntimePort::new(Arc::clone(&runtime_services)),
                sessions,
                RuntimeEventService::from_runtime_services(&runtime_services),
            ),
            runtime_services,
        )
    }

    #[tokio::test]
    async fn mission_service_projects_runtime_control_surfaces() {
        let service = scoped_mission_service();
        let mission_id = format!("mission-service-test-{}", uuid::Uuid::new_v4());
        let result = service
            .execute_mission_control_command(MissionCommand {
                command_id: format!("command-{}", uuid::Uuid::new_v4()),
                action: MissionCommandAction::Create,
                target: MissionCommandTarget::Mission {
                    mission_id: mission_id.clone(),
                },
                actor: "test".to_string(),
                expected_revision: Some(0),
                correlation_id: "mission-service-test".to_string(),
                payload: serde_json::json!({"objective": "verify Mission application saga"}),
                evidence_refs: Vec::new(),
            })
            .await
            .expect("Mission command");

        assert_eq!(result["ok"], true);
        assert_eq!(result["saga"]["phase"], "finalized");
        assert_eq!(
            result["snapshot"]["projection"]["kind"],
            "mission_control.projection"
        );
        let projection = service.projection();
        assert_eq!(projection["mission"]["kind"], "mission.runtime");
        assert_eq!(projection["mission"]["schema_version"], 6);
        assert_eq!(
            projection["mission"]["conflict_projection"]["kind"],
            "runtime.conflicts"
        );
        assert_eq!(
            projection["mission"]["capability_projection"]["name"],
            "cowd-runtime-capability-catalog"
        );
        assert_eq!(
            service.approvals()["approvals"]["kind"],
            "runtime.global_approvals"
        );
        assert_eq!(
            service.relations()["relations"]["kind"],
            "runtime.session_relations"
        );
        assert_eq!(
            service.conflicts()["conflicts"]["kind"],
            "runtime.conflicts"
        );
    }

    #[tokio::test]
    async fn mission_materialized_delta_uses_cursor_and_revision() {
        let service = scoped_mission_service();
        let snapshot = service.materialized_snapshot().await.expect("snapshot");
        let delta = service
            .materialized_delta(snapshot.cursor, Some(snapshot.revision))
            .await
            .expect("delta");
        assert!(!delta.needs_resync);
        assert!(delta.changed_domains.is_empty());
    }

    #[tokio::test]
    async fn mission_summary_contract_is_bounded_and_typed_snapshot_stays_full() {
        let service = scoped_mission_service();

        let summary = service
            .mission_control_summary(None)
            .await
            .expect("mission summary");
        assert_eq!(summary["ok"], true);
        assert!(
            summary.get("snapshot").is_none(),
            "summary contract must not carry the full typed snapshot"
        );
        assert_eq!(summary["summary"]["graph"]["available"], true);
        assert!(summary["summary"]["graph"]["node_count"].is_u64());
        assert!(summary["summary"]["graph"]["edge_count"].is_u64());
        assert!(
            summary["summary"]["graph"]["hash"]
                .as_str()
                .is_some_and(|hash| !hash.is_empty())
        );

        let control = service
            .mission_control(None, "graph")
            .await
            .expect("full graph snapshot");
        let snapshot: harness_contract::mission::MissionMaterializedSnapshot =
            serde_json::from_value(control["snapshot"].clone())
                .expect("typed snapshot must round-trip");
        let _ = snapshot.projection.mission_graph.nodes.len();
    }

    #[tokio::test]
    async fn mission_control_rejects_unknown_detail() {
        let service = scoped_mission_service();
        let error = service
            .mission_control(None, "bogus")
            .await
            .expect_err("unknown detail must fail closed");
        assert!(error.contains("unsupported mission detail"));
    }

    #[tokio::test]
    async fn gateway_mission_saga_resumes_after_effect_commit_without_repeating_external_effect() {
        let service = scoped_mission_service();
        let command = MissionCommand {
            command_id: format!("mission-saga-command-{}", uuid::Uuid::new_v4()),
            action: MissionCommandAction::Pause,
            target: MissionCommandTarget::Session {
                session_id: "session-external-effect".to_string(),
            },
            actor: "test".to_string(),
            expected_revision: None,
            correlation_id: "mission-saga-resume".to_string(),
            payload: serde_json::Value::Null,
            evidence_refs: Vec::new(),
        };
        service
            .runtime()
            .reserve_command(command.clone())
            .expect("reserve");
        service
            .runtime()
            .commit_command_effect(
                &command.command_id,
                serde_json::json!({"unloaded": true}),
                Vec::new(),
            )
            .expect("effect commit");

        let resumed = service
            .execute_mission_control_command(command)
            .await
            .expect("resume");
        assert_eq!(resumed["saga"]["phase"], "finalized");
        assert_eq!(resumed["receipt"]["result"]["unloaded"], true);
    }

    #[tokio::test]
    async fn mission_materialized_delta_requests_resync_after_a_ten_thousand_commit_gap() {
        let service = scoped_mission_service();
        let initial = service.materialized_snapshot().await.expect("initial");
        for index in 0..10_000_u64 {
            service
                .events()
                .append_fixture(runtime::RuntimeEventInput {
                    stream_id: "mission-delta-load".to_string(),
                    scope: runtime::RuntimeEventScope::Mission,
                    kind: "mission.load.fixture.v1".to_string(),
                    status: Some("committed".to_string()),
                    actor: Some("test".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"index": index}),
                })
                .expect("append load event");
        }

        let delta = service
            .materialized_delta(initial.cursor, Some(initial.revision))
            .await
            .expect("bounded delta");
        assert!(delta.needs_resync);
        assert!(delta.events.is_empty());
        assert!(delta.to_cursor > initial.cursor);
    }

    #[test]
    fn mission_service_exposes_runtime_owned_schedule_policy() {
        let service = scoped_mission_service();
        let projection = service.schedules();
        assert_eq!(projection["policy"]["enabled"], true);
        assert!(projection["policy"]["tick_interval_ms"].as_u64().is_some());
    }
}
