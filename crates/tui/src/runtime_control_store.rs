use crate::app::App;
use crate::gateway_client::GatewayApiClient;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskSummary {
    pub id: String,
    pub objective: String,
    pub status: String,
    pub current_phase: Option<String>,
    pub yolo_mode: bool,
    pub failure_count: u64,
    pub review_result: Option<String>,
    pub artifact_count: u64,
    pub blocker_reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApprovalSummary {
    pub id: String,
    pub tool_name: String,
    pub risk: Option<String>,
    pub requester: Option<String>,
    pub input_preview: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorAccountSummary {
    pub provider: String,
    pub account_id: String,
    pub auth_mode: String,
    pub status: String,
    pub reason: Option<String>,
    pub binding_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorCapabilitySummary {
    pub capability_id: String,
    pub provider: String,
    pub plane: String,
    pub risk: String,
    pub supports_commit: bool,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorResourceSummary {
    pub reference: String,
    pub provider: String,
    pub resource_type: String,
    pub title: String,
    pub indexed_state: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeActionReceiptSummary {
    pub status: String,
    pub dispatch_status: String,
    pub mode: String,
    pub capability: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceSummary {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub status: String,
    pub lifecycle: String,
    pub transport: String,
    pub capability_count: u64,
    pub route_count: u64,
    pub resource_count: u64,
    pub entry: Option<String>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceHealthSummary {
    pub status: String,
    pub surface_count: u64,
    pub external_surface_count: u64,
    pub route_count: u64,
    pub resource_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceEventSummary {
    pub surface: String,
    pub event_type: String,
    pub detail: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CowdKernelSummary {
    pub capability_count: u64,
    pub projection_capability_count: u64,
    pub webui_tui_full_parity: bool,
    pub cli_is_minimal_control: bool,
    pub release_gate_status: String,
    pub release_gate_failed_checks: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StructuredDataSummary {
    pub source_count: u64,
    pub fact_count: u64,
    pub evidence_count: u64,
    pub watermark_count: u64,
    pub sample_sources: Vec<String>,
    pub sample_facts: Vec<String>,
    pub sample_evidence: Vec<String>,
    pub sample_watermarks: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeControlSnapshot {
    pub gateway_running: bool,
    pub active_sessions: usize,
    pub uptime_secs: Option<u64>,
    pub session_ids: Vec<String>,
    pub runtime_readiness: Option<String>,
    pub runtime_components: Option<u64>,
    pub task_count: Option<u64>,
    pub tasks: Vec<TaskSummary>,
    pub pending_approvals: Option<u64>,
    pub approval_items: Vec<ApprovalSummary>,
    pub lease_owner: Option<String>,
    pub lease_mode: Option<String>,
    pub memory_status: Option<String>,
    pub memory_total_entries: Option<usize>,
    pub memory_vector_count: Option<usize>,
    pub memory_layer_counts: [usize; 5],
    pub cross_plane_grants_active: Option<u64>,
    pub cross_plane_actions_24h: Option<u64>,
    pub connector_accounts: Vec<ConnectorAccountSummary>,
    pub connector_capabilities: Vec<ConnectorCapabilitySummary>,
    pub connector_resources: Vec<ConnectorResourceSummary>,
    pub action_receipts: Vec<RuntimeActionReceiptSummary>,
    pub surfaces: Vec<SurfaceSummary>,
    pub surface_health: Option<SurfaceHealthSummary>,
    pub surface_events: Vec<SurfaceEventSummary>,
    pub cowd_kernel: Option<CowdKernelSummary>,
    pub structured_data: Option<StructuredDataSummary>,
    pub connector_degraded_reasons: Vec<String>,
    pub degraded_reasons: Vec<String>,
}

impl RuntimeControlSnapshot {
    pub fn from_gateway_snapshot(value: &serde_json::Value) -> Self {
        let session_ids = value
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut state = Self {
            gateway_running: value
                .get("ok")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true),
            active_sessions: value
                .get("active_sessions")
                .and_then(serde_json::Value::as_u64)
                .map(|count| count as usize)
                .unwrap_or(session_ids.len()),
            uptime_secs: value.get("uptime_secs").and_then(serde_json::Value::as_u64),
            session_ids,
            ..Self::default()
        };
        if let Some(lease) = value
            .pointer("/leases/items")
            .and_then(serde_json::Value::as_array)
            .and_then(|items| items.first())
        {
            state.apply_lease_value(lease);
        }
        state
    }

    pub fn from_app(app: &App) -> Self {
        Self {
            gateway_running: app.server_running,
            active_sessions: app.active_api_sessions,
            uptime_secs: app.server_uptime_secs,
            runtime_readiness: app.gateway_runtime_readiness.clone(),
            runtime_components: app.gateway_runtime_components,
            task_count: app.gateway_task_count,
            tasks: app.gateway_tasks.clone(),
            pending_approvals: app.gateway_pending_approvals,
            approval_items: app.gateway_approval_items.clone(),
            lease_owner: app.gateway_lease_owner.clone(),
            lease_mode: app.gateway_lease_mode.clone(),
            memory_status: app.memory_status.clone(),
            memory_total_entries: app.memory_total_entries,
            memory_vector_count: app.memory_vector_count,
            memory_layer_counts: app.memory_layer_counts,
            cross_plane_grants_active: app.gateway_cross_plane_grants_active,
            cross_plane_actions_24h: app.gateway_cross_plane_actions_24h,
            connector_accounts: app.gateway_connector_accounts.clone(),
            connector_capabilities: app.gateway_connector_capabilities.clone(),
            connector_resources: app.gateway_connector_resources.clone(),
            action_receipts: app.gateway_action_receipts.clone(),
            surfaces: app.gateway_surfaces.clone(),
            surface_health: app.gateway_surface_health.clone(),
            surface_events: app.gateway_surface_events.clone(),
            cowd_kernel: app.gateway_cowd_kernel.clone(),
            structured_data: app.gateway_structured_data.clone(),
            connector_degraded_reasons: app.gateway_connector_degraded_reasons.clone(),
            degraded_reasons: app.gateway_degraded_reasons.clone(),
            ..Self::default()
        }
    }

    pub fn apply_lease_value(&mut self, lease: &serde_json::Value) {
        self.lease_owner = lease
            .get("owner")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.lease_mode = lease
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
    }

    pub fn apply_to_app(&self, app: &mut App) {
        app.server_running = self.gateway_running;
        app.server_uptime_secs = self.uptime_secs;
        app.active_api_sessions = self.active_sessions;
        app.gateway_runtime_readiness = self.runtime_readiness.clone();
        app.gateway_runtime_components = self.runtime_components;
        app.gateway_task_count = self.task_count;
        app.gateway_tasks = self.tasks.clone();
        app.gateway_pending_approvals = self.pending_approvals;
        app.gateway_approval_items = self.approval_items.clone();
        app.memory_status = self.memory_status.clone();
        app.memory_total_entries = self.memory_total_entries;
        app.memory_vector_count = self.memory_vector_count;
        app.memory_layer_counts = self.memory_layer_counts;
        app.gateway_cross_plane_grants_active = self.cross_plane_grants_active;
        app.gateway_cross_plane_actions_24h = self.cross_plane_actions_24h;
        app.gateway_connector_accounts = self.connector_accounts.clone();
        app.gateway_connector_capabilities = self.connector_capabilities.clone();
        app.gateway_connector_resources = self.connector_resources.clone();
        app.gateway_action_receipts = self.action_receipts.clone();
        app.gateway_surfaces = self.surfaces.clone();
        app.gateway_surface_health = self.surface_health.clone();
        app.gateway_surface_events = self.surface_events.clone();
        app.gateway_cowd_kernel = self.cowd_kernel.clone();
        app.gateway_structured_data = self.structured_data.clone();
        app.gateway_connector_degraded_reasons = self.connector_degraded_reasons.clone();
        app.gateway_degraded_reasons = self.degraded_reasons.clone();
        app.gateway_lease_owner = self.lease_owner.clone();
        app.gateway_lease_mode = self.lease_mode.clone();
    }

    pub fn ingest_session_ids(&mut self, session_ids: Vec<String>) {
        self.active_sessions = session_ids.len();
        self.session_ids = session_ids;
    }

    pub fn ingest_session_list(&mut self, value: &serde_json::Value) {
        let sessions = value
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        item.get("id")
                            .or_else(|| item.get("session_id"))
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.ingest_session_ids(sessions);
    }

    pub fn ingest_runtime_control_plane(&mut self, value: &serde_json::Value) {
        self.runtime_readiness = value
            .pointer("/readiness/score")
            .or_else(|| value.pointer("/diagnostics/readiness_score"))
            .and_then(serde_json::Value::as_u64)
            .map(|score| format!("{score}%"))
            .or_else(|| Some("unknown".to_string()));
        self.runtime_components = value
            .pointer("/diagnostics/component_count")
            .and_then(serde_json::Value::as_u64);
    }

    pub fn ingest_task_status(&mut self, value: &serde_json::Value) {
        self.tasks = value
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .map(|tasks| {
                tasks
                    .iter()
                    .filter_map(task_summary_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.task_count = Some(self.tasks.len() as u64);
    }

    pub fn ingest_pending_approvals(&mut self, value: &serde_json::Value) {
        self.approval_items = value
            .as_array()
            .or_else(|| value.get("approvals").and_then(serde_json::Value::as_array))
            .or_else(|| value.get("pending").and_then(serde_json::Value::as_array))
            .map(|items| {
                items
                    .iter()
                    .filter_map(approval_summary_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.pending_approvals = Some(self.approval_items.len() as u64);
    }

    pub fn ingest_memory_status(&mut self, value: &serde_json::Value) {
        self.memory_status = value
            .get("status")
            .or_else(|| value.pointer("/memory/status"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.memory_total_entries = value
            .get("total_entries")
            .or_else(|| value.pointer("/memory/total_entries"))
            .or_else(|| {
                value
                    .get("entries")
                    .and_then(|entries| entries.get("total"))
            })
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as usize);
        self.memory_vector_count = value
            .get("vector_count")
            .or_else(|| value.pointer("/memory/vector_count"))
            .or_else(|| {
                value
                    .get("vectors")
                    .and_then(|vectors| vectors.get("total"))
            })
            .and_then(serde_json::Value::as_u64)
            .map(|value| value as usize);
        self.memory_layer_counts = memory_layer_counts_from_json(value);
    }

    pub fn ingest_cross_plane_summary(&mut self, value: &serde_json::Value) {
        self.cross_plane_grants_active = value
            .pointer("/grants/active")
            .and_then(serde_json::Value::as_u64);
        self.cross_plane_actions_24h = value
            .pointer("/interop/actions_24h")
            .and_then(serde_json::Value::as_u64);
    }

    pub fn ingest_cowd_projection_state(
        &mut self,
        capabilities: &serde_json::Value,
        projection: &serde_json::Value,
        surfaces: &serde_json::Value,
        release_gate: &serde_json::Value,
    ) {
        let capability_count = capabilities
            .get("capability_count")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                capabilities
                    .get("capabilities")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| items.len() as u64)
            })
            .unwrap_or_default();
        let projection_capability_count = projection
            .get("capability_count")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                projection
                    .get("capabilities")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| items.len() as u64)
            })
            .unwrap_or_default();
        let release_gate_failed_checks = release_gate
            .get("checks")
            .and_then(serde_json::Value::as_array)
            .map(|checks| {
                checks
                    .iter()
                    .filter(|check| {
                        check.get("status").and_then(serde_json::Value::as_str) != Some("pass")
                    })
                    .count() as u64
            })
            .unwrap_or_default();
        self.cowd_kernel = Some(CowdKernelSummary {
            capability_count,
            projection_capability_count,
            webui_tui_full_parity: surfaces
                .get("webui_tui_full_parity")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            cli_is_minimal_control: surfaces
                .get("cli_is_minimal_control")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            release_gate_status: release_gate
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            release_gate_failed_checks,
        });
    }

    pub fn ingest_structured_data(
        &mut self,
        sources: &serde_json::Value,
        facts: &serde_json::Value,
        evidence: &serde_json::Value,
        watermarks: &serde_json::Value,
    ) {
        self.structured_data = Some(StructuredDataSummary {
            source_count: structured_count(sources),
            fact_count: structured_count(facts),
            evidence_count: structured_count(evidence),
            watermark_count: structured_count(watermarks),
            sample_sources: structured_samples(sources, &["source_id", "source_ref", "id"]),
            sample_facts: structured_samples(facts, &["fact_id", "id"]),
            sample_evidence: structured_samples(evidence, &["evidence_id", "id"]),
            sample_watermarks: structured_samples(watermarks, &["source_ref", "id"]),
        });
    }

    pub fn ingest_connector_accounts(&mut self, value: &serde_json::Value) {
        self.connector_accounts = value
            .get("accounts")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(connector_account_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_connector_capabilities(&mut self, value: &serde_json::Value) {
        self.connector_capabilities = value
            .get("capabilities")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(connector_capability_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_connector_resources(&mut self, value: &serde_json::Value) {
        self.connector_resources = value
            .get("resources")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(connector_resource_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(reason) = value
            .get("degraded_reason")
            .and_then(serde_json::Value::as_str)
            .filter(|reason| !reason.trim().is_empty())
        {
            self.connector_degraded_reasons.push(reason.to_string());
        }
    }

    pub fn ingest_surface_registry(&mut self, value: &serde_json::Value) {
        self.surfaces = value
            .pointer("/registry/surfaces")
            .or_else(|| value.pointer("/surfaces"))
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(surface_summary_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_surface_health(&mut self, value: &serde_json::Value) {
        let host = value.get("host").unwrap_or(value);
        self.surface_health = Some(SurfaceHealthSummary {
            status: host
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| {
                    value
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                })
                .to_string(),
            surface_count: host
                .get("surface_count")
                .or_else(|| value.get("surface_count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(self.surfaces.len() as u64),
            external_surface_count: host
                .get("external_surface_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    self.surfaces
                        .iter()
                        .filter(|surface| surface.entry.is_some())
                        .count() as u64
                }),
            route_count: host
                .get("route_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    self.surfaces
                        .iter()
                        .map(|surface| surface.route_count)
                        .sum()
                }),
            resource_count: host
                .get("resource_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| {
                    self.surfaces
                        .iter()
                        .map(|surface| surface.resource_count)
                        .sum()
                }),
        });
    }

    pub fn ingest_surface_events(&mut self, surface: &str, value: &serde_json::Value) {
        let mut events = value
            .get("events")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| surface_event_summary_from_json(surface, item))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        self.surface_events.append(&mut events);
        self.surface_events.truncate(24);
    }

    pub fn begin_surface_event_refresh(&mut self) {
        self.surface_events.clear();
    }

    pub fn degrade(&mut self, reason: impl Into<String>) {
        self.degraded_reasons.push(reason.into());
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeControlLocalStore {
    snapshot: RuntimeControlSnapshot,
}

impl RuntimeControlLocalStore {
    pub fn from_app(app: &App) -> Self {
        Self {
            snapshot: RuntimeControlSnapshot::from_app(app),
        }
    }

    pub fn snapshot(&self) -> &RuntimeControlSnapshot {
        &self.snapshot
    }

    pub fn apply_to_app(&self, app: &mut App) {
        self.snapshot.apply_to_app(app);
    }

    pub fn apply_approval_response(&mut self, approval_id: &str) {
        self.snapshot
            .approval_items
            .retain(|approval| approval.id != approval_id);
        self.snapshot.pending_approvals = Some(self.snapshot.approval_items.len() as u64);
    }

    pub fn apply_task_status(&mut self, task_id: &str, status: &str) {
        for task in &mut self.snapshot.tasks {
            if task.id == task_id {
                task.status = status.to_string();
                if matches!(status, "completed" | "cancelled" | "canceled") {
                    task.blocker_reason = None;
                }
            }
        }
        self.snapshot.task_count = Some(self.snapshot.tasks.len() as u64);
    }

    pub fn apply_connector_resource_state(&mut self, reference: &str, state: &str) {
        for resource in &mut self.snapshot.connector_resources {
            if resource.reference == reference {
                resource.indexed_state = state.to_string();
            }
        }
    }

    pub fn push_action_receipt(
        &mut self,
        status: &str,
        dispatch_status: &str,
        mode: &str,
        capability: &str,
        idempotency_key: Option<String>,
    ) {
        self.snapshot.action_receipts.insert(
            0,
            RuntimeActionReceiptSummary {
                status: status.to_string(),
                dispatch_status: truncate_receipt_field(dispatch_status, 80),
                mode: mode.to_string(),
                capability: capability.to_string(),
                idempotency_key,
            },
        );
        self.snapshot.action_receipts.truncate(8);
    }
}

fn truncate_receipt_field(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn memory_layer_counts_from_json(value: &serde_json::Value) -> [usize; 5] {
    let mut counts = [0; 5];
    let layers = value
        .get("layers")
        .or_else(|| value.pointer("/memory/layers"))
        .and_then(serde_json::Value::as_array);
    if let Some(layers) = layers {
        for (fallback_idx, layer) in layers.iter().enumerate() {
            let count = layer
                .get("entry_count")
                .or_else(|| layer.get("count"))
                .or_else(|| layer.get("entries"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default() as usize;
            let idx = layer
                .get("layer")
                .or_else(|| layer.get("name"))
                .or_else(|| layer.get("id"))
                .and_then(serde_json::Value::as_str)
                .and_then(memory_layer_index_from_str)
                .unwrap_or(fallback_idx);
            if idx < counts.len() {
                counts[idx] = count;
            }
        }
    }
    counts
}

fn memory_layer_index_from_str(value: &str) -> Option<usize> {
    let normalized = value.trim().to_ascii_uppercase();
    let mut chars = normalized.chars();
    while let Some(ch) = chars.next() {
        if ch == 'L' {
            if let Some(digit) = chars.next().and_then(|next| next.to_digit(10)) {
                let idx = digit as usize;
                if idx < 5 {
                    return Some(idx);
                }
            }
        }
    }
    None
}

fn task_summary_from_json(value: &serde_json::Value) -> Option<TaskSummary> {
    let id = value.get("id").and_then(serde_json::Value::as_str)?;
    let objective = value
        .get("objective")
        .or_else(|| value.get("title"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let status = value
        .get("status")
        .or_else(|| value.get("phase"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let current_phase = value
        .get("current_phase")
        .or_else(|| value.get("currentPhase"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let yolo_mode = value
        .get("yolo_mode")
        .or_else(|| value.get("yoloMode"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let failure_count = value
        .get("failure_count")
        .or_else(|| value.get("failureCount"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let review_result = value
        .get("review_result")
        .or_else(|| value.get("reviewResult"))
        .or_else(|| value.get("review"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let artifact_count = value
        .get("artifact_count")
        .or_else(|| value.get("artifactCount"))
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            value
                .get("artifacts")
                .and_then(serde_json::Value::as_array)
                .map(|items| items.len() as u64)
        })
        .unwrap_or_default();
    let blocker_reason = value
        .get("blocker_reason")
        .or_else(|| value.get("blockerReason"))
        .or_else(|| value.get("blocker"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    Some(TaskSummary {
        id: id.to_string(),
        objective,
        status,
        current_phase,
        yolo_mode,
        failure_count,
        review_result,
        artifact_count,
        blocker_reason,
    })
}

fn approval_summary_from_json(value: &serde_json::Value) -> Option<ApprovalSummary> {
    let id = value
        .get("id")
        .or_else(|| value.get("approval_id"))
        .or_else(|| value.get("approvalId"))
        .and_then(serde_json::Value::as_str)?;
    let tool_name = value
        .get("tool_name")
        .or_else(|| value.get("toolName"))
        .or_else(|| value.get("tool"))
        .or_else(|| value.get("capability"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let risk = value
        .get("risk")
        .or_else(|| value.get("risk_level"))
        .or_else(|| value.get("riskLevel"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let requester = value
        .get("requester")
        .or_else(|| value.get("session_id"))
        .or_else(|| value.get("sessionId"))
        .or_else(|| value.get("source"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let input_preview = value
        .get("input_preview")
        .or_else(|| value.get("inputPreview"))
        .or_else(|| value.get("preview"))
        .or_else(|| value.get("command"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(ApprovalSummary {
        id: id.to_string(),
        tool_name,
        risk,
        requester,
        input_preview,
    })
}

fn structured_count(value: &serde_json::Value) -> u64 {
    value
        .get("count")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            value
                .get("items")
                .and_then(serde_json::Value::as_array)
                .map(|items| items.len() as u64)
        })
        .unwrap_or_default()
}

fn structured_samples(value: &serde_json::Value, keys: &[&str]) -> Vec<String> {
    value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    keys.iter()
                        .filter_map(|key| item.get(*key).and_then(serde_json::Value::as_str))
                        .find(|sample| !sample.trim().is_empty())
                        .map(ToOwned::to_owned)
                })
                .take(4)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn connector_account_from_json(value: &serde_json::Value) -> Option<ConnectorAccountSummary> {
    let provider = value.get("provider").and_then(serde_json::Value::as_str)?;
    let account_id = value
        .get("account_id")
        .or_else(|| value.get("accountId"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(provider);
    let health = value.get("health").unwrap_or(value);
    Some(ConnectorAccountSummary {
        provider: provider.to_string(),
        account_id: account_id.to_string(),
        auth_mode: value
            .get("auth_mode")
            .or_else(|| value.get("authMode"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        status: health
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        reason: health
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        binding_count: value
            .get("enabled_bindings")
            .or_else(|| value.get("enabledBindings"))
            .and_then(serde_json::Value::as_array)
            .map(|items| items.len() as u64)
            .unwrap_or_default(),
    })
}

fn connector_capability_from_json(value: &serde_json::Value) -> Option<ConnectorCapabilitySummary> {
    let capability_id = value
        .get("capability_id")
        .or_else(|| value.get("capabilityId"))
        .and_then(serde_json::Value::as_str)?;
    Some(ConnectorCapabilitySummary {
        capability_id: capability_id.to_string(),
        provider: value
            .get("provider")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        plane: value
            .get("plane")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        risk: value
            .get("risk")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        supports_commit: value
            .get("supports_commit")
            .or_else(|| value.get("supportsCommit"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        requires_approval: value
            .get("requires_approval")
            .or_else(|| value.get("requiresApproval"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    })
}

fn connector_resource_from_json(value: &serde_json::Value) -> Option<ConnectorResourceSummary> {
    let reference = value.get("reference").and_then(serde_json::Value::as_str)?;
    Some(ConnectorResourceSummary {
        reference: reference.to_string(),
        provider: value
            .get("provider")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        resource_type: value
            .get("resource_type")
            .or_else(|| value.get("resourceType"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("resource")
            .to_string(),
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(reference)
            .to_string(),
        indexed_state: value
            .get("indexed_state")
            .or_else(|| value.get("indexedState"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
    })
}

fn surface_summary_from_json(value: &serde_json::Value) -> Option<SurfaceSummary> {
    let id = value.get("id").and_then(serde_json::Value::as_str)?;
    let capabilities = value
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
        .unwrap_or_default();
    let routes = value
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
        .unwrap_or_default();
    let resources = value
        .get("resources")
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
        .unwrap_or_default();
    Some(SurfaceSummary {
        id: id.to_string(),
        name: value
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(id)
            .to_string(),
        kind: value
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        status: value
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        lifecycle: value
            .get("lifecycle")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        transport: value
            .get("transport")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("stdio-jsonl")
            .to_string(),
        capability_count: capabilities,
        route_count: routes,
        resource_count: resources,
        entry: value
            .get("entry")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        diagnostics: value
            .get("diagnostics")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .take(3)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    })
}

fn surface_event_summary_from_json(
    fallback_surface: &str,
    value: &serde_json::Value,
) -> Option<SurfaceEventSummary> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .and_then(serde_json::Value::as_str)?;
    let surface = value
        .get("surface")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(fallback_surface);
    let detail = value
        .get("message")
        .or_else(|| value.get("code"))
        .or_else(|| value.get("payload"))
        .map(|item| match item.as_str() {
            Some(text) => text.to_string(),
            None => truncate_json(item),
        })
        .unwrap_or_default();
    Some(SurfaceEventSummary {
        surface: surface.to_string(),
        event_type: event_type.to_string(),
        detail,
    })
}

fn truncate_json(value: &serde_json::Value) -> String {
    let rendered = value.to_string();
    if rendered.chars().count() <= 96 {
        rendered
    } else {
        format!("{}...", rendered.chars().take(93).collect::<String>())
    }
}

pub async fn refresh_runtime_control_snapshot(
    gateway_client: Option<&GatewayApiClient>,
    session_id: Option<&str>,
) -> RuntimeControlSnapshot {
    let Some(projection) = gateway_client else {
        let mut snapshot = RuntimeControlSnapshot::default();
        snapshot.degrade("Gateway API unavailable");
        return snapshot;
    };

    let mut snapshot = match projection.runtime_snapshot().await {
        Ok(value) => RuntimeControlSnapshot::from_gateway_snapshot(&value),
        Err(err) => {
            let mut snapshot = RuntimeControlSnapshot::default();
            snapshot.degrade(format!("runtime host snapshot unavailable: {err}"));
            snapshot
        }
    };

    if snapshot.session_ids.is_empty() {
        match projection.list_sessions().await {
            Ok(value) => snapshot.ingest_session_list(&value),
            Err(err) => snapshot.degrade(format!("session list unavailable: {err}")),
        }
    }

    match projection.runtime_control_plane().await {
        Ok(value) => snapshot.ingest_runtime_control_plane(&value),
        Err(err) => snapshot.degrade(format!("Gateway API unavailable: {err}")),
    }
    match projection.task_status().await {
        Ok(value) => snapshot.ingest_task_status(&value),
        Err(err) => snapshot.degrade(format!("task Gateway API unavailable: {err}")),
    }
    match projection.pending_approvals().await {
        Ok(value) => snapshot.ingest_pending_approvals(&value),
        Err(err) => snapshot.degrade(format!("approval Gateway API unavailable: {err}")),
    }
    match projection.memory_status().await {
        Ok(value) => snapshot.ingest_memory_status(&value),
        Err(err) => snapshot.degrade(format!("memory Gateway API unavailable: {err}")),
    }
    let (capabilities, projection_state, surfaces, release_gate) = tokio::join!(
        projection.cowd_capabilities(),
        projection.cowd_projection("tui"),
        projection.cowd_surfaces(),
        projection.cowd_release_gate()
    );
    match (capabilities, projection_state, surfaces, release_gate) {
        (Ok(capabilities), Ok(projection_state), Ok(surfaces), Ok(release_gate)) => snapshot
            .ingest_cowd_projection_state(
                &capabilities,
                &projection_state,
                &surfaces,
                &release_gate,
            ),
        (capabilities, projection_state, surfaces, release_gate) => {
            let mut reasons = Vec::new();
            if let Err(err) = capabilities {
                reasons.push(format!("capabilities: {err}"));
            }
            if let Err(err) = projection_state {
                reasons.push(format!("projection: {err}"));
            }
            if let Err(err) = surfaces {
                reasons.push(format!("surfaces: {err}"));
            }
            if let Err(err) = release_gate {
                reasons.push(format!("release gate: {err}"));
            }
            snapshot.degrade(format!(
                "cowd kernel projection unavailable: {}",
                reasons.join("; ")
            ));
        }
    }
    let (sources, facts, evidence, watermarks) = tokio::join!(
        projection.structured_sources(),
        projection.structured_facts(),
        projection.structured_evidence(),
        projection.structured_watermarks()
    );
    match (sources, facts, evidence, watermarks) {
        (Ok(sources), Ok(facts), Ok(evidence), Ok(watermarks)) => {
            snapshot.ingest_structured_data(&sources, &facts, &evidence, &watermarks);
        }
        (sources, facts, evidence, watermarks) => {
            let mut reasons = Vec::new();
            if let Err(err) = sources {
                reasons.push(format!("sources: {err}"));
            }
            if let Err(err) = facts {
                reasons.push(format!("facts: {err}"));
            }
            if let Err(err) = evidence {
                reasons.push(format!("evidence: {err}"));
            }
            if let Err(err) = watermarks {
                reasons.push(format!("watermarks: {err}"));
            }
            snapshot.degrade(format!(
                "structured data projection unavailable: {}",
                reasons.join("; ")
            ));
        }
    }
    match projection.cross_plane_summary().await {
        Ok(value) => snapshot.ingest_cross_plane_summary(&value),
        Err(err) => snapshot.degrade(format!("cross-plane projection unavailable: {err}")),
    }
    match projection.connector_accounts().await {
        Ok(value) => snapshot.ingest_connector_accounts(&value),
        Err(err) => snapshot.degrade(format!("connector accounts unavailable: {err}")),
    }
    match projection.connector_capabilities().await {
        Ok(value) => snapshot.ingest_connector_capabilities(&value),
        Err(err) => snapshot.degrade(format!("connector capabilities unavailable: {err}")),
    }
    match projection.connector_resources(None, 20, 0).await {
        Ok(value) => snapshot.ingest_connector_resources(&value),
        Err(err) => snapshot.degrade(format!("connector resources unavailable: {err}")),
    }
    match projection.surface_registry().await {
        Ok(value) => snapshot.ingest_surface_registry(&value),
        Err(err) => snapshot.degrade(format!("surface registry unavailable: {err}")),
    }
    match projection.surface_health_summary().await {
        Ok(value) => snapshot.ingest_surface_health(&value),
        Err(err) => snapshot.degrade(format!("surface health unavailable: {err}")),
    }
    let surface_ids = snapshot
        .surfaces
        .iter()
        .map(|surface| surface.id.clone())
        .take(6)
        .collect::<Vec<_>>();
    snapshot.begin_surface_event_refresh();
    for surface_id in surface_ids {
        match projection.surface_events(&surface_id).await {
            Ok(value) => snapshot.ingest_surface_events(&surface_id, &value),
            Err(err) => {
                snapshot.degrade(format!("surface `{surface_id}` events unavailable: {err}"))
            }
        }
    }

    if let Some(session_id) = session_id {
        match projection.current_context(Some(session_id)).await {
            Ok(value) => {
                if value
                    .get("degraded")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
                {
                    let reason = value
                        .get("degraded_reason")
                        .and_then(|value| value.as_str())
                        .unwrap_or("context degraded");
                    snapshot.degrade(format!("context degraded: {reason}"));
                }
            }
            Err(err) => snapshot.degrade(format!("context Gateway API unavailable: {err}")),
        }
    }

    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gateway_snapshot() -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "protocol_version": 1,
            "runtime_host": "gateway-runtime-host",
            "active_sessions": 2,
            "uptime_secs": 9,
            "sessions": ["s1", "s2"],
            "leases": {"total": 0, "items": []},
        })
    }

    #[test]
    fn snapshot_extracts_projection_summaries() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_session_ids(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        snapshot.ingest_runtime_control_plane(&serde_json::json!({
            "diagnostics": {
                "readiness_score": 87,
                "component_count": 12
            }
        }));
        snapshot.ingest_task_status(&serde_json::json!({
            "tasks": [
                {
                    "id": "t1",
                    "review_result": "accepted",
                    "artifacts": [{"path": "report.md"}],
                    "blocker_reason": "none"
                },
                {"id": "t2"}
            ]
        }));
        snapshot.ingest_pending_approvals(&serde_json::json!([
            {"id": "a1", "tool_name": "bash", "risk": "high", "preview": "rm -rf /tmp/x"}
        ]));
        snapshot.ingest_memory_status(&serde_json::json!({
            "status": "available"
        }));
        snapshot.ingest_cross_plane_summary(&serde_json::json!({
            "grants": {"active": 4},
            "interop": {"actions_24h": 7}
        }));
        snapshot.ingest_connector_accounts(&serde_json::json!({
            "accounts": [{
                "provider": "mock",
                "account_id": "mock-docs",
                "auth_mode": "none",
                "enabled_bindings": ["service.mock.docs.read"],
                "health": {"status": "ready"}
            }]
        }));
        snapshot.ingest_connector_capabilities(&serde_json::json!({
            "capabilities": [{
                "capability_id": "service.mock.docs.read",
                "provider": "mock",
                "plane": "service",
                "risk": "low",
                "supports_commit": true,
                "requires_approval": false
            }]
        }));
        snapshot.ingest_connector_resources(&serde_json::json!({
            "degraded_reason": "resource directory unavailable",
            "resources": [{
                "reference": "service://mock/docs/ready",
                "provider": "mock",
                "resource_type": "document",
                "title": "Ready Mock Document",
                "indexed_state": "indexed"
            }]
        }));

        assert!(snapshot.gateway_running);
        assert_eq!(snapshot.active_sessions, 3);
        assert_eq!(snapshot.runtime_readiness.as_deref(), Some("87%"));
        assert_eq!(snapshot.runtime_components, Some(12));
        assert_eq!(snapshot.task_count, Some(2));
        assert_eq!(snapshot.tasks.len(), 2);
        assert_eq!(snapshot.tasks[0].id, "t1");
        assert_eq!(snapshot.tasks[0].review_result.as_deref(), Some("accepted"));
        assert_eq!(snapshot.tasks[0].artifact_count, 1);
        assert_eq!(snapshot.tasks[0].blocker_reason.as_deref(), Some("none"));
        assert_eq!(snapshot.pending_approvals, Some(1));
        assert_eq!(snapshot.approval_items.len(), 1);
        assert_eq!(snapshot.approval_items[0].tool_name, "bash");
        assert_eq!(snapshot.memory_status.as_deref(), Some("available"));
        assert_eq!(snapshot.cross_plane_grants_active, Some(4));
        assert_eq!(snapshot.cross_plane_actions_24h, Some(7));
        assert_eq!(snapshot.connector_accounts.len(), 1);
        assert_eq!(snapshot.connector_accounts[0].status, "ready");
        assert_eq!(snapshot.connector_accounts[0].reason.as_deref(), None);
        assert_eq!(snapshot.connector_capabilities.len(), 1);
        assert!(snapshot.connector_capabilities[0].supports_commit);
        assert_eq!(snapshot.connector_resources.len(), 1);
        assert_eq!(snapshot.connector_resources[0].title, "Ready Mock Document");
        assert_eq!(
            snapshot.connector_degraded_reasons[0],
            "resource directory unavailable"
        );
    }

    #[test]
    fn snapshot_extracts_cowd_and_structured_summaries() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_cowd_projection_state(
            &serde_json::json!({
                "capability_count": 9,
                "capabilities": []
            }),
            &serde_json::json!({
                "surface": "tui",
                "capability_count": 8,
                "capabilities": []
            }),
            &serde_json::json!({
                "webui_tui_full_parity": true,
                "cli_is_minimal_control": true
            }),
            &serde_json::json!({
                "status": "fail",
                "checks": [
                    {"check_id": "webui_tui_parity", "status": "pass"},
                    {"check_id": "structured_data", "status": "fail"}
                ]
            }),
        );
        snapshot.ingest_structured_data(
            &serde_json::json!({
                "count": 1,
                "items": [{"source_id": "pack-tui"}]
            }),
            &serde_json::json!({
                "items": [{"fact_id": "fact-tui"}]
            }),
            &serde_json::json!({
                "items": [{"evidence_id": "evidence-tui"}]
            }),
            &serde_json::json!({
                "items": [{"source_ref": "pack-tui", "high_watermark": "2026-06-14T00:00:00Z"}]
            }),
        );

        let kernel = snapshot.cowd_kernel.as_ref().expect("kernel summary");
        assert_eq!(kernel.capability_count, 9);
        assert_eq!(kernel.projection_capability_count, 8);
        assert!(kernel.webui_tui_full_parity);
        assert!(kernel.cli_is_minimal_control);
        assert_eq!(kernel.release_gate_status, "fail");
        assert_eq!(kernel.release_gate_failed_checks, 1);

        let data = snapshot
            .structured_data
            .as_ref()
            .expect("structured summary");
        assert_eq!(data.source_count, 1);
        assert_eq!(data.fact_count, 1);
        assert_eq!(data.evidence_count, 1);
        assert_eq!(data.watermark_count, 1);
        assert_eq!(data.sample_sources, vec!["pack-tui"]);
        assert_eq!(data.sample_facts, vec!["fact-tui"]);
        assert_eq!(data.sample_evidence, vec!["evidence-tui"]);
        assert_eq!(data.sample_watermarks, vec!["pack-tui"]);
    }

    #[test]
    fn snapshot_round_trips_cowd_structured_through_app() {
        let mut app = App::new("claude-sonnet-4-6", "session-cowd-structured");
        let snapshot = RuntimeControlSnapshot {
            cowd_kernel: Some(CowdKernelSummary {
                capability_count: 12,
                projection_capability_count: 12,
                webui_tui_full_parity: true,
                cli_is_minimal_control: true,
                release_gate_status: "pass".to_string(),
                release_gate_failed_checks: 0,
            }),
            structured_data: Some(StructuredDataSummary {
                source_count: 2,
                fact_count: 3,
                evidence_count: 4,
                watermark_count: 1,
                sample_sources: vec!["pack-a".to_string()],
                sample_facts: vec!["fact-a".to_string()],
                sample_evidence: vec!["evidence-a".to_string()],
                sample_watermarks: vec!["pack-a".to_string()],
            }),
            ..RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot())
        };

        snapshot.apply_to_app(&mut app);
        assert_eq!(
            app.gateway_cowd_kernel
                .as_ref()
                .map(|kernel| kernel.release_gate_status.as_str()),
            Some("pass")
        );
        assert_eq!(
            app.gateway_structured_data
                .as_ref()
                .map(|data| data.fact_count),
            Some(3)
        );

        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(restored.cowd_kernel, snapshot.cowd_kernel);
        assert_eq!(restored.structured_data, snapshot.structured_data);
    }

    #[test]
    fn snapshot_reads_gateway_runtime_snapshot() {
        let snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "protocol_version": 1,
            "runtime_host": "gateway-runtime-host",
            "active_sessions": 2,
            "uptime_secs": 42,
            "sessions": ["s1", "s2"],
            "leases": {
                "total": 1,
                "items": [{
                    "session_id": "s1",
                    "owner": "tui:fast",
                    "mode": "collaborative"
                }]
            }
        }));

        assert!(snapshot.gateway_running);
        assert_eq!(snapshot.active_sessions, 2);
        assert_eq!(snapshot.uptime_secs, Some(42));
        assert_eq!(snapshot.session_ids, vec!["s1", "s2"]);
        assert_eq!(snapshot.lease_owner.as_deref(), Some("tui:fast"));
        assert_eq!(snapshot.lease_mode.as_deref(), Some("collaborative"));
    }

    #[test]
    fn snapshot_tracks_partial_degradation() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.degrade("task projection unavailable");
        snapshot.degrade("memory projection unavailable");

        assert!(snapshot.gateway_running);
        assert_eq!(snapshot.degraded_reasons.len(), 2);
        assert!(snapshot
            .degraded_reasons
            .iter()
            .any(|reason| reason.contains("task")));
    }

    #[test]
    fn surface_event_refresh_replaces_previous_batch() {
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());

        snapshot.begin_surface_event_refresh();
        snapshot.ingest_surface_events(
            "webui",
            &serde_json::json!({
                "events": [{
                    "type": "surface.message.sent",
                    "surface": "webui",
                    "message": "first"
                }]
            }),
        );
        assert_eq!(snapshot.surface_events.len(), 1);
        assert_eq!(snapshot.surface_events[0].detail, "first");

        snapshot.begin_surface_event_refresh();
        snapshot.ingest_surface_events(
            "webui",
            &serde_json::json!({
                "events": [{
                    "type": "surface.message.sent",
                    "surface": "webui",
                    "message": "second"
                }]
            }),
        );

        assert_eq!(snapshot.surface_events.len(), 1);
        assert_eq!(snapshot.surface_events[0].detail, "second");
    }

    #[test]
    fn snapshot_round_trips_memory_status_through_app() {
        let mut app = App::new("claude-sonnet-4-6", "session-memory-status");
        let mut snapshot = RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot());
        snapshot.ingest_memory_status(&serde_json::json!({
            "status": "available",
            "total_entries": 42,
            "vector_count": 17,
            "layers": [
                {"layer": "L0", "entry_count": 1},
                {"layer": "L1", "entry_count": 2},
                {"layer": "L2", "entry_count": 3},
                {"layer": "L3", "entry_count": 4},
                {"layer": "L4", "entry_count": 5}
            ]
        }));

        snapshot.apply_to_app(&mut app);
        assert_eq!(app.memory_status.as_deref(), Some("available"));
        assert_eq!(app.memory_total_entries, Some(42));
        assert_eq!(app.memory_vector_count, Some(17));
        assert_eq!(app.memory_layer_counts, [1, 2, 3, 4, 5]);

        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(restored.memory_status.as_deref(), Some("available"));
        assert_eq!(restored.memory_total_entries, Some(42));
        assert_eq!(restored.memory_vector_count, Some(17));
        assert_eq!(restored.memory_layer_counts, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn snapshot_round_trips_action_receipts_through_app() {
        let mut app = App::new("claude-sonnet-4-6", "session-action-receipt");
        let snapshot = RuntimeControlSnapshot {
            action_receipts: vec![RuntimeActionReceiptSummary {
                status: "ok".to_string(),
                dispatch_status: "completed".to_string(),
                mode: "daemon-control".to_string(),
                capability: "daemon.task.complete".to_string(),
                idempotency_key: Some("task-1".to_string()),
            }],
            ..RuntimeControlSnapshot::from_gateway_snapshot(&gateway_snapshot())
        };

        snapshot.apply_to_app(&mut app);
        assert_eq!(app.gateway_action_receipts.len(), 1);

        let restored = RuntimeControlSnapshot::from_app(&app);
        assert_eq!(restored.action_receipts.len(), 1);
        assert_eq!(
            restored.action_receipts[0].capability,
            "daemon.task.complete"
        );
    }

    #[test]
    fn local_store_applies_runtime_mutations_and_receipt_limits() {
        let mut app = App::new("claude-sonnet-4-6", "session-local-store");
        app.gateway_approval_items = vec![ApprovalSummary {
            id: "approval-1".to_string(),
            tool_name: "bash".to_string(),
            risk: Some("high".to_string()),
            requester: Some("session".to_string()),
            input_preview: "run command".to_string(),
        }];
        app.gateway_pending_approvals = Some(1);
        app.gateway_tasks = vec![TaskSummary {
            id: "task-1".to_string(),
            objective: "finish task".to_string(),
            status: "blocked".to_string(),
            current_phase: None,
            yolo_mode: false,
            failure_count: 0,
            review_result: None,
            artifact_count: 0,
            blocker_reason: Some("waiting".to_string()),
        }];
        app.gateway_task_count = Some(1);
        app.gateway_connector_resources = vec![ConnectorResourceSummary {
            reference: "service://mock.docs/document/1".to_string(),
            provider: "mock.docs".to_string(),
            resource_type: "document".to_string(),
            title: "Doc".to_string(),
            indexed_state: "indexed".to_string(),
        }];

        let mut store = RuntimeControlLocalStore::from_app(&app);
        store.apply_approval_response("approval-1");
        store.apply_task_status("task-1", "completed");
        store.apply_connector_resource_state("service://mock.docs/document/1", "stale");
        store.push_action_receipt(
            "failed",
            &"x".repeat(100),
            "daemon-control",
            "connector.resource.revalidate",
            Some("service://mock.docs/document/1".to_string()),
        );
        store.apply_to_app(&mut app);

        assert_eq!(app.gateway_pending_approvals, Some(0));
        assert!(app.gateway_approval_items.is_empty());
        assert_eq!(app.gateway_tasks[0].status, "completed");
        assert_eq!(app.gateway_tasks[0].blocker_reason, None);
        assert_eq!(app.gateway_connector_resources[0].indexed_state, "stale");
        assert_eq!(app.gateway_action_receipts.len(), 1);
        assert_eq!(
            app.gateway_action_receipts[0]
                .dispatch_status
                .chars()
                .count(),
            83
        );
        assert!(app.gateway_action_receipts[0]
            .dispatch_status
            .ends_with("..."));
    }
}
