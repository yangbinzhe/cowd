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
    pub source_kind: Option<String>,
    pub resource_ref: Option<String>,
    pub review_ref: Option<String>,
}

impl ApprovalSummary {
    #[must_use]
    pub fn application_source_id(&self) -> Option<&str> {
        self.source_kind
            .as_deref()
            .filter(|source| !source.trim().is_empty())
    }

    #[must_use]
    pub fn has_application_review(&self) -> bool {
        self.application_source_id().is_some()
            && self
                .review_ref
                .as_deref()
                .is_some_and(|review| !review.trim().is_empty())
    }
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
    pub active: bool,
    pub pid: Option<u64>,
    pub consecutive_failures: u64,
    pub restart_count: u64,
    pub circuit_open: bool,
    pub next_retry_at: Option<String>,
    pub last_error: Option<String>,
    pub entry: Option<String>,
    pub diagnostics: Vec<String>,
}

impl SurfaceSummary {
    #[must_use]
    pub fn is_external(&self) -> bool {
        match self.lifecycle.as_str() {
            "managed" | "one-shot" => true,
            "builtin" => false,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceHealthSummary {
    pub status: String,
    pub surface_count: u64,
    pub external_surface_count: u64,
    pub route_count: u64,
    pub resource_count: u64,
    pub ready_count: u64,
    pub degraded_count: u64,
    pub failed_count: u64,
    pub circuit_open_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceEventSummary {
    pub surface: String,
    pub event_type: String,
    pub detail: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageConnectorSummary {
    pub connector: String,
    pub name: String,
    pub configuration_status: String,
    pub runtime_status: String,
    pub enabled: bool,
    pub configured: bool,
    pub capability_count: u64,
    pub missing_required_count: u64,
    pub consecutive_failures: u64,
    pub restart_count: u64,
    pub circuit_open: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageEndpointSummary {
    pub endpoint_id: String,
    pub connector: String,
    pub kind: String,
    pub status: String,
    pub configured: bool,
    pub capability_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageRouteSummary {
    pub route_id: String,
    pub connector: String,
    pub policy: String,
    pub status: String,
    pub configured: bool,
    pub capability_count: u64,
    pub runtime_status: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageBindingSummary {
    pub binding_id: String,
    pub connector: String,
    pub endpoint: String,
    pub direction: String,
    pub status: String,
    pub runtime_session_id: Option<String>,
    pub resource_count: u64,
    pub last_seen_at_ms: Option<u64>,
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
pub struct GatewayCapabilityRouteSummary {
    pub id: String,
    pub domain: String,
    pub title: String,
    pub method: String,
    pub path: String,
    pub risk: String,
    pub criticality: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayOpenAiToolSummary {
    pub name: String,
    pub description: String,
    pub parameter_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatewayCapabilityContractSummary {
    pub kind: String,
    pub schema_version: u64,
    pub owner: String,
    pub route_count: u64,
    pub capability_count: u64,
    pub p1_count: u64,
    pub ai_visible_count: u64,
    pub openapi_path_count: u64,
    pub openai_tool_count: u64,
    pub route_contract_parity: bool,
    pub sample_routes: Vec<GatewayCapabilityRouteSummary>,
    pub sample_tools: Vec<GatewayOpenAiToolSummary>,
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
pub struct RealityCoreSummary {
    pub status: String,
    pub fact_status: String,
    pub memory_status: String,
    pub matrix_status: String,
    pub matrix_context_status: String,
    pub growth_status: String,
    pub context_status: String,
    pub audit_status: String,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FactFlowSummary {
    pub source: String,
    pub session_id: Option<String>,
    pub stage_count: u64,
    pub event_count: u64,
    pub promotion_count: u64,
    pub boundary_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MissionSessionSummary {
    pub session_id: String,
    pub title: String,
    pub status: String,
    pub team_count: u64,
    pub agent_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MissionControlSummary {
    pub mission_id: Option<String>,
    pub active_session_id: Option<String>,
    pub session_count: u64,
    pub active_count: u64,
    pub background_count: u64,
    pub paused_count: u64,
    pub closed_count: u64,
    pub team_count: u64,
    pub agent_count: u64,
    pub pending_approvals: u64,
    pub relation_count: u64,
    pub execution_graph_count: u64,
    pub conflict_count: u64,
    pub evidence_count: u64,
    pub capability_action_count: u64,
    pub event_count: u64,
    pub control_ready_count: u64,
    pub control_blocked_count: u64,
    pub control_requires_approval_count: u64,
    pub sessions: Vec<MissionSessionSummary>,
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
    pub memory_context_envelope_status: Option<String>,
    pub memory_context_envelope_compression: Option<String>,
    pub memory_context_envelope_used_ratio: Option<u64>,
    pub memory_context_envelope_checkpoint: Option<String>,
    pub cross_plane_grants_active: Option<u64>,
    pub cross_plane_actions_24h: Option<u64>,
    pub connector_accounts: Vec<ConnectorAccountSummary>,
    pub connector_capabilities: Vec<ConnectorCapabilitySummary>,
    pub connector_resources: Vec<ConnectorResourceSummary>,
    pub action_receipts: Vec<RuntimeActionReceiptSummary>,
    pub surfaces: Vec<SurfaceSummary>,
    pub surface_health: Option<SurfaceHealthSummary>,
    pub surface_events: Vec<SurfaceEventSummary>,
    pub message_connectors: Vec<MessageConnectorSummary>,
    pub message_endpoints: Vec<MessageEndpointSummary>,
    pub message_routes: Vec<MessageRouteSummary>,
    pub message_bindings: Vec<MessageBindingSummary>,
    pub cowd_kernel: Option<CowdKernelSummary>,
    pub gateway_capability_contract: Option<GatewayCapabilityContractSummary>,
    pub structured_data: Option<StructuredDataSummary>,
    pub reality_core: Option<RealityCoreSummary>,
    pub fact_flow: Option<FactFlowSummary>,
    pub mission_control: Option<MissionControlSummary>,
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
            memory_context_envelope_status: app.memory_context_envelope_status.clone(),
            memory_context_envelope_compression: app.memory_context_envelope_compression.clone(),
            memory_context_envelope_used_ratio: app.memory_context_envelope_used_ratio,
            memory_context_envelope_checkpoint: app.memory_context_envelope_checkpoint.clone(),
            cross_plane_grants_active: app.gateway_cross_plane_grants_active,
            cross_plane_actions_24h: app.gateway_cross_plane_actions_24h,
            connector_accounts: app.gateway_connector_accounts.clone(),
            connector_capabilities: app.gateway_connector_capabilities.clone(),
            connector_resources: app.gateway_connector_resources.clone(),
            action_receipts: app.gateway_action_receipts.clone(),
            surfaces: app.gateway_surfaces.clone(),
            surface_health: app.gateway_surface_health.clone(),
            surface_events: app.gateway_surface_events.clone(),
            message_connectors: app.gateway_message_connectors.clone(),
            message_endpoints: app.gateway_message_endpoints.clone(),
            message_routes: app.gateway_message_routes.clone(),
            message_bindings: app.gateway_message_bindings.clone(),
            cowd_kernel: app.gateway_cowd_kernel.clone(),
            gateway_capability_contract: app.gateway_capability_contract.clone(),
            structured_data: app.gateway_structured_data.clone(),
            reality_core: app.gateway_reality_core.clone(),
            fact_flow: app.gateway_fact_flow.clone(),
            mission_control: app.gateway_mission_control.clone(),
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
        app.memory_context_envelope_status = self.memory_context_envelope_status.clone();
        app.memory_context_envelope_compression = self.memory_context_envelope_compression.clone();
        app.memory_context_envelope_used_ratio = self.memory_context_envelope_used_ratio;
        app.memory_context_envelope_checkpoint = self.memory_context_envelope_checkpoint.clone();
        app.gateway_cross_plane_grants_active = self.cross_plane_grants_active;
        app.gateway_cross_plane_actions_24h = self.cross_plane_actions_24h;
        app.gateway_connector_accounts = self.connector_accounts.clone();
        app.gateway_connector_capabilities = self.connector_capabilities.clone();
        app.gateway_connector_resources = self.connector_resources.clone();
        app.gateway_action_receipts = self.action_receipts.clone();
        app.gateway_surfaces = self.surfaces.clone();
        app.gateway_surface_health = self.surface_health.clone();
        app.gateway_surface_events = self.surface_events.clone();
        app.gateway_message_connectors = self.message_connectors.clone();
        app.gateway_message_endpoints = self.message_endpoints.clone();
        app.gateway_message_routes = self.message_routes.clone();
        app.gateway_message_bindings = self.message_bindings.clone();
        app.gateway_cowd_kernel = self.cowd_kernel.clone();
        app.gateway_capability_contract = self.gateway_capability_contract.clone();
        app.gateway_structured_data = self.structured_data.clone();
        app.gateway_reality_core = self.reality_core.clone();
        app.gateway_fact_flow = self.fact_flow.clone();
        app.gateway_mission_control = self.mission_control.clone();
        app.gateway_connector_degraded_reasons = self.connector_degraded_reasons.clone();
        app.gateway_degraded_reasons = self.degraded_reasons.clone();
        // The account-wide Runtime snapshot can contain leases belonging to
        // other observers. Once this Surface has an explicit lease outcome,
        // that local admission result is authoritative and must not be
        // overwritten by the first item in the global lease list.
        if app.gateway_lease_owner.is_none() && app.gateway_lease_mode.is_none() {
            app.gateway_lease_owner = self.lease_owner.clone();
            app.gateway_lease_mode = self.lease_mode.clone();
        }
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
        let readiness = value
            .pointer("/readiness/score")
            .or_else(|| value.pointer("/diagnostics/readiness_score"))
            .and_then(serde_json::Value::as_u64)
            .map_or_else(|| "unknown".to_string(), |score| format!("{score}%"));
        self.runtime_readiness = value
            .pointer("/components/capacity/data")
            .map(|data| {
                let active = data
                    .get("active")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let capacity = data
                    .get("capacity")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let queued = data
                    .get("queued")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let p95 = data
                    .pointer("/run/p95_ms")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                format!("{readiness} · cap {active}/{capacity} q{queued} p95 {p95}ms")
            })
            .or(Some(readiness));
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
        let envelope = value
            .get("context_envelope_projection")
            .or_else(|| value.pointer("/memory/context_envelope_projection"));
        self.memory_context_envelope_status = envelope
            .and_then(|item| item.get("status"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.memory_context_envelope_compression = envelope
            .and_then(|item| item.get("compression_status"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        self.memory_context_envelope_used_ratio = envelope
            .and_then(|item| item.get("used_ratio"))
            .and_then(serde_json::Value::as_f64)
            .map(|ratio| (ratio * 100.0).round().clamp(0.0, 100.0) as u64);
        self.memory_context_envelope_checkpoint = envelope
            .and_then(|item| item.get("latest_checkpoint_id"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned);
        if value
            .pointer("/kernel_health/degraded")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            let reasons = value
                .pointer("/kernel_health/degraded_reasons")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .filter(|reasons| !reasons.is_empty())
                .unwrap_or_else(|| "unspecified kernel degradation".to_string());
            self.degrade(format!("memory kernel degraded: {reasons}"));
        }
        if let Some(error) = value
            .pointer("/kernel_health/background_extraction/last_error")
            .and_then(serde_json::Value::as_str)
        {
            self.degrade(format!("memory background extraction failed: {error}"));
        }
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

    pub fn ingest_gateway_capability_contract(
        &mut self,
        contract: &serde_json::Value,
        openai_tools: &serde_json::Value,
    ) {
        let coverage = contract.get("coverage").unwrap_or(&serde_json::Value::Null);
        let tools = openai_tools
            .get("tools")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let sample_tools = tools
            .iter()
            .filter_map(gateway_openai_tool_summary)
            .take(8)
            .collect::<Vec<_>>();
        let sample_routes = contract
            .get("capabilities")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter(|item| {
                        item.pointer("/surface_visibility/tui")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false)
                    })
                    .filter_map(gateway_capability_route_summary)
                    .take(14)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let contract_tool_count = coverage
            .get("openai_tool_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        let actual_tool_count = openai_tools
            .get("tool_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(tools.len() as u64);
        if contract_tool_count != 0 && actual_tool_count != contract_tool_count {
            self.degrade(format!(
                "gateway openai tools count mismatch: contract={contract_tool_count}, tools={actual_tool_count}"
            ));
        }
        self.gateway_capability_contract = Some(GatewayCapabilityContractSummary {
            kind: contract
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("gateway.capability_contract")
                .to_string(),
            schema_version: contract
                .get("schema_version")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            owner: contract
                .get("owner")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("gateway")
                .to_string(),
            route_count: coverage
                .get("route_count")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| {
                    contract
                        .get("route_count")
                        .and_then(serde_json::Value::as_u64)
                })
                .unwrap_or_default(),
            capability_count: coverage
                .get("capability_count")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| {
                    contract
                        .get("capability_count")
                        .and_then(serde_json::Value::as_u64)
                })
                .unwrap_or_default(),
            p1_count: coverage
                .get("p1_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            ai_visible_count: coverage
                .get("ai_visible_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            openapi_path_count: coverage
                .get("openapi_path_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            openai_tool_count: actual_tool_count,
            route_contract_parity: coverage
                .get("route_contract_parity")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            sample_routes,
            sample_tools,
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

    pub fn ingest_reality_status(&mut self, value: &serde_json::Value) {
        self.reality_core = Some(RealityCoreSummary {
            status: value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            fact_status: value
                .pointer("/capabilities/fact_runtime/status")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| reality_component_status(value, "fact_kernel")),
            memory_status: reality_component_status(value, "memory"),
            matrix_status: reality_component_status(value, "matrix"),
            matrix_context_status: value
                .pointer("/capabilities/matrix_context_source/status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            growth_status: reality_component_status(value, "growth"),
            context_status: reality_component_status(value, "context"),
            audit_status: reality_component_status(value, "audit"),
            degraded_reasons: value
                .get("degraded_reasons")
                .and_then(serde_json::Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        });
    }

    pub fn ingest_fact_flow(
        &mut self,
        flow: &serde_json::Value,
        boundaries: Option<&serde_json::Value>,
    ) {
        self.fact_flow = Some(FactFlowSummary {
            source: flow
                .get("source")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            session_id: flow
                .get("session_id")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            stage_count: json_array_len(flow, "stages"),
            event_count: json_array_len(flow, "events"),
            promotion_count: json_array_len(flow, "promotions"),
            boundary_count: boundaries
                .map(|value| json_array_len(value, "boundaries"))
                .unwrap_or_default(),
        });
    }

    pub fn ingest_mission_projection(&mut self, value: &serde_json::Value) {
        let projection = value.get("projection").unwrap_or(value);
        let mission = projection.get("mission").unwrap_or(projection);
        let sessions = mission
            .get("sessions")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(mission_session_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut active_count = 0;
        let mut background_count = 0;
        let mut paused_count = 0;
        let mut closed_count = 0;
        let mut team_count = 0;
        let mut agent_count = 0;
        for session in &sessions {
            match session.status.as_str() {
                "active" => active_count += 1,
                "background" => background_count += 1,
                "paused" => paused_count += 1,
                "closed" => closed_count += 1,
                _ => {}
            }
            team_count += session.team_count;
            agent_count += session.agent_count;
        }
        self.mission_control = Some(MissionControlSummary {
            mission_id: mission
                .get("mission_id")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            active_session_id: mission
                .get("active_session_id")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            session_count: sessions.len() as u64,
            active_count,
            background_count,
            paused_count,
            closed_count,
            team_count,
            agent_count,
            pending_approvals: mission
                .pointer("/approval_projection/pending_count")
                .or_else(|| projection.pointer("/approvals/pending_count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            relation_count: mission
                .pointer("/relation_projection/relation_count")
                .or_else(|| projection.pointer("/relations/relation_count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            execution_graph_count: mission
                .pointer("/execution_graph_projection/count")
                .or_else(|| projection.pointer("/execution_graphs/count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            conflict_count: mission
                .pointer("/conflict_projection/count")
                .or_else(|| projection.pointer("/conflicts/count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            evidence_count: mission
                .pointer("/evidence_projection/count")
                .or_else(|| projection.pointer("/evidence/count"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            capability_action_count: mission
                .pointer("/capability_projection/action_contracts")
                .or_else(|| projection.pointer("/capabilities/action_contracts"))
                .and_then(serde_json::Value::as_array)
                .map(|items| items.len() as u64)
                .unwrap_or_default(),
            event_count: mission
                .get("events")
                .and_then(serde_json::Value::as_array)
                .map(|events| events.len() as u64)
                .or_else(|| {
                    projection
                        .pointer("/event_digest/latest")
                        .and_then(serde_json::Value::as_array)
                        .map(|events| events.len() as u64)
                })
                .unwrap_or_default(),
            control_ready_count: projection
                .pointer("/control_readiness/ready_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            control_blocked_count: projection
                .pointer("/control_readiness/blocked_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            control_requires_approval_count: projection
                .pointer("/control_readiness/actions")
                .and_then(serde_json::Value::as_array)
                .map(|actions| {
                    actions
                        .iter()
                        .filter(|action| {
                            action
                                .get("requires_approval")
                                .and_then(serde_json::Value::as_bool)
                                .unwrap_or(false)
                        })
                        .count() as u64
                })
                .unwrap_or_default(),
            sessions,
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
                        .filter(|surface| surface.is_external())
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
            ready_count: host
                .get("ready_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            degraded_count: host
                .get("degraded_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            failed_count: host
                .get("failed_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            circuit_open_count: host
                .get("circuit_open_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
        });
        if let Some(runtime) = value.get("runtime").and_then(serde_json::Value::as_array) {
            for item in runtime {
                let Some(surface_id) = item.get("surface").and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                if let Some(surface) = self
                    .surfaces
                    .iter_mut()
                    .find(|surface| surface.id == surface_id)
                {
                    apply_surface_runtime(surface, item);
                }
            }
        }
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
        if let Some(supervisor_events) = value
            .get("supervisor_events")
            .and_then(serde_json::Value::as_array)
        {
            events.extend(
                supervisor_events
                    .iter()
                    .filter_map(|item| surface_event_summary_from_json(surface, item)),
            );
        }
        self.surface_events.append(&mut events);
        self.surface_events.truncate(24);
    }

    pub fn ingest_message_connectors(&mut self, value: &serde_json::Value) {
        self.message_connectors = value
            .get("connectors")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_connector_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_message_endpoints(&mut self, value: &serde_json::Value) {
        self.message_endpoints = value
            .get("endpoints")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_endpoint_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_message_routes(&mut self, value: &serde_json::Value) {
        self.message_routes = value
            .get("routes")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_route_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }

    pub fn ingest_message_bindings(&mut self, value: &serde_json::Value) {
        self.message_bindings = value
            .get("bindings")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(message_binding_from_json)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
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
        .or_else(|| value.get("action"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let risk = value
        .get("risk")
        .or_else(|| value.get("risk_level"))
        .or_else(|| value.get("riskLevel"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let source = value.get("source");
    let requester = value
        .get("requester")
        .or_else(|| value.get("session_id"))
        .or_else(|| value.get("sessionId"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            source.and_then(|source| {
                [
                    "session_id",
                    "agent_id",
                    "team_id",
                    "mission_id",
                    "resource_ref",
                ]
                .into_iter()
                .find_map(|key| source.get(key).and_then(serde_json::Value::as_str))
                .map(ToOwned::to_owned)
            })
        });
    let input_preview = value
        .get("input_preview")
        .or_else(|| value.get("inputPreview"))
        .or_else(|| value.get("preview"))
        .or_else(|| value.get("command"))
        .or_else(|| value.get("summary"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(ApprovalSummary {
        id: id.to_string(),
        tool_name,
        risk,
        requester,
        input_preview,
        source_kind: source
            .and_then(|source| source.get("kind"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        resource_ref: source
            .and_then(|source| source.get("resource_ref"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        review_ref: source
            .and_then(|source| source.get("review_ref"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
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

fn gateway_capability_route_summary(
    value: &serde_json::Value,
) -> Option<GatewayCapabilityRouteSummary> {
    let http = value.get("http")?;
    Some(GatewayCapabilityRouteSummary {
        id: value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        domain: value
            .get("domain")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("gateway")
            .to_string(),
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        method: http
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("GET")
            .to_string(),
        path: http
            .get("path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        risk: value
            .get("risk")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        criticality: http
            .get("criticality")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("p2")
            .to_string(),
    })
}

fn gateway_openai_tool_summary(value: &serde_json::Value) -> Option<GatewayOpenAiToolSummary> {
    let function = value.get("function")?;
    let parameters = function
        .get("parameters")
        .and_then(|item| item.get("properties"))
        .and_then(serde_json::Value::as_object)
        .map(|properties| properties.len() as u64)
        .unwrap_or_default();
    Some(GatewayOpenAiToolSummary {
        name: function
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string(),
        description: function
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string(),
        parameter_count: parameters,
    })
}

fn reality_component_status(value: &serde_json::Value, component: &str) -> String {
    value
        .get("engines")
        .and_then(|engines| engines.get(component))
        .and_then(|engine| engine.get("status"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            value
                .get(component)
                .and_then(|engine| engine.get("status"))
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("unknown")
        .to_string()
}

fn mission_session_from_json(value: &serde_json::Value) -> Option<MissionSessionSummary> {
    let session_id = value
        .get("session_id")
        .or_else(|| value.get("id"))
        .and_then(serde_json::Value::as_str)?;
    Some(MissionSessionSummary {
        session_id: session_id.to_string(),
        title: value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(session_id)
            .to_string(),
        status: value
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string(),
        team_count: value
            .get("active_team_ids")
            .or_else(|| value.get("team_ids"))
            .and_then(serde_json::Value::as_array)
            .map(|items| items.len() as u64)
            .unwrap_or_default(),
        agent_count: value
            .get("active_agent_ids")
            .or_else(|| value.get("agent_ids"))
            .and_then(serde_json::Value::as_array)
            .map(|items| items.len() as u64)
            .unwrap_or_default(),
    })
}

fn json_array_len(value: &serde_json::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| items.len() as u64)
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
            .unwrap_or("unknown")
            .to_string(),
        capability_count: capabilities,
        route_count: routes,
        resource_count: resources,
        active: false,
        pid: None,
        consecutive_failures: 0,
        restart_count: 0,
        circuit_open: false,
        next_retry_at: None,
        last_error: None,
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

fn apply_surface_runtime(surface: &mut SurfaceSummary, value: &serde_json::Value) {
    if let Some(status) = value.get("status").and_then(serde_json::Value::as_str) {
        surface.status = status.to_string();
    }
    surface.active = value
        .get("active")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(surface.active);
    surface.pid = value.get("pid").and_then(serde_json::Value::as_u64);
    surface.consecutive_failures = value
        .get("consecutive_failures")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    surface.restart_count = value
        .get("restart_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    surface.circuit_open = value
        .get("circuit_open")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_default();
    surface.next_retry_at = value
        .get("next_retry_at")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    surface.last_error = value
        .pointer("/last_error/message")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
}

fn surface_event_summary_from_json(
    fallback_surface: &str,
    value: &serde_json::Value,
) -> Option<SurfaceEventSummary> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("status"))
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

fn message_connector_from_json(value: &serde_json::Value) -> Option<MessageConnectorSummary> {
    let connector = json_string(value, &["connector", "id", "platform_type"])?;
    let runtime = value.get("runtime").unwrap_or(&serde_json::Value::Null);
    Some(MessageConnectorSummary {
        name: json_string(value, &["name"]).unwrap_or_else(|| connector.clone()),
        configuration_status: json_string(value, &["configuration_status", "status"])
            .unwrap_or_else(|| "unknown".to_string()),
        runtime_status: json_string(runtime, &["status"]).unwrap_or_else(|| {
            if runtime.is_null() {
                "not_running".to_string()
            } else {
                "unknown".to_string()
            }
        }),
        enabled: value
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        configured: value
            .get("configured")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        capability_count: json_array_len(value, "capabilities"),
        missing_required_count: json_array_len(value, "missing_required"),
        consecutive_failures: runtime
            .get("consecutive_failures")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        restart_count: runtime
            .get("restart_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        circuit_open: runtime
            .get("circuit_open")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        connector,
    })
}

fn message_endpoint_from_json(value: &serde_json::Value) -> Option<MessageEndpointSummary> {
    let endpoint_id = json_string(value, &["endpoint_id", "id"])?;
    Some(MessageEndpointSummary {
        connector: json_string(value, &["connector"]).unwrap_or_else(|| "unknown".to_string()),
        kind: json_string(value, &["kind"]).unwrap_or_else(|| "unknown".to_string()),
        status: json_string(value, &["status"]).unwrap_or_else(|| "unknown".to_string()),
        configured: value
            .get("configured")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        capability_count: json_array_len(value, "capabilities"),
        endpoint_id,
    })
}

fn message_route_from_json(value: &serde_json::Value) -> Option<MessageRouteSummary> {
    let route_id = json_string(value, &["route_id", "id"])?;
    let runtime = value.get("runtime").unwrap_or(&serde_json::Value::Null);
    Some(MessageRouteSummary {
        connector: json_string(value, &["connector"]).unwrap_or_else(|| "unknown".to_string()),
        policy: json_string(value, &["policy"]).unwrap_or_else(|| "origin".to_string()),
        status: json_string(value, &["status"]).unwrap_or_else(|| "unknown".to_string()),
        configured: value
            .get("configured")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        capability_count: json_array_len(value, "capabilities"),
        runtime_status: json_string(runtime, &["status"]).unwrap_or_else(|| {
            if runtime.is_null() {
                "not_running".to_string()
            } else {
                "unknown".to_string()
            }
        }),
        route_id,
    })
}

fn message_binding_from_json(value: &serde_json::Value) -> Option<MessageBindingSummary> {
    let binding_id = json_string(value, &["binding_id", "id"])?;
    Some(MessageBindingSummary {
        connector: json_string(value, &["connector"]).unwrap_or_else(|| "unknown".to_string()),
        endpoint: json_string(value, &["endpoint"]).unwrap_or_else(|| "-".to_string()),
        direction: json_string(value, &["direction"]).unwrap_or_else(|| "unknown".to_string()),
        status: json_string(value, &["status", "outbound_status"])
            .unwrap_or_else(|| "unknown".to_string()),
        runtime_session_id: json_string(value, &["runtime_session_id", "source_session_id"]),
        resource_count: value
            .get("resource_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        last_seen_at_ms: value
            .get("last_seen_at_ms")
            .and_then(serde_json::Value::as_u64),
        binding_id,
    })
}

fn json_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .find(|item| !item.is_empty())
        .map(ToOwned::to_owned)
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
    match projection.mission_control().await {
        Ok(value) => snapshot.ingest_mission_projection(&value),
        Err(err) => snapshot.degrade(format!("mission control projection unavailable: {err}")),
    }
    match projection.memory_status().await {
        Ok(value) => snapshot.ingest_memory_status(&value),
        Err(err) => snapshot.degrade(format!("memory Gateway API unavailable: {err}")),
    }
    let (reality_status, reality_flow, reality_boundaries) = tokio::join!(
        projection.reality_status(),
        projection.reality_flow(session_id),
        projection.reality_boundaries()
    );
    match (reality_status, reality_flow, reality_boundaries) {
        (Ok(status), Ok(flow), Ok(boundaries)) => {
            snapshot.ingest_reality_status(&status);
            snapshot.ingest_fact_flow(&flow, Some(&boundaries));
        }
        (status, flow, boundaries) => {
            let mut reasons = Vec::new();
            if let Err(err) = status {
                reasons.push(format!("status: {err}"));
            }
            if let Err(err) = flow {
                reasons.push(format!("fact flow: {err}"));
            }
            if let Err(err) = boundaries {
                reasons.push(format!("boundaries: {err}"));
            }
            snapshot.degrade(format!(
                "reality core projection unavailable: {}",
                reasons.join("; ")
            ));
        }
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
    let (gateway_contract, openai_tools) = tokio::join!(
        projection.gateway_capability_contract(),
        projection.gateway_openai_tools()
    );
    match (gateway_contract, openai_tools) {
        (Ok(contract), Ok(tools)) => snapshot.ingest_gateway_capability_contract(&contract, &tools),
        (contract, tools) => {
            let mut reasons = Vec::new();
            if let Err(err) = contract {
                reasons.push(format!("contract: {err}"));
            }
            if let Err(err) = tools {
                reasons.push(format!("openai tools: {err}"));
            }
            snapshot.degrade(format!(
                "gateway capability contract unavailable: {}",
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
    let (message_connectors, message_endpoints, message_routes, message_bindings) = tokio::join!(
        projection.message_connectors(),
        projection.message_endpoints(),
        projection.message_routes(),
        projection.message_bindings()
    );
    match (
        message_connectors,
        message_endpoints,
        message_routes,
        message_bindings,
    ) {
        (Ok(connectors), Ok(endpoints), Ok(routes), Ok(bindings)) => {
            snapshot.ingest_message_connectors(&connectors);
            snapshot.ingest_message_endpoints(&endpoints);
            snapshot.ingest_message_routes(&routes);
            snapshot.ingest_message_bindings(&bindings);
        }
        (connectors, endpoints, routes, bindings) => {
            let mut reasons = Vec::new();
            if let Err(err) = connectors {
                reasons.push(format!("connectors: {err}"));
            }
            if let Err(err) = endpoints {
                reasons.push(format!("endpoints: {err}"));
            }
            if let Err(err) = routes {
                reasons.push(format!("routes: {err}"));
            }
            if let Err(err) = bindings {
                reasons.push(format!("bindings: {err}"));
            }
            snapshot.degrade(format!(
                "message plane projection unavailable: {}",
                reasons.join("; ")
            ));
        }
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

    #[test]
    fn memory_kernel_failure_becomes_a_visible_tui_degraded_reason() {
        let mut snapshot = RuntimeControlSnapshot::default();

        snapshot.ingest_memory_status(&serde_json::json!({
            "status": "degraded",
            "kernel_health": {
                "degraded": true,
                "degraded_reasons": ["BackgroundExtraction"],
                "background_extraction": {
                    "last_error": "provider unavailable"
                }
            }
        }));

        assert!(snapshot
            .degraded_reasons
            .iter()
            .any(|reason| reason.contains("memory kernel degraded")));
        assert!(snapshot
            .degraded_reasons
            .iter()
            .any(|reason| reason.contains("provider unavailable")));
    }

    #[test]
    fn global_snapshot_cannot_overwrite_this_surfaces_read_only_admission() {
        let mut app = App::new("model", "session");
        app.gateway_lease_mode = Some("read-only".to_string());
        let snapshot = RuntimeControlSnapshot {
            lease_owner: Some("another-surface".to_string()),
            lease_mode: Some("exclusive".to_string()),
            ..RuntimeControlSnapshot::default()
        };

        snapshot.apply_to_app(&mut app);

        assert_eq!(app.gateway_lease_owner, None);
        assert_eq!(app.gateway_lease_mode.as_deref(), Some("read-only"));
    }
}
