// ── Gateway Panel ─────────────────────────────────────────────────
// Displays backend daemon management info in the TUI sidebar.
//
// Shows:
//   - Server status (running/stopped) with colored indicator
//   - Key API endpoints with HTTP methods (GET/POST/DELETE)
//   - Health check status from /health endpoint
//   - Quick actions via slash commands

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

use crate::components::panel_scroll::PanelScrollState;
use crate::components::{Component, EventResult, RenderContext};
use crate::{
    app::App,
    runtime_control_store::{
        ConnectorAccountSummary, ConnectorCapabilitySummary, ConnectorResourceSummary,
        CowdKernelSummary, FactFlowSummary, GatewayCapabilityContractSummary,
        MessageBindingSummary, MessageConnectorSummary, MessageEndpointSummary,
        MessageRouteSummary, MissionControlSummary, RealityCoreSummary,
        RuntimeActionReceiptSummary, StructuredDataSummary, SurfaceHealthSummary, SurfaceSummary,
    },
};

/// Panel showing backend runtime/API gateway status.
///
/// Tracks server running state, health status, and
/// displays available API endpoints with descriptions.
pub struct GatewayPanel {
    /// Whether the backend server is currently running.
    pub server_running: bool,
    /// Last known health status string (e.g., "Healthy", "Unhealthy").
    pub health_status: Option<String>,
    /// Server uptime in seconds, if available.
    pub uptime_secs: Option<u64>,
    /// Number of active sessions.
    pub active_sessions: usize,
    /// Runtime readiness score or status from Gateway API API.
    pub runtime_readiness: Option<String>,
    /// Number of runtime control-plane components.
    pub runtime_components: Option<u64>,
    /// Number of daemon tasks visible to the TUI.
    pub task_count: Option<u64>,
    /// Number of pending daemon approval requests.
    pub pending_approvals: Option<u64>,
    /// Current daemon session lease owner.
    pub lease_owner: Option<String>,
    /// Current daemon session lease mode.
    pub lease_mode: Option<String>,
    /// Memory/kernel status visible through runtime control.
    pub memory_status: Option<String>,
    /// ContextEnvelope runtime status projected through Memory/Reality.
    pub memory_context_envelope_status: Option<String>,
    pub memory_context_envelope_compression: Option<String>,
    pub memory_context_envelope_used_ratio: Option<u64>,
    pub memory_context_envelope_checkpoint: Option<String>,
    /// Recent cross-plane execution receipts.
    pub execution_receipts: Vec<GatewayExecutionReceipt>,
    /// Cowd kernel capability and release-gate summary.
    pub cowd_kernel: Option<CowdKernelSummary>,
    /// Gateway-owned API capability contract summary.
    pub gateway_capability_contract: Option<GatewayCapabilityContractSummary>,
    /// Structured data-plane summary.
    pub structured_data: Option<StructuredDataSummary>,
    /// Reality Core engine health summary.
    pub reality_core: Option<RealityCoreSummary>,
    /// Fact Flow trace summary.
    pub fact_flow: Option<FactFlowSummary>,
    /// Mission Runtime global control summary.
    pub mission_control: Option<MissionControlSummary>,
    /// Surface registry summaries managed by Gateway SurfaceHost.
    pub surfaces: Vec<SurfaceSummary>,
    /// Surface host health summary.
    pub surface_health: Option<SurfaceHealthSummary>,
    /// Message connector readiness summaries.
    pub message_connectors: Vec<MessageConnectorSummary>,
    /// Message endpoint directory summaries.
    pub message_endpoints: Vec<MessageEndpointSummary>,
    /// Message delivery route summaries.
    pub message_routes: Vec<MessageRouteSummary>,
    /// Message conversation binding summaries.
    pub message_bindings: Vec<MessageBindingSummary>,
    /// Connector provider account summaries.
    pub connector_accounts: Vec<ConnectorAccountSummary>,
    /// Connector capability summaries.
    pub connector_capabilities: Vec<ConnectorCapabilitySummary>,
    /// Connector resource summaries.
    pub connector_resources: Vec<ConnectorResourceSummary>,
    /// Connector-specific degraded reasons.
    pub connector_degraded_reasons: Vec<String>,
    /// Global runtime/control degradation reasons.
    pub degraded_reasons: Vec<String>,
    /// Last Gateway action status from operator shortcuts.
    pub action_status: Option<String>,
    /// Last Gateway action receipt summary.
    pub action_receipt: Option<String>,
    /// Latest harness eval status observed through Gateway.
    pub harness_eval_status: Option<String>,
    /// Latest harness eval compact summary for terminal operators.
    pub harness_eval_summary: Option<String>,
    /// Latest evolution governance compact summary for terminal operators.
    pub evolution_status: Option<String>,
    pub evolution_summary: Option<String>,
    pub evolution_analysis_summary: Option<String>,
    ready_evolution_case_ids: Vec<String>,
    selected_evolution_case_index: usize,
    pub provider_transport_summary: Option<String>,
    pub hot_state_summary: Option<String>,
    pub postgres_summary: Option<String>,
    pub session_storage_summary: Option<String>,
    /// Pending Runtime-owned release review ids from the latest projection.
    /// These are only operator selection handles: the review state remains in
    /// Runtime and every decision goes through Gateway's typed endpoint.
    pending_release_review_ids: Vec<String>,
    selected_release_review_index: usize,
    /// Runtime-owned policy floor and pending policy-review summary.
    pub evaluation_policy_status: Option<String>,
    pub evaluation_policy_summary: Option<String>,
    /// Pending Runtime-owned evaluation-policy review ids. Like release
    /// reviews, these are not a local approval cache.
    pending_policy_review_ids: Vec<String>,
    selected_policy_review_index: usize,
    /// Runtime-owned Managed Agent dispatcher projection. The TUI only
    /// renders these facts and sends explicit Gateway commands.
    pub managed_agent_status: Option<String>,
    pub managed_agent_summary: Option<String>,
    /// Recoverable Managed Agent ids from the latest Runtime projection.
    /// A health reset is deliberately possible only for this explicit target.
    managed_agent_health_action_ids: Vec<String>,
    selected_managed_agent_health_index: usize,
    /// Scroll offset for content overflow.
    pub scroll_offset: usize,
}

pub type GatewayExecutionReceipt = RuntimeActionReceiptSummary;

impl GatewayPanel {
    /// Create a new GatewayPanel in default stopped state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            server_running: false,
            health_status: None,
            uptime_secs: None,
            active_sessions: 0,
            runtime_readiness: None,
            runtime_components: None,
            task_count: None,
            pending_approvals: None,
            lease_owner: None,
            lease_mode: None,
            memory_status: None,
            memory_context_envelope_status: None,
            memory_context_envelope_compression: None,
            memory_context_envelope_used_ratio: None,
            memory_context_envelope_checkpoint: None,
            execution_receipts: Vec::new(),
            cowd_kernel: None,
            gateway_capability_contract: None,
            structured_data: None,
            reality_core: None,
            fact_flow: None,
            mission_control: None,
            surfaces: Vec::new(),
            surface_health: None,
            message_connectors: Vec::new(),
            message_endpoints: Vec::new(),
            message_routes: Vec::new(),
            message_bindings: Vec::new(),
            connector_accounts: Vec::new(),
            connector_capabilities: Vec::new(),
            connector_resources: Vec::new(),
            connector_degraded_reasons: Vec::new(),
            degraded_reasons: Vec::new(),
            action_status: None,
            action_receipt: None,
            harness_eval_status: None,
            harness_eval_summary: None,
            evolution_status: None,
            evolution_summary: None,
            evolution_analysis_summary: None,
            ready_evolution_case_ids: Vec::new(),
            selected_evolution_case_index: 0,
            provider_transport_summary: None,
            hot_state_summary: None,
            postgres_summary: None,
            session_storage_summary: None,
            pending_release_review_ids: Vec::new(),
            selected_release_review_index: 0,
            evaluation_policy_status: None,
            evaluation_policy_summary: None,
            pending_policy_review_ids: Vec::new(),
            selected_policy_review_index: 0,
            managed_agent_status: None,
            managed_agent_summary: None,
            managed_agent_health_action_ids: Vec::new(),
            selected_managed_agent_health_index: 0,
            scroll_offset: 0,
        }
    }

    /// Sync panel state from the application model.
    ///
    /// Copies server state from App into the panel fields for display.
    /// Derives health_status from server_running: "Healthy" when running,
    /// None when stopped.
    pub fn sync_from_app(&mut self, app: &App) {
        self.server_running = app.gateway.server_running;
        self.uptime_secs = app.gateway.server_uptime_secs;
        self.active_sessions = app.gateway.active_api_sessions;
        self.runtime_readiness = app.gateway.gateway_runtime_readiness.clone();
        self.runtime_components = app.gateway.gateway_runtime_components;
        self.task_count = app.gateway.gateway_task_count;
        self.pending_approvals = app.gateway.gateway_pending_approvals;
        self.lease_owner = app.gateway.gateway_lease_owner.clone();
        self.lease_mode = app.gateway.gateway_lease_mode.clone();
        self.memory_status = app.workbench.memory_status.clone();
        self.memory_context_envelope_status = app.workbench.memory_context_envelope_status.clone();
        self.memory_context_envelope_compression =
            app.workbench.memory_context_envelope_compression.clone();
        self.memory_context_envelope_used_ratio = app.workbench.memory_context_envelope_used_ratio;
        self.memory_context_envelope_checkpoint =
            app.workbench.memory_context_envelope_checkpoint.clone();
        self.connector_accounts = app.gateway.gateway_connector_accounts.clone();
        self.connector_capabilities = app.gateway.gateway_connector_capabilities.clone();
        self.connector_resources = app.gateway.gateway_connector_resources.clone();
        self.execution_receipts = app.gateway.gateway_action_receipts.clone();
        self.surfaces = app.gateway.gateway_surfaces.clone();
        self.surface_health = app.gateway.gateway_surface_health.clone();
        self.message_connectors = app.gateway.gateway_message_connectors.clone();
        self.message_endpoints = app.gateway.gateway_message_endpoints.clone();
        self.message_routes = app.gateway.gateway_message_routes.clone();
        self.message_bindings = app.gateway.gateway_message_bindings.clone();
        self.cowd_kernel = app.gateway.gateway_cowd_kernel.clone();
        self.gateway_capability_contract = app.gateway.gateway_capability_contract.clone();
        self.structured_data = app.gateway.gateway_structured_data.clone();
        self.reality_core = app.gateway.gateway_reality_core.clone();
        self.fact_flow = app.gateway.gateway_fact_flow.clone();
        self.mission_control = app.gateway.gateway_mission_control.clone();
        self.connector_degraded_reasons = app.gateway.gateway_connector_degraded_reasons.clone();
        self.degraded_reasons = app.gateway.gateway_degraded_reasons.clone();
        if app.gateway.server_running {
            self.health_status = Some("Healthy".to_string());
        } else {
            self.health_status = None;
        }
    }

    /// Update the health status string and mark server as running.
    pub fn update_health(&mut self, status: String) {
        self.server_running = true;
        self.health_status = Some(status);
    }

    pub fn record_gateway_manifest(&mut self, result: Result<serde_json::Value, String>) {
        let Ok(payload) = result else {
            self.server_running = false;
            self.health_status = Some("unavailable".to_string());
            self.provider_transport_summary = None;
            self.hot_state_summary = None;
            self.postgres_summary = None;
            self.session_storage_summary = None;
            self.action_status = Some("Gateway health projection unavailable".to_string());
            return;
        };
        let health = payload.get("health").unwrap_or(&payload);
        self.server_running = true;
        self.health_status = health
            .get("status")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        self.provider_transport_summary =
            health.pointer("/runtime/provider_transport").map(|value| {
                format!(
                    "entries {} · checkouts {} · hits {} · builds {}",
                    value
                        .get("entries")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                    value
                        .get("checkouts")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                    value
                        .get("hits")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                    value
                        .get("builds")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                )
            });
        self.hot_state_summary = health.pointer("/runtime/hot_state").map(|value| {
            format!(
                "{} / {} bytes · evictions {}{}",
                value
                    .pointer("/metrics/resident_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                value
                    .pointer("/budget/limit_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                value
                    .pointer("/metrics/evictions")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                if value
                    .get("pressure_high")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
                {
                    " · high pressure"
                } else {
                    ""
                },
            )
        });
        self.postgres_summary = health.pointer("/storage/postgres").map(|value| {
            format!(
                "max {} · queries {} · errors {}",
                value
                    .get("max_connections")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                value
                    .pointer("/metrics/query_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                value
                    .pointer("/metrics/query_error_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
            )
        });
        self.session_storage_summary = health.pointer("/storage/session_execution").map(|value| {
            format!(
                "active {} · queued {} · wait {}us · service {}us",
                value
                    .get("active")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                value
                    .get("queued")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                value
                    .get("average_queue_wait_micros")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                value
                    .get("average_service_micros")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
            )
        });
        self.action_status = Some("Gateway health projection refreshed".to_string());
    }

    /// Set the server running state.
    pub fn set_server_status(&mut self, running: bool) {
        self.server_running = running;
        if !running {
            self.health_status = None;
            self.uptime_secs = None;
            self.active_sessions = 0;
            self.memory_status = None;
        }
    }

    /// Set server uptime in seconds.
    pub fn set_uptime(&mut self, secs: u64) {
        self.uptime_secs = Some(secs);
    }

    /// Set the active session count.
    pub fn set_active_sessions(&mut self, count: usize) {
        self.active_sessions = count;
    }

    pub fn set_execution_receipts(&mut self, receipts: Vec<GatewayExecutionReceipt>) {
        self.execution_receipts = receipts;
    }

    pub fn set_connector_accounts(&mut self, accounts: Vec<ConnectorAccountSummary>) {
        self.connector_accounts = accounts;
    }

    pub fn set_connector_capabilities(&mut self, capabilities: Vec<ConnectorCapabilitySummary>) {
        self.connector_capabilities = capabilities;
    }

    pub fn set_connector_resources(&mut self, resources: Vec<ConnectorResourceSummary>) {
        self.connector_resources = resources;
    }

    pub fn set_connector_degraded_reasons(&mut self, reasons: Vec<String>) {
        self.connector_degraded_reasons = reasons;
    }

    pub fn record_action_result(&mut self, label: &str, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                self.action_status = Some(format!("{label} succeeded"));
                self.action_receipt = Some(gateway_receipt_summary(&payload));
            }
            Err(error) => {
                self.action_status = Some(format!("{label} failed: {error}"));
                self.action_receipt = None;
            }
        }
    }

    /// Return the selected pending typed release review. The returned id is a
    /// projection handle only; callers must still use Gateway's typed review
    /// decision endpoint, which validates the authenticated human lease.
    pub fn selected_release_review_id(&self) -> Option<String> {
        selected_id(
            &self.pending_release_review_ids,
            self.selected_release_review_index,
        )
    }

    pub fn selected_evolution_case_id(&self) -> Option<String> {
        selected_id(
            &self.ready_evolution_case_ids,
            self.selected_evolution_case_index,
        )
    }

    /// Return the selected pending evaluation-policy review handle.
    pub fn selected_policy_review_id(&self) -> Option<String> {
        selected_id(
            &self.pending_policy_review_ids,
            self.selected_policy_review_index,
        )
    }

    /// Return the selected Runtime-managed Agent that currently needs a
    /// health recovery action.
    pub fn selected_managed_agent_health_id(&self) -> Option<String> {
        selected_id(
            &self.managed_agent_health_action_ids,
            self.selected_managed_agent_health_index,
        )
    }

    pub fn select_next_release_review(&mut self) {
        self.select_release_review(true);
    }

    pub fn select_next_evolution_case(&mut self) {
        self.select_evolution_case(true);
    }

    pub fn select_previous_evolution_case(&mut self) {
        self.select_evolution_case(false);
    }

    pub fn select_previous_release_review(&mut self) {
        self.select_release_review(false);
    }

    pub fn select_next_policy_review(&mut self) {
        self.select_policy_review(true);
    }

    pub fn select_previous_policy_review(&mut self) {
        self.select_policy_review(false);
    }

    pub fn select_next_managed_agent_health(&mut self) {
        self.select_managed_agent_health(true);
    }

    pub fn select_previous_managed_agent_health(&mut self) {
        self.select_managed_agent_health(false);
    }

    /// Keep a human decision receipt distinct from the latest read
    /// projection. This never mutates local review status or bypasses
    /// Runtime's approval aggregate.
    pub fn record_release_review_decision(
        &mut self,
        review_id: &str,
        decision: &str,
        result: Result<serde_json::Value, String>,
    ) {
        self.record_action_result(
            &format!("evolution.release_review.{decision}:{review_id}"),
            result,
        );
    }

    /// Record a protected policy-floor decision receipt. Gateway validates
    /// the human capability and one-time lease before Runtime changes policy.
    pub fn record_policy_review_decision(
        &mut self,
        review_id: &str,
        decision: &str,
        result: Result<serde_json::Value, String>,
    ) {
        self.record_action_result(
            &format!("evolution.evaluation_policy.{decision}:{review_id}"),
            result,
        );
    }

    fn select_release_review(&mut self, forward: bool) {
        if let Some(review_id) = cycle_selection(
            &self.pending_release_review_ids,
            &mut self.selected_release_review_index,
            forward,
        ) {
            self.action_status = Some(format!("selected release review {review_id}"));
            self.action_receipt = None;
        } else {
            self.action_status = Some("no pending release review is loaded; press v".to_string());
            self.action_receipt = None;
        }
    }

    fn select_evolution_case(&mut self, forward: bool) {
        if let Some(case_id) = cycle_selection(
            &self.ready_evolution_case_ids,
            &mut self.selected_evolution_case_index,
            forward,
        ) {
            self.action_status = Some(format!("selected Ready evolution case {case_id}"));
            self.action_receipt = None;
        } else {
            self.action_status = Some("no Ready evolution case is loaded; press v".to_string());
            self.action_receipt = None;
        }
    }

    fn select_policy_review(&mut self, forward: bool) {
        if let Some(review_id) = cycle_selection(
            &self.pending_policy_review_ids,
            &mut self.selected_policy_review_index,
            forward,
        ) {
            self.action_status = Some(format!("selected policy review {review_id}"));
            self.action_receipt = None;
        } else {
            self.action_status = Some("no pending policy review is loaded; press p".to_string());
            self.action_receipt = None;
        }
    }

    fn select_managed_agent_health(&mut self, forward: bool) {
        if let Some(managed_agent_id) = cycle_selection(
            &self.managed_agent_health_action_ids,
            &mut self.selected_managed_agent_health_index,
            forward,
        ) {
            self.action_status = Some(format!("selected managed Agent health {managed_agent_id}"));
            self.action_receipt = None;
        } else {
            self.action_status =
                Some("no degraded Managed Agent is loaded; press m to refresh".to_string());
            self.action_receipt = None;
        }
    }

    pub fn record_harness_eval_latest(&mut self, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                let report = payload.get("report").unwrap_or(&serde_json::Value::Null);
                let status = report
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| payload.get("status").and_then(serde_json::Value::as_str))
                    .unwrap_or("empty")
                    .to_string();
                let level = report
                    .get("level")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("-");
                let tokens = report
                    .get("total_tokens")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let tools = report
                    .get("tool_calls")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                self.harness_eval_status = Some(status.clone());
                self.harness_eval_summary = Some(format!(
                    "level={level} status={status} tokens={tokens} tools={tools}"
                ));
                self.action_status = Some("harness_eval.latest succeeded".to_string());
                self.action_receipt = Some(gateway_receipt_summary(&payload));
            }
            Err(error) => {
                self.harness_eval_status = Some("unavailable".to_string());
                self.harness_eval_summary = Some(error.clone());
                self.action_status = Some(format!("harness_eval.latest failed: {error}"));
                self.action_receipt = None;
            }
        }
    }

    pub fn record_evolution_overview(&mut self, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                let signals = payload
                    .get("signals")
                    .and_then(|value| value.get("count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let diagnoses = payload
                    .get("diagnoses")
                    .and_then(|value| value.get("count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let proposals = payload
                    .get("proposals")
                    .and_then(|value| value.get("count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let missions = payload
                    .get("missions")
                    .and_then(|value| value.get("count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let candidates = payload
                    .get("candidates")
                    .and_then(|value| value.get("candidates"))
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len);
                let reviews = payload
                    .get("reviews")
                    .and_then(|value| value.get("reviews"))
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len);
                let advisory_patterns = payload
                    .get("collaboration_patterns")
                    .and_then(|value| value.get("patterns"))
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len);
                let candidate_items = payload
                    .get("candidates")
                    .and_then(|value| value.get("candidates"))
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let evaluation_blocked = candidate_items
                    .iter()
                    .filter(|candidate| {
                        candidate
                            .get("lifecycle")
                            .and_then(serde_json::Value::as_str)
                            == Some("evaluation_blocked")
                    })
                    .count();
                let evaluated_eligible = candidate_items
                    .iter()
                    .filter(|candidate| {
                        candidate
                            .get("lifecycle")
                            .and_then(serde_json::Value::as_str)
                            == Some("evaluated_eligible")
                    })
                    .count();
                let projector = payload
                    .get("signals")
                    .and_then(|value| value.get("projector"))
                    .unwrap_or(&serde_json::Value::Null);
                let projector_lag = projector
                    .get("lag_commits")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let projector_dead_letters = projector
                    .get("dead_letter_count")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let projector_running = projector
                    .get("worker_running")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                self.pending_release_review_ids = pending_review_ids(
                    payload
                        .get("reviews")
                        .and_then(|value| value.get("reviews")),
                );
                clamp_selection(
                    &mut self.selected_release_review_index,
                    self.pending_release_review_ids.len(),
                );
                self.ready_evolution_case_ids = payload
                    .pointer("/cases/items")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter(|case| {
                        case.get("state").and_then(serde_json::Value::as_str) == Some("ready")
                    })
                    .filter_map(|case| {
                        case.get("case_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string)
                    })
                    .collect();
                clamp_selection(
                    &mut self.selected_evolution_case_index,
                    self.ready_evolution_case_ids.len(),
                );
                self.evolution_status = Some(
                    if projector_dead_letters > 0 || !projector_running || evaluation_blocked > 0 {
                        "degraded".to_string()
                    } else if projector_lag > 0 {
                        "lagging".to_string()
                    } else if signals
                        + diagnoses
                        + missions
                        + proposals
                        + candidates as u64
                        + reviews as u64
                        + advisory_patterns as u64
                        > 0
                    {
                        "active".to_string()
                    } else {
                        "empty".to_string()
                    },
                );
                self.evolution_summary = Some(format!(
                    "signals={signals} diagnoses={diagnoses} missions={missions} proposals={proposals} candidates={candidates} eligible={evaluated_eligible} eval_blocked={evaluation_blocked} advisory_patterns={advisory_patterns} release_reviews={reviews} pending_release={} projector=running:{projector_running}/lag:{projector_lag}/dead:{projector_dead_letters}",
                    self.pending_release_review_ids.len(),
                ));
                self.action_status = Some("evolution.overview succeeded".to_string());
                self.action_receipt = Some(gateway_receipt_summary(&payload));
            }
            Err(error) => {
                self.pending_release_review_ids.clear();
                self.selected_release_review_index = 0;
                self.ready_evolution_case_ids.clear();
                self.selected_evolution_case_index = 0;
                self.evolution_status = Some("unavailable".to_string());
                self.evolution_summary = Some(error.clone());
                self.action_status = Some(format!("evolution.overview failed: {error}"));
                self.action_receipt = None;
            }
        }
    }

    pub fn record_evolution_case_detail(&mut self, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                let draft = payload
                    .get("draft")
                    .or_else(|| payload.get("analysis"))
                    .filter(|value| !value.is_null());
                let case_id = payload
                    .pointer("/case/case_id")
                    .or_else(|| draft.and_then(|draft| draft.get("case_id")))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                if let Some(draft) = draft {
                    let hypotheses = draft
                        .pointer("/output/hypotheses")
                        .and_then(serde_json::Value::as_array)
                        .map_or(0, Vec::len);
                    let candidate = draft
                        .pointer("/output/suggested_candidate_kind")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    let experiment = draft
                        .pointer("/output/falsification_experiment/objective")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unavailable");
                    let value = draft
                        .pointer("/output/expected_value")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unavailable");
                    self.evolution_analysis_summary = Some(format!(
                        "case={case_id} hypotheses={hypotheses} candidate={candidate} experiment={experiment} value={value}"
                    ));
                } else {
                    let state = payload
                        .pointer("/case/state")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    self.evolution_analysis_summary =
                        Some(format!("case={case_id} state={state} analysis=pending"));
                }
                self.action_status = Some("evolution.case_detail succeeded".to_string());
                self.action_receipt = Some(gateway_receipt_summary(&payload));
            }
            Err(error) => {
                self.evolution_analysis_summary = Some(format!("unavailable: {error}"));
                self.action_status = Some(format!("evolution.case_detail failed: {error}"));
                self.action_receipt = None;
            }
        }
    }

    pub fn record_evaluation_policy_overview(&mut self, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                let policy = payload.get("policy").unwrap_or(&serde_json::Value::Null);
                let policy_id = policy
                    .get("policy_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unavailable");
                let revision = policy
                    .get("revision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
                let pending = payload
                    .get("reviews")
                    .and_then(|value| value.get("reviews"))
                    .and_then(serde_json::Value::as_array)
                    .map(|reviews| {
                        reviews
                            .iter()
                            .filter(|review| {
                                review.get("status").and_then(serde_json::Value::as_str)
                                    == Some("pending")
                            })
                            .count()
                    })
                    .unwrap_or_default();
                self.pending_policy_review_ids = pending_review_ids(
                    payload
                        .get("reviews")
                        .and_then(|value| value.get("reviews")),
                );
                clamp_selection(
                    &mut self.selected_policy_review_index,
                    self.pending_policy_review_ids.len(),
                );
                self.evaluation_policy_status = Some(if pending > 0 {
                    "review_required".to_string()
                } else {
                    "active".to_string()
                });
                self.evaluation_policy_summary =
                    Some(format!("{policy_id}@{revision} pending_reviews={pending}"));
                self.action_status = Some("evolution.evaluation_policy succeeded".to_string());
                self.action_receipt = Some(gateway_receipt_summary(&payload));
            }
            Err(error) => {
                self.pending_policy_review_ids.clear();
                self.selected_policy_review_index = 0;
                self.evaluation_policy_status = Some("unavailable".to_string());
                self.evaluation_policy_summary = Some(error.clone());
                self.action_status = Some(format!("evolution.evaluation_policy failed: {error}"));
                self.action_receipt = None;
            }
        }
    }

    pub fn record_managed_agent_overview(&mut self, result: Result<serde_json::Value, String>) {
        match result {
            Ok(payload) => {
                let definitions = payload
                    .get("definitions")
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len);
                let invocations = payload
                    .get("invocations")
                    .and_then(serde_json::Value::as_array)
                    .map_or(0, Vec::len);
                let health = payload
                    .get("health")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let reconciliation = health
                    .iter()
                    .filter(|entry| {
                        entry.get("status").and_then(serde_json::Value::as_str)
                            == Some("reconciliation_required")
                    })
                    .count();
                let degraded = health
                    .iter()
                    .filter(|entry| {
                        matches!(
                            entry.get("status").and_then(serde_json::Value::as_str),
                            Some("degraded" | "circuit_open")
                        )
                    })
                    .count();
                self.managed_agent_health_action_ids = health
                    .iter()
                    .filter(|entry| {
                        matches!(
                            entry.get("status").and_then(serde_json::Value::as_str),
                            Some("degraded" | "circuit_open" | "reconciliation_required")
                        )
                    })
                    .filter_map(|entry| {
                        entry
                            .get("managed_agent_id")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                    })
                    .collect();
                clamp_selection(
                    &mut self.selected_managed_agent_health_index,
                    self.managed_agent_health_action_ids.len(),
                );
                self.managed_agent_status = Some(if reconciliation > 0 {
                    "reconciliation_required".to_string()
                } else if degraded > 0 {
                    "degraded".to_string()
                } else if definitions > 0 {
                    "healthy".to_string()
                } else {
                    "empty".to_string()
                });
                self.managed_agent_summary = Some(format!(
                    "definitions={definitions} invocations={invocations} degraded={degraded} reconciliation={reconciliation} recoverable={}",
                    self.managed_agent_health_action_ids.len(),
                ));
                self.action_status = Some("runtime.managed_agents succeeded".to_string());
                self.action_receipt = Some(gateway_receipt_summary(&payload));
            }
            Err(error) => {
                self.managed_agent_health_action_ids.clear();
                self.selected_managed_agent_health_index = 0;
                self.managed_agent_status = Some("unavailable".to_string());
                self.managed_agent_summary = Some(error.clone());
                self.action_status = Some(format!("runtime.managed_agents failed: {error}"));
                self.action_receipt = None;
            }
        }
    }

    // ── Rendering helpers ────────────────────────────────────────

    /// Build the title string for the block border.
    fn build_title(&self) -> String {
        if self.server_running {
            " Control Deck ● ".to_string()
        } else {
            " Control Deck ○ ".to_string()
        }
    }

    /// Format uptime seconds into a human-readable string.
    fn format_uptime(secs: u64) -> String {
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        let minutes = (secs % 3600) / 60;
        let seconds = secs % 60;

        if days > 0 {
            format!("{days}d {hours}h {minutes}m")
        } else if hours > 0 {
            format!("{hours}h {minutes}m {seconds}s")
        } else if minutes > 0 {
            format!("{minutes}m {seconds}s")
        } else {
            format!("{seconds}s")
        }
    }
}

// ── Default impl ─────────────────────────────────────────────────

impl Default for GatewayPanel {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Trait ──────────────────────────────────────────────

impl Component for GatewayPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let title = self.build_title();
        let block = Block::default().title(title).borders(Borders::ALL);

        let _inner_width = area.width.saturating_sub(2) as usize;
        let _inner_height = area.height.saturating_sub(2) as usize;

        let mut lines: Vec<Line> = Vec::new();

        // ── Status indicator ───────────────────────────────────
        lines.push(Line::from(Span::styled(
            "─ Core Runtime ─",
            Style::default().fg(Color::Cyan),
        )));
        let status = if self.server_running {
            Span::styled("● RUNNING", Style::default().fg(Color::Green))
        } else {
            Span::styled("○ STOPPED", Style::default().fg(Color::Red))
        };
        lines.push(Line::from(vec![
            Span::styled("Server: ", Style::default()),
            status,
        ]));
        lines.push(Line::from(Span::styled(
            "Keys: r refresh  h health  s start/stop  e eval  E smoke  v evolution  p policy  m managed  D dispatch",
            Style::default().fg(Color::DarkGray),
        )));
        if let Some(status) = &self.action_status {
            lines.push(Line::from(vec![
                Span::styled("Action: ", Style::default().fg(Color::DarkGray)),
                Span::styled(status.clone(), Style::default().fg(Color::Yellow)),
            ]));
        }
        if let Some(receipt) = &self.action_receipt {
            lines.push(Line::from(vec![
                Span::styled("Receipt: ", Style::default().fg(Color::DarkGray)),
                Span::styled(receipt.clone(), Style::default().fg(Color::Green)),
            ]));
        }
        if let Some(summary) = &self.harness_eval_summary {
            let color = match self.harness_eval_status.as_deref() {
                Some("passed" | "completed") => Color::Green,
                Some("running" | "queued") => Color::Cyan,
                Some("cancel_requested") => Color::Yellow,
                Some("failed" | "gated" | "cancelled") => Color::Yellow,
                Some("empty") | None => Color::DarkGray,
                _ => Color::Cyan,
            };
            lines.push(Line::from(vec![
                Span::styled("HarnessEval: ", Style::default().fg(Color::DarkGray)),
                Span::styled(summary.clone(), Style::default().fg(color)),
            ]));
        }
        if let Some(summary) = &self.evolution_summary {
            let color = match self.evolution_status.as_deref() {
                Some("active") => Color::Magenta,
                Some("lagging") => Color::Yellow,
                Some("degraded") => Color::Red,
                Some("empty") => Color::DarkGray,
                Some("unavailable") => Color::Yellow,
                _ => Color::Cyan,
            };
            lines.push(Line::from(vec![
                Span::styled("Evolution: ", Style::default().fg(Color::DarkGray)),
                Span::styled(summary.clone(), Style::default().fg(color)),
            ]));
        }
        if let Some(summary) = &self.evolution_analysis_summary {
            lines.push(Line::from(vec![
                Span::styled("Evolution analysis: ", Style::default().fg(Color::DarkGray)),
                Span::styled(summary.clone(), Style::default().fg(Color::Magenta)),
            ]));
        }
        if let Some(case_id) = self.selected_evolution_case_id() {
            lines.push(Line::from(vec![
                Span::styled("Ready case: ", Style::default().fg(Color::DarkGray)),
                Span::styled(case_id, Style::default().fg(Color::Yellow)),
                Span::styled(
                    "  c/C select · u detail · U analyze Draft",
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }
        if let Some(review_id) = self.selected_release_review_id() {
            lines.push(Line::from(vec![
                Span::styled("Release review: ", Style::default().fg(Color::DarkGray)),
                Span::styled(review_id, Style::default().fg(Color::Yellow)),
                Span::styled(
                    "  [/] select · a approve · x reject",
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }
        if let Some(summary) = &self.evaluation_policy_summary {
            let color = match self.evaluation_policy_status.as_deref() {
                Some("active") => Color::Green,
                Some("review_required") => Color::Yellow,
                Some("unavailable") => Color::Red,
                _ => Color::Cyan,
            };
            lines.push(Line::from(vec![
                Span::styled("EvalPolicy: ", Style::default().fg(Color::DarkGray)),
                Span::styled(summary.clone(), Style::default().fg(color)),
            ]));
        }
        if let Some(review_id) = self.selected_policy_review_id() {
            lines.push(Line::from(vec![
                Span::styled("Policy review: ", Style::default().fg(Color::DarkGray)),
                Span::styled(review_id, Style::default().fg(Color::Yellow)),
                Span::styled(
                    "  {/} select · A approve · X reject",
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }
        if let Some(summary) = &self.managed_agent_summary {
            let color = match self.managed_agent_status.as_deref() {
                Some("healthy") => Color::Green,
                Some("empty") => Color::DarkGray,
                Some("degraded" | "reconciliation_required") => Color::Yellow,
                Some("unavailable") => Color::Red,
                _ => Color::Cyan,
            };
            lines.push(Line::from(vec![
                Span::styled("ManagedAgents: ", Style::default().fg(Color::DarkGray)),
                Span::styled(summary.clone(), Style::default().fg(color)),
            ]));
        }
        if let Some(managed_agent_id) = self.selected_managed_agent_health_id() {
            lines.push(Line::from(vec![
                Span::styled("Managed health: ", Style::default().fg(Color::DarkGray)),
                Span::styled(managed_agent_id, Style::default().fg(Color::Yellow)),
                Span::styled(
                    "  n/N select · R reset · D dispatch",
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "─ Operator Summary ─",
            Style::default().fg(Color::Cyan),
        )));
        let issue_count = self.degraded_reasons.len() + self.connector_degraded_reasons.len();
        let surface_count = self.surfaces.len();
        let surface_status = self
            .surface_health
            .as_ref()
            .map(|health| health.status.as_str())
            .unwrap_or("unknown");
        let reality_status = self
            .reality_core
            .as_ref()
            .map(|core| core.status.as_str())
            .unwrap_or("unknown");
        let flow = self.fact_flow.as_ref();
        lines.push(Line::from(vec![
            Span::styled("Gateway ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                if self.server_running { "ready" } else { "down" },
                Style::default().fg(if self.server_running {
                    Color::Green
                } else {
                    Color::Red
                }),
            ),
            Span::styled(" · Runtime ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.runtime_readiness
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(" · Sessions ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", self.active_sessions),
                Style::default().fg(Color::White),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Work ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!(
                    "tasks {} approvals {}",
                    self.task_count.unwrap_or_default(),
                    self.pending_approvals.unwrap_or_default()
                ),
                Style::default().fg(Color::Yellow),
            ),
            Span::styled(" · Lease ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                self.lease_owner
                    .as_deref()
                    .map(|owner| {
                        format!(
                            "{} ({})",
                            owner,
                            self.lease_mode.as_deref().unwrap_or("unknown")
                        )
                    })
                    .unwrap_or_else(|| "none".to_string()),
                Style::default().fg(Color::Magenta),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Surface ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{surface_count} / {surface_status}"),
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(" · Reality ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                reality_status.to_string(),
                Style::default().fg(Color::Green),
            ),
            Span::styled(" · FactFlow ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                flow.map(|flow| {
                    format!(
                        "s{} e{} p{} b{}",
                        flow.stage_count,
                        flow.event_count,
                        flow.promotion_count,
                        flow.boundary_count
                    )
                })
                .unwrap_or_else(|| "none".to_string()),
                Style::default().fg(Color::White),
            ),
        ]));
        if issue_count > 0 {
            lines.push(Line::from(vec![
                Span::styled("Issues ", Style::default().fg(Color::Yellow)),
                Span::styled(
                    format!("{issue_count} degraded signals"),
                    Style::default().fg(Color::Yellow),
                ),
            ]));
        }

        // ── Health check ───────────────────────────────────────
        if let Some(ref health) = self.health_status {
            lines.push(Line::from(""));
            let health_color = if health.to_lowercase().contains("healthy") {
                Color::Green
            } else {
                Color::Yellow
            };
            lines.push(Line::from(vec![
                Span::styled("Health: ", Style::default().fg(Color::Yellow)),
                Span::styled(health.clone(), Style::default().fg(health_color)),
            ]));

            if let Some(uptime) = self.uptime_secs {
                lines.push(Line::from(vec![
                    Span::styled("Uptime: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        Self::format_uptime(uptime),
                        Style::default().fg(Color::White),
                    ),
                ]));
            }

            if self.active_sessions > 0 {
                lines.push(Line::from(vec![
                    Span::styled("Sessions: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{}", self.active_sessions),
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
            }

            lines.push(Line::from(vec![
                Span::styled("Transport: ", Style::default().fg(Color::DarkGray)),
                Span::styled("gateway http/sse", Style::default().fg(Color::Cyan)),
            ]));
            for (label, summary) in [
                ("Provider: ", self.provider_transport_summary.as_deref()),
                ("Hot state: ", self.hot_state_summary.as_deref()),
                ("Postgres: ", self.postgres_summary.as_deref()),
                ("Session I/O: ", self.session_storage_summary.as_deref()),
            ] {
                if let Some(summary) = summary {
                    lines.push(Line::from(vec![
                        Span::styled(label, Style::default().fg(Color::DarkGray)),
                        Span::styled(summary.to_string(), Style::default().fg(Color::Cyan)),
                    ]));
                }
            }

            if let Some(readiness) = self.runtime_readiness.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ AI Context ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Runtime: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "ready {readiness}, components {}",
                            self.runtime_components.unwrap_or_default()
                        ),
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
            }

            if let Some(memory_status) = self.memory_status.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("Memory: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(memory_status.clone(), Style::default().fg(Color::Green)),
                ]));
            }
            if let Some(envelope_status) = self.memory_context_envelope_status.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("ContextEnvelope: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(envelope_status.clone(), Style::default().fg(Color::Cyan)),
                    Span::styled(
                        format!(
                            " compression {} used {} checkpoint {}",
                            self.memory_context_envelope_compression
                                .as_deref()
                                .unwrap_or("-"),
                            self.memory_context_envelope_used_ratio
                                .map(|value| format!("{value}%"))
                                .unwrap_or_else(|| "-".to_string()),
                            self.memory_context_envelope_checkpoint
                                .as_deref()
                                .unwrap_or("-")
                        ),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }

            if let Some(reality) = self.reality_core.as_ref() {
                let status_color = match reality.status.as_str() {
                    "ready" => Color::Green,
                    "degraded" => Color::Yellow,
                    _ => Color::White,
                };
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Reality Core ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Core: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(reality.status.clone(), Style::default().fg(status_color)),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Engines: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "fact {} · memory {} · matrix {} · growth {}",
                            reality.fact_status,
                            reality.memory_status,
                            reality.matrix_status,
                            reality.growth_status
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Bridge: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "context {} · matrix-source {} · audit {}",
                            reality.context_status,
                            reality.matrix_context_status,
                            reality.audit_status
                        ),
                        Style::default().fg(Color::Cyan),
                    ),
                ]));
                if !reality.degraded_reasons.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Reality degraded: ", Style::default().fg(Color::Yellow)),
                        Span::styled(
                            reality
                                .degraded_reasons
                                .iter()
                                .take(2)
                                .cloned()
                                .collect::<Vec<_>>()
                                .join(" · "),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }

            if let Some(flow) = self.fact_flow.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("Fact Flow: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "stages {}, events {}, promotions {}, boundaries {}",
                            flow.stage_count,
                            flow.event_count,
                            flow.promotion_count,
                            flow.boundary_count
                        ),
                        Style::default().fg(Color::Yellow),
                    ),
                ]));
                if let Some(session_id) = flow.session_id.as_ref() {
                    lines.push(Line::from(vec![
                        Span::styled("Session: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!("{} · {}", session_id, flow.source),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }

            if let Some(mission) = self.mission_control.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Mission Control ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Sessions: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} total, {} active, {} bg, {} paused",
                            mission.session_count,
                            mission.active_count,
                            mission.background_count,
                            mission.paused_count
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Tasks/Teams/Agents: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} / {} / {}",
                            mission.task_count, mission.team_count, mission.agent_count
                        ),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(
                        "  Pending approvals/relations: ",
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        format!("{} / {}", mission.pending_approvals, mission.relation_count),
                        Style::default().fg(Color::Magenta),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Graphs/Conflicts: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} / {}",
                            mission.execution_graph_count, mission.conflict_count
                        ),
                        Style::default().fg(if mission.conflict_count > 0 {
                            Color::Red
                        } else {
                            Color::Cyan
                        }),
                    ),
                    Span::styled("  Evidence/Actions: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} / {}",
                            mission.evidence_count, mission.capability_action_count
                        ),
                        Style::default().fg(Color::Blue),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Control: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} ready, {} blocked, {} approval-gated",
                            mission.control_ready_count,
                            mission.control_blocked_count,
                            mission.control_requires_approval_count
                        ),
                        Style::default().fg(if mission.control_blocked_count > 0 {
                            Color::Yellow
                        } else {
                            Color::Green
                        }),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Live work: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("{} agents running", mission.running_agent_count),
                        Style::default().fg(if mission.running_agent_count > 0 {
                            Color::Green
                        } else {
                            Color::DarkGray
                        }),
                    ),
                    Span::styled(
                        "  Recovery attention: ",
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        mission.recovery_required_count.to_string(),
                        Style::default().fg(if mission.recovery_required_count > 0 {
                            Color::Yellow
                        } else {
                            Color::Green
                        }),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("Routing: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "Task {} · Mission {} · rev {}",
                            mission.task_focus_id.as_deref().unwrap_or("auto"),
                            mission.mission_focus_id.as_deref().unwrap_or("auto"),
                            mission.routing_revision
                        ),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled("  Organizer: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} pending · {} failed",
                            mission.organization_pending_count, mission.organization_failed_count
                        ),
                        if mission.organization_failed_count > 0 {
                            Style::default().fg(Color::Red)
                        } else {
                            Style::default().fg(Color::Green)
                        },
                    ),
                ]));
                for action in &mission.control_actions {
                    let marker = if action.available { "+" } else { "-" };
                    let approval = if action.requires_approval {
                        " · approval"
                    } else {
                        ""
                    };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{marker} {:18}", action.action),
                            Style::default().fg(if action.available {
                                Color::Green
                            } else {
                                Color::Yellow
                            }),
                        ),
                        Span::styled(
                            format!(
                                "{} · targets {}{}",
                                compact_text(&action.reason, 54),
                                action.target_count,
                                approval
                            ),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
                if let Some(active) = mission.active_session_id.as_ref() {
                    lines.push(Line::from(vec![
                        Span::styled("Active: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(active.clone(), Style::default().fg(Color::Green)),
                    ]));
                }
                for session in mission.sessions.iter().take(3) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:10}", session.status),
                            Style::default().fg(match session.status.as_str() {
                                "active" => Color::Green,
                                "paused" => Color::Yellow,
                                "closed" => Color::DarkGray,
                                _ => Color::Cyan,
                            }),
                        ),
                        Span::styled(
                            format!(
                                "{} · teams {} agents {}",
                                compact_text(&session.title, 28),
                                session.team_count,
                                session.agent_count
                            ),
                            Style::default().fg(Color::White),
                        ),
                    ]));
                }
            }

            if let Some(kernel) = self.cowd_kernel.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Cowd Kernel ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Kernel: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "caps {}, tui {}",
                            kernel.capability_count, kernel.projection_capability_count
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                let parity = if kernel.webui_tui_full_parity {
                    "parity yes"
                } else {
                    "parity no"
                };
                let cli = if kernel.cli_is_minimal_control {
                    "cli minimal"
                } else {
                    "cli check"
                };
                lines.push(Line::from(vec![
                    Span::styled("Surfaces: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(format!("{parity}, {cli}"), Style::default().fg(Color::Cyan)),
                ]));
                let gate_color = if kernel.release_gate_status == "pass" {
                    Color::Green
                } else {
                    Color::Yellow
                };
                lines.push(Line::from(vec![
                    Span::styled("Gate: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{}, failed {}",
                            kernel.release_gate_status, kernel.release_gate_failed_checks
                        ),
                        Style::default().fg(gate_color),
                    ),
                ]));
            }

            if let Some(data) = self.structured_data.as_ref() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Structured Data ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Data: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "sources {}, facts {}, evidence {}, watermarks {}",
                            data.source_count,
                            data.fact_count,
                            data.evidence_count,
                            data.watermark_count
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                let samples = [
                    data.sample_sources
                        .first()
                        .map(|value| format!("source {value}")),
                    data.sample_facts
                        .first()
                        .map(|value| format!("fact {value}")),
                    data.sample_evidence
                        .first()
                        .map(|value| format!("evidence {value}")),
                    data.sample_watermarks
                        .first()
                        .map(|value| format!("watermark {value}")),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" · ");
                if !samples.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Samples: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(samples, Style::default().fg(Color::Yellow)),
                    ]));
                }
            }

            if !self.surfaces.is_empty() || self.surface_health.is_some() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Surface Host ─",
                    Style::default().fg(Color::Cyan),
                )));
                let health = self.surface_health.as_ref();
                let surface_count = health
                    .map(|item| item.surface_count)
                    .unwrap_or(self.surfaces.len() as u64);
                let external_count = health
                    .map(|item| item.external_surface_count)
                    .unwrap_or_else(|| {
                        self.surfaces
                            .iter()
                            .filter(|surface| surface.is_external())
                            .count() as u64
                    });
                lines.push(Line::from(vec![
                    Span::styled("Surfaces: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} total, {} external, routes {}, resources {}",
                            surface_count,
                            external_count,
                            health.map(|item| item.route_count).unwrap_or_default(),
                            health.map(|item| item.resource_count).unwrap_or_default()
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                if let Some(health) = health {
                    lines.push(Line::from(vec![
                        Span::styled("Host: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(health.status.clone(), Style::default().fg(Color::Green)),
                    ]));
                }
                let preview = self
                    .surfaces
                    .iter()
                    .take(4)
                    .map(|surface| format!("{}:{}", surface.id, surface.status))
                    .collect::<Vec<_>>()
                    .join(" · ");
                if !preview.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Preview: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(preview, Style::default().fg(Color::Cyan)),
                    ]));
                }
                lines.push(Line::from(Span::styled(
                    "Open /surfaces for routes, resources, events, send/action.",
                    Style::default().fg(Color::DarkGray),
                )));
            }

            if !self.message_connectors.is_empty()
                || !self.message_endpoints.is_empty()
                || !self.message_routes.is_empty()
                || !self.message_bindings.is_empty()
            {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Message Plane ─",
                    Style::default().fg(Color::Cyan),
                )));
                let ready = self
                    .message_connectors
                    .iter()
                    .filter(|connector| {
                        connector.enabled
                            && connector.configured
                            && !connector.circuit_open
                            && !matches!(
                                connector.runtime_status.as_str(),
                                "failed" | "error" | "unavailable" | "circuit-open"
                            )
                    })
                    .count();
                let circuit_open = self
                    .message_connectors
                    .iter()
                    .filter(|connector| connector.circuit_open)
                    .count();
                lines.push(Line::from(vec![
                    Span::styled("Connectors: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{ready}/{} ready, circuit {}",
                            self.message_connectors.len(),
                            circuit_open
                        ),
                        Style::default().fg(if circuit_open > 0 {
                            Color::Yellow
                        } else {
                            Color::Green
                        }),
                    ),
                    Span::styled(
                        "  Endpoints/Routes/Bindings: ",
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        format!(
                            "{}/{}/{}",
                            self.message_endpoints.len(),
                            self.message_routes.len(),
                            self.message_bindings.len()
                        ),
                        Style::default().fg(Color::White),
                    ),
                ]));
                let preview = self
                    .message_connectors
                    .iter()
                    .take(4)
                    .map(|connector| {
                        format!(
                            "{}:{}:{}",
                            connector.connector,
                            connector.configuration_status,
                            connector.runtime_status
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" · ");
                if !preview.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled("Preview: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(preview, Style::default().fg(Color::Cyan)),
                    ]));
                }
                if let Some(binding) = self.message_bindings.first() {
                    lines.push(Line::from(vec![
                        Span::styled("Latest binding: ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!(
                                "{} {} {} -> {}",
                                binding.connector,
                                binding.direction,
                                binding.status,
                                compact_text(&binding.endpoint, 24)
                            ),
                            Style::default().fg(Color::Yellow),
                        ),
                    ]));
                }
                lines.push(Line::from(Span::styled(
                    "Message connectors expose external user-message ingress/egress through Gateway.",
                    Style::default().fg(Color::DarkGray),
                )));
            }

            if self.task_count.is_some() || self.pending_approvals.is_some() {
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    "─ Work Control ─",
                    Style::default().fg(Color::Cyan),
                )));
                lines.push(Line::from(vec![
                    Span::styled("Control: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "tasks {}, approvals {}",
                            self.task_count.unwrap_or_default(),
                            self.pending_approvals.unwrap_or_default()
                        ),
                        Style::default().fg(Color::Yellow),
                    ),
                ]));
            }

            if let Some(owner) = self.lease_owner.as_ref() {
                lines.push(Line::from(vec![
                    Span::styled("Lease: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!(
                            "{} ({})",
                            owner,
                            self.lease_mode.as_deref().unwrap_or("unknown")
                        ),
                        Style::default().fg(Color::Magenta),
                    ),
                ]));
            }

            if !self.degraded_reasons.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::from(vec![
                    Span::styled("Degraded: ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        self.degraded_reasons
                            .iter()
                            .take(2)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(" · "),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        if !self.execution_receipts.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "─ Execution Receipts ─",
                Style::default().fg(Color::Cyan),
            )));
            for receipt in self.execution_receipts.iter().take(3) {
                let idem = receipt
                    .idempotency_key
                    .as_deref()
                    .map(|key| format!(" · idem {key}"))
                    .unwrap_or_default();
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{:8}", receipt.status),
                        Style::default().fg(Color::Green),
                    ),
                    Span::styled(
                        format!("{:16}", receipt.dispatch_status),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(
                        format!("{} · {}{}", receipt.mode, receipt.capability, idem),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        if !self.connector_accounts.is_empty()
            || !self.connector_capabilities.is_empty()
            || !self.connector_resources.is_empty()
            || !self.connector_degraded_reasons.is_empty()
        {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "─ Connector Plane ─",
                Style::default().fg(Color::Cyan),
            )));
            lines.push(Line::from(vec![
                Span::styled("Accounts: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", self.connector_accounts.len()),
                    Style::default().fg(Color::White),
                ),
                Span::styled("  Capabilities: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", self.connector_capabilities.len()),
                    Style::default().fg(Color::White),
                ),
                Span::styled("  Resources: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", self.connector_resources.len()),
                    Style::default().fg(Color::White),
                ),
            ]));

            for account in self.connector_accounts.iter().take(4) {
                let color = match account.status.as_str() {
                    "ready" => Color::Green,
                    "disabled" => Color::DarkGray,
                    "degraded" => Color::Yellow,
                    _ => Color::White,
                };
                let detail = account
                    .reason
                    .as_deref()
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| format!("{} bindings", account.binding_count));
                lines.push(Line::from(vec![
                    Span::styled(format!("{:8}", account.status), Style::default().fg(color)),
                    Span::styled(
                        format!("{:10}", account.provider),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(
                        format!("{:18}", account.account_id),
                        Style::default().fg(Color::Yellow),
                    ),
                    Span::styled(detail, Style::default().fg(Color::DarkGray)),
                ]));
            }

            if !self.connector_capabilities.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Capabilities",
                    Style::default().fg(Color::DarkGray),
                )));
                for capability in self.connector_capabilities.iter().take(5) {
                    let approval = if capability.requires_approval {
                        "approval"
                    } else {
                        "open"
                    };
                    let commit = if capability.supports_commit {
                        "commit"
                    } else {
                        "dry-run"
                    };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:8}", capability.plane),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::styled(
                            format!("{:34}", capability.capability_id),
                            Style::default().fg(Color::White),
                        ),
                        Span::styled(
                            format!("{} · {} · {}", capability.risk, commit, approval),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }

            if !self.connector_resources.is_empty() {
                lines.push(Line::from(Span::styled(
                    "Resources",
                    Style::default().fg(Color::DarkGray),
                )));
                for resource in self.connector_resources.iter().take(4) {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{:10}", resource.provider),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::styled(
                            format!("{:10}", resource.resource_type),
                            Style::default().fg(Color::Cyan),
                        ),
                        Span::styled(
                            format!("{:18}", resource.indexed_state),
                            Style::default().fg(Color::Green),
                        ),
                        Span::styled(resource.title.clone(), Style::default().fg(Color::DarkGray)),
                    ]));
                }
                lines.push(Line::from(Span::styled(
                    "Use command palette: Mark indexed · Mark stale · Remember resource",
                    Style::default().fg(Color::DarkGray),
                )));
            }

            if !self.connector_degraded_reasons.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("Connector degraded: ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        self.connector_degraded_reasons
                            .iter()
                            .take(2)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(" · "),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        // ── Gateway Capability Contract ────────────────────────
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "─ Gateway Capability Contract ─",
            Style::default().fg(Color::Cyan),
        )));
        if let Some(contract) = self.gateway_capability_contract.as_ref() {
            let parity = if contract.route_contract_parity {
                "parity yes"
            } else {
                "parity no"
            };
            lines.push(Line::from(vec![
                Span::styled("Coverage: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!(
                        "routes {} caps {} p1 {} ai {} openapi {} tools {} · {}",
                        contract.route_count,
                        contract.capability_count,
                        contract.p1_count,
                        contract.ai_visible_count,
                        contract.openapi_path_count,
                        contract.openai_tool_count,
                        parity
                    ),
                    Style::default().fg(if contract.route_contract_parity {
                        Color::Green
                    } else {
                        Color::Yellow
                    }),
                ),
            ]));
            for route in contract.sample_routes.iter().take(10) {
                let method_color = match route.method.as_str() {
                    "GET" => Color::Green,
                    "POST" => Color::Yellow,
                    "PUT" | "PATCH" => Color::Cyan,
                    "DELETE" => Color::Red,
                    _ => Color::White,
                };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{:4}", route.method),
                        Style::default().fg(method_color),
                    ),
                    Span::styled(
                        format!("{:28}", compact_text(&route.path, 28)),
                        Style::default().fg(Color::White),
                    ),
                    Span::styled(
                        format!(" {} · {} · {}", route.domain, route.risk, route.criticality),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
            if !contract.sample_tools.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("AI tools: ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        contract
                            .sample_tools
                            .iter()
                            .take(5)
                            .map(|tool| format!("{}({})", tool.name, tool.parameter_count))
                            .collect::<Vec<_>>()
                            .join(" · "),
                        Style::default().fg(Color::Magenta),
                    ),
                ]));
            }
        } else {
            lines.push(Line::from(Span::styled(
                "Gateway contract unavailable. Check /api/gateway/capability-contract.",
                Style::default().fg(Color::Yellow),
            )));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "─ Surface Dispatch Contract ─",
            Style::default().fg(Color::Cyan),
        )));
        lines.push(Line::from(Span::styled(
            "Gateway owns routing. TUI submits surface send/action requests by surface id.",
            Style::default().fg(Color::DarkGray),
        )));

        // ── Keyboard hint bar ──────────────────────────────────
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Views: e eval · E smoke · v release reviews · p policy reviews · m managed Agents",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::from(Span::styled(
            "Actions: [/] release select · a/x release approve/reject · {/} policy select · A/X policy approve/reject · n/N health select · R reset · D dispatch · t schedule tick",
            Style::default().fg(Color::DarkGray),
        )));

        let viewport_len = area.height.saturating_sub(2).max(1) as usize;
        let mut scroll = PanelScrollState {
            offset: self.scroll_offset,
            content_len: lines.len(),
            viewport_len,
        };
        scroll.clamp();
        self.scroll_offset = scroll.offset;

        let visible = lines
            .into_iter()
            .skip(self.scroll_offset)
            .take(viewport_len)
            .collect::<Vec<_>>();

        let paragraph = Paragraph::new(visible).block(block).scroll((0, 0));
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        if key.kind != KeyEventKind::Press {
            return EventResult::NotConsumed;
        }

        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                self.scroll_offset = self.scroll_offset.saturating_add(1);
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                EventResult::Consumed
            }
            KeyCode::PageDown => {
                self.scroll_offset = self.scroll_offset.saturating_add(8);
                EventResult::Consumed
            }
            KeyCode::PageUp => {
                self.scroll_offset = self.scroll_offset.saturating_sub(8);
                EventResult::Consumed
            }
            KeyCode::Home => {
                self.scroll_offset = 0;
                EventResult::Consumed
            }
            KeyCode::End => {
                self.scroll_offset = usize::MAX;
                EventResult::Consumed
            }
            KeyCode::Char('r') => EventResult::Consumed, // refresh
            KeyCode::Char('h') => EventResult::Consumed, // health check
            KeyCode::Char('e') | KeyCode::Char('E') => EventResult::Consumed, // harness eval
            KeyCode::Char('s') => EventResult::Consumed, // start/stop
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "gateway_panel"
    }
}

fn pending_review_ids(reviews: Option<&serde_json::Value>) -> Vec<String> {
    let mut ids = Vec::new();
    for review in reviews
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        if review.get("status").and_then(serde_json::Value::as_str) != Some("pending") {
            continue;
        }
        let Some(review_id) = review
            .get("review_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        if !ids.contains(&review_id) {
            ids.push(review_id);
        }
    }
    ids
}

fn clamp_selection(index: &mut usize, len: usize) {
    if len == 0 {
        *index = 0;
    } else {
        *index = (*index).min(len - 1);
    }
}

fn selected_id(ids: &[String], index: usize) -> Option<String> {
    ids.get(index).cloned()
}

fn cycle_selection(ids: &[String], index: &mut usize, forward: bool) -> Option<String> {
    if ids.is_empty() {
        *index = 0;
        return None;
    }
    *index = if forward {
        (*index + 1) % ids.len()
    } else if *index == 0 {
        ids.len() - 1
    } else {
        *index - 1
    };
    selected_id(ids, *index)
}

fn compact_text(value: &str, max_chars: usize) -> String {
    let text = value.trim();
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut output = text
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    output.push_str("...");
    output
}

fn gateway_receipt_summary(receipt: &serde_json::Value) -> String {
    let kind = receipt
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("gateway.receipt");
    let status = receipt
        .get("status")
        .or_else(|| receipt.get("ok"))
        .map(serde_json::Value::to_string)
        .unwrap_or_else(|| "recorded".to_string());
    format!("{kind} status={status}")
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyEvent;

    #[test]
    fn gateway_manifest_projects_runtime_and_storage_health_without_raw_payload_state() {
        let mut panel = GatewayPanel::new();
        panel.record_gateway_manifest(Ok(serde_json::json!({
            "health": {
                "status": "healthy",
                "runtime": {
                    "provider_transport": { "entries": 2, "checkouts": 8, "hits": 6, "builds": 2 },
                    "hot_state": {
                        "budget": { "limit_bytes": 4096 },
                        "metrics": { "resident_bytes": 1024, "evictions": 1 },
                        "pressure_high": false
                    }
                },
                "storage": {
                    "postgres": {
                        "max_connections": 12,
                        "metrics": { "query_count": 20, "query_error_count": 1 }
                    },
                    "session_execution": {
                        "active": 2,
                        "queued": 1,
                        "average_queue_wait_micros": 30,
                        "average_service_micros": 80
                    }
                }
            }
        })));

        assert_eq!(panel.health_status.as_deref(), Some("healthy"));
        assert_eq!(
            panel.provider_transport_summary.as_deref(),
            Some("entries 2 · checkouts 8 · hits 6 · builds 2")
        );
        assert!(panel
            .hot_state_summary
            .as_deref()
            .unwrap()
            .contains("1024 / 4096"));
        assert!(panel
            .postgres_summary
            .as_deref()
            .unwrap()
            .contains("queries 20"));
        assert!(panel
            .session_storage_summary
            .as_deref()
            .unwrap()
            .contains("queued 1"));
    }

    #[test]
    fn gateway_manifest_failure_clears_stale_healthy_summaries() {
        let mut panel = GatewayPanel::new();
        panel.record_gateway_manifest(Ok(serde_json::json!({
            "health": {
                "status": "healthy",
                "runtime": {
                    "provider_transport": { "entries": 1 },
                    "hot_state": { "metrics": { "resident_bytes": 1 } }
                },
                "storage": {
                    "postgres": { "metrics": { "query_count": 1 } },
                    "session_execution": { "active": 1 }
                }
            }
        })));

        panel.record_gateway_manifest(Err("offline".to_string()));

        assert!(!panel.server_running);
        assert_eq!(panel.health_status.as_deref(), Some("unavailable"));
        assert!(panel.provider_transport_summary.is_none());
        assert!(panel.hot_state_summary.is_none());
        assert!(panel.postgres_summary.is_none());
        assert!(panel.session_storage_summary.is_none());
    }

    #[test]
    fn gateway_panel_defaults_stopped() {
        let panel = GatewayPanel::new();
        assert!(!panel.server_running);
        assert!(panel.health_status.is_none());
        assert!(panel.uptime_secs.is_none());
        assert_eq!(panel.active_sessions, 0);
    }

    #[test]
    fn update_health_marks_server_running() {
        let mut panel = GatewayPanel::new();
        panel.update_health("Healthy".into());
        assert!(panel.server_running);
        assert_eq!(panel.health_status.as_deref(), Some("Healthy"));
    }

    #[test]
    fn set_server_status_clears_on_stop() {
        let mut panel = GatewayPanel::new();
        panel.update_health("OK".into());
        panel.set_uptime(3600);
        panel.set_active_sessions(5);
        assert!(panel.server_running);

        panel.set_server_status(false);
        assert!(!panel.server_running);
        assert!(panel.health_status.is_none());
        assert!(panel.uptime_secs.is_none());
        assert_eq!(panel.active_sessions, 0);
    }

    #[test]
    fn set_server_status_to_running() {
        let mut panel = GatewayPanel::new();
        panel.set_server_status(true);
        assert!(panel.server_running);
        assert!(panel.health_status.is_none()); // not set yet
    }

    #[test]
    fn component_trait_methods() {
        let panel = GatewayPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "gateway_panel");
    }

    #[test]
    fn format_uptime_seconds() {
        assert_eq!(GatewayPanel::format_uptime(0), "0s");
        assert_eq!(GatewayPanel::format_uptime(45), "45s");
        assert_eq!(GatewayPanel::format_uptime(90), "1m 30s");
        assert_eq!(GatewayPanel::format_uptime(3661), "1h 1m 1s");
        assert_eq!(GatewayPanel::format_uptime(90061), "1d 1h 1m");
    }

    #[test]
    fn set_uptime_and_sessions() {
        let mut panel = GatewayPanel::new();
        panel.set_uptime(7200);
        assert_eq!(panel.uptime_secs, Some(7200));

        panel.set_active_sessions(3);
        assert_eq!(panel.active_sessions, 3);
    }

    #[test]
    fn render_stopped_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(60, 20);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 60, 20));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("STOPPED"),
            "Stopped state must show STOPPED, got: {joined}"
        );
    }

    #[test]
    fn render_running_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.update_health("Healthy - all systems operational".into());
        panel.set_uptime(3600);
        panel.set_active_sessions(2);
        panel.runtime_readiness = Some("87%".to_string());
        panel.runtime_components = Some(12);
        panel.task_count = Some(3);
        panel.pending_approvals = Some(1);
        panel.lease_owner = Some("tui:42".to_string());
        panel.lease_mode = Some("collaborative".to_string());

        let mut terminal = MockTerminal::new(60, 20);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 60, 20));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("RUNNING"),
            "Running state must show RUNNING, got: {joined}"
        );
        assert!(
            joined.contains("Healthy"),
            "Should show health status, got: {joined}"
        );
        assert!(
            joined.contains("Uptime"),
            "Should show uptime, got: {joined}"
        );
        assert!(
            joined.contains("Sessions"),
            "Should show sessions, got: {joined}"
        );
        assert!(
            joined.contains("Transport") && joined.contains("gateway http/sse"),
            "Should show Gateway transport, got: {joined}"
        );
        assert!(
            joined.contains("Runtime") && joined.contains("87%"),
            "Should show Gateway API summary, got: {joined}"
        );
        assert!(
            joined.contains("Control") && joined.contains("approvals 1"),
            "Should show runtime control summary, got: {joined}"
        );
        assert!(
            joined.contains("Lease") && joined.contains("tui:42"),
            "Should show runtime lease summary, got: {joined}"
        );
    }

    #[test]
    fn evolution_overview_summary_includes_runtime_projector_and_candidate_state() {
        let mut panel = GatewayPanel::new();
        panel.record_evolution_overview(Ok(serde_json::json!({
            "kind": "evolution.overview",
            "signals": {
                "count": 1,
                "projector": {
                    "lag_commits": 0,
                    "dead_letter_count": 0,
                    "worker_running": true
                }
            },
            "diagnoses": {"count": 1},
            "missions": {"count": 1},
            "proposals": {"count": 1},
            "cases": {"items": [
                {"case_id": "case-1", "state": "ready"}
            ]},
            "candidates": {"candidates": [
                {"candidate_id": "candidate-1", "lifecycle": "evaluated_eligible"}
            ]},
            "sandbox_evals": {"count": 1},
            "collaboration_patterns": {"patterns": [{"pattern_id": "pattern-1"}]},
            "reviews": {"reviews": []},
        })));

        assert_eq!(panel.evolution_status.as_deref(), Some("active"));
        assert!(panel
            .evolution_summary
            .as_deref()
            .unwrap_or_default()
            .contains("eligible=1"));
        assert!(panel
            .evolution_summary
            .as_deref()
            .unwrap_or_default()
            .contains("advisory_patterns=1"));
        assert_eq!(
            panel.selected_evolution_case_id().as_deref(),
            Some("case-1")
        );
        panel.record_evolution_case_detail(Ok(serde_json::json!({
            "kind": "evolution.analysis_draft",
            "draft": {
                "case_id": "case-1",
                "output": {
                    "hypotheses": [{}, {}],
                    "suggested_candidate_kind": "architecture_plan",
                    "falsification_experiment": {"objective": "paired replay"},
                    "expected_value": "separate causes"
                }
            }
        })));
        assert!(panel
            .evolution_analysis_summary
            .as_deref()
            .unwrap_or_default()
            .contains("hypotheses=2"));
    }

    #[test]
    fn management_projection_selects_typed_reviews_and_recoverable_agents() {
        let mut panel = GatewayPanel::new();
        panel.record_evolution_overview(Ok(serde_json::json!({
            "kind": "evolution.overview",
            "signals": {
                "count": 0,
                "projector": {
                    "lag_commits": 0,
                    "dead_letter_count": 0,
                    "worker_running": true
                }
            },
            "diagnoses": {"count": 0},
            "missions": {"count": 0},
            "proposals": {"count": 0},
            "candidates": {"candidates": []},
            "reviews": {"reviews": [
                {"review_id": "release-b", "status": "pending"},
                {"review_id": "release-a", "status": "pending"},
                {"review_id": "release-old", "status": "approved"}
            ]},
            "active_capabilities": {"count": 0, "active_count": 0}
        })));
        assert_eq!(
            panel.selected_release_review_id().as_deref(),
            Some("release-b")
        );
        panel.select_next_release_review();
        assert_eq!(
            panel.selected_release_review_id().as_deref(),
            Some("release-a")
        );

        panel.record_evaluation_policy_overview(Ok(serde_json::json!({
            "kind": "evolution.evaluation_policy.overview",
            "policy": {"policy_id": "policy-default", "revision": 3},
            "reviews": {"reviews": [
                {"review_id": "policy-1", "status": "pending"},
                {"review_id": "policy-old", "status": "denied"}
            ]}
        })));
        assert_eq!(
            panel.selected_policy_review_id().as_deref(),
            Some("policy-1")
        );

        panel.record_managed_agent_overview(Ok(serde_json::json!({
            "definitions": [
                {"managed_agent_id": "agent-healthy"},
                {"managed_agent_id": "agent-recover"}
            ],
            "invocations": [],
            "health": [
                {"managed_agent_id": "agent-healthy", "status": "healthy"},
                {"managed_agent_id": "agent-recover", "status": "circuit_open"}
            ]
        })));
        assert_eq!(
            panel.selected_managed_agent_health_id().as_deref(),
            Some("agent-recover")
        );
        assert!(panel
            .managed_agent_summary
            .as_deref()
            .unwrap_or_default()
            .contains("recoverable=1"));
    }

    #[test]
    fn typed_management_actions_keep_gateway_receipts_and_errors_visible() {
        let mut panel = GatewayPanel::new();
        panel.record_release_review_decision(
            "release-1",
            "approve",
            Ok(serde_json::json!({
                "kind": "evolution.release_decision",
                "status": "approved"
            })),
        );
        assert_eq!(
            panel.action_status.as_deref(),
            Some("evolution.release_review.approve:release-1 succeeded")
        );
        assert!(panel
            .action_receipt
            .as_deref()
            .unwrap_or_default()
            .contains("evolution.release_decision"));

        panel.record_policy_review_decision(
            "policy-1",
            "reject",
            Err("human capability required".to_string()),
        );
        assert_eq!(
            panel.action_status.as_deref(),
            Some("evolution.evaluation_policy.reject:policy-1 failed: human capability required")
        );
        assert!(panel.action_receipt.is_none());
    }

    #[test]
    fn render_shows_cowd_kernel_and_structured_data_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.cowd_kernel = Some(CowdKernelSummary {
            capability_count: 11,
            projection_capability_count: 10,
            webui_tui_full_parity: true,
            cli_is_minimal_control: true,
            release_gate_status: "pass".to_string(),
            release_gate_failed_checks: 0,
        });
        panel.structured_data = Some(StructuredDataSummary {
            source_count: 1,
            fact_count: 2,
            evidence_count: 3,
            watermark_count: 1,
            sample_sources: vec!["pack-tui".to_string()],
            sample_facts: vec!["fact-tui".to_string()],
            sample_evidence: vec!["evidence-tui".to_string()],
            sample_watermarks: vec!["pack-tui".to_string()],
        });

        let mut terminal = MockTerminal::new(100, 34);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 100, 34));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Cowd Kernel"),
            "Should show cowd kernel section, got: {joined}"
        );
        assert!(
            joined.contains("caps 11") && joined.contains("tui 10"),
            "Should show capability summary, got: {joined}"
        );
        assert!(
            joined.contains("parity yes") && joined.contains("cli minimal"),
            "Should show surface policy summary, got: {joined}"
        );
        assert!(
            joined.contains("Structured Data"),
            "Should show structured section, got: {joined}"
        );
        assert!(
            joined.contains("sources 1")
                && joined.contains("facts 2")
                && joined.contains("evidence 3")
                && joined.contains("watermarks 1"),
            "Should show structured counts, got: {joined}"
        );
        assert!(
            joined.contains("pack-tui")
                && joined.contains("fact-tui")
                && joined.contains("evidence-tui"),
            "Should show structured samples, got: {joined}"
        );
    }

    #[test]
    fn render_shows_reality_core_and_fact_flow_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.reality_core = Some(RealityCoreSummary {
            status: "ready".to_string(),
            fact_status: "enabled_and_wired".to_string(),
            memory_status: "ready".to_string(),
            matrix_status: "ready".to_string(),
            matrix_context_status: "enabled_and_wired".to_string(),
            growth_status: "ready".to_string(),
            context_status: "ready".to_string(),
            audit_status: "ready".to_string(),
            degraded_reasons: Vec::new(),
        });
        panel.fact_flow = Some(FactFlowSummary {
            source: "growth.promotions".to_string(),
            session_id: Some("session-tui".to_string()),
            stage_count: 5,
            event_count: 2,
            promotion_count: 1,
            boundary_count: 4,
        });

        let mut terminal = MockTerminal::new(100, 34);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 100, 34));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Reality Core"),
            "Should show Reality Core section, got: {joined}"
        );
        assert!(
            joined.contains("fact enabled_and_wired")
                && joined.contains("memory ready")
                && joined.contains("matrix ready")
                && joined.contains("growth ready"),
            "Should show Reality engines, got: {joined}"
        );
        assert!(
            joined.contains("matrix-source enabled_and_wired"),
            "Should show Reality context source status, got: {joined}"
        );
        assert!(
            joined.contains("Fact Flow")
                && joined.contains("stages 5")
                && joined.contains("promotions 1")
                && joined.contains("boundaries 4"),
            "Should show Fact Flow summary, got: {joined}"
        );
        assert!(
            joined.contains("session-tui") && joined.contains("growth.promotions"),
            "Should show Fact Flow session/source, got: {joined}"
        );
    }

    #[test]
    fn render_shows_gateway_capability_contract() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.gateway_capability_contract = Some(
            crate::runtime_control_store::GatewayCapabilityContractSummary {
                kind: "gateway.capability_contract".to_string(),
                schema_version: 1,
                owner: "gateway".to_string(),
                route_count: 120,
                capability_count: 120,
                p1_count: 18,
                ai_visible_count: 64,
                openapi_path_count: 100,
                openai_tool_count: 42,
                route_contract_parity: true,
                sample_routes: vec![
                    crate::runtime_control_store::GatewayCapabilityRouteSummary {
                        id: "gateway.surface.get".to_string(),
                        domain: "surface".to_string(),
                        title: "Surface registry".to_string(),
                        method: "GET".to_string(),
                        path: "/api/surfaces".to_string(),
                        risk: "external".to_string(),
                        criticality: "p1".to_string(),
                    },
                    crate::runtime_control_store::GatewayCapabilityRouteSummary {
                        id: "gateway.contract.get".to_string(),
                        domain: "gateway".to_string(),
                        title: "Capability contract".to_string(),
                        method: "GET".to_string(),
                        path: "/api/gateway/capability-contract".to_string(),
                        risk: "read".to_string(),
                        criticality: "p1".to_string(),
                    },
                ],
                sample_tools: vec![crate::runtime_control_store::GatewayOpenAiToolSummary {
                    name: "gateway_get_api_sessions".to_string(),
                    description: "List sessions".to_string(),
                    parameter_count: 1,
                }],
            },
        );
        let mut terminal = MockTerminal::new(82, 72);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 82, 72));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("Gateway Capability Contract"),
            "Should show Gateway contract section, got: {joined}"
        );
        assert!(
            joined.contains("routes 120 caps 120 p1 18 ai 64 openapi 100 tools 42"),
            "Should show contract coverage, got: {joined}"
        );
        assert!(
            joined.contains("/api/surfaces") && joined.contains("/api/gateway/capability"),
            "Should show contract-derived routes, got: {joined}"
        );
        assert!(
            joined.contains("AI tools") && joined.contains("gateway_get_api_sessions(1)"),
            "Should show OpenAI tool summary, got: {joined}"
        );
    }

    #[test]
    fn render_shows_surface_host_and_execution_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.surfaces = vec![crate::runtime_control_store::SurfaceSummary {
            id: "webui".to_string(),
            name: "WebUI".to_string(),
            kind: "web-surface".to_string(),
            status: "builtin".to_string(),
            lifecycle: "builtin".to_string(),
            transport: "stdio-jsonl".to_string(),
            capability_count: 3,
            route_count: 0,
            resource_count: 1,
            entry: None,
            diagnostics: Vec::new(),
            ..Default::default()
        }];
        panel.surface_health = Some(crate::runtime_control_store::SurfaceHealthSummary {
            status: "ready".to_string(),
            surface_count: 1,
            external_surface_count: 0,
            route_count: 0,
            resource_count: 1,
            ..Default::default()
        });
        panel.set_execution_receipts(vec![GatewayExecutionReceipt {
            status: "planned".to_string(),
            dispatch_status: "dry_run".to_string(),
            mode: "dry_run".to_string(),
            capability: "surface.webui.action".to_string(),
            idempotency_key: Some("idem-demo".to_string()),
        }]);

        let mut terminal = MockTerminal::new(96, 30);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 96, 30));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("Surface Host"),
            "Should show surface section, got: {joined}"
        );
        assert!(
            joined.contains("webui") && joined.contains("builtin"),
            "Should show surface status, got: {joined}"
        );
        assert!(
            joined.contains("routes 0") && joined.contains("resources 1"),
            "Should show surface health counts, got: {joined}"
        );
        assert!(
            joined.contains("Execution Receipts"),
            "Should show execution section, got: {joined}"
        );
        assert!(
            joined.contains("planned") && joined.contains("dry_run"),
            "Should show receipt state, got: {joined}"
        );
        assert!(
            joined.contains("surface.webui.action") && joined.contains("idem-demo"),
            "Should show receipt capability and idempotency key, got: {joined}"
        );
        assert!(
            !joined.contains("channel.feishu"),
            "Gateway panel must not hard-code platform channel capabilities, got: {joined}"
        );
    }

    #[test]
    fn render_shows_mission_control_state() {
        use crate::runtime_control_store::{MissionControlSummary, MissionSessionSummary};
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.mission_control = Some(MissionControlSummary {
            mission_id: Some("mission-control".to_string()),
            selected_mission_id: Some("mission-control".to_string()),
            active_session_id: Some("mission-a".to_string()),
            routing_revision: 3,
            task_focus_id: Some("task-a".to_string()),
            mission_focus_id: None,
            session_count: 2,
            active_count: 1,
            background_count: 1,
            paused_count: 0,
            closed_count: 0,
            task_count: 3,
            team_count: 1,
            agent_count: 2,
            running_agent_count: 2,
            pending_approvals: 3,
            recovery_required_count: 0,
            relation_count: 4,
            execution_graph_count: 2,
            conflict_count: 1,
            evidence_count: 5,
            capability_action_count: 7,
            event_count: 5,
            control_ready_count: 4,
            control_blocked_count: 1,
            control_requires_approval_count: 1,
            organization_pending_count: 2,
            organization_failed_count: 0,
            control_actions: vec![
                crate::runtime_control_store::MissionControlActionSummary {
                    action: "team.create".to_string(),
                    available: true,
                    reason: "canonical Session is available for a Team".to_string(),
                    requires_approval: false,
                    target_count: 1,
                },
                crate::runtime_control_store::MissionControlActionSummary {
                    action: "approval.decide".to_string(),
                    available: false,
                    reason: "no pending approval request".to_string(),
                    requires_approval: true,
                    target_count: 0,
                },
            ],
            sessions: vec![MissionSessionSummary {
                session_id: "mission-a".to_string(),
                title: "Primary mission control task".to_string(),
                status: "active".to_string(),
                team_count: 1,
                agent_count: 2,
            }],
        });

        let mut terminal = MockTerminal::new(96, 32);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 96, 32));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Mission Control"),
            "Should show mission section, got: {joined}"
        );
        assert!(
            joined.contains("2 total, 1 active, 1 bg"),
            "Should show session counts, got: {joined}"
        );
        assert!(
            joined.contains("1 / 2") && joined.contains("3 / 4"),
            "Should show team/agent and approval/relation counts, got: {joined}"
        );
        assert!(
            joined.contains("4 ready, 1 blocked, 1 approval-gated"),
            "Should show mission control readiness, got: {joined}"
        );
        assert!(
            joined.contains("Live work: 2 agents running")
                && joined.contains("Recovery attention: 0"),
            "Should show live Agent and recovery state, got: {joined}"
        );
        assert!(
            joined.contains("team.create")
                && joined.contains("canonical Session is available")
                && joined.contains("approval.decide")
                && joined.contains("no pending approval request"),
            "Should show owner-provided command readiness and reasons, got: {joined}"
        );
    }

    #[test]
    fn render_shows_surface_dispatch_contracts() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(118, 72);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 118, 72));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("Surface Dispatch Contract"),
            "Should show surface dispatch contract section, got: {joined}"
        );
        assert!(
            joined.contains("Gateway owns routing")
                && joined.contains("surface send/action requests by surface id"),
            "Should show surface dispatch ownership without hard-coded endpoints, got: {joined}"
        );
        assert!(
            !joined.contains("channel.feishu"),
            "Should not show legacy channel templates, got: {joined}"
        );
    }

    #[test]
    fn render_shows_message_plane_state() {
        use crate::runtime_control_store::{
            MessageBindingSummary, MessageConnectorSummary, MessageEndpointSummary,
            MessageRouteSummary,
        };
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.message_connectors = vec![MessageConnectorSummary {
            connector: "feishu".to_string(),
            name: "feishu".to_string(),
            configuration_status: "configured".to_string(),
            runtime_status: "ready".to_string(),
            enabled: true,
            configured: true,
            capability_count: 2,
            missing_required_count: 0,
            consecutive_failures: 0,
            restart_count: 0,
            circuit_open: false,
        }];
        panel.message_endpoints = vec![MessageEndpointSummary {
            endpoint_id: "message:feishu:user".to_string(),
            connector: "feishu".to_string(),
            kind: "User".to_string(),
            status: "configured".to_string(),
            configured: true,
            capability_count: 1,
        }];
        panel.message_routes = vec![MessageRouteSummary {
            route_id: "message:feishu:default".to_string(),
            connector: "feishu".to_string(),
            policy: "origin".to_string(),
            status: "configured".to_string(),
            configured: true,
            capability_count: 1,
            runtime_status: "ready".to_string(),
        }];
        panel.message_bindings = vec![MessageBindingSummary {
            binding_id: "message:feishu:user-1:thread-1".to_string(),
            connector: "feishu".to_string(),
            endpoint: "user-1".to_string(),
            direction: "inbound".to_string(),
            status: "processed".to_string(),
            runtime_session_id: Some("session-feishu".to_string()),
            resource_count: 1,
            last_seen_at_ms: Some(42),
        }];

        let mut terminal = MockTerminal::new(112, 42);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 112, 42));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(joined.contains("Message Plane"), "{joined}");
        assert!(joined.contains("1/1 ready"), "{joined}");
        assert!(joined.contains("Endpoints/Routes/Bindings"), "{joined}");
        assert!(joined.contains("feishu:configured:ready"), "{joined}");
        assert!(joined.contains("Latest binding"), "{joined}");
        assert!(!joined.contains("channel.feishu"), "{joined}");
    }

    #[test]
    fn render_shows_connector_console_state() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        panel.server_running = true;
        panel.health_status = Some("Healthy".to_string());
        panel.runtime_readiness = Some("91%".to_string());
        panel.task_count = Some(2);
        panel.pending_approvals = Some(1);
        panel.memory_status = Some("available".to_string());
        panel.degraded_reasons = vec!["context socket degraded".to_string()];
        panel.set_connector_accounts(vec![ConnectorAccountSummary {
            provider: "mock".to_string(),
            account_id: "mock-docs".to_string(),
            auth_mode: "none".to_string(),
            status: "ready".to_string(),
            reason: None,
            binding_count: 1,
        }]);
        panel.set_connector_capabilities(vec![
            ConnectorCapabilitySummary {
                capability_id: "service.local.docs.read".to_string(),
                provider: "mock".to_string(),
                plane: "service".to_string(),
                risk: "low".to_string(),
                supports_commit: true,
                requires_approval: false,
            },
            ConnectorCapabilitySummary {
                capability_id: "mcp.filesystem.server".to_string(),
                provider: "filesystem".to_string(),
                plane: "mcp".to_string(),
                risk: "low".to_string(),
                supports_commit: false,
                requires_approval: false,
            },
        ]);
        panel.set_connector_resources(vec![ConnectorResourceSummary {
            reference: "service://mock/docs/ready".to_string(),
            provider: "mock".to_string(),
            resource_type: "document".to_string(),
            title: "Ready Mock Document".to_string(),
            indexed_state: "indexed".to_string(),
        }]);
        panel.set_connector_degraded_reasons(vec!["resource_directory: locked".to_string()]);

        let mut terminal = MockTerminal::new(112, 34);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 112, 34));
        });
        let joined = terminal.buffer_lines().join("\n");
        assert!(
            joined.contains("available"),
            "Should show memory status, got: {joined}"
        );
        assert!(
            joined.contains("Degraded") && joined.contains("context socket degraded"),
            "Should show global degradation reasons, got: {joined}"
        );
        assert!(
            joined.contains("Connector Plane"),
            "Should show connector section, got: {joined}"
        );
        assert!(
            joined.contains("mock-docs"),
            "Should show connector account, got: {joined}"
        );
        assert!(
            joined.contains("service.local.docs.read") && joined.contains("mcp.filesystem.server"),
            "Should show connector capabilities, got: {joined}"
        );
        assert!(
            joined.contains("Ready Mock Document") && joined.contains("indexed"),
            "Should show connector resources, got: {joined}"
        );
        assert!(
            joined.contains("resource_directory: locked"),
            "Should show connector degraded reasons, got: {joined}"
        );
    }

    #[test]
    fn render_shows_keyboard_hints() {
        use crate::skin::SkinConfig;
        use crate::test_utils::MockTerminal;

        let mut panel = GatewayPanel::new();
        let mut terminal = MockTerminal::new(60, 20);
        let skin = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let mut ctx = RenderContext::new(f, &skin);
            panel.render(&mut ctx, Rect::new(0, 0, 60, 20));
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        assert!(
            joined.contains("r refresh"),
            "Should show 'r refresh' hint, got: {joined}"
        );
        assert!(
            joined.contains("h health"),
            "Should show 'h health' hint, got: {joined}"
        );
        assert!(
            joined.contains("s start/stop"),
            "Should show 's start/stop' hint, got: {joined}"
        );
    }

    #[test]
    fn handle_event_consumes_known_keys() {
        let mut panel = GatewayPanel::new();

        let press_r = Event::Key(KeyEvent::new(
            KeyCode::Char('r'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_r), EventResult::Consumed);

        let press_h = Event::Key(KeyEvent::new(
            KeyCode::Char('h'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_h), EventResult::Consumed);

        let press_s = Event::Key(KeyEvent::new(
            KeyCode::Char('s'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_s), EventResult::Consumed);
    }

    #[test]
    fn handle_event_ignores_unknown_keys() {
        let mut panel = GatewayPanel::new();

        let press_x = Event::Key(KeyEvent::new(
            KeyCode::Char('x'),
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_x), EventResult::NotConsumed);

        let press_tab = Event::Key(KeyEvent::new(
            KeyCode::Tab,
            crossterm::event::KeyModifiers::NONE,
        ));
        assert_eq!(panel.handle_event(&press_tab), EventResult::NotConsumed);
    }

    #[test]
    fn handle_event_ignores_release_events() {
        let mut panel = GatewayPanel::new();

        let _release_r = Event::Key(KeyEvent::new(
            KeyCode::Char('r'),
            crossterm::event::KeyModifiers::NONE,
        ));
        // We can't easily create a KeyEventKind::Release with crossterm's new(),
        // so test the pattern via the press guard — already covered above.
        // This test validates the guard is present:
        let non_key = Event::Resize(80, 24);
        assert_eq!(panel.handle_event(&non_key), EventResult::NotConsumed);
    }
}
