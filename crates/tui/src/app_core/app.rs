#![allow(dead_code)]
pub use crate::app_model::{
    ChatMessage, MessageIdentity, MessageSource, PendingInputPreview, PendingResource,
    SessionActivityStats, SessionSummary, SystemNotice, SystemNoticeKind, Theme, TimelineCausality,
    TimelineEntry, TimelinePage, ToolCard, TuiTelemetry,
};
use crate::app_model::{LiveMessageKey, ToolInstanceIdentity};
use crate::components::composer::model::ComposerModel;
use crate::components::turn_interaction::TurnInteractionState;
use crate::layout::{build_default_layout, LayoutState, LayoutTree};
use crate::runtime_control_store::{
    ApprovalGrantSummary, ApprovalSummary, ConnectorAccountSummary, ConnectorCapabilitySummary,
    ConnectorResourceSummary, CowdKernelSummary, FactFlowSummary, GatewayCapabilityContractSummary,
    MessageBindingSummary, MessageConnectorSummary, MessageEndpointSummary, MessageRouteSummary,
    MissionControlSummary, RealityCoreSummary, RuntimeActionReceiptSummary, StructuredDataSummary,
    SurfaceEventSummary, SurfaceHealthSummary, SurfaceSummary, TaskSummary,
};
use crate::CowdEvent;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

const PAGE_SIZE: usize = 500;
// Keep the complete transcript within Gateway's matching activation safety
// boundary. Above this explicit limit the view becomes a newest-message
// window and reports that fact instead of silently pretending history is
// complete.
const SOFT_CAP: usize = 50000;
const HARD_CAP: usize = 50000;

/// Narrow composition root for terminal read models. Each slice has one
/// lifecycle and stays below the structural field limit.
pub struct App {
    pub shell: AppShellState,
    pub timeline: TimelineState,
    pub workbench: WorkbenchViewState,
    pub gateway: GatewayViewState,
    pub execution: ExecutionViewState,
    pub history: HistoryViewState,
}

pub struct AppShellState {
    pub model: String,
    /// Model requested by the caller/session record. This is not proof that a
    /// provider actually used it.
    pub requested_model: Option<String>,
    /// Provider/model observed in Runtime's canonical live projection.
    pub effective_model: Option<String>,
    /// Canonical origin of the effective model fact.
    pub model_source: Option<String>,
    pub session_id: String,
    /// Gateway-owned Session execution preset projected into the TUI.
    pub execution_policy_preset: String,
    /// Canonical five-axis execution policy returned by Gateway. The TUI
    /// renders these actual values and never derives sandbox or approval from
    /// the preset label.
    pub execution_policy_snapshot: Option<harness_contract::policy::SessionExecutionPolicyResponse>,
    pub current_task: Option<CurrentTaskSummary>,
    /// Canonical composer bytes and cursor. Visual rows are derived from the
    /// actual terminal rectangle by `components::composer::layout`.
    pub input: ComposerModel,
    pub spinner_idx: usize,
    pub should_quit: bool,

    pub token_count: u64,
    pub compaction_count: u32,
    pub cache_hits: u64,
    pub picker_active: bool,
    pub picker_sessions: Vec<SessionSummary>,
    pub picker_idx: usize,
    pub theme: Theme,
    pub input_history: Vec<String>,
    pub history_idx: Option<usize>,
    pub help_visible: bool,
    pub available_models: Vec<String>,
    pub model_dirty: bool,
    pub notification: Option<String>,
    notification_ttl: u32,
    pub sessions: Vec<(String, String, String)>,
    pub active_session_name: String,
    pub layout_tree: LayoutTree,
    pub layout_state: LayoutState,
    pub compact_chat: bool,
}

pub struct TimelineState {
    pub timeline_pages: VecDeque<TimelinePage>,
    pub total_entries: usize,
    pub timeline_cursor: usize,
    /// Absolute position of logical timeline entry zero. Message identities
    /// point at absolute positions, so front eviction never turns every
    /// subsequent upsert into an O(n) reindex.
    timeline_base_position: u64,
    message_timeline_positions: HashMap<String, u64>,
    live_timeline_positions: HashMap<LiveMessageKey, u64>,
    tool_timeline_positions: HashMap<String, u64>,
    /// Provider-local id -> unfinished durable tool instance keys. History is
    /// ordered, so a result binds to the most recent unmatched use with the
    /// same provider id instead of overwriting an earlier turn.
    pending_history_tool_instances: HashMap<String, VecDeque<String>>,
    non_text_durable_owner: HashMap<String, String>,
    thinking_id_counter: u64,

    pub scroll_offset: usize,
    pub auto_scroll: bool,
    pub msg_version: u64,
    pub render_version: u64,
    pub timeline_full_sync_revision: u64,
    pub timeline_mutation_revision: u64,
    timeline_dirty_log: VecDeque<(u64, usize, String)>,
    pub last_drawn_version: u64,
    pub last_drawn_render_version: u64,
    pub cached_chat_lines: Vec<ratatui::text::Line<'static>>,
    pub entry_line_counts: Vec<usize>,
    pub lines_dirty: bool,
    last_built_line_count: usize,
    pub search_query: String,
    pub search_matches: Vec<usize>,
    pub search_current: usize,
    pub search_active: bool,
    searchable_content_revision: u64,
    search_index_revision: u64,
    search_text_index: Vec<String>,
    pub viewport_height: usize,
}

pub struct WorkbenchViewState {
    pub gateway_sessions: Vec<GatewaySession>,
    pub gateway_platform: String,
    pub file_entries: Vec<FileEntry>,
    pub delegate_tasks: Vec<DelegateTask>,
    pub memory_entries: Vec<MemoryEntry>,
    pub skill_list: Vec<SkillSummary>,
    pub pending_resources: Vec<PendingResource>,
    /// Current visible SessionIngress inputs, decoded from Gateway's
    /// canonical projection. Kept bounded for the terminal presentation.
    pub pending_inputs: Vec<PendingInputPreview>,
    pub system_notices: VecDeque<SystemNotice>,
    pub skin: crate::skin::SkinConfig,
    pub memory_status: Option<String>,
    pub memory_total_entries: Option<usize>,
    pub memory_vector_count: Option<usize>,
    pub memory_layer_counts: [usize; 5],
    pub memory_context_envelope_status: Option<String>,
    pub memory_context_envelope_compression: Option<String>,
    pub memory_context_envelope_used_ratio: Option<u64>,
    pub memory_context_envelope_checkpoint: Option<String>,
    pub memory_governance: Option<serde_json::Value>,
    /// Governed Runtime-to-Memory knowledge candidates.
    pub gateway_knowledge_candidates: Vec<crate::runtime_control_store::KnowledgeCandidateSummary>,

    /// Reputation score of the currently selected agent (if any).
    pub selected_agent_reputation: Option<f64>,

    /// Number of MCP servers connected.
    pub mcp_count: usize,
    /// Number of LSP servers available.
    pub lsp_available: usize,
    /// Number of pending permission requests.
    pub permission_count: usize,
}

pub struct GatewayViewState {
    /// Wave execution state for agentic loop tracking.
    pub wave_state: crate::components::status_bar::WaveState,

    /// Whether the API server is currently running.
    pub server_running: bool,
    /// Server uptime in seconds.
    pub server_uptime_secs: Option<u64>,
    /// Number of active API sessions.
    pub active_api_sessions: usize,
    /// Runtime host readiness summary from the HTTP projection API.
    pub gateway_runtime_readiness: Option<String>,
    /// Runtime host component count from the HTTP projection API.
    pub gateway_runtime_components: Option<u64>,
    /// Number of tasks observed through the Gateway API API.
    pub gateway_task_count: Option<u64>,
    /// Runtime host task summaries observed through the runtime control snapshot.
    pub gateway_tasks: Vec<TaskSummary>,
    /// Number of pending approvals observed through the Gateway API API.
    pub gateway_pending_approvals: Option<u64>,
    /// Pending approval summaries observed through the Gateway API API.
    pub gateway_approval_items: Vec<ApprovalSummary>,
    pub gateway_approval_grants: Vec<ApprovalGrantSummary>,
    /// Number of active cross-plane grants observed through the Gateway API API.
    pub gateway_cross_plane_grants_active: Option<u64>,
    /// Number of cross-plane interop actions observed over the last 24h.
    pub gateway_cross_plane_actions_24h: Option<u64>,
    /// Connector provider accounts observed through the Gateway API API.
    pub gateway_connector_accounts: Vec<ConnectorAccountSummary>,
    /// Connector capabilities observed through the Gateway API API.
    pub gateway_connector_capabilities: Vec<ConnectorCapabilitySummary>,
    /// Connector resources observed through the Gateway API API.
    pub gateway_connector_resources: Vec<ConnectorResourceSummary>,
    /// Recent runtime action receipts produced by TUI controls.
    pub gateway_action_receipts: Vec<RuntimeActionReceiptSummary>,
    /// Surface registry summaries observed through Gateway SurfaceHost.
    pub gateway_surfaces: Vec<SurfaceSummary>,
    /// Surface host health observed through Gateway SurfaceHost.
    pub gateway_surface_health: Option<SurfaceHealthSummary>,
    /// Recent surface events observed through Gateway SurfaceHost.
    pub gateway_surface_events: Vec<SurfaceEventSummary>,
    /// Message connector readiness and runtime summaries observed through Gateway.
    pub gateway_message_connectors: Vec<MessageConnectorSummary>,
    /// Message endpoint directory observed through Gateway.
    pub gateway_message_endpoints: Vec<MessageEndpointSummary>,
    /// Message delivery routes observed through Gateway.
    pub gateway_message_routes: Vec<MessageRouteSummary>,
    /// Message conversation bindings observed through Gateway.
    pub gateway_message_bindings: Vec<MessageBindingSummary>,
    /// Cowd kernel capability and release-gate summary observed through projection API.
    pub gateway_cowd_kernel: Option<CowdKernelSummary>,
    /// Gateway-owned API capability contract summary observed through Gateway contract API.
    pub gateway_capability_contract: Option<GatewayCapabilityContractSummary>,
    /// Structured data-plane summary observed through projection API.
    pub gateway_structured_data: Option<StructuredDataSummary>,
    /// Reality Core engine health observed through Gateway projection API.
    pub gateway_reality_core: Option<RealityCoreSummary>,
    /// Fact Flow trace summary observed through Gateway projection API.
    pub gateway_fact_flow: Option<FactFlowSummary>,
    /// Mission Runtime global control summary observed through Gateway projection API.
    pub gateway_mission_control: Option<MissionControlSummary>,
    /// Typed Mission materialized view used to apply cursor/revision deltas.
    pub gateway_mission_materialized:
        Option<harness_contract::mission::MissionMaterializedSnapshot>,
    /// Connector-specific degraded reasons observed through the Gateway API API.
    pub gateway_connector_degraded_reasons: Vec<String>,
    /// Degraded Gateway API/control reasons collected during snapshot refresh.
    pub gateway_degraded_reasons: Vec<String>,
    /// Current runtime session lease owner for the attached TUI session.
    pub gateway_lease_owner: Option<String>,
    /// Current runtime session lease mode for the attached TUI session.
    pub gateway_lease_mode: Option<String>,
}

pub struct ExecutionViewState {
    /// Canonical TUI transport/execution presentation state.  Timeline text
    /// and legacy stream events may decorate a turn but cannot decide whether
    /// a Runtime execution is active.
    pub turn_interaction: TurnInteractionState,
    streaming_received: bool,
    /// Bounded causal tombstones for executions that already emitted a
    /// durable terminal. Projection and session streams are independent, so
    /// an older non-terminal snapshot can otherwise reopen a completed turn
    /// and make a passive Surface reject the next execution's deltas.
    terminal_correlations: VecDeque<(String, String)>,
    /// Durable ingress identities that may later become the active execution.
    /// Session input projections own queue state; this set only lets a
    /// subsequent live event prove that its already-committed execution has
    /// started even when an intermediate phase envelope was coalesced.
    committed_ingress_correlations: BTreeSet<(String, String)>,

    pub context_window: u64,
    pub latest_context_envelope: Option<Value>,
    pub latest_runtime_policy: Option<crate::RuntimePolicyDecisionSummary>,
    pub latest_execution_graph_summary: Option<crate::RuntimeExecutionGraphSummary>,
    /// Canonical execution snapshot received from Gateway. This is the only
    /// TUI source for graph lifecycle state; stream prose remains display-only.
    pub latest_execution_projection: Option<crate::protocol::ExecutionProjection>,
    pub current_execution_status: Option<harness_contract::projection::ExecutionLiveStatus>,
    pub current_execution_status_detail: Option<String>,
    pub current_execution_id: Option<String>,
    pub current_turn_id: Option<String>,
    pub execution_started_at_ms: Option<u64>,
    pub last_progress_at_ms: Option<u64>,
    pub current_run_metrics: Option<harness_contract::projection::RunMetricsProjection>,
    pub current_execution_latency: Option<harness_contract::projection::ExecutionLatencyProjection>,
    pub latest_model_telemetry: Option<crate::protocol::RunModelTelemetryProjection>,
    pub context_used_tokens: Option<u64>,
    pub context_window_tokens: Option<u64>,
    pub context_remaining_tokens: Option<u64>,
    pub context_usage_percent_bp: Option<u16>,
    pub context_usage_source: Option<String>,
    pub telemetry: TuiTelemetry,
    pub stream_connection_state: crate::protocol::SessionStreamConnectionState,
    pub projection_connection_state: Option<crate::protocol::SessionStreamConnectionState>,
    pub live_output_snapshot_gap: bool,
    live_stream_revisions: HashMap<LiveMessageKey, u64>,
    seen_terminal_ids: BTreeSet<String>,
    seen_cancellation_ids: BTreeMap<String, harness_contract::turn::CancellationStatus>,
    hydrated_non_text_message_ids: BTreeSet<String>,
    pending_message_admissions: BTreeMap<String, u64>,
    pub turn_input_tokens: u64,
    pub turn_output_tokens: u64,
    pub turn_usage_known: bool,
    pub current_turn_tool_count: usize,
    pub current_turn_thinking_count: usize,
    pre_turn_input: u64,
    pre_turn_output: u64,
}

pub struct HistoryViewState {
    pub history_hydrated: bool,
    pub session_history_index: Option<crate::protocol::SessionHistoryIndexProjection>,
    pub history_hydration_error: Option<String>,
    pub history_window_truncated: bool,
    pub history_oldest_offset: usize,
    pub history_window_end_offset: usize,
    pub history_total_messages: usize,
    pub history_has_older: bool,
    pub history_loading_older: bool,
    pub history_loading_newer: bool,
    pub history_prepend_revision: u64,
    pub history_prepend_anchor_message_id: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub durable_session_input_tokens: u64,
    pub durable_session_output_tokens: u64,
    /// Gateway-owned full-session totals. These remain distinct from
    /// `durable_session_*`, which only cover the currently hydrated window.
    pub authoritative_session_input_tokens: Option<u64>,
    pub authoritative_session_output_tokens: Option<u64>,
    durable_message_usage: BTreeMap<String, (u64, u64)>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryEntry {
    pub id: Option<String>,
    pub layer: String,
    pub content: String,
    pub priority: String,
}

#[derive(Debug, Clone)]
pub struct GatewaySession {
    pub platform: String,
    pub id: String,
    pub title: String,
    pub message_count: usize,
}

#[derive(Debug, Clone)]
pub struct SkillSummary {
    pub id: String,
    pub name: String,
    pub description: String,
    pub installed: bool,
    pub category: String,
    pub source: String,
    pub status: String,
    pub risk: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct DelegateTask {
    pub id: String,
    pub description: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTaskSummary {
    pub id: String,
    pub objective: String,
    pub status: String,
    pub current_phase: Option<String>,
    pub phase_status: Option<String>,
    pub review_result: Option<String>,
    pub artifact_count: usize,
    pub blocker_reason: Option<String>,
}

impl App {
    pub fn new(model: &str, session_id: &str) -> Self {
        Self {
            shell: AppShellState {
                model: model.to_string(),
                requested_model: (!model.trim().is_empty()
                    && model != "default"
                    && model != "unresolved")
                    .then(|| model.to_string()),
                effective_model: None,
                model_source: None,
                session_id: session_id.to_string(),
                execution_policy_preset: "unavailable".to_string(),
                execution_policy_snapshot: None,
                current_task: None,
                input: ComposerModel::default(),
                spinner_idx: 0,
                should_quit: false,
                token_count: 0,
                compaction_count: 0,
                cache_hits: 0,
                picker_active: false,
                picker_sessions: Vec::new(),
                picker_idx: 0,
                theme: Theme::Dark,
                input_history: Vec::new(),
                history_idx: None,
                help_visible: false,
                available_models: vec![model.to_string()],
                model_dirty: false,
                notification: None,
                notification_ttl: 0,
                sessions: Vec::new(),
                active_session_name: String::new(),
                layout_tree: build_default_layout(),
                layout_state: LayoutState::new(),
                compact_chat: false,
            },
            timeline: TimelineState {
                timeline_pages: VecDeque::new(),
                total_entries: 0,
                timeline_cursor: 0,
                timeline_base_position: 0,
                message_timeline_positions: HashMap::new(),
                live_timeline_positions: HashMap::new(),
                tool_timeline_positions: HashMap::new(),
                pending_history_tool_instances: HashMap::new(),
                non_text_durable_owner: HashMap::new(),
                thinking_id_counter: 0,
                scroll_offset: 0,
                auto_scroll: true,
                msg_version: 0,
                render_version: 0,
                timeline_full_sync_revision: 0,
                timeline_mutation_revision: 0,
                timeline_dirty_log: VecDeque::new(),
                last_drawn_version: u64::MAX,
                last_drawn_render_version: u64::MAX,
                cached_chat_lines: Vec::new(),
                entry_line_counts: Vec::new(),
                lines_dirty: true,
                last_built_line_count: 0,
                search_query: String::new(),
                search_matches: Vec::new(),
                search_current: 0,
                search_active: false,
                searchable_content_revision: 0,
                search_index_revision: u64::MAX,
                search_text_index: Vec::new(),
                viewport_height: 24,
            },
            workbench: WorkbenchViewState {
                gateway_sessions: Vec::new(),
                gateway_platform: String::new(),
                file_entries: Vec::new(),
                delegate_tasks: Vec::new(),
                memory_entries: Vec::new(),
                skill_list: Vec::new(),
                pending_resources: Vec::new(),
                pending_inputs: Vec::new(),
                system_notices: VecDeque::new(),
                skin: crate::skin::SkinConfig::default(),
                memory_status: None,
                memory_total_entries: None,
                memory_vector_count: None,
                memory_layer_counts: [0; 5],
                memory_context_envelope_status: None,
                memory_context_envelope_compression: None,
                memory_context_envelope_used_ratio: None,
                memory_context_envelope_checkpoint: None,
                memory_governance: None,
                gateway_knowledge_candidates: Vec::new(),
                selected_agent_reputation: None,
                mcp_count: 0,
                lsp_available: 0,
                permission_count: 0,
            },
            gateway: GatewayViewState {
                wave_state: crate::components::status_bar::WaveState::default(),
                server_running: false,
                server_uptime_secs: None,
                active_api_sessions: 0,
                gateway_runtime_readiness: None,
                gateway_runtime_components: None,
                gateway_task_count: None,
                gateway_tasks: Vec::new(),
                gateway_pending_approvals: None,
                gateway_approval_items: Vec::new(),
                gateway_approval_grants: Vec::new(),
                gateway_cross_plane_grants_active: None,
                gateway_cross_plane_actions_24h: None,
                gateway_connector_accounts: Vec::new(),
                gateway_connector_capabilities: Vec::new(),
                gateway_connector_resources: Vec::new(),
                gateway_action_receipts: Vec::new(),
                gateway_surfaces: Vec::new(),
                gateway_surface_health: None,
                gateway_surface_events: Vec::new(),
                gateway_message_connectors: Vec::new(),
                gateway_message_endpoints: Vec::new(),
                gateway_message_routes: Vec::new(),
                gateway_message_bindings: Vec::new(),
                gateway_cowd_kernel: None,
                gateway_capability_contract: None,
                gateway_structured_data: None,
                gateway_reality_core: None,
                gateway_fact_flow: None,
                gateway_mission_control: None,
                gateway_mission_materialized: None,
                gateway_connector_degraded_reasons: Vec::new(),
                gateway_degraded_reasons: Vec::new(),
                gateway_lease_owner: None,
                gateway_lease_mode: None,
            },
            execution: ExecutionViewState {
                turn_interaction: TurnInteractionState::default(),
                streaming_received: false,
                terminal_correlations: VecDeque::new(),
                committed_ingress_correlations: BTreeSet::new(),
                context_window: 0,
                latest_context_envelope: None,
                latest_runtime_policy: None,
                latest_execution_graph_summary: None,
                latest_execution_projection: None,
                current_execution_status: None,
                current_execution_status_detail: None,
                current_execution_id: None,
                current_turn_id: None,
                execution_started_at_ms: None,
                last_progress_at_ms: None,
                current_run_metrics: None,
                current_execution_latency: None,
                latest_model_telemetry: None,
                context_used_tokens: None,
                context_window_tokens: None,
                context_remaining_tokens: None,
                context_usage_percent_bp: None,
                context_usage_source: None,
                telemetry: TuiTelemetry::default(),
                stream_connection_state: crate::protocol::SessionStreamConnectionState::Connecting,
                projection_connection_state: None,
                live_output_snapshot_gap: false,
                live_stream_revisions: HashMap::new(),
                seen_terminal_ids: BTreeSet::new(),
                seen_cancellation_ids: BTreeMap::new(),
                hydrated_non_text_message_ids: BTreeSet::new(),
                pending_message_admissions: BTreeMap::new(),
                turn_input_tokens: 0,
                turn_output_tokens: 0,
                turn_usage_known: false,
                current_turn_tool_count: 0,
                current_turn_thinking_count: 0,
                pre_turn_input: 0,
                pre_turn_output: 0,
            },
            history: HistoryViewState {
                history_hydrated: false,
                session_history_index: None,
                history_hydration_error: None,
                history_window_truncated: false,
                history_oldest_offset: 0,
                history_window_end_offset: 0,
                history_total_messages: 0,
                history_has_older: false,
                history_loading_older: false,
                history_loading_newer: false,
                history_prepend_revision: 0,
                history_prepend_anchor_message_id: None,
                input_tokens: 0,
                output_tokens: 0,
                durable_session_input_tokens: 0,
                durable_session_output_tokens: 0,
                authoritative_session_input_tokens: None,
                authoritative_session_output_tokens: None,
                durable_message_usage: BTreeMap::new(),
            },
        }
    }

    pub fn refresh_model_mismatch_telemetry(&mut self) {
        let mismatch = self
            .shell
            .requested_model
            .as_deref()
            .zip(self.shell.effective_model.as_deref())
            .is_some_and(|(requested, effective)| requested != effective);
        if mismatch && !self.execution.telemetry.model_mismatch_active {
            self.execution.telemetry.model_mismatch_count = self
                .execution
                .telemetry
                .model_mismatch_count
                .saturating_add(1);
            tracing::warn!(
                session_id = %self.shell.session_id,
                requested_model = self.shell.requested_model.as_deref().unwrap_or("unknown"),
                effective_model = self.shell.effective_model.as_deref().unwrap_or("unknown"),
                mismatch_count = self.execution.telemetry.model_mismatch_count,
                "TUI observed a requested/effective model mismatch"
            );
        }
        self.execution.telemetry.model_mismatch_active = mismatch;
    }

    pub fn apply_session_stats(&mut self, stats: Value) {
        if stats
            .get("session_id")
            .and_then(Value::as_str)
            .is_some_and(|session_id| session_id != self.shell.session_id)
        {
            return;
        }
        if let Some(tokens) = stats.get("tokens") {
            self.history.authoritative_session_input_tokens =
                tokens.get("input").and_then(Value::as_u64);
            self.history.authoritative_session_output_tokens =
                tokens.get("output").and_then(Value::as_u64);
        }
        if let Some(total) = stats.pointer("/tokens/total").and_then(Value::as_u64) {
            self.shell.token_count = total;
        }
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
    }

    /// Install the canonical execution snapshot if it is not an older replay
    /// for the same execution.  All derived TUI views read this owner, so this
    /// is the single monotonicity gate rather than allowing one panel to keep
    /// a newer revision while another is overwritten by delayed SSE data.
    pub fn apply_execution_projection(
        &mut self,
        mut projection: crate::protocol::ExecutionProjection,
    ) -> bool {
        if self.turn_is_active()
            && self
                .execution
                .current_execution_id
                .as_deref()
                .is_some_and(|current| current != projection.execution_id.as_str())
        {
            return false;
        }
        if self.execution_is_terminalized(&projection.execution_id)
            && projection
                .live
                .as_ref()
                .is_none_or(|live| !live.status.is_terminal())
        {
            return false;
        }
        if let Some(current) = self
            .execution
            .latest_execution_projection
            .as_ref()
            .filter(|current| current.execution_id == projection.execution_id)
        {
            let current_live_revision = current.live.as_ref().map(|live| live.revision);
            let incoming_live_revision = projection.live.as_ref().map(|live| live.revision);
            if projection.revision < current.revision {
                if incoming_live_revision
                    .zip(current_live_revision)
                    .is_some_and(|(incoming, current)| incoming > current)
                {
                    // A live snapshot and the graph ledger advance on separate
                    // monotonic streams. Preserve the newer graph while
                    // accepting a strictly newer live state.
                    let live = projection.live.take();
                    projection = current.clone();
                    projection.live = live;
                } else {
                    return false;
                }
            } else if incoming_live_revision
                .zip(current_live_revision)
                .is_some_and(|(incoming, current)| incoming < current)
            {
                // Never let a newer graph snapshot roll back provider/context
                // telemetry that was already observed on the live stream.
                projection.live = current.live.clone();
            }
        }
        let cancellation_receipt = projection.cancellation_receipt.clone();
        let terminal_presentation = projection.terminal_presentation.clone();
        let snapshot_has_active_root = terminal_presentation.as_ref().is_some_and(|presentation| {
            matches!(
                presentation.state,
                harness_contract::outcome::TerminalPresentationState::Started
                    | harness_contract::outcome::TerminalPresentationState::Streaming
                    | harness_contract::outcome::TerminalPresentationState::Validating
            )
        });
        let snapshot_has_durable_winner = self.execution_is_terminalized(&projection.execution_id)
            || terminal_presentation.as_ref().is_some_and(|presentation| {
                presentation.state
                    == harness_contract::outcome::TerminalPresentationState::Committed
            })
            || cancellation_receipt.as_ref().is_some_and(|receipt| {
                matches!(
                    receipt.status,
                    harness_contract::turn::CancellationStatus::Cancelled
                        | harness_contract::turn::CancellationStatus::AlreadyTerminal
                )
            });
        let snapshot_execution_id = projection.execution_id.clone();
        let snapshot_turn_id = projection
            .live
            .as_ref()
            .and_then(|live| live.turn_id.clone())
            .or_else(|| projection.turn_id.clone())
            .or_else(|| self.execution.current_turn_id.clone());
        let total_nodes = projection.graph.nodes.len();
        let terminal_nodes = projection
            .graph
            .nodes
            .iter()
            .filter(|node| node.status.is_terminal())
            .count();
        let status = if projection.graph.nodes.iter().any(|node| {
            matches!(
                node.status,
                harness_contract::execution_graph::ExecutionNodeStatus::Failed
            )
        }) {
            "failed"
        } else if total_nodes > 0 && terminal_nodes == total_nodes {
            "terminal"
        } else if projection.graph.nodes.iter().any(|node| {
            matches!(
                node.status,
                harness_contract::execution_graph::ExecutionNodeStatus::WaitingExternal
            )
        }) {
            "waiting_external"
        } else {
            "running"
        };
        self.execution.latest_execution_graph_summary = Some(crate::RuntimeExecutionGraphSummary {
            graph_id: Some(projection.execution_id.clone()),
            board_id: None,
            status: status.to_string(),
            agent_tasks: projection.agents.len(),
            child_executions: projection.child_executions.len(),
            memory_candidates: projection.context.len(),
            conflicts: projection
                .interventions
                .iter()
                .filter(|item| item.status.as_deref() == Some("blocked"))
                .count(),
            completion_rate: (total_nodes > 0)
                .then_some(terminal_nodes as f32 / total_nodes as f32),
            synthesis_lift: None,
            complementarity_score: None,
        });
        let preserved_model_telemetry = (self.execution.current_execution_id.as_deref()
            == Some(projection.execution_id.as_str()))
        .then(|| self.execution.latest_model_telemetry.clone())
        .flatten();
        if let Some(live) = projection.live.as_ref() {
            self.install_execution_live_facts(
                &projection.execution_id,
                live,
                preserved_model_telemetry,
            );
        } else {
            self.reset_live_execution_facts();
            self.execution.latest_model_telemetry = preserved_model_telemetry;
            // The execution identity is canonical even before Runtime has
            // materialized live facts. Every other field remains unknown.
            self.execution.current_execution_id = Some(projection.execution_id.clone());
        }
        self.execution.latest_execution_projection = Some(projection);
        if !snapshot_has_active_root
            && !snapshot_has_durable_winner
            && self
                .execution
                .turn_interaction
                .clear_root_presentation_from_snapshot()
        {
            // Abort/Supersede is reconstructible and may be dropped at a slow
            // Surface boundary. The canonical snapshot wins: remove the
            // orphaned preview without declaring the execution terminal.
            self.remove_stale_live_assistant_parts(
                Some(&snapshot_execution_id),
                snapshot_turn_id.as_deref(),
            );
        }
        if let Some(presentation) = terminal_presentation {
            match presentation.state {
                harness_contract::outcome::TerminalPresentationState::Started
                | harness_contract::outcome::TerminalPresentationState::Streaming
                | harness_contract::outcome::TerminalPresentationState::Validating => {
                    self.execution.turn_interaction.begin_root_presentation(
                        presentation.presentation_id,
                        presentation.attempt_id,
                        presentation.envelope_id,
                        presentation.envelope_revision,
                    );
                }
                harness_contract::outcome::TerminalPresentationState::Committed
                | harness_contract::outcome::TerminalPresentationState::Aborted
                | harness_contract::outcome::TerminalPresentationState::Superseded => {
                    self.execution.turn_interaction.end_root_presentation(
                        &presentation.presentation_id,
                        &presentation.attempt_id,
                    );
                }
            }
        }
        if let Some(receipt) = cancellation_receipt {
            self.apply_cancellation_receipt(receipt);
        }
        self.refresh_model_mismatch_telemetry();
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
        true
    }

    /// Apply a coalesced live-only update without rebuilding or retransmitting
    /// the durable graph/entity projection.
    pub fn apply_execution_live_update(
        &mut self,
        update: crate::protocol::ExecutionLiveUpdate,
    ) -> bool {
        if update.schema_version
            != harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION
        {
            return false;
        }
        if self.execution_is_terminalized(&update.execution_id) && !update.live.status.is_terminal()
        {
            return false;
        }
        let Some(current) = self
            .execution
            .latest_execution_projection
            .as_ref()
            .filter(|projection| projection.execution_id == update.execution_id)
        else {
            return false;
        };
        if current
            .live
            .as_ref()
            .is_some_and(|live| live.revision >= update.live.revision)
        {
            return false;
        }
        let preserved_model_telemetry = (self.execution.current_execution_id.as_deref()
            == Some(update.execution_id.as_str()))
        .then(|| self.execution.latest_model_telemetry.clone())
        .flatten();
        if let Some(projection) = self.execution.latest_execution_projection.as_mut() {
            projection.live = Some(update.live.clone());
        }
        self.install_execution_live_facts(
            &update.execution_id,
            &update.live,
            preserved_model_telemetry,
        );
        self.refresh_model_mismatch_telemetry();
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
        true
    }

    fn install_execution_live_facts(
        &mut self,
        execution_id: &str,
        live: &harness_contract::projection::ExecutionLiveState,
        preserved_model_telemetry: Option<crate::protocol::RunModelTelemetryProjection>,
    ) {
        if live.status == harness_contract::projection::ExecutionLiveStatus::Cancelled {
            self.remove_stale_live_assistant_parts(execution_id.into(), live.turn_id.as_deref());
            self.execution.turn_interaction.terminal_observed();
            self.record_terminal_correlation(&crate::protocol::GatewayEventCorrelation {
                session_id: self.shell.session_id.clone(),
                execution_id: Some(execution_id.to_string()),
                turn_id: live.turn_id.clone(),
                ..Default::default()
            });
        } else {
            self.reconcile_live_output_parts(
                execution_id,
                live.turn_id.as_deref(),
                &live.output_parts,
                live.output_bytes,
            );
        }
        self.reset_live_execution_facts();
        self.execution.latest_model_telemetry = preserved_model_telemetry;
        self.execution.current_execution_status = Some(live.status);
        self.execution.current_execution_status_detail = live.status_detail.clone();
        self.execution.current_execution_id = Some(execution_id.to_string());
        self.execution.current_turn_id = live.turn_id.clone();
        self.execution.execution_started_at_ms = Some(live.started_at_ms);
        self.execution.last_progress_at_ms = Some(live.last_progress_at_ms);
        self.execution.current_run_metrics = Some(live.metrics.clone());
        self.execution.current_execution_latency = Some(live.latency.clone());
        self.execution.turn_input_tokens = live.metrics.input_tokens;
        self.execution.turn_output_tokens = live.metrics.output_tokens;
        self.execution.turn_usage_known = live.context_usage.as_ref().is_some_and(|usage| {
            usage
                .input_source
                .as_deref()
                .is_some_and(|source| source != "runtime_request_budget_estimate")
        });
        self.history.input_tokens = live.metrics.input_tokens;
        self.history.output_tokens = live.metrics.output_tokens;
        self.shell.token_count = live.metrics.total_tokens;
        if let Some(context) = live.context_usage.as_ref() {
            self.shell.effective_model = context.model.clone();
            if self.shell.effective_model.is_some() {
                self.shell.model_source =
                    Some("runtime.execution_live.context_usage.model".to_string());
            }
            self.execution.context_used_tokens = context.input_tokens;
            self.execution.context_window_tokens = context.window_tokens;
            self.execution.context_remaining_tokens = context.remaining_tokens;
            self.execution.context_usage_percent_bp = context.usage_percent_bp;
            self.execution.context_usage_source = context.input_source.clone();
            self.execution.context_window = context.window_tokens.unwrap_or_default();
        }
    }

    fn reconcile_live_output_parts(
        &mut self,
        execution_id: &str,
        turn_id: Option<&str>,
        parts: &[harness_contract::projection::ExecutionLiveOutputPart],
        output_bytes: u64,
    ) {
        self.execution.live_output_snapshot_gap = false;
        if self
            .timeline_correlated_assistant_index(Some(execution_id), turn_id)
            .is_some_and(|index| {
                matches!(
                    self.timeline_get(index),
                    Some(TimelineEntry::Message {
                        identity: Some(MessageIdentity {
                            source: MessageSource::DurableHistory | MessageSource::DurableTerminal,
                            ..
                        }),
                        ..
                    })
                )
            })
        {
            return;
        }
        if parts.is_empty() {
            self.execution.live_output_snapshot_gap = output_bytes > 0;
            return;
        }
        let mut recovered_bytes = 0_u64;
        let mut ordered = parts.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|part| part.causal_sequence);
        for part in ordered {
            recovered_bytes = recovered_bytes.saturating_add(part.bytes);
            let Some(preview) = part.preview.as_deref() else {
                self.execution.live_output_snapshot_gap |= part.bytes > 0;
                continue;
            };
            let Ok(preview_start) = usize::try_from(part.preview_start_bytes) else {
                self.execution.live_output_snapshot_gap = true;
                continue;
            };
            let part_id = Some(part.part_id.as_str());
            if let Some(index) =
                self.timeline_live_message_index(Some(execution_id), turn_id, part_id)
            {
                let mut repaired = false;
                if let Some(TimelineEntry::Message { content, .. }) = self.timeline_get_mut(index) {
                    if content.len() >= preview_start && content.is_char_boundary(preview_start) {
                        content.truncate(preview_start);
                        content.push_str(preview);
                        repaired = true;
                    }
                }
                if repaired {
                    self.note_searchable_content_changed();
                } else {
                    self.execution.live_output_snapshot_gap = true;
                }
            } else if preview_start == 0 {
                self.timeline_push(TimelineEntry::Message {
                    role: "assistant".to_string(),
                    content: preview.to_string(),
                    timestamp: App::format_timestamp(),
                    identity: Some(MessageIdentity {
                        message_id: None,
                        sequence: None,
                        execution_id: Some(execution_id.to_string()),
                        turn_id: turn_id.map(ToOwned::to_owned),
                        part_id: Some(part.part_id.clone()),
                        source: MessageSource::Live,
                    }),
                });
            } else {
                self.execution.live_output_snapshot_gap = true;
            }
        }
        if recovered_bytes != output_bytes {
            self.execution.live_output_snapshot_gap = true;
        }
        if self.execution.live_output_snapshot_gap {
            self.add_system_notice(
                SystemNoticeKind::Warning,
                "Canonical live output has an incomplete per-item byte range; preserving verified segments until the durable terminal arrives",
            );
        }
    }

    fn reset_live_execution_facts(&mut self) {
        self.execution.current_execution_status = None;
        self.execution.current_execution_status_detail = None;
        self.execution.current_execution_id = None;
        self.execution.current_turn_id = None;
        self.execution.execution_started_at_ms = None;
        self.execution.last_progress_at_ms = None;
        self.execution.current_run_metrics = None;
        self.execution.current_execution_latency = None;
        self.execution.latest_model_telemetry = None;
        self.shell.effective_model = None;
        self.shell.model_source = None;
        self.execution.context_used_tokens = None;
        self.execution.context_window_tokens = None;
        self.execution.context_remaining_tokens = None;
        self.execution.context_usage_percent_bp = None;
        self.execution.context_usage_source = None;
        self.execution.context_window = 0;
        self.execution.live_stream_revisions.clear();
        self.execution.turn_input_tokens = 0;
        self.execution.turn_output_tokens = 0;
        self.execution.turn_usage_known = false;
        self.history.input_tokens = 0;
        self.history.output_tokens = 0;
        self.shell.token_count = 0;
    }

    /// Drop an execution projection as soon as Gateway revokes the caller or
    /// rejects its contract.  Retaining the last full snapshot would keep
    /// strategy, agent and evidence detail visible after the authority that
    /// produced it has expired.
    pub fn invalidate_execution_projection(&mut self, execution_id: &str) -> bool {
        let matches_projection = self
            .execution
            .latest_execution_projection
            .as_ref()
            .is_some_and(|projection| projection.execution_id == execution_id);
        let matches_current_execution =
            self.execution.current_execution_id.as_deref() == Some(execution_id);
        let matches_interaction = self
            .execution
            .turn_interaction
            .execution
            .execution_id
            .as_deref()
            == Some(execution_id);
        if !matches_projection && !matches_current_execution && !matches_interaction {
            return false;
        }
        if matches_projection {
            self.execution.latest_execution_projection = None;
        }
        if matches_projection || matches_current_execution {
            self.reset_live_execution_facts();
        }
        if self
            .execution
            .latest_execution_graph_summary
            .as_ref()
            .is_some_and(|summary| summary.graph_id.as_deref() == Some(execution_id))
        {
            self.execution.latest_execution_graph_summary = None;
        }
        self.execution
            .turn_interaction
            .clear_execution_if_matches(execution_id);
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
        true
    }

    /// Remove every session-derived projection immediately when Gateway
    /// revokes this observer. Reusing selected fields is intentionally
    /// avoided: transcript, evidence, model facts, drafts and cached panels
    /// all belonged to the expired authority.
    pub fn revoke_session_authorization(&mut self, reason: &str) {
        let session_id = self.shell.session_id.clone();
        let skin = self.workbench.skin.clone();
        let theme = self.shell.theme;
        let execution_policy_preset = self.shell.execution_policy_preset.clone();
        let execution_policy_snapshot = self.shell.execution_policy_snapshot.clone();
        let mut clean = Self::new("unavailable", &session_id);
        clean.workbench.skin = skin;
        clean.shell.theme = theme;
        clean.shell.execution_policy_preset = execution_policy_preset;
        clean.shell.execution_policy_snapshot = execution_policy_snapshot;
        clean.history.history_hydration_error = Some("session authorization revoked".to_string());
        clean.add_system_notice(
            SystemNoticeKind::Error,
            &format!("Session authorization revoked: {reason}"),
        );
        clean.show_notification(
            "Session authorization revoked; sensitive session state was cleared",
        );
        *self = clean;
    }

    pub fn mark_dirty(&mut self) {
        self.timeline.lines_dirty = true;
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
    }

    /// Request a frame without invalidating the width-aware transcript cache.
    /// Composer edits, focus movement, search selection and modal state do not
    /// change timeline content.
    pub fn request_redraw(&mut self) {
        self.timeline.render_version = self.timeline.render_version.wrapping_add(1);
    }

    fn message_timestamp(created_at_ms: u64) -> String {
        let secs = created_at_ms / 1_000;
        let h = (secs / 3_600) % 24;
        let m = (secs / 60) % 60;
        format!("{h:02}:{m:02}")
    }

    fn message_source_rank(source: MessageSource) -> u8 {
        match source {
            MessageSource::Local => 0,
            MessageSource::Live => 1,
            MessageSource::DurableIngress => 2,
            MessageSource::ReplayedTerminal => 3,
            MessageSource::DurableTerminal => 4,
            // Once history has been read back from the store it is the
            // transcript authority, regardless of live/history arrival order.
            MessageSource::DurableHistory => 5,
        }
    }

    fn merge_message_identity(
        existing: Option<MessageIdentity>,
        incoming: MessageIdentity,
    ) -> MessageIdentity {
        let Some(existing) = existing else {
            return incoming;
        };
        let source = if Self::message_source_rank(existing.source)
            >= Self::message_source_rank(incoming.source)
        {
            existing.source
        } else {
            incoming.source
        };
        MessageIdentity {
            message_id: incoming.message_id.or(existing.message_id),
            sequence: incoming.sequence.or(existing.sequence),
            execution_id: incoming.execution_id.or(existing.execution_id),
            turn_id: incoming.turn_id.or(existing.turn_id),
            part_id: incoming.part_id.or(existing.part_id),
            source,
        }
    }

    fn timeline_message_index(&self, message_id: &str) -> Option<usize> {
        let absolute = *self.timeline.message_timeline_positions.get(message_id)?;
        self.logical_timeline_index(absolute)
    }

    fn logical_timeline_index(&self, absolute: u64) -> Option<usize> {
        let logical = absolute.checked_sub(self.timeline.timeline_base_position)?;
        usize::try_from(logical)
            .ok()
            .filter(|index| *index < self.timeline.total_entries)
    }

    fn timeline_message_by_id_mut(&mut self, message_id: &str) -> Option<&mut TimelineEntry> {
        let index = self.timeline_message_index(message_id)?;
        self.timeline_get_mut(index)
    }

    fn index_message_identity_at(&mut self, index: usize) {
        let absolute = self
            .timeline
            .timeline_base_position
            .saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
        self.timeline
            .live_timeline_positions
            .retain(|_, position| *position != absolute);
        let Some(TimelineEntry::Message {
            identity: Some(identity),
            ..
        }) = self.timeline_get(index)
        else {
            return;
        };
        let message_id = identity.message_id.clone();
        let live_key = (identity.source == MessageSource::Live).then(|| LiveMessageKey {
            execution_id: identity.execution_id.clone(),
            turn_id: identity.turn_id.clone(),
            part_id: identity.part_id.clone(),
        });
        if let Some(message_id) = message_id {
            self.timeline
                .message_timeline_positions
                .insert(message_id, absolute);
        }
        if let Some(live_key) = live_key {
            self.timeline
                .live_timeline_positions
                .insert(live_key, absolute);
        }
    }

    fn rebuild_timeline_positions(&mut self) {
        self.timeline.message_timeline_positions.clear();
        self.timeline.live_timeline_positions.clear();
        self.timeline.tool_timeline_positions.clear();
        let indexed = self
            .timeline_iter()
            .filter_map(|(index, entry)| match entry {
                TimelineEntry::Message {
                    identity: Some(identity),
                    ..
                } => Some((
                    identity.message_id.clone(),
                    (identity.source == MessageSource::Live).then(|| LiveMessageKey {
                        execution_id: identity.execution_id.clone(),
                        turn_id: identity.turn_id.clone(),
                        part_id: identity.part_id.clone(),
                    }),
                    None,
                    index,
                )),
                TimelineEntry::ToolCall { id, .. } => Some((None, None, Some(id.clone()), index)),
                _ => None,
            })
            .collect::<Vec<_>>();
        for (message_id, live_key, tool_id, index) in indexed {
            let absolute = self
                .timeline
                .timeline_base_position
                .saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
            if let Some(message_id) = message_id {
                self.timeline
                    .message_timeline_positions
                    .insert(message_id, absolute);
            }
            if let Some(live_key) = live_key {
                self.timeline
                    .live_timeline_positions
                    .insert(live_key, absolute);
            }
            if let Some(tool_id) = tool_id {
                self.timeline
                    .tool_timeline_positions
                    .insert(tool_id, absolute);
            }
        }
    }

    fn note_searchable_content_changed(&mut self) {
        self.timeline.searchable_content_revision =
            self.timeline.searchable_content_revision.wrapping_add(1);
    }

    fn ensure_search_text_index(&mut self) {
        if self.timeline.search_index_revision == self.timeline.searchable_content_revision
            && self.timeline.search_text_index.len() == self.timeline.total_entries
        {
            return;
        }
        self.timeline.search_text_index = self
            .timeline_iter()
            .map(|(_, entry)| {
                let visible_in_chat = matches!(
                    entry,
                    TimelineEntry::Message { role, .. }
                        if role == "user" || role == "assistant"
                ) || matches!(entry, TimelineEntry::SlashOutput { .. });
                if visible_in_chat {
                    entry.full_text().to_lowercase()
                } else {
                    String::new()
                }
            })
            .collect();
        self.timeline.search_index_revision = self.timeline.searchable_content_revision;
    }

    fn timeline_live_message_index(
        &self,
        execution_id: Option<&str>,
        turn_id: Option<&str>,
        part_id: Option<&str>,
    ) -> Option<usize> {
        let key = LiveMessageKey {
            execution_id: execution_id.map(ToOwned::to_owned),
            turn_id: turn_id.map(ToOwned::to_owned),
            part_id: part_id.map(ToOwned::to_owned),
        };
        self.timeline
            .live_timeline_positions
            .get(&key)
            .copied()
            .and_then(|absolute| self.logical_timeline_index(absolute))
            .filter(|index| {
                matches!(
                    self.timeline_get(*index),
                    Some(TimelineEntry::Message {
                        identity: Some(MessageIdentity {
                            source: MessageSource::Live,
                            ..
                        }),
                        ..
                    })
                )
            })
    }

    fn timeline_live_message_mut(
        &mut self,
        execution_id: Option<&str>,
        turn_id: Option<&str>,
        part_id: Option<&str>,
    ) -> Option<&mut TimelineEntry> {
        let index = self.timeline_live_message_index(execution_id, turn_id, part_id)?;
        self.timeline_get_mut(index)
    }

    fn timeline_live_assistant_index(
        &self,
        execution_id: Option<&str>,
        turn_id: Option<&str>,
    ) -> Option<usize> {
        let turn_id = turn_id?;
        let mut matches =
            self.timeline
                .live_timeline_positions
                .iter()
                .filter_map(|(key, absolute)| {
                    (key.turn_id.as_deref() == Some(turn_id)
                        && execution_id
                            .is_none_or(|expected| key.execution_id.as_deref() == Some(expected)))
                    .then(|| self.logical_timeline_index(*absolute))
                    .flatten()
                    .filter(|index| {
                        matches!(
                            self.timeline_get(*index),
                            Some(TimelineEntry::Message {
                                role,
                                identity: Some(MessageIdentity {
                                    source: MessageSource::Live,
                                    ..
                                }),
                                ..
                            }) if role == "assistant"
                        )
                    })
                });
        let only = matches.next()?;
        matches.next().is_none().then_some(only)
    }

    fn remove_stale_live_assistant_parts(
        &mut self,
        execution_id: Option<&str>,
        turn_id: Option<&str>,
    ) {
        let Some(turn_id) = turn_id else {
            return;
        };
        let entries = self.timeline_clone_vec();
        let before = entries.len();
        let retained = entries
            .into_iter()
            .filter(|entry| {
                !matches!(
                    entry,
                    TimelineEntry::Message {
                        role,
                        identity: Some(MessageIdentity {
                            execution_id: candidate_execution,
                            turn_id: Some(candidate_turn),
                            source: MessageSource::Live,
                            ..
                        }),
                        ..
                    } if role == "assistant"
                        && candidate_turn == turn_id
                        && execution_id.is_none_or(|expected| {
                            candidate_execution.as_deref() == Some(expected)
                        })
                )
            })
            .collect::<Vec<_>>();
        if retained.len() != before {
            self.replace_timeline_entries(retained);
        }
    }

    fn timeline_correlated_assistant_index(
        &self,
        execution_id: Option<&str>,
        turn_id: Option<&str>,
    ) -> Option<usize> {
        let turn_id = turn_id?;
        let mut legacy_match = None;
        for index in (0..self.timeline_len()).rev() {
            let Some(entry) = self.timeline_get(index) else {
                continue;
            };
            let TimelineEntry::Message {
                role,
                identity: Some(identity),
                ..
            } = entry
            else {
                continue;
            };
            if role != "assistant" || identity.turn_id.as_deref() != Some(turn_id) {
                continue;
            }
            match execution_id {
                Some(expected) if identity.execution_id.as_deref() == Some(expected) => {
                    return Some(index);
                }
                Some(_) if identity.execution_id.is_none() => {
                    if legacy_match.replace(index).is_some() {
                        return None;
                    }
                }
                None => {
                    if legacy_match.replace(index).is_some() {
                        return None;
                    }
                }
                _ => {}
            }
        }
        legacy_match
    }

    fn apply_history_page(&mut self, page: crate::protocol::SessionMessagesPage) {
        if page.session_id != self.shell.session_id {
            return;
        }
        if page.total > SOFT_CAP && !self.history.history_window_truncated {
            self.history.history_window_truncated = true;
            let warning = format!(
                "Durable history has {} messages; this TUI keeps the newest {} visible. Compact or checkpoint the session to restore a complete interactive window.",
                page.total, SOFT_CAP
            );
            self.add_system_notice(SystemNoticeKind::Warning, &warning);
            self.show_notification(&warning);
        }
        for message in page.messages {
            let content = message.visible_text();
            let turn_id = message.turn_id().map(ToOwned::to_owned);
            let execution_id = message.execution_id().map(ToOwned::to_owned);
            let intermediate_assistant =
                message.role == "assistant" && message.id.contains(":transcript:");
            if let Some(usage) = message.token_usage.as_ref() {
                self.record_durable_message_usage(&message.id, usage);
            }
            if matches!(message.role.as_str(), "user" | "assistant")
                && !intermediate_assistant
                && !content.is_empty()
            {
                let part_id = (message.role == "assistant")
                    .then(|| format!("terminal-message:{}", message.id));
                let identity = MessageIdentity {
                    message_id: Some(message.id.clone()),
                    sequence: Some(message.sequence),
                    execution_id: execution_id.clone(),
                    turn_id: turn_id.clone(),
                    part_id,
                    source: MessageSource::DurableHistory,
                };
                let existing_index = self.timeline_message_index(&message.id).or_else(|| {
                    (message.role == "assistant")
                        .then(|| {
                            self.timeline_live_assistant_index(
                                execution_id.as_deref(),
                                turn_id.as_deref(),
                            )
                        })
                        .flatten()
                });
                if let Some(index) = existing_index {
                    if let Some(TimelineEntry::Message {
                        role,
                        content: existing_content,
                        timestamp,
                        identity: existing_identity,
                    }) = self.timeline_get_mut(index)
                    {
                        *role = message.role.clone();
                        *existing_content = content;
                        *timestamp = Self::message_timestamp(message.created_at_ms);
                        *existing_identity = Some(Self::merge_message_identity(
                            existing_identity.take(),
                            identity,
                        ));
                    }
                    self.index_message_identity_at(index);
                    self.note_searchable_content_changed();
                } else {
                    self.timeline_push(TimelineEntry::Message {
                        role: message.role.clone(),
                        content,
                        timestamp: Self::message_timestamp(message.created_at_ms),
                        identity: Some(identity),
                    });
                }
                if message.role == "assistant" {
                    self.remove_stale_live_assistant_parts(
                        execution_id.as_deref(),
                        turn_id.as_deref(),
                    );
                }
            }
            if self
                .execution
                .hydrated_non_text_message_ids
                .insert(message.id.clone())
            {
                self.hydrate_non_text_blocks(&message.id, message.sequence, &message.blocks);
            }
        }
        if !page.has_more {
            self.reorder_messages_by_durable_sequence();
            self.rebuild_timeline_positions();
            self.rebuild_input_history_from_durable_messages();
            self.history.history_hydrated = true;
            self.history.history_hydration_error = None;
        }
        self.timeline.timeline_full_sync_revision =
            self.timeline.timeline_full_sync_revision.wrapping_add(1);
        if self.timeline.auto_scroll {
            self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
        }
        self.mark_dirty();
    }

    fn make_room_for_older_history(&mut self, required_entries: usize) {
        let overflow = self
            .timeline_len()
            .saturating_add(required_entries)
            .saturating_sub(SOFT_CAP);
        if overflow == 0 {
            return;
        }
        let entries = self.timeline_clone_vec();
        let mut remove = BTreeSet::new();
        for entry in entries.iter().rev() {
            if remove.len() >= overflow {
                break;
            }
            if let TimelineEntry::Message {
                identity:
                    Some(MessageIdentity {
                        message_id: Some(message_id),
                        sequence: Some(_),
                        ..
                    }),
                ..
            } = entry
            {
                remove.insert(message_id.clone());
            }
        }
        if remove.is_empty() {
            return;
        }
        let non_text_owner = self.timeline.non_text_durable_owner.clone();
        let retained = entries
            .into_iter()
            .filter(|entry| match entry {
                TimelineEntry::Message {
                    identity:
                        Some(MessageIdentity {
                            message_id: Some(message_id),
                            ..
                        }),
                    ..
                } => !remove.contains(message_id),
                TimelineEntry::ToolCall { id, .. } => non_text_owner
                    .get(&format!("tool|{id}"))
                    .is_none_or(|owner| !remove.contains(owner)),
                TimelineEntry::Thinking { id, .. } => non_text_owner
                    .get(&format!("thinking|{id}"))
                    .is_none_or(|owner| !remove.contains(owner)),
                _ => true,
            })
            .collect::<Vec<_>>();
        self.replace_timeline_entries(retained);
    }

    fn make_room_for_newer_history(&mut self, required_entries: usize) {
        let overflow = self
            .timeline_len()
            .saturating_add(required_entries)
            .saturating_sub(SOFT_CAP);
        if overflow == 0 {
            return;
        }
        let entries = self.timeline_clone_vec();
        let mut remove = BTreeSet::new();
        for entry in &entries {
            if remove.len() >= overflow {
                break;
            }
            if let TimelineEntry::Message {
                identity:
                    Some(MessageIdentity {
                        message_id: Some(message_id),
                        sequence: Some(_),
                        ..
                    }),
                ..
            } = entry
            {
                remove.insert(message_id.clone());
            }
        }
        if remove.is_empty() {
            return;
        }
        let non_text_owner = self.timeline.non_text_durable_owner.clone();
        let retained = entries
            .into_iter()
            .filter(|entry| match entry {
                TimelineEntry::Message {
                    identity:
                        Some(MessageIdentity {
                            message_id: Some(message_id),
                            ..
                        }),
                    ..
                } => !remove.contains(message_id),
                TimelineEntry::ToolCall { id, .. } => non_text_owner
                    .get(&format!("tool|{id}"))
                    .is_none_or(|owner| !remove.contains(owner)),
                TimelineEntry::Thinking { id, .. } => non_text_owner
                    .get(&format!("thinking|{id}"))
                    .is_none_or(|owner| !remove.contains(owner)),
                _ => true,
            })
            .collect::<Vec<_>>();
        self.replace_timeline_entries(retained);
    }

    fn clear_durable_history_window(&mut self) {
        let retained = self
            .timeline_clone_vec()
            .into_iter()
            .filter(|entry| {
                let owned_non_text = match entry {
                    TimelineEntry::ToolCall { id, .. } => self
                        .timeline
                        .non_text_durable_owner
                        .contains_key(&format!("tool|{id}")),
                    TimelineEntry::Thinking { id, .. } => self
                        .timeline
                        .non_text_durable_owner
                        .contains_key(&format!("thinking|{id}")),
                    _ => false,
                };
                !owned_non_text
                    && !matches!(
                        entry,
                        TimelineEntry::Message {
                            identity: Some(MessageIdentity {
                                sequence: Some(_),
                                ..
                            }),
                            ..
                        }
                    )
            })
            .collect::<Vec<_>>();
        self.execution.hydrated_non_text_message_ids.clear();
        self.timeline.pending_history_tool_instances.clear();
        self.timeline.non_text_durable_owner.clear();
        self.replace_timeline_entries(retained);
    }

    fn replace_timeline_entries(&mut self, entries: Vec<TimelineEntry>) {
        let retained_non_text_keys = entries
            .iter()
            .filter_map(|entry| match entry {
                TimelineEntry::ToolCall { id, .. } => Some(format!("tool|{id}")),
                TimelineEntry::Thinking { id, .. } => Some(format!("thinking|{id}")),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        self.timeline
            .non_text_durable_owner
            .retain(|key, _| retained_non_text_keys.contains(key));
        self.timeline.timeline_pages.clear();
        self.timeline.total_entries = 0;
        self.timeline.timeline_base_position = 0;
        self.timeline.message_timeline_positions.clear();
        self.timeline.live_timeline_positions.clear();
        self.timeline.tool_timeline_positions.clear();
        for chunk in entries.chunks(PAGE_SIZE) {
            let start_index = self.timeline.total_entries;
            self.timeline.timeline_pages.push_back(TimelinePage {
                entries: chunk.to_vec(),
                start_index,
            });
            self.timeline.total_entries = self.timeline.total_entries.saturating_add(chunk.len());
        }
        self.timeline.timeline_cursor = self
            .timeline
            .timeline_cursor
            .min(self.timeline.total_entries.saturating_sub(1));
        self.rebuild_timeline_positions();
        self.note_searchable_content_changed();
        self.timeline.timeline_dirty_log.clear();
        self.timeline.timeline_full_sync_revision =
            self.timeline.timeline_full_sync_revision.wrapping_add(1);
    }

    fn hydrate_non_text_blocks(
        &mut self,
        durable_message_id: &str,
        durable_sequence: usize,
        blocks: &[Value],
    ) {
        for (block_index, block) in blocks.iter().enumerate() {
            match block.get("type").and_then(Value::as_str) {
                Some("thinking") => {
                    let Some(thinking) = block
                        .get("thinking")
                        .and_then(Value::as_str)
                        .filter(|thinking| !thinking.is_empty())
                    else {
                        continue;
                    };
                    let id = self.timeline.thinking_id_counter;
                    self.timeline.thinking_id_counter =
                        self.timeline.thinking_id_counter.saturating_add(1);
                    self.timeline
                        .non_text_durable_owner
                        .insert(format!("thinking|{id}"), durable_message_id.to_string());
                    self.timeline_push(TimelineEntry::Thinking {
                        id,
                        causal_item_id: None,
                        causality: None,
                        content: thinking.to_string(),
                        complete: true,
                        expanded: false,
                    });
                }
                Some("tool_use") => {
                    let Some(provider_tool_id) = block
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                    else {
                        continue;
                    };
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_string();
                    let preview = block
                        .get("input")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .chars()
                        .take(160)
                        .collect::<String>();
                    let instance_id = block
                        .get("cowd_tool_instance_id")
                        .and_then(Value::as_str)
                        .unwrap_or(provider_tool_id);
                    let id = ToolInstanceIdentity {
                        session_id: self.shell.session_id.clone(),
                        execution_id: block
                            .get("cowd_execution_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        turn_id: block
                            .get("cowd_turn_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        part_id: None,
                        durable_message_id: Some(durable_message_id.to_string()),
                        durable_sequence: Some(durable_sequence),
                        block_index: Some(block_index),
                        provider_tool_id: instance_id.to_string(),
                    }
                    .stable_key();
                    if self
                        .timeline
                        .tool_timeline_positions
                        .get(&id)
                        .copied()
                        .and_then(|absolute| self.logical_timeline_index(absolute))
                        .is_some()
                    {
                        continue;
                    }
                    self.timeline
                        .pending_history_tool_instances
                        .entry(provider_tool_id.to_string())
                        .or_default()
                        .push_back(id.clone());
                    self.timeline
                        .non_text_durable_owner
                        .insert(format!("tool|{id}"), durable_message_id.to_string());
                    self.timeline_push(TimelineEntry::ToolCall {
                        id,
                        name,
                        preview,
                        output: String::new(),
                        done: false,
                        expanded: false,
                        exit_code: None,
                        causality: None,
                    });
                }
                Some("tool_result") => {
                    let Some(tool_use_id) = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                    else {
                        continue;
                    };
                    let tool_name = block
                        .get("tool_name")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_string();
                    let output = block
                        .get("output")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let is_error = block
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let matched_instance = self
                        .timeline
                        .pending_history_tool_instances
                        .get_mut(tool_use_id)
                        .and_then(VecDeque::pop_front);
                    let tool_index = matched_instance
                        .as_ref()
                        .and_then(|id| self.timeline.tool_timeline_positions.get(id))
                        .copied()
                        .and_then(|absolute| self.logical_timeline_index(absolute));
                    let mut found = false;
                    if let Some(TimelineEntry::ToolCall {
                        output: existing_output,
                        done,
                        expanded,
                        exit_code,
                        ..
                    }) = tool_index.and_then(|index| self.timeline_get_mut(index))
                    {
                        *existing_output = output.clone();
                        *done = true;
                        *expanded = false;
                        *exit_code = Some(if is_error { 1 } else { 0 });
                        found = true;
                    }
                    if !found {
                        let id = ToolInstanceIdentity {
                            session_id: self.shell.session_id.clone(),
                            execution_id: block
                                .get("cowd_execution_id")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned),
                            turn_id: block
                                .get("cowd_turn_id")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned),
                            part_id: None,
                            durable_message_id: Some(durable_message_id.to_string()),
                            durable_sequence: Some(durable_sequence),
                            block_index: Some(block_index),
                            provider_tool_id: block
                                .get("cowd_tool_instance_id")
                                .and_then(Value::as_str)
                                .unwrap_or(tool_use_id)
                                .to_string(),
                        }
                        .stable_key();
                        self.timeline
                            .non_text_durable_owner
                            .insert(format!("tool|{id}"), durable_message_id.to_string());
                        self.timeline_push(TimelineEntry::ToolCall {
                            id,
                            name: tool_name,
                            preview: String::new(),
                            output,
                            done: true,
                            expanded: false,
                            exit_code: Some(if is_error { 1 } else { 0 }),
                            causality: None,
                        });
                    }
                }
                _ => {}
            }
        }
    }

    /// Reconcile multi-Surface history by immutable storage sequence and
    /// explicit turn causality.
    ///
    /// Reconnect pages may discover a message that another Surface committed
    /// between two messages already present in the local timeline. Upserting
    /// by id prevents duplication but does not repair that ordering. Reorder
    /// only message slots after the final page, keeping local tool/thinking
    /// entries anchored and keeping sequence-less in-flight messages at the
    /// tail in their existing stable order.
    fn reorder_messages_by_durable_sequence(&mut self) {
        let mut messages = self
            .timeline_iter()
            .filter_map(|(_, entry)| {
                matches!(entry, TimelineEntry::Message { .. }).then(|| entry.clone())
            })
            .collect::<Vec<_>>();
        let turn_ingress_sequence = messages
            .iter()
            .filter_map(|entry| match entry {
                TimelineEntry::Message {
                    role,
                    identity: Some(identity),
                    ..
                } if role == "user" => identity
                    .turn_id
                    .as_ref()
                    .zip(identity.sequence)
                    .map(|(turn_id, sequence)| (turn_id.clone(), sequence)),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        messages.sort_by_key(|entry| match entry {
            TimelineEntry::Message { role, identity, .. } => {
                let physical = identity
                    .as_ref()
                    .and_then(|identity| identity.sequence)
                    .unwrap_or(usize::MAX);
                let anchor = identity
                    .as_ref()
                    .and_then(|identity| identity.turn_id.as_ref())
                    .and_then(|turn_id| turn_ingress_sequence.get(turn_id))
                    .copied()
                    .unwrap_or(physical);
                (anchor, usize::from(role != "user"), physical)
            }
            _ => (usize::MAX, usize::MAX, usize::MAX),
        });
        let mut messages = messages.into_iter();
        for entry in self.timeline_iter_mut() {
            if matches!(entry, TimelineEntry::Message { .. }) {
                if let Some(message) = messages.next() {
                    *entry = message;
                }
            }
        }
    }

    fn record_durable_message_usage(&mut self, message_id: &str, usage: &Value) {
        let input = usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let output = usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if let Some((previous_input, previous_output)) = self
            .history
            .durable_message_usage
            .insert(message_id.to_string(), (input, output))
        {
            self.history.durable_session_input_tokens = self
                .history
                .durable_session_input_tokens
                .saturating_sub(previous_input);
            self.history.durable_session_output_tokens = self
                .history
                .durable_session_output_tokens
                .saturating_sub(previous_output);
        }
        self.history.durable_session_input_tokens = self
            .history
            .durable_session_input_tokens
            .saturating_add(input);
        self.history.durable_session_output_tokens = self
            .history
            .durable_session_output_tokens
            .saturating_add(output);
    }

    fn rebuild_input_history_from_durable_messages(&mut self) {
        const INPUT_HISTORY_LIMIT: usize = 1_000;
        let mut history = self
            .timeline_iter()
            .filter_map(|(_, entry)| match entry {
                TimelineEntry::Message {
                    role,
                    content,
                    identity: Some(identity),
                    ..
                } if role == "user" && identity.sequence.is_some() => Some(content.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if history.len() > INPUT_HISTORY_LIMIT {
            history.drain(..history.len() - INPUT_HISTORY_LIMIT);
        }
        self.shell.input_history = history;
        self.shell.history_idx = None;
    }

    pub fn record_input_history(&mut self, input: String) {
        const INPUT_HISTORY_LIMIT: usize = 1_000;
        if input.is_empty()
            || self
                .shell
                .input_history
                .last()
                .is_some_and(|previous| previous == &input)
        {
            self.shell.history_idx = None;
            return;
        }
        self.shell.input_history.push(input);
        if self.shell.input_history.len() > INPUT_HISTORY_LIMIT {
            self.shell.input_history.remove(0);
        }
        self.shell.history_idx = None;
    }

    fn correlation_is_current(
        &self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> bool {
        if correlation.session_id != self.shell.session_id
            || correlation.execution_id.is_none()
            || correlation.turn_id.is_none()
        {
            return false;
        }
        self.execution
            .current_execution_id
            .as_deref()
            .is_none_or(|current| correlation.execution_id.as_deref() == Some(current))
            && self
                .execution
                .current_turn_id
                .as_deref()
                .is_none_or(|current| correlation.turn_id.as_deref() == Some(current))
    }

    pub(crate) fn execution_is_terminalized(&self, execution_id: &str) -> bool {
        self.execution
            .terminal_correlations
            .iter()
            .any(|(terminal_execution_id, _)| terminal_execution_id == execution_id)
    }

    fn correlation_is_terminalized(
        &self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> bool {
        correlation
            .execution_id
            .as_deref()
            .zip(correlation.turn_id.as_deref())
            .is_some_and(|(execution_id, turn_id)| {
                self.execution.terminal_correlations.iter().any(
                    |(terminal_execution_id, terminal_turn_id)| {
                        terminal_execution_id == execution_id && terminal_turn_id == turn_id
                    },
                )
            })
    }

    fn record_terminal_correlation(
        &mut self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) {
        const TERMINAL_CORRELATION_CAPACITY: usize = 1_024;
        let Some((execution_id, turn_id)) = correlation
            .execution_id
            .as_ref()
            .zip(correlation.turn_id.as_ref())
        else {
            return;
        };
        if self
            .execution
            .terminal_correlations
            .iter()
            .any(|(known_execution_id, known_turn_id)| {
                known_execution_id == execution_id && known_turn_id == turn_id
            })
        {
            return;
        }
        self.execution
            .terminal_correlations
            .push_back((execution_id.clone(), turn_id.clone()));
        while self.execution.terminal_correlations.len() > TERMINAL_CORRELATION_CAPACITY {
            self.execution.terminal_correlations.pop_front();
        }
    }

    fn adopt_live_correlation(
        &mut self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> bool {
        if !self.correlation_is_current(correlation) {
            self.execution.telemetry.orphan_event_count = self
                .execution
                .telemetry
                .orphan_event_count
                .saturating_add(1);
            let orphan_count = self.execution.telemetry.orphan_event_count;
            tracing::warn!(
                session_id = %self.shell.session_id,
                event_session_id = %correlation.session_id,
                execution_id = correlation.execution_id.as_deref().unwrap_or("missing"),
                turn_id = correlation.turn_id.as_deref().unwrap_or("missing"),
                orphan_event_count = orphan_count,
                "TUI rejected an event outside the active correlated turn"
            );
            // Surface the first mismatch immediately and then at exponentially
            // spaced counts. This makes causal loss visible without allowing a
            // noisy stale stream to flood the transcript.
            if orphan_count == 1 || orphan_count.is_power_of_two() {
                let warning =
                    format!(
                    "Ignored {orphan_count} event(s) outside the active session/execution/turn \
                     (current execution={}, turn={}, status={:?}; incoming execution={}, turn={}); \
                     canonical history and projection remain authoritative",
                    self.execution.current_execution_id.as_deref().unwrap_or("none"),
                    self.execution.current_turn_id.as_deref().unwrap_or("none"),
                    self.execution.current_execution_status,
                    correlation.execution_id.as_deref().unwrap_or("missing"),
                    correlation.turn_id.as_deref().unwrap_or("missing"),
                );
                self.add_system_notice(SystemNoticeKind::Warning, &warning);
                self.show_notification(&warning);
            }
            return false;
        }
        self.execution.current_execution_id = correlation.execution_id.clone();
        self.execution.current_turn_id = correlation.turn_id.clone();
        true
    }

    /// A non-queued Runtime phase is the canonical fact that an admitted
    /// execution has actually started. A durable user message alone may still
    /// represent a queued follow-up, so it cannot replace a running turn. Once
    /// Runtime starts that follow-up, however, every observing Surface must
    /// atomically leave the previous execution before accepting live deltas.
    fn adopt_started_execution_correlation(
        &mut self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> bool {
        if correlation.session_id != self.shell.session_id
            || correlation.execution_id.is_none()
            || correlation.turn_id.is_none()
        {
            return self.adopt_live_correlation(correlation);
        }
        if self.execution.current_execution_id.as_deref() != correlation.execution_id.as_deref()
            || self.execution.current_turn_id.as_deref() != correlation.turn_id.as_deref()
        {
            if let Some(previous_execution_id) = self.execution.current_execution_id.clone() {
                if let Some(previous_turn_id) = self.execution.current_turn_id.clone() {
                    self.record_terminal_correlation(&crate::protocol::GatewayEventCorrelation {
                        session_id: self.shell.session_id.clone(),
                        execution_id: Some(previous_execution_id.clone()),
                        turn_id: Some(previous_turn_id),
                        ..crate::protocol::GatewayEventCorrelation::default()
                    });
                }
                self.invalidate_execution_projection(&previous_execution_id);
            } else {
                self.reset_live_execution_facts();
            }
            self.execution.current_execution_id = correlation.execution_id.clone();
            self.execution.current_turn_id = correlation.turn_id.clone();
            if let Some(execution_id) = correlation.execution_id.as_deref() {
                self.execution
                    .turn_interaction
                    .ingress_accepted(execution_id);
            }
        }
        self.adopt_live_correlation(correlation)
    }

    fn adopt_active_execution_correlation(
        &mut self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> bool {
        let incoming_is_committed = correlation
            .execution_id
            .as_deref()
            .zip(correlation.turn_id.as_deref())
            .is_some_and(|(execution_id, turn_id)| {
                self.execution
                    .committed_ingress_correlations
                    .contains(&(execution_id.to_string(), turn_id.to_string()))
            });
        let incoming_differs = self.execution.current_execution_id.as_deref()
            != correlation.execution_id.as_deref()
            || self.execution.current_turn_id.as_deref() != correlation.turn_id.as_deref();
        let current_is_terminal = self
            .execution
            .current_execution_id
            .as_deref()
            .is_some_and(|execution_id| self.execution_is_terminalized(execution_id))
            || self
                .execution
                .current_execution_status
                .is_some_and(harness_contract::projection::ExecutionLiveStatus::is_terminal);
        let incoming_is_live = !self.correlation_is_terminalized(correlation);
        if incoming_differs && incoming_is_live && (incoming_is_committed || current_is_terminal) {
            return self.adopt_started_execution_correlation(correlation);
        }
        self.adopt_live_correlation(correlation)
    }

    fn correlated_tool_instance_key(
        &self,
        correlation: &crate::protocol::GatewayEventCorrelation,
        provider_tool_id: &str,
    ) -> String {
        ToolInstanceIdentity {
            session_id: correlation.session_id.clone(),
            execution_id: correlation.execution_id.clone(),
            turn_id: correlation.turn_id.clone(),
            part_id: correlation.part_id.clone(),
            durable_message_id: correlation.message_id.clone(),
            durable_sequence: None,
            block_index: None,
            provider_tool_id: provider_tool_id.to_string(),
        }
        .stable_key()
    }

    fn apply_correlated_thinking_delta(
        &mut self,
        correlation: &crate::protocol::GatewayEventCorrelation,
        thinking: String,
    ) {
        let causal_item_id = correlation
            .segment_id
            .clone()
            .or_else(|| correlation.item_id.clone());
        if let Some(causal_item_id) = causal_item_id.as_deref() {
            let mut found = false;
            for index in (0..self.timeline_len()).rev() {
                if let Some(TimelineEntry::Thinking {
                    causal_item_id: existing,
                    causality,
                    content,
                    complete,
                    ..
                }) = self.timeline_get_mut(index)
                {
                    if existing.as_deref() == Some(causal_item_id) && !*complete {
                        if causality
                            .as_ref()
                            .and_then(|value| value.delta_sequence)
                            .zip(correlation.delta_sequence)
                            .is_some_and(|(accepted, incoming)| incoming <= accepted)
                        {
                            return;
                        }
                        content.push_str(&thinking);
                        let mut incoming = TimelineCausality::from_correlation(correlation);
                        if let Some(previous) = causality.as_ref() {
                            incoming.causal_sequence =
                                previous.causal_sequence.or(incoming.causal_sequence);
                        }
                        *causality = Some(incoming);
                        found = true;
                        break;
                    }
                }
            }
            if found {
                self.timeline.lines_dirty = true;
                self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
                return;
            }
        }
        let id = self.timeline.thinking_id_counter;
        self.timeline.thinking_id_counter = self.timeline.thinking_id_counter.saturating_add(1);
        self.timeline_push(TimelineEntry::Thinking {
            id,
            causal_item_id,
            causality: Some(TimelineCausality::from_correlation(correlation)),
            content: thinking,
            complete: false,
            expanded: false,
        });
        self.execution.current_turn_thinking_count =
            self.execution.current_turn_thinking_count.saturating_add(1);
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
        self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
    }

    fn complete_correlated_thinking(
        &mut self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) {
        let causal_item_id = correlation
            .segment_id
            .as_deref()
            .or(correlation.item_id.as_deref());
        let Some(causal_item_id) = causal_item_id else {
            return;
        };
        for index in (0..self.timeline_len()).rev() {
            if let Some(TimelineEntry::Thinking {
                causal_item_id: existing,
                complete,
                expanded,
                ..
            }) = self.timeline_get_mut(index)
            {
                if existing.as_deref() == Some(causal_item_id) {
                    *complete = true;
                    *expanded = false;
                    self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
                    return;
                }
            }
        }
    }

    fn correlated_tool_causality(
        &self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> TimelineCausality {
        let mut causality = TimelineCausality::from_correlation(correlation);
        causality.wave = causality
            .causal_parent_ids
            .iter()
            .filter_map(|parent| {
                self.timeline_iter().find_map(|(_, entry)| match entry {
                    TimelineEntry::ToolCall {
                        causality: Some(known),
                        ..
                    } if known.tool_call_id.as_deref() == Some(parent.as_str()) => {
                        Some(known.wave.saturating_add(1))
                    }
                    _ => None,
                })
            })
            .max()
            .unwrap_or(0);
        causality.lane = self
            .timeline_iter()
            .filter(|(_, entry)| {
                matches!(
                    entry,
                    TimelineEntry::ToolCall {
                        causality: Some(known),
                        ..
                    } if known.model_step_id == causality.model_step_id
                        && known.wave == causality.wave
                )
            })
            .count();
        causality.lane_count = causality.lane.saturating_add(1);
        causality
    }

    fn annotate_correlated_tool(&mut self, tool_instance_id: &str, causality: TimelineCausality) {
        let Some(absolute) = self
            .timeline
            .tool_timeline_positions
            .get(tool_instance_id)
            .copied()
        else {
            return;
        };
        let Some(index) = self.logical_timeline_index(absolute) else {
            return;
        };
        if let Some(TimelineEntry::ToolCall {
            causality: value, ..
        }) = self.timeline_get_mut(index)
        {
            *value = Some(causality.clone());
        }
        let group = self
            .timeline_iter()
            .filter_map(|(index, entry)| match entry {
                TimelineEntry::ToolCall {
                    causality: Some(known),
                    ..
                } if known.model_step_id == causality.model_step_id
                    && known.wave == causality.wave =>
                {
                    Some(index)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let lane_count = group.len();
        for index in group {
            if let Some(TimelineEntry::ToolCall {
                causality: Some(known),
                ..
            }) = self.timeline_get_mut(index)
            {
                known.lane_count = lane_count;
            }
        }
        self.timeline.lines_dirty = true;
    }

    fn current_tool_instance_key(&self, provider_tool_id: &str) -> String {
        if provider_tool_id.starts_with("tool-instance|") {
            return provider_tool_id.to_string();
        }
        if self.execution.current_execution_id.is_none() && self.execution.current_turn_id.is_none()
        {
            return provider_tool_id.to_string();
        }
        ToolInstanceIdentity {
            session_id: self.shell.session_id.clone(),
            execution_id: self.execution.current_execution_id.clone(),
            turn_id: self.execution.current_turn_id.clone(),
            part_id: None,
            durable_message_id: None,
            durable_sequence: None,
            block_index: None,
            provider_tool_id: provider_tool_id.to_string(),
        }
        .stable_key()
    }

    fn apply_gateway_text_delta(
        &mut self,
        mut correlation: crate::protocol::GatewayEventCorrelation,
        text: String,
        start_bytes: usize,
        end_bytes: usize,
        stream_revision: u64,
        presentation_owner: Option<(String, String)>,
    ) {
        // Establish the incoming causal identity before consulting presentation
        // state. A terminalized execution closes its root preview; the first
        // live delta for a newer execution is also sufficient evidence that the
        // newer turn started, even when its admission/phase event was coalesced.
        // Adopting first resets only the superseded turn's presentation state
        // and prevents that stale `root_closed` bit from dropping new output.
        if !self.adopt_active_execution_correlation(&correlation) {
            self.add_system_notice(
                SystemNoticeKind::Warning,
                "Ignored an assistant delta without the current session/execution/turn identity",
            );
            return;
        }
        let presentation_owner = presentation_owner.or_else(|| {
            // Runtime providers still emit their byte stream through the
            // ordinary causal event. Once a root presentation starts, adopt
            // those bytes into its explicit owner instead of showing a second
            // assistant bubble.
            self.execution.turn_interaction.active_root_owner()
        });
        if presentation_owner.is_none() && self.execution.turn_interaction.root_preview_closed() {
            self.execution.telemetry.text_delta_dedupe_count = self
                .execution
                .telemetry
                .text_delta_dedupe_count
                .saturating_add(1);
            return;
        }
        if let Some((presentation_id, attempt_id)) = presentation_owner {
            use crate::components::turn_interaction::PresentationDeltaAdmission;
            match self.execution.turn_interaction.admit_root_delta(
                &presentation_id,
                &attempt_id,
                start_bytes as u64,
                end_bytes as u64,
            ) {
                PresentationDeltaAdmission::Accepted => {}
                PresentationDeltaAdmission::Duplicate | PresentationDeltaAdmission::NotOwner => {
                    self.execution.telemetry.text_delta_dedupe_count = self
                        .execution
                        .telemetry
                        .text_delta_dedupe_count
                        .saturating_add(1);
                    return;
                }
                PresentationDeltaAdmission::Gap => {
                    self.execution.live_output_snapshot_gap = true;
                    self.add_system_notice(
                        SystemNoticeKind::Warning,
                        "Root answer stream reported a byte gap; waiting for the durable terminal",
                    );
                    return;
                }
            }
            let presentation_part = format!("terminal-presentation:{presentation_id}:{attempt_id}");
            if start_bytes == 0
                && self
                    .timeline_live_message_index(
                        correlation.execution_id.as_deref(),
                        correlation.turn_id.as_deref(),
                        Some(&presentation_part),
                    )
                    .is_none()
            {
                // A provider preview may have arrived before the presentation
                // gate selected its owner. Replace that transient bubble at
                // the first owned byte; never display both.
                let execution_id = correlation.execution_id.clone();
                let turn_id = correlation.turn_id.clone();
                self.remove_stale_live_assistant_parts(execution_id.as_deref(), turn_id.as_deref());
            }
            correlation.part_id = Some(presentation_part);
        }
        let stream_key = LiveMessageKey {
            execution_id: correlation.execution_id.clone(),
            turn_id: correlation.turn_id.clone(),
            part_id: correlation.part_id.clone(),
        };
        if self
            .execution
            .live_stream_revisions
            .get(&stream_key)
            .is_some_and(|accepted| stream_revision <= *accepted)
        {
            self.execution.telemetry.text_delta_dedupe_count = self
                .execution
                .telemetry
                .text_delta_dedupe_count
                .saturating_add(1);
            return;
        }
        self.execution.streaming_received = true;
        if let Some(TimelineEntry::Message { content, .. }) = self.timeline_live_message_mut(
            correlation.execution_id.as_deref(),
            correlation.turn_id.as_deref(),
            correlation.part_id.as_deref(),
        ) {
            let accepted = content.len();
            if end_bytes <= accepted {
                self.execution.telemetry.text_delta_dedupe_count = self
                    .execution
                    .telemetry
                    .text_delta_dedupe_count
                    .saturating_add(1);
            } else if start_bytes <= accepted
                && text.is_char_boundary(accepted.saturating_sub(start_bytes))
            {
                content.push_str(&text[accepted.saturating_sub(start_bytes)..]);
                self.note_searchable_content_changed();
            } else {
                self.execution.live_output_snapshot_gap = true;
                self.add_system_notice(
                    SystemNoticeKind::Warning,
                    "Assistant stream reported a byte gap; waiting for the canonical projection/terminal instead of duplicating or inventing text",
                );
            }
        } else {
            if self
                .timeline_correlated_assistant_index(
                    correlation.execution_id.as_deref(),
                    correlation.turn_id.as_deref(),
                )
                .is_some()
            {
                self.execution.telemetry.text_delta_dedupe_count = self
                    .execution
                    .telemetry
                    .text_delta_dedupe_count
                    .saturating_add(1);
                self.execution
                    .live_stream_revisions
                    .insert(stream_key, stream_revision);
                return;
            }
            if start_bytes != 0 {
                self.execution.live_output_snapshot_gap = true;
                self.execution
                    .live_stream_revisions
                    .insert(stream_key, stream_revision);
                self.add_system_notice(
                    SystemNoticeKind::Warning,
                    "Assistant stream began after a missing byte range; waiting for canonical recovery",
                );
                return;
            }
            self.timeline_push(TimelineEntry::Message {
                role: "assistant".to_string(),
                content: text,
                timestamp: App::format_timestamp(),
                identity: Some(MessageIdentity {
                    message_id: None,
                    sequence: None,
                    execution_id: correlation.execution_id,
                    turn_id: correlation.turn_id,
                    part_id: correlation.part_id,
                    source: MessageSource::Live,
                }),
            });
        }
        self.execution
            .live_stream_revisions
            .insert(stream_key, stream_revision);
        self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
        self.mark_dirty();
    }

    fn apply_cancellation_receipt(&mut self, receipt: harness_contract::turn::CancellationReceipt) {
        if receipt.session_id != self.shell.session_id {
            return;
        }
        let previous_status = self
            .execution
            .seen_cancellation_ids
            .get(&receipt.cancellation_id)
            .copied();
        if previous_status.is_some_and(|previous| {
            previous == receipt.status
                || previous != harness_contract::turn::CancellationStatus::Requested
        }) {
            return;
        }
        if previous_status == Some(harness_contract::turn::CancellationStatus::Requested) {
            self.workbench
                .system_notices
                .retain(|notice| !notice.content.contains(&receipt.cancellation_id));
        }
        self.execution
            .seen_cancellation_ids
            .insert(receipt.cancellation_id.clone(), receipt.status);
        const CANCELLATION_DEDUPE_CAPACITY: usize = 2_048;
        while self.execution.seen_cancellation_ids.len() > CANCELLATION_DEDUPE_CAPACITY {
            let Some(oldest) = self.execution.seen_cancellation_ids.keys().next().cloned() else {
                break;
            };
            self.execution.seen_cancellation_ids.remove(&oldest);
        }

        let settles_current = receipt.status
            == harness_contract::turn::CancellationStatus::Cancelled
            && (receipt.execution_id.is_empty()
                || self.execution.current_execution_id.as_deref()
                    == Some(receipt.execution_id.as_str()));
        if settles_current {
            let execution_id = (!receipt.execution_id.is_empty())
                .then_some(receipt.execution_id.clone())
                .or_else(|| self.execution.current_execution_id.clone());
            let turn_id = (!receipt.turn_id.is_empty())
                .then_some(receipt.turn_id.clone())
                .or_else(|| self.execution.current_turn_id.clone());
            self.remove_stale_live_assistant_parts(execution_id.as_deref(), turn_id.as_deref());
            self.execution.current_execution_status =
                Some(harness_contract::projection::ExecutionLiveStatus::Cancelled);
            self.execution.current_execution_status_detail = receipt.reason.clone();
            self.execution.turn_interaction.terminal_observed();
            self.record_terminal_correlation(&crate::protocol::GatewayEventCorrelation {
                session_id: receipt.session_id.clone(),
                execution_id,
                turn_id,
                ..Default::default()
            });
        }
        let effective_at_ms = receipt.effective_at_ms.unwrap_or(receipt.requested_at_ms);
        self.add_system_notice(
            SystemNoticeKind::Info,
            &format!(
                "Cancellation {} at {} ms (id {})",
                match receipt.status {
                    harness_contract::turn::CancellationStatus::Requested => "requested",
                    harness_contract::turn::CancellationStatus::Cancelled => "completed",
                    harness_contract::turn::CancellationStatus::AlreadyTerminal => {
                        "observed after the turn was already terminal"
                    }
                },
                effective_at_ms,
                receipt.cancellation_id
            ),
        );
        self.mark_dirty();
    }

    fn apply_gateway_session_event(&mut self, event: crate::protocol::GatewaySessionEvent) {
        let event = match self.apply_gateway_session_ingress_event(event) {
            Ok(()) => return,
            Err(event) => event,
        };
        let event = match self.apply_gateway_session_progress_event(event) {
            Ok(()) => return,
            Err(event) => event,
        };
        let _ = self.apply_gateway_session_terminal_event(event);
    }

    fn apply_gateway_session_ingress_event(
        &mut self,
        event: crate::protocol::GatewaySessionEvent,
    ) -> Result<(), crate::protocol::GatewaySessionEvent> {
        use crate::protocol::GatewaySessionEvent;
        match event {
            GatewaySessionEvent::UserMessageCommitted {
                correlation,
                content,
                sequence,
                created_at_ms,
            } => {
                if correlation.session_id != self.shell.session_id {
                    return Ok(());
                }
                let Some(message_id) = correlation.message_id else {
                    self.add_system_notice(
                        SystemNoticeKind::Warning,
                        "Ignored a committed user message without stable identity",
                    );
                    return Ok(());
                };
                let incoming_execution = correlation.execution_id.clone();
                let incoming_turn = correlation.turn_id.clone();
                if let Some(identity) = incoming_execution
                    .as_ref()
                    .zip(incoming_turn.as_ref())
                    .map(|(execution_id, turn_id)| (execution_id.clone(), turn_id.clone()))
                {
                    self.execution
                        .committed_ingress_correlations
                        .insert(identity);
                }
                let selects_visible_execution = self.execution.current_execution_id.is_none()
                    || self.execution.current_execution_id.as_deref()
                        == incoming_execution.as_deref()
                    || !self.turn_is_active()
                    || self.execution.current_execution_status.is_some_and(
                        harness_contract::projection::ExecutionLiveStatus::is_terminal,
                    );
                if selects_visible_execution
                    && self.execution.current_execution_id.as_deref()
                        != incoming_execution.as_deref()
                {
                    self.reset_live_execution_facts();
                }
                let identity = MessageIdentity {
                    message_id: Some(message_id.clone()),
                    sequence: Some(sequence),
                    execution_id: incoming_execution.clone(),
                    turn_id: incoming_turn.clone(),
                    part_id: correlation.part_id.clone(),
                    source: MessageSource::DurableIngress,
                };
                if selects_visible_execution {
                    self.execution.current_execution_id = incoming_execution;
                    self.execution.current_turn_id = incoming_turn;
                    self.execution.current_execution_status =
                        Some(harness_contract::projection::ExecutionLiveStatus::Queued);
                    self.execution.current_execution_status_detail =
                        Some("input durably admitted".to_string());
                }
                self.record_input_history(content.clone());
                if let Some(index) = self.timeline_message_index(&message_id) {
                    if let Some(TimelineEntry::Message {
                        role,
                        content: existing_content,
                        timestamp,
                        identity: existing_identity,
                    }) = self.timeline_get_mut(index)
                    {
                        let history_is_authoritative =
                            existing_identity.as_ref().is_some_and(|identity| {
                                identity.source == MessageSource::DurableHistory
                            });
                        if !history_is_authoritative {
                            *role = "user".to_string();
                            *existing_content = content;
                            *timestamp = Self::message_timestamp(created_at_ms);
                        }
                        *existing_identity = Some(Self::merge_message_identity(
                            existing_identity.take(),
                            identity,
                        ));
                    }
                    self.index_message_identity_at(index);
                    self.note_searchable_content_changed();
                } else {
                    self.timeline_push(TimelineEntry::Message {
                        role: "user".to_string(),
                        content,
                        timestamp: Self::message_timestamp(created_at_ms),
                        identity: Some(identity),
                    });
                }
                self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.mark_dirty();
            }
            GatewaySessionEvent::TextDelta {
                correlation,
                text,
                start_bytes,
                end_bytes,
                stream_revision,
            } => {
                self.apply_gateway_text_delta(
                    correlation,
                    text,
                    start_bytes,
                    end_bytes,
                    stream_revision,
                    None,
                );
            }
            GatewaySessionEvent::TerminalDelivery {
                correlation,
                delivery,
            } => {
                use harness_contract::live::TerminalDeliveryEvent;
                match delivery {
                    TerminalDeliveryEvent::TerminalPresentationStarted {
                        presentation_id,
                        attempt_id,
                        envelope_id,
                        envelope_revision,
                        objective_scope,
                    } => {
                        if objective_scope != harness_contract::outcome::AnswerObjectiveScope::Root
                            || !self.adopt_active_execution_correlation(&correlation)
                            || self.correlation_is_terminalized(&correlation)
                        {
                            return Ok(());
                        }
                        self.execution.turn_interaction.begin_root_presentation(
                            presentation_id,
                            attempt_id,
                            envelope_id,
                            envelope_revision,
                        );
                        self.execution.current_execution_status_detail =
                            Some("preparing root answer presentation".to_string());
                        self.mark_dirty();
                    }
                    TerminalDeliveryEvent::TextDelta {
                        presentation_id,
                        attempt_id,
                        byte_start,
                        byte_end,
                        delta,
                    } => {
                        let (Ok(start_bytes), Ok(end_bytes)) =
                            (usize::try_from(byte_start), usize::try_from(byte_end))
                        else {
                            self.execution.live_output_snapshot_gap = true;
                            return Ok(());
                        };
                        self.apply_gateway_text_delta(
                            correlation,
                            delta,
                            start_bytes,
                            end_bytes,
                            byte_end,
                            Some((presentation_id, attempt_id)),
                        );
                    }
                    TerminalDeliveryEvent::TerminalPresentationSuperseded {
                        presentation_id,
                        attempt_id,
                        reason,
                    }
                    | TerminalDeliveryEvent::TerminalPresentationAborted {
                        presentation_id,
                        attempt_id,
                        reason,
                    } => {
                        if self
                            .execution
                            .turn_interaction
                            .end_root_presentation(&presentation_id, &attempt_id)
                        {
                            let execution_id = correlation.execution_id.clone();
                            let turn_id = correlation.turn_id.clone();
                            self.remove_stale_live_assistant_parts(
                                execution_id.as_deref(),
                                turn_id.as_deref(),
                            );
                            self.execution.current_execution_status_detail = Some(reason);
                            self.mark_dirty();
                        }
                    }
                    TerminalDeliveryEvent::TerminalPresentationCommitted {
                        presentation_id,
                        attempt_id,
                        ..
                    } => {
                        // The committed lifecycle fact closes preview writes.
                        // TerminalCommitted/history remains the sole text and
                        // immutable message identity authority.
                        self.execution
                            .turn_interaction
                            .end_root_presentation(&presentation_id, &attempt_id);
                    }
                    TerminalDeliveryEvent::CancellationCommitted { receipt } => {
                        self.apply_cancellation_receipt(receipt);
                    }
                }
            }
            event => return Err(event),
        }
        Ok(())
    }

    fn apply_gateway_session_progress_event(
        &mut self,
        event: crate::protocol::GatewaySessionEvent,
    ) -> Result<(), crate::protocol::GatewaySessionEvent> {
        use crate::protocol::GatewaySessionEvent;
        match event {
            GatewaySessionEvent::ReasoningSummaryDelta {
                correlation,
                summary,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    self.apply_correlated_thinking_delta(&correlation, summary);
                }
            }
            GatewaySessionEvent::ModelStepStarted { correlation }
            | GatewaySessionEvent::ModelStepCompleted { correlation, .. }
            | GatewaySessionEvent::ItemStarted { correlation, .. } => {
                let _ = self.adopt_active_execution_correlation(&correlation);
            }
            GatewaySessionEvent::ItemCompleted { correlation, kind } => {
                if self.adopt_active_execution_correlation(&correlation)
                    && kind == "public_reasoning"
                {
                    self.complete_correlated_thinking(&correlation);
                }
            }
            GatewaySessionEvent::ToolStart {
                correlation,
                id,
                name,
                preview,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    let id = self.correlated_tool_instance_key(&correlation, &id);
                    let causality = self.correlated_tool_causality(&correlation);
                    self.apply_event(CowdEvent::ToolStart {
                        id: id.clone(),
                        name,
                        preview,
                    });
                    self.annotate_correlated_tool(&id, causality);
                }
            }
            GatewaySessionEvent::ToolProgress {
                correlation,
                id,
                name,
                progress,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    let id = self.correlated_tool_instance_key(&correlation, &id);
                    self.apply_event(CowdEvent::ToolProgress { id, name, progress });
                }
            }
            GatewaySessionEvent::ToolComplete {
                correlation,
                id,
                name,
                summary,
                exit_code,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    let id = self.correlated_tool_instance_key(&correlation, &id);
                    self.apply_event(CowdEvent::ToolComplete {
                        id,
                        name,
                        summary,
                        exit_code,
                    });
                }
            }
            GatewaySessionEvent::ExecutionPhase {
                correlation,
                status,
                detail,
            } => {
                if self.correlation_is_terminalized(&correlation) && !status.is_terminal() {
                    return Ok(());
                }
                let adopted = if status == harness_contract::projection::ExecutionLiveStatus::Queued
                {
                    self.adopt_live_correlation(&correlation)
                } else {
                    self.adopt_started_execution_correlation(&correlation)
                };
                if !adopted {
                    return Ok(());
                }
                self.execution.current_execution_status = Some(status);
                self.execution.current_execution_status_detail = detail;
                self.mark_dirty();
            }
            GatewaySessionEvent::ProviderAttempt {
                correlation,
                model,
                models_tried,
                context_window_tokens,
                context_window_source,
                packed_input_tokens,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    self.apply_event(CowdEvent::ProviderAttempt {
                        model,
                        models_tried,
                        context_window_tokens,
                        context_window_source,
                        packed_input_tokens,
                    });
                }
            }
            GatewaySessionEvent::ContextEnvelope {
                correlation,
                envelope,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    self.apply_event(CowdEvent::ContextEnvelope { envelope });
                }
            }
            GatewaySessionEvent::ContextWindow { correlation, value } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    self.apply_event(CowdEvent::ContextWindow(value));
                }
            }
            GatewaySessionEvent::TokenUsage {
                correlation,
                input,
                output,
                cache_create,
                cache_read,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    self.apply_event(CowdEvent::TokenUsage {
                        input,
                        output,
                        cache_create,
                        cache_read,
                    });
                }
            }
            GatewaySessionEvent::RunModelTelemetry {
                correlation,
                telemetry,
            } => {
                if self.adopt_active_execution_correlation(&correlation) {
                    self.apply_event(CowdEvent::RunModelTelemetry { telemetry });
                }
            }
            event => return Err(event),
        }
        Ok(())
    }

    fn apply_gateway_session_terminal_event(
        &mut self,
        event: crate::protocol::GatewaySessionEvent,
    ) -> Result<(), crate::protocol::GatewaySessionEvent> {
        use crate::protocol::GatewaySessionEvent;
        match event {
            GatewaySessionEvent::TerminalCommitted {
                correlation,
                assistant_text,
                sequence,
                token_usage,
                ..
            } => {
                if correlation.session_id != self.shell.session_id {
                    return Ok(());
                }
                let complete_identity = correlation.execution_id.is_some()
                    && correlation.turn_id.is_some()
                    && correlation.message_id.is_some()
                    && correlation.terminal_id.is_some();
                if !complete_identity {
                    self.execution.telemetry.orphan_event_count = self
                        .execution
                        .telemetry
                        .orphan_event_count
                        .saturating_add(1);
                    let warning = format!(
                        "Rejected terminal without complete execution/turn/message/terminal identity (orphan #{})",
                        self.execution.telemetry.orphan_event_count
                    );
                    self.add_system_notice(SystemNoticeKind::Warning, &warning);
                    self.show_notification(&warning);
                    return Ok(());
                }
                if correlation.replayed {
                    // Durable history is the only transcript authority for
                    // replay. A replayed commit is an ordering/cursor fact and
                    // must never append assistant prose on its own.
                    return Ok(());
                }
                let settles_current = self.adopt_live_correlation(&correlation);
                if !settles_current {
                    return Ok(());
                }
                if self.execution.current_execution_status
                    == Some(harness_contract::projection::ExecutionLiveStatus::Cancelled)
                {
                    // Cancellation won the execution terminal CAS. A delayed
                    // outbox commit from the same execution cannot resurrect
                    // assistant output or flip the surface back to Complete.
                    return Ok(());
                }
                if settles_current {
                    // Transcript durability does not classify GoalCompletion.
                    // Partial/blocked turns also commit a friendly assistant
                    // answer; lifecycle status remains owned by ExecutionLive.
                    self.execution.current_execution_id = correlation.execution_id.clone();
                    self.execution.current_turn_id = correlation.turn_id.clone();
                }
                if let Some(terminal_id) = correlation.terminal_id.as_ref() {
                    if !self.execution.seen_terminal_ids.insert(terminal_id.clone()) {
                        self.execution.telemetry.replay_terminal_dedupe_count = self
                            .execution
                            .telemetry
                            .replay_terminal_dedupe_count
                            .saturating_add(1);
                        return Ok(());
                    }
                }
                self.record_terminal_correlation(&correlation);
                if let Some(identity) = correlation
                    .execution_id
                    .as_ref()
                    .zip(correlation.turn_id.as_ref())
                    .map(|(execution_id, turn_id)| (execution_id.clone(), turn_id.clone()))
                {
                    self.execution
                        .committed_ingress_correlations
                        .remove(&identity);
                }
                if let Some(message_id) = correlation.message_id.as_deref() {
                    if let Some(usage) = token_usage.as_ref() {
                        self.record_durable_message_usage(message_id, usage);
                    }
                }
                let source = if correlation.replayed {
                    MessageSource::ReplayedTerminal
                } else {
                    MessageSource::DurableTerminal
                };
                let identity = MessageIdentity {
                    message_id: correlation.message_id,
                    sequence,
                    execution_id: correlation.execution_id.clone(),
                    turn_id: correlation.turn_id.clone(),
                    part_id: correlation.part_id.clone(),
                    source,
                };
                let mut reconciled = false;
                if let Some(message_id) = identity.message_id.as_deref() {
                    if let Some(index) = self.timeline_message_index(message_id) {
                        if let Some(TimelineEntry::Message {
                            role,
                            content,
                            identity: existing_identity,
                            ..
                        }) = self.timeline_get_mut(index)
                        {
                            let history_is_authoritative =
                                existing_identity.as_ref().is_some_and(|identity| {
                                    identity.source == MessageSource::DurableHistory
                                });
                            *role = "assistant".to_string();
                            if !history_is_authoritative && !assistant_text.is_empty() {
                                *content = assistant_text.clone();
                            }
                            *existing_identity = Some(Self::merge_message_identity(
                                existing_identity.take(),
                                identity.clone(),
                            ));
                            reconciled = true;
                        }
                        self.index_message_identity_at(index);
                        self.note_searchable_content_changed();
                    }
                }
                if !reconciled {
                    let live_index = self
                        .timeline_live_message_index(
                            correlation.execution_id.as_deref(),
                            correlation.turn_id.as_deref(),
                            correlation.part_id.as_deref(),
                        )
                        .or_else(|| {
                            self.timeline_live_assistant_index(
                                correlation.execution_id.as_deref(),
                                correlation.turn_id.as_deref(),
                            )
                        });
                    if let Some(index) = live_index {
                        if let Some(TimelineEntry::Message {
                            content,
                            identity: existing_identity,
                            ..
                        }) = self.timeline_get_mut(index)
                        {
                            // Some older/recovered terminal envelopes carry
                            // only durable identity and token metadata. Their
                            // empty snapshot must settle the correlated live
                            // bubble without erasing text already received
                            // through typed deltas.
                            if !assistant_text.is_empty() {
                                *content = assistant_text.clone();
                            }
                            *existing_identity = Some(Self::merge_message_identity(
                                existing_identity.take(),
                                identity.clone(),
                            ));
                            reconciled = true;
                        }
                        self.index_message_identity_at(index);
                        self.note_searchable_content_changed();
                    }
                }
                if !reconciled && !assistant_text.is_empty() {
                    self.timeline_push(TimelineEntry::Message {
                        role: "assistant".to_string(),
                        content: assistant_text,
                        timestamp: App::format_timestamp(),
                        identity: Some(identity),
                    });
                }
                self.remove_stale_live_assistant_parts(
                    correlation.execution_id.as_deref(),
                    correlation.turn_id.as_deref(),
                );
                for entry in self.timeline_iter_mut() {
                    match entry {
                        TimelineEntry::Thinking { expanded, .. }
                        | TimelineEntry::ToolCall { expanded, .. } => *expanded = false,
                        _ => {}
                    }
                }
                if settles_current {
                    self.execution.turn_interaction.terminal_observed();
                }
                self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.mark_dirty();
            }
            GatewaySessionEvent::TurnError { correlation, error } => {
                if self.adopt_live_correlation(&correlation) {
                    self.execution.current_execution_status =
                        Some(harness_contract::projection::ExecutionLiveStatus::Error);
                    self.execution.current_execution_status_detail = Some(error.clone());
                    self.execution.current_execution_id = correlation.execution_id;
                    self.execution.current_turn_id = correlation.turn_id;
                    self.execution.turn_interaction.terminal_observed();
                    self.add_system_notice(SystemNoticeKind::Error, &format!("Error: {error}"));
                }
            }
            event => return Err(event),
        }
        Ok(())
    }

    pub fn apply_session_input_projection(&mut self, projection: Value) {
        // A projection can be replayed after reconnect. Remember which queue
        // records were already announced so the TUI exposes the canonical id
        // exactly when it first becomes actionable, without creating a local
        // execution queue or repeating notices for the same snapshot.
        let announced_queued_ids = self
            .workbench
            .pending_inputs
            .iter()
            .filter(|input| input.status == "queued_next")
            .map(|input| input.input_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut inputs = projection
            .get("inputs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|input| {
                let status = input.get("status")?.as_str()?.to_string();
                let pending = matches!(
                    status.as_str(),
                    "received"
                        | "persisted"
                        | "classified"
                        | "attached_to_turn"
                        | "queued_next"
                        | "interrupt_requested"
                        | "control_resolved"
                );
                pending.then(|| PendingInputPreview {
                    input_id: input
                        .get("input_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    status,
                    decision: input
                        .get("decision")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string(),
                    content_preview: input
                        .get("content_preview")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                })
            })
            .filter(|input| !input.input_id.is_empty())
            .collect::<Vec<_>>();
        inputs.sort_by(|left, right| left.input_id.cmp(&right.input_id));
        inputs.truncate(32);
        let newly_queued = inputs
            .iter()
            .filter(|input| {
                input.status == "queued_next"
                    && !announced_queued_ids.contains(input.input_id.as_str())
            })
            .map(|input| input.input_id.clone())
            .collect::<Vec<_>>();
        self.workbench.pending_inputs = inputs;
        self.mark_dirty();
        for input_id in newly_queued {
            self.add_system_notice(
                SystemNoticeKind::Info,
                &format!(
                    "Follow-up queued ({input_id}). Use /queue edit {input_id} to revise it or \
                     /queue cancel {input_id} to remove it."
                ),
            );
        }
    }

    fn apply_session_input_disposition(&mut self, receipt: Value) {
        let state = receipt
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let action = receipt
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("route_input");
        let summary = receipt
            .get("error")
            .and_then(Value::as_str)
            .or_else(|| receipt.get("summary").and_then(Value::as_str))
            .unwrap_or("Runtime input disposition updated");
        if state == "applied" {
            let applied = receipt
                .get("input_ids")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect::<std::collections::HashSet<_>>();
            self.workbench
                .pending_inputs
                .retain(|input| !applied.contains(input.input_id.as_str()));
        }
        let kind = if state == "failed" {
            SystemNoticeKind::Error
        } else {
            SystemNoticeKind::Info
        };
        self.add_system_notice(kind, &format!("Input {action} {state}: {summary}"));
        self.mark_dirty();
    }

    #[must_use]
    pub fn queued_follow_up_count(&self) -> usize {
        self.workbench
            .pending_inputs
            .iter()
            .filter(|input| input.status == "queued_next")
            .count()
    }

    #[must_use]
    pub fn queued_follow_up_preview(&self) -> Option<&PendingInputPreview> {
        self.workbench
            .pending_inputs
            .iter()
            .find(|input| input.status == "queued_next")
    }

    pub fn timeline_len(&self) -> usize {
        self.timeline.total_entries
    }

    pub fn timeline_is_empty(&self) -> bool {
        self.timeline.total_entries == 0
    }

    pub fn timeline_get(&self, idx: usize) -> Option<&TimelineEntry> {
        if idx >= self.timeline.total_entries {
            return None;
        }
        for page in &self.timeline.timeline_pages {
            if idx >= page.start_index && idx < page.start_index + page.entries.len() {
                return page.entries.get(idx - page.start_index);
            }
        }
        None
    }

    pub fn timeline_entry(&self, idx: usize) -> Option<TimelineEntry> {
        self.timeline_get(idx).cloned()
    }

    pub fn timeline_get_mut(&mut self, idx: usize) -> Option<&mut TimelineEntry> {
        if idx >= self.timeline.total_entries {
            return None;
        }
        let key = self.timeline_get(idx).map(Self::timeline_entry_key)?;
        self.note_timeline_entry_mutated(idx, key);
        for page in &mut self.timeline.timeline_pages {
            if idx >= page.start_index && idx < page.start_index + page.entries.len() {
                return page.entries.get_mut(idx - page.start_index);
            }
        }
        None
    }

    pub fn timeline_last_mut(&mut self) -> Option<&mut TimelineEntry> {
        self.timeline
            .total_entries
            .checked_sub(1)
            .and_then(|index| self.timeline_get_mut(index))
    }

    pub fn timeline_last(&self) -> Option<&TimelineEntry> {
        self.timeline
            .timeline_pages
            .back()
            .and_then(|page| page.entries.last())
    }

    pub fn timeline_push(&mut self, entry: TimelineEntry) {
        let absolute_position = self
            .timeline
            .timeline_base_position
            .saturating_add(u64::try_from(self.timeline.total_entries).unwrap_or(u64::MAX));
        let (message_id, live_key, tool_id) = match &entry {
            TimelineEntry::Message {
                identity: Some(identity),
                ..
            } => (
                identity.message_id.clone(),
                (identity.source == MessageSource::Live).then(|| LiveMessageKey {
                    execution_id: identity.execution_id.clone(),
                    turn_id: identity.turn_id.clone(),
                    part_id: identity.part_id.clone(),
                }),
                None,
            ),
            TimelineEntry::ToolCall { id, .. } => (None, None, Some(id.clone())),
            _ => (None, None, None),
        };
        if self.timeline.timeline_pages.is_empty()
            || self
                .timeline
                .timeline_pages
                .back()
                .is_none_or(|p| p.entries.len() >= PAGE_SIZE)
        {
            let start = self
                .timeline
                .timeline_pages
                .back()
                .map_or(0, |p| p.start_index + p.entries.len());
            self.timeline.timeline_pages.push_back(TimelinePage {
                entries: Vec::with_capacity(PAGE_SIZE),
                start_index: start,
            });
        }
        let Some(page) = self.timeline.timeline_pages.back_mut() else {
            return;
        };
        page.entries.push(entry);
        self.timeline.total_entries += 1;
        if let Some(message_id) = message_id {
            self.timeline
                .message_timeline_positions
                .insert(message_id, absolute_position);
        }
        if let Some(live_key) = live_key {
            self.timeline
                .live_timeline_positions
                .insert(live_key, absolute_position);
        }
        if let Some(tool_id) = tool_id {
            self.timeline
                .tool_timeline_positions
                .insert(tool_id, absolute_position);
        }
        self.note_searchable_content_changed();
        self.soft_evict();
        self.hard_evict();
    }

    pub fn timeline_iter(&self) -> impl Iterator<Item = (usize, &TimelineEntry)> + '_ {
        self.timeline.timeline_pages.iter().flat_map(|page| {
            let start = page.start_index;
            page.entries
                .iter()
                .enumerate()
                .map(move |(i, e)| (start + i, e))
        })
    }

    pub fn timeline_iter_mut(&mut self) -> impl Iterator<Item = &mut TimelineEntry> + '_ {
        self.timeline.timeline_full_sync_revision =
            self.timeline.timeline_full_sync_revision.wrapping_add(1);
        self.timeline.timeline_dirty_log.clear();
        self.timeline
            .timeline_pages
            .iter_mut()
            .flat_map(|page| page.entries.iter_mut())
    }

    pub fn timeline_clone_vec(&self) -> Vec<TimelineEntry> {
        let mut v = Vec::with_capacity(self.timeline.total_entries);
        for page in &self.timeline.timeline_pages {
            v.extend(page.entries.iter().cloned());
        }
        v
    }

    fn timeline_entry_key(entry: &TimelineEntry) -> String {
        match entry {
            TimelineEntry::Message {
                identity: Some(identity),
                ..
            } => identity.message_id.as_ref().map_or_else(
                || {
                    format!(
                        "live|{}|{}|{}",
                        identity.execution_id.as_deref().unwrap_or("-"),
                        identity.turn_id.as_deref().unwrap_or("-"),
                        identity.part_id.as_deref().unwrap_or("-")
                    )
                },
                |message_id| format!("message|{message_id}"),
            ),
            TimelineEntry::Message { .. } => "message|unidentified".to_string(),
            TimelineEntry::Thinking { id, .. } => format!("thinking|{id}"),
            TimelineEntry::ToolCall { id, .. } => format!("tool|{id}"),
            TimelineEntry::SlashOutput { command, .. } => format!("slash|{command}"),
        }
    }

    fn note_timeline_entry_mutated(&mut self, index: usize, key: String) {
        const DIRTY_LOG_CAP: usize = 8_192;
        self.timeline.timeline_mutation_revision =
            self.timeline.timeline_mutation_revision.wrapping_add(1);
        self.timeline.timeline_dirty_log.push_back((
            self.timeline.timeline_mutation_revision,
            index,
            key,
        ));
        if self.timeline.timeline_dirty_log.len() > DIRTY_LOG_CAP {
            self.timeline.timeline_dirty_log.clear();
            self.timeline.timeline_full_sync_revision =
                self.timeline.timeline_full_sync_revision.wrapping_add(1);
        }
    }

    /// Return exact mutated entries since a consumer cursor. `None` means the
    /// bounded log was superseded and the consumer must perform a full sync.
    pub fn timeline_dirty_entries_since(
        &self,
        revision: u64,
    ) -> Option<(u64, Vec<(usize, TimelineEntry)>)> {
        if revision == self.timeline.timeline_mutation_revision {
            return Some((revision, Vec::new()));
        }
        let first_revision = self
            .timeline
            .timeline_dirty_log
            .front()
            .map(|(revision, _, _)| *revision)?;
        if revision.saturating_add(1) < first_revision {
            return None;
        }
        let mut dirty = BTreeMap::<usize, (&str, u64)>::new();
        for (mutation_revision, index, key) in self
            .timeline
            .timeline_dirty_log
            .iter()
            .filter(|(mutation_revision, _, _)| *mutation_revision > revision)
        {
            dirty.insert(*index, (key.as_str(), *mutation_revision));
        }
        let mut entries = Vec::with_capacity(dirty.len());
        for (index, (key, _)) in dirty {
            let entry = self.timeline_get(index)?;
            if Self::timeline_entry_key(entry) != key {
                return None;
            }
            entries.push((index, entry.clone()));
        }
        Some((self.timeline.timeline_mutation_revision, entries))
    }

    fn soft_evict(&mut self) {
        while self.timeline.total_entries > SOFT_CAP {
            let Some(front) = self.timeline.timeline_pages.front() else {
                break;
            };
            let evict_count = front.entries.len();
            let evicted_message_ids = front
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    TimelineEntry::Message {
                        identity:
                            Some(MessageIdentity {
                                message_id: Some(message_id),
                                ..
                            }),
                        ..
                    } => Some(message_id.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();

            let evicted_lines: usize = if !self.timeline.entry_line_counts.is_empty() {
                let count = evict_count.min(self.timeline.entry_line_counts.len());
                self.timeline
                    .entry_line_counts
                    .iter()
                    .take(count)
                    .map(|&c| c + 1)
                    .sum()
            } else {
                0
            };

            let drain_count = evict_count.min(self.timeline.entry_line_counts.len());
            self.timeline.entry_line_counts.drain(0..drain_count);
            self.timeline.scroll_offset = self.timeline.scroll_offset.saturating_sub(evicted_lines);
            self.timeline.timeline_cursor =
                self.timeline.timeline_cursor.saturating_sub(evict_count);
            self.timeline.search_matches.retain(|&m| m >= evict_count);
            self.timeline
                .search_matches
                .iter_mut()
                .for_each(|m| *m -= evict_count);

            self.timeline.timeline_pages.pop_front();
            self.timeline.total_entries -= evict_count;
            self.timeline.timeline_base_position = self
                .timeline
                .timeline_base_position
                .saturating_add(u64::try_from(evict_count).unwrap_or(u64::MAX));
            for message_id in evicted_message_ids {
                self.timeline.message_timeline_positions.remove(&message_id);
            }
            self.timeline
                .live_timeline_positions
                .retain(|_, position| *position >= self.timeline.timeline_base_position);
            self.timeline
                .tool_timeline_positions
                .retain(|_, position| *position >= self.timeline.timeline_base_position);
            self.note_searchable_content_changed();
            self.timeline.timeline_full_sync_revision =
                self.timeline.timeline_full_sync_revision.wrapping_add(1);

            let mut next_start = 0usize;
            for page in &mut self.timeline.timeline_pages {
                page.start_index = next_start;
                next_start += page.entries.len();
            }
        }
    }

    fn hard_evict(&mut self) {
        while self.timeline.total_entries > HARD_CAP {
            let Some(front) = self.timeline.timeline_pages.front() else {
                break;
            };
            let evict_count = front.entries.len();
            let evicted_message_ids = front
                .entries
                .iter()
                .filter_map(|entry| match entry {
                    TimelineEntry::Message {
                        identity:
                            Some(MessageIdentity {
                                message_id: Some(message_id),
                                ..
                            }),
                        ..
                    } => Some(message_id.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();

            let evicted_lines: usize = if !self.timeline.entry_line_counts.is_empty() {
                let count = evict_count.min(self.timeline.entry_line_counts.len());
                self.timeline
                    .entry_line_counts
                    .iter()
                    .take(count)
                    .map(|&c| c + 1)
                    .sum()
            } else {
                0
            };

            let drain_count = evict_count.min(self.timeline.entry_line_counts.len());
            self.timeline.entry_line_counts.drain(0..drain_count);
            self.timeline.scroll_offset = self.timeline.scroll_offset.saturating_sub(evicted_lines);
            self.timeline.timeline_cursor =
                self.timeline.timeline_cursor.saturating_sub(evict_count);
            self.timeline.search_matches.retain(|&m| m >= evict_count);
            self.timeline
                .search_matches
                .iter_mut()
                .for_each(|m| *m -= evict_count);

            self.timeline.timeline_pages.pop_front();
            self.timeline.total_entries -= evict_count;
            self.timeline.timeline_base_position = self
                .timeline
                .timeline_base_position
                .saturating_add(u64::try_from(evict_count).unwrap_or(u64::MAX));
            for message_id in evicted_message_ids {
                self.timeline.message_timeline_positions.remove(&message_id);
            }
            self.timeline
                .live_timeline_positions
                .retain(|_, position| *position >= self.timeline.timeline_base_position);
            self.timeline
                .tool_timeline_positions
                .retain(|_, position| *position >= self.timeline.timeline_base_position);
            self.note_searchable_content_changed();
            self.timeline.timeline_full_sync_revision =
                self.timeline.timeline_full_sync_revision.wrapping_add(1);

            let mut next_start = 0usize;
            for page in &mut self.timeline.timeline_pages {
                page.start_index = next_start;
                next_start += page.entries.len();
            }
        }
    }

    pub fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        F[self.shell.spinner_idx % F.len()]
    }

    pub fn tick(&mut self) {
        self.shell.spinner_idx = self.shell.spinner_idx.wrapping_add(1);
        if self.shell.notification_ttl > 0 {
            self.shell.notification_ttl -= 1;
            if self.shell.notification_ttl == 0 {
                self.shell.notification = None;
            }
        }
    }

    #[must_use]
    pub fn turn_is_active(&self) -> bool {
        self.execution.turn_interaction.is_active()
    }

    pub fn next_model(&mut self) -> Option<String> {
        if self.shell.available_models.len() <= 1 {
            return None;
        }
        if let Some(pos) = self
            .shell
            .available_models
            .iter()
            .position(|m| m == &self.shell.model)
        {
            let idx = (pos + 1) % self.shell.available_models.len();
            self.shell.model = self.shell.available_models[idx].clone();
            self.shell.model_dirty = true;
            Some(self.shell.model.clone())
        } else {
            self.shell.model = self.shell.available_models[0].clone();
            self.shell.model_dirty = true;
            Some(self.shell.model.clone())
        }
    }

    pub fn show_notification(&mut self, msg: &str) {
        self.shell.notification = Some(msg.to_string());
        self.shell.notification_ttl = 30;
    }

    pub fn format_timestamp() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = dur.as_secs();
        let h = (secs / 3600) % 24;
        let m = (secs / 60) % 60;
        format!("{h:02}:{m:02}")
    }

    pub fn open_session_picker(&mut self, sessions: Vec<SessionSummary>) {
        self.shell.picker_sessions = sessions;
        self.shell.picker_idx = 0;
        self.shell.picker_active = true;
    }

    pub fn close_session_picker(&mut self) {
        self.shell.picker_active = false;
        self.shell.picker_sessions.clear();
        self.shell.picker_idx = 0;
    }

    pub fn picker_up(&mut self) {
        if self.shell.picker_idx > 0 {
            self.shell.picker_idx -= 1;
        }
    }

    pub fn picker_down(&mut self) {
        if self.shell.picker_idx + 1 < self.shell.picker_sessions.len() {
            self.shell.picker_idx += 1;
        }
    }

    pub fn picker_selected_id(&self) -> Option<&str> {
        self.shell
            .picker_sessions
            .get(self.shell.picker_idx)
            .map(|s| s.id.as_str())
    }

    pub fn cursor_up(&mut self) -> bool {
        if self.timeline_is_empty() {
            return false;
        }
        let mut idx = self.timeline.timeline_cursor;
        loop {
            if idx == 0 {
                break;
            }
            idx -= 1;
            if self.timeline_get(idx).is_some_and(|e| e.is_collapsible()) {
                self.timeline.timeline_cursor = idx;
                self.timeline.auto_scroll = false;
                return true;
            }
        }
        false
    }

    pub fn cursor_down(&mut self) -> bool {
        if self.timeline_is_empty() {
            return false;
        }
        let mut idx = self.timeline.timeline_cursor;
        while idx + 1 < self.timeline_len() {
            idx += 1;
            if self.timeline_get(idx).is_some_and(|e| e.is_collapsible()) {
                self.timeline.timeline_cursor = idx;
                self.timeline.auto_scroll = true;
                return true;
            }
        }
        false
    }

    pub fn toggle_expand_current(&mut self) {
        if let Some(entry) = self.timeline_get_mut(self.timeline.timeline_cursor) {
            entry.toggle();
            self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
        }
    }

    pub fn add_message(&mut self, role: &str, content: &str) {
        self.add_message_with_id(role, content, None);
    }

    pub fn add_message_with_id(&mut self, role: &str, content: &str, message_id: Option<String>) {
        if role == "system" {
            let kind = if content.to_ascii_lowercase().contains("error")
                || content.to_ascii_lowercase().contains("failed")
                || content.to_ascii_lowercase().contains("unavailable")
            {
                SystemNoticeKind::Error
            } else if content.to_ascii_lowercase().contains("degraded")
                || content.to_ascii_lowercase().contains("warning")
            {
                SystemNoticeKind::Warning
            } else {
                SystemNoticeKind::Info
            };
            self.add_system_notice(kind, content);
            return;
        }
        self.timeline_push(TimelineEntry::Message {
            role: role.to_string(),
            content: content.to_string(),
            timestamp: App::format_timestamp(),
            identity: Some(MessageIdentity {
                message_id,
                sequence: None,
                execution_id: None,
                turn_id: None,
                part_id: None,
                source: MessageSource::Local,
            }),
        });
        self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
    }

    pub fn begin_message_admission(
        &mut self,
        content: &str,
        client_message_id: String,
        submission_generation: u64,
        starts_new_turn: bool,
    ) {
        self.execution
            .pending_message_admissions
            .insert(client_message_id.clone(), submission_generation);
        self.add_message_with_id("user", content, Some(client_message_id));
        if starts_new_turn {
            self.apply_event(CowdEvent::TurnStarted);
        }
    }

    pub fn add_system_notice(&mut self, kind: SystemNoticeKind, content: &str) {
        let trimmed = content.trim();
        if trimmed.is_empty() {
            return;
        }
        self.workbench.system_notices.push_back(SystemNotice {
            kind,
            content: trimmed.to_string(),
            timestamp: App::format_timestamp(),
        });
        const SYSTEM_NOTICE_CAP: usize = 500;
        while self.workbench.system_notices.len() > SYSTEM_NOTICE_CAP {
            self.workbench.system_notices.pop_front();
        }
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
    }

    pub fn recent_system_notice_labels(&self, limit: usize) -> Vec<String> {
        self.workbench
            .system_notices
            .iter()
            .rev()
            .take(limit)
            .map(SystemNotice::label)
            .collect()
    }

    pub fn add_slash_output(&mut self, command: &str, output: &str) {
        let trimmed = output.trim();
        if trimmed.is_empty() {
            return;
        }
        self.timeline_push(TimelineEntry::SlashOutput {
            command: command.to_string(),
            output: trimmed.to_string(),
            expanded: trimmed.lines().count() <= 3,
        });
        self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
        self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
    }

    pub fn copy_focused_content(&self) -> bool {
        let Some(entry) = self.timeline_get(self.timeline.timeline_cursor) else {
            return false;
        };
        let text = entry.full_text();
        if text.is_empty() {
            return false;
        }
        crate::osc52::write_osc52_clipboard(&text)
    }

    pub fn session_activity_stats(&self) -> SessionActivityStats {
        let mut stats = SessionActivityStats::default();
        stats.event_count = self.timeline_len();
        for (_, entry) in self.timeline_iter() {
            match entry {
                TimelineEntry::Thinking { .. } => stats.thinking_count += 1,
                TimelineEntry::ToolCall { .. } => stats.tool_count += 1,
                TimelineEntry::Message { role, .. } => {
                    if role == "user" || role == "assistant" {
                        stats.message_count += 1;
                    }
                }
                TimelineEntry::SlashOutput { .. } => {}
            }
        }
        stats
    }

    pub fn execute_search(&mut self, query: &str) {
        self.timeline.search_query = query.to_string();
        self.timeline.search_matches.clear();
        self.timeline.search_current = 0;

        let lower = query.to_lowercase();
        self.ensure_search_text_index();
        self.timeline.search_matches = self
            .timeline
            .search_text_index
            .iter()
            .enumerate()
            .filter_map(|(index, text)| {
                (!text.is_empty() && text.contains(&lower)).then_some(index)
            })
            .collect();

        self.go_search_match(0);
    }

    pub fn search_next(&mut self) {
        if self.timeline.search_matches.is_empty() {
            return;
        }
        let idx = if self.timeline.search_current + 1 < self.timeline.search_matches.len() {
            self.timeline.search_current + 1
        } else {
            0
        };
        self.go_search_match(idx);
    }

    pub fn search_prev(&mut self) {
        if self.timeline.search_matches.is_empty() {
            return;
        }
        let idx = if self.timeline.search_current > 0 {
            self.timeline.search_current - 1
        } else {
            self.timeline.search_matches.len() - 1
        };
        self.go_search_match(idx);
    }

    fn go_search_match(&mut self, match_idx: usize) {
        if let Some(&entry_idx) = self.timeline.search_matches.get(match_idx) {
            self.timeline.search_current = match_idx;
            self.timeline.timeline_cursor = entry_idx;
            self.timeline.auto_scroll = false;
            // ChatView owns the wrapped visual-row index and performs the
            // actual scroll after width-aware cache reconciliation.
            self.request_redraw();
        }
    }

    pub fn cancel_search(&mut self) {
        self.timeline.search_query.clear();
        self.timeline.search_matches.clear();
        self.timeline.search_current = 0;
        self.timeline.search_active = false;
    }

    pub fn scroll_to_entry(&mut self, entry_idx: usize) {
        let vh = self.timeline.viewport_height.max(1);
        let mut offset: usize = 0;
        for i in 0..entry_idx.min(self.timeline.entry_line_counts.len()) {
            offset += self.timeline.entry_line_counts[i] + 1;
        }
        let entry_h = self
            .timeline
            .entry_line_counts
            .get(entry_idx)
            .copied()
            .unwrap_or(1);

        let scroll = self.timeline.scroll_offset;
        if offset < scroll {
            self.timeline.scroll_offset = offset;
        } else if offset + entry_h > scroll + vh {
            self.timeline.scroll_offset = offset.saturating_sub(vh.saturating_sub(entry_h));
        }
    }

    pub fn scroll_page_up(&mut self) {
        let amount = self.timeline.viewport_height.max(1).saturating_sub(1);
        self.timeline.scroll_offset = self.timeline.scroll_offset.saturating_sub(amount);
    }

    pub fn scroll_page_down(&mut self) {
        let amount = self.timeline.viewport_height.max(1).saturating_sub(1);
        self.timeline.scroll_offset = self.timeline.scroll_offset.saturating_add(amount);
    }

    pub fn history_prev(&mut self) -> Option<String> {
        if self.shell.input_history.is_empty() {
            return None;
        }
        let idx = match self.shell.history_idx {
            Some(0) => return None,
            Some(i) => i - 1,
            None => self.shell.input_history.len().saturating_sub(1),
        };
        self.shell.history_idx = Some(idx);
        self.shell.input_history.get(idx).cloned()
    }

    pub fn history_next(&mut self) -> Option<String> {
        let idx = match self.shell.history_idx {
            Some(i) if i + 1 < self.shell.input_history.len() => i + 1,
            _ => {
                self.shell.history_idx = None;
                return Some(String::new());
            }
        };
        self.shell.history_idx = Some(idx);
        self.shell.input_history.get(idx).cloned()
    }

    pub fn apply_event(&mut self, event: CowdEvent) {
        let event = match self.apply_session_history_event(event) {
            Ok(()) => return,
            Err(event) => event,
        };
        let event = match self.apply_session_control_event(event) {
            Ok(()) => return,
            Err(event) => event,
        };
        let event = match self.apply_turn_activity_event(event) {
            Ok(()) => return,
            Err(event) => event,
        };
        let _ = self.apply_shell_projection_event(event);
    }

    fn apply_session_history_event(&mut self, event: CowdEvent) -> Result<(), CowdEvent> {
        match event {
            CowdEvent::SessionScoped {
                session_id, event, ..
            } => {
                if session_id == self.shell.session_id {
                    self.apply_event(*event);
                }
            }
            CowdEvent::GatewaySession { event } => {
                self.apply_gateway_session_event(event);
            }
            CowdEvent::SessionHistoryPage { page } => {
                self.apply_history_page(page);
            }
            CowdEvent::SessionHistoryIndexLoaded { projection } => {
                if projection.session_id == self.shell.session_id {
                    self.history.history_total_messages = projection.total_messages as usize;
                    self.history.history_has_older =
                        projection.total_messages as usize > self.timeline_len();
                    if !matches!(
                        projection.recovery_state,
                        crate::protocol::SessionHistoryRecoveryState::Ready
                            | crate::protocol::SessionHistoryRecoveryState::ManifestRebuilt
                    ) {
                        self.add_system_notice(
                            SystemNoticeKind::Warning,
                            &format!(
                                "Session history index recovery state: {:?}",
                                projection.recovery_state
                            ),
                        );
                    }
                    self.history.session_history_index = Some(projection);
                    self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
                }
            }
            CowdEvent::SessionHistoryCatchupPage { page } => {
                let visible_at_tail =
                    self.history.history_window_end_offset >= self.history.history_total_messages;
                if visible_at_tail {
                    self.apply_history_page(page);
                }
            }
            CowdEvent::SessionHistoryHydrated {
                session_id,
                kind,
                duration_ms,
                message_count,
                page_count,
                oldest_offset,
                total_messages,
                next_sequence: _,
                has_older,
            } => {
                if session_id == self.shell.session_id {
                    self.history.history_hydrated = true;
                    self.history.history_hydration_error = None;
                    match kind {
                        crate::protocol::SessionHistoryHydrationKind::InitialWindow => {
                            self.history.history_oldest_offset = oldest_offset;
                            self.history.history_window_end_offset =
                                oldest_offset.saturating_add(message_count);
                            self.history.history_total_messages =
                                total_messages.max(self.history.history_window_end_offset);
                            self.history.history_has_older = has_older;
                        }
                        crate::protocol::SessionHistoryHydrationKind::IncrementalCatchup => {
                            let previous_total = self.history.history_total_messages;
                            let was_at_tail =
                                self.history.history_window_end_offset >= previous_total;
                            self.history.history_total_messages =
                                self.history.history_total_messages.max(total_messages);
                            if was_at_tail {
                                self.history.history_window_end_offset =
                                    self.history.history_total_messages;
                                let visible_span = self
                                    .history
                                    .history_window_end_offset
                                    .saturating_sub(self.history.history_oldest_offset);
                                if visible_span > SOFT_CAP {
                                    self.history.history_oldest_offset = self
                                        .history
                                        .history_window_end_offset
                                        .saturating_sub(SOFT_CAP);
                                }
                                self.history.history_has_older =
                                    self.history.history_oldest_offset > 0;
                            }
                        }
                    }
                    self.history.history_window_truncated = self.history.history_has_older;
                    self.execution.telemetry.history_hydration_duration_ms = Some(duration_ms);
                    self.execution.telemetry.history_hydrated_messages = self
                        .execution
                        .telemetry
                        .history_hydrated_messages
                        .saturating_add(message_count);
                    self.execution.telemetry.history_hydration_pages = self
                        .execution
                        .telemetry
                        .history_hydration_pages
                        .saturating_add(page_count);
                    if self.history.history_has_older {
                        self.add_system_notice(
                            SystemNoticeKind::Info,
                            "Older durable history remains available; use /history older to load the preceding page without blocking live events.",
                        );
                    }
                    self.mark_dirty();
                }
            }
            CowdEvent::SessionHistoryOlderPage {
                mut page,
                oldest_offset,
                has_older,
            } => {
                if page.session_id == self.shell.session_id {
                    if self.turn_is_active() {
                        self.history.history_loading_older = false;
                        self.add_system_notice(
                            SystemNoticeKind::Warning,
                            "Older history was not installed because a live turn started while the page was loading",
                        );
                        return Ok(());
                    }
                    self.history.history_prepend_anchor_message_id = self
                        .timeline_get(self.timeline.timeline_cursor)
                        .and_then(|entry| match entry {
                            TimelineEntry::Message {
                                identity:
                                    Some(MessageIdentity {
                                        message_id: Some(message_id),
                                        ..
                                    }),
                                ..
                            } => Some(message_id.clone()),
                            _ => None,
                        })
                        .or_else(|| {
                            self.timeline_iter().find_map(|(_, entry)| match entry {
                                TimelineEntry::Message {
                                    identity:
                                        Some(MessageIdentity {
                                            message_id: Some(message_id),
                                            ..
                                        }),
                                    ..
                                } => Some(message_id.clone()),
                                _ => None,
                            })
                        });
                    self.make_room_for_older_history(page.messages.len());
                    self.history.history_loading_older = false;
                    self.history.history_oldest_offset = oldest_offset;
                    self.history.history_window_end_offset = self
                        .history
                        .history_window_end_offset
                        .saturating_sub(page.messages.len());
                    self.history.history_total_messages =
                        self.history.history_total_messages.max(page.total);
                    self.history.history_has_older = has_older;
                    // API `has_more` points toward newer messages. This page
                    // is nevertheless complete for the older-window action.
                    page.has_more = false;
                    self.apply_history_page(page);
                    if let Some(anchor) = self.history.history_prepend_anchor_message_id.as_deref()
                    {
                        if let Some(index) = self.timeline_message_index(anchor) {
                            self.timeline.timeline_cursor = index;
                            self.timeline.auto_scroll = false;
                        }
                    }
                    self.history.history_prepend_revision =
                        self.history.history_prepend_revision.wrapping_add(1);
                }
            }
            CowdEvent::SessionHistoryNewerPage {
                mut page,
                window_end_offset,
                has_newer,
            } => {
                if page.session_id == self.shell.session_id {
                    self.history.history_loading_newer = false;
                    if self.turn_is_active() {
                        self.add_system_notice(
                            SystemNoticeKind::Warning,
                            "Newer history was not installed because a live turn started while the page was loading",
                        );
                        return Ok(());
                    }
                    let loaded = page.messages.len();
                    self.make_room_for_newer_history(loaded);
                    self.history.history_oldest_offset =
                        self.history.history_oldest_offset.saturating_add(loaded);
                    self.history.history_window_end_offset = window_end_offset;
                    self.history.history_total_messages =
                        self.history.history_total_messages.max(page.total);
                    self.history.history_has_older = self.history.history_oldest_offset > 0;
                    page.has_more = false;
                    self.apply_history_page(page);
                    self.timeline.auto_scroll = !has_newer;
                    if self.timeline.auto_scroll {
                        self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
                    }
                }
            }
            CowdEvent::SessionHistoryLatestPage {
                mut page,
                oldest_offset,
            } => {
                if page.session_id == self.shell.session_id {
                    self.history.history_loading_newer = false;
                    if self.turn_is_active() {
                        self.add_system_notice(
                            SystemNoticeKind::Warning,
                            "Latest history was not installed because a live turn started while the page was loading",
                        );
                        return Ok(());
                    }
                    self.clear_durable_history_window();
                    self.history.history_oldest_offset = oldest_offset;
                    self.history.history_window_end_offset =
                        oldest_offset.saturating_add(page.messages.len());
                    self.history.history_total_messages = page.total;
                    self.history.history_has_older = oldest_offset > 0;
                    page.has_more = false;
                    self.apply_history_page(page);
                    self.timeline.auto_scroll = true;
                    self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
                }
            }
            event => return Err(event),
        }
        Ok(())
    }

    fn apply_session_control_event(&mut self, event: CowdEvent) -> Result<(), CowdEvent> {
        match event {
            CowdEvent::SessionHistoryOlderFailed { session_id, error } => {
                if session_id == self.shell.session_id {
                    self.history.history_loading_older = false;
                    self.history.history_loading_newer = false;
                    self.add_system_notice(
                        SystemNoticeKind::Error,
                        &format!("Loading older durable history failed: {error}"),
                    );
                }
            }
            CowdEvent::SessionHistoryHydrationFailed { session_id, error } => {
                if session_id == self.shell.session_id {
                    self.history.history_hydrated = false;
                    self.history.history_hydration_error = Some(error.clone());
                    self.add_system_notice(
                        SystemNoticeKind::Error,
                        &format!("Session history unavailable: {error}"),
                    );
                }
            }
            CowdEvent::MessageAdmissionAccepted {
                session_id,
                client_message_id,
                submission_generation,
            } => {
                if session_id == self.shell.session_id
                    && self
                        .execution
                        .pending_message_admissions
                        .get(&client_message_id)
                        .is_some_and(|generation| *generation == submission_generation)
                {
                    self.execution
                        .pending_message_admissions
                        .remove(&client_message_id);
                }
            }
            CowdEvent::MessageAdmissionFailed {
                session_id,
                client_message_id,
                submission_generation,
                original_text,
                started_new_turn,
                error,
            } => {
                if session_id != self.shell.session_id
                    || self
                        .execution
                        .pending_message_admissions
                        .get(&client_message_id)
                        .is_none_or(|generation| *generation != submission_generation)
                {
                    return Ok(());
                }
                self.execution
                    .pending_message_admissions
                    .remove(&client_message_id);
                let retained = self
                    .timeline_clone_vec()
                    .into_iter()
                    .filter(|entry| {
                        !matches!(
                            entry,
                            TimelineEntry::Message {
                                identity: Some(MessageIdentity {
                                    message_id: Some(message_id),
                                    ..
                                }),
                                ..
                            } if message_id == &client_message_id
                        )
                    })
                    .collect();
                self.replace_timeline_entries(retained);
                if self.shell.input.text().is_empty() {
                    self.shell.input.set_text(&original_text);
                }
                if started_new_turn
                    && self.execution.pending_message_admissions.is_empty()
                    && matches!(
                        self.execution.turn_interaction.transport,
                        crate::components::turn_interaction::TransportState::Submitting
                    )
                {
                    self.execution
                        .turn_interaction
                        .reduce(crate::components::turn_interaction::TurnInteractionAction::Reset);
                }
                self.add_system_notice(
                    SystemNoticeKind::Error,
                    &format!(
                        "Gateway rejected the message before durable admission; the draft was restored: {error}"
                    ),
                );
                self.mark_dirty();
            }
            CowdEvent::SessionAuthorizationRevoked { session_id, reason } => {
                if session_id == self.shell.session_id {
                    self.revoke_session_authorization(&reason);
                }
            }
            CowdEvent::SessionStreamConnection { session_id, state } => {
                if session_id == self.shell.session_id {
                    self.execution.stream_connection_state = state.clone();
                    match state {
                        crate::protocol::SessionStreamConnectionState::Connecting => {
                            self.execution.turn_interaction.reconnecting();
                        }
                        crate::protocol::SessionStreamConnectionState::Connected => {}
                        crate::protocol::SessionStreamConnectionState::Reconnecting {
                            after_cursor,
                            ..
                        } => {
                            self.execution.telemetry.session_sse_reconnect_count = self
                                .execution
                                .telemetry
                                .session_sse_reconnect_count
                                .saturating_add(1);
                            self.execution.telemetry.session_sse_last_cursor = after_cursor;
                            self.execution.turn_interaction.reconnecting();
                        }
                    }
                    self.mark_dirty();
                }
            }
            CowdEvent::ExecutionProjectionConnection { state, .. } => {
                self.execution.projection_connection_state = Some(state.clone());
                match state {
                    crate::protocol::SessionStreamConnectionState::Connecting => {
                        self.execution.turn_interaction.reconnecting();
                    }
                    crate::protocol::SessionStreamConnectionState::Reconnecting {
                        after_cursor,
                        ..
                    } => {
                        self.execution.telemetry.projection_sse_reconnect_count = self
                            .execution
                            .telemetry
                            .projection_sse_reconnect_count
                            .saturating_add(1);
                        self.execution.telemetry.projection_sse_last_cursor = after_cursor;
                        self.execution.turn_interaction.reconnecting();
                    }
                    crate::protocol::SessionStreamConnectionState::Connected => {
                        // A transport handshake alone does not make metrics
                        // current. The next canonical snapshot clears stale.
                    }
                }
                self.mark_dirty();
            }
            CowdEvent::MissionProjectionSnapshot {
                snapshot: materialized,
                ..
            } => {
                let mut snapshot =
                    crate::runtime_control_store::RuntimeControlSnapshot::from_app(self);
                snapshot.ingest_mission_snapshot(materialized);
                snapshot.apply_to_app(self);
                self.mark_dirty();
            }
            CowdEvent::MissionProjectionDelta { delta, .. } => {
                let mut snapshot =
                    crate::runtime_control_store::RuntimeControlSnapshot::from_app(self);
                if !snapshot.ingest_mission_delta(&delta) {
                    self.add_system_notice(
                        SystemNoticeKind::Warning,
                        "Mission projection delta requires a fresh snapshot",
                    );
                }
                snapshot.apply_to_app(self);
                self.mark_dirty();
            }
            event => return Err(event),
        }
        Ok(())
    }

    fn apply_turn_activity_event(&mut self, event: CowdEvent) -> Result<(), CowdEvent> {
        match event {
            CowdEvent::ReasoningSummaryDelta { summary } => {
                let mut found = false;
                if let Some(TimelineEntry::Thinking {
                    content, complete, ..
                }) = self.timeline_last_mut()
                {
                    if !*complete {
                        if summary.starts_with(content.as_str()) {
                            content.clear();
                            content.push_str(&summary);
                        } else {
                            content.push_str(&summary);
                        }
                        found = true;
                    }
                }
                if !found {
                    let id = self.timeline.thinking_id_counter;
                    self.timeline.thinking_id_counter += 1;
                    self.timeline_push(TimelineEntry::Thinking {
                        id,
                        causal_item_id: None,
                        causality: None,
                        content: summary,
                        complete: false,
                        expanded: false,
                    });
                    self.execution.current_turn_thinking_count =
                        self.execution.current_turn_thinking_count.saturating_add(1);
                    self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
                } else {
                    self.timeline.lines_dirty = true;
                }
                self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
            }

            CowdEvent::ToolStart { id, name, preview } => {
                let id = self.current_tool_instance_key(&id);
                if self
                    .timeline
                    .tool_timeline_positions
                    .get(&id)
                    .copied()
                    .and_then(|position| self.logical_timeline_index(position))
                    .is_some()
                {
                    return Ok(());
                }
                self.timeline_push(TimelineEntry::ToolCall {
                    id,
                    name,
                    preview,
                    output: String::new(),
                    done: false,
                    expanded: true,
                    exit_code: None,
                    causality: None,
                });
                self.execution.current_turn_tool_count =
                    self.execution.current_turn_tool_count.saturating_add(1);
                self.timeline.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::ToolProgress {
                id,
                name: _,
                progress,
            } => {
                let id = self.current_tool_instance_key(&id);
                let tool_index = self
                    .timeline
                    .tool_timeline_positions
                    .get(&id)
                    .copied()
                    .and_then(|absolute| self.logical_timeline_index(absolute));
                let found_output =
                    tool_index.and_then(|index| match self.timeline_get_mut(index) {
                        Some(TimelineEntry::ToolCall { output, .. }) => Some(output),
                        _ => None,
                    });
                if let Some(output) = found_output {
                    output.push_str(&progress);
                    if output.len() > 4096 {
                        let mut keep_from = output.len() - 4096;
                        while keep_from < output.len() && !output.is_char_boundary(keep_from) {
                            keep_from += 1;
                        }
                        output.drain(..keep_from);
                    }
                    self.timeline.lines_dirty = true;
                }
            }

            CowdEvent::ToolComplete {
                id,
                name: _,
                summary,
                exit_code,
            } => {
                let id = self.current_tool_instance_key(&id);
                let tool_index = self
                    .timeline
                    .tool_timeline_positions
                    .get(&id)
                    .copied()
                    .and_then(|absolute| self.logical_timeline_index(absolute));
                let found = tool_index.and_then(|index| match self.timeline_get_mut(index) {
                    Some(TimelineEntry::ToolCall {
                        output,
                        done,
                        expanded,
                        exit_code,
                        ..
                    }) => Some((output, done, expanded, exit_code)),
                    _ => None,
                });
                if let Some((output, done, expanded, ec)) = found {
                    *output = summary;
                    *done = true;
                    *expanded = false;
                    *ec = exit_code;
                }
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::TokenUsage {
                input,
                output,
                cache_create,
                cache_read,
            } => {
                self.history.input_tokens = input;
                self.history.output_tokens = output;
                self.shell.token_count = input + output + cache_create + cache_read;
                self.execution.turn_input_tokens =
                    input.saturating_sub(self.execution.pre_turn_input);
                self.execution.turn_output_tokens =
                    output.saturating_sub(self.execution.pre_turn_output);
                self.execution.turn_usage_known = true;
            }
            CowdEvent::RunModelTelemetry { telemetry } => {
                if let Some(model) = telemetry
                    .model
                    .as_ref()
                    .filter(|model| !model.trim().is_empty())
                {
                    self.shell.effective_model = Some(model.clone());
                    self.shell.model = model.clone();
                    self.shell.model_source = Some("runtime.run_model_telemetry".to_string());
                }
                self.history.input_tokens = telemetry.input_tokens;
                self.history.output_tokens = telemetry.output_tokens;
                self.shell.token_count = telemetry.total_tokens;
                self.execution.turn_input_tokens = telemetry.input_tokens;
                self.execution.turn_output_tokens = telemetry.output_tokens;
                self.execution.turn_usage_known = !matches!(
                    telemetry.usage_source.as_str(),
                    "" | "unknown" | "pending" | "runtime_request_budget_estimate"
                );
                let metrics = self
                    .execution
                    .current_run_metrics
                    .get_or_insert_with(Default::default);
                metrics.input_tokens = telemetry.input_tokens;
                metrics.output_tokens = telemetry.output_tokens;
                metrics.total_tokens = telemetry.total_tokens;
                self.execution.latest_model_telemetry = Some(telemetry);
                self.refresh_model_mismatch_telemetry();
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            event => return Err(event),
        }
        Ok(())
    }

    fn apply_shell_projection_event(&mut self, event: CowdEvent) -> Result<(), CowdEvent> {
        match event {
            CowdEvent::ContextWindow(ctx) => {
                self.execution.context_window = ctx;
                self.execution.context_window_tokens = Some(ctx);
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }
            CowdEvent::ProviderAttempt {
                model,
                context_window_tokens,
                context_window_source,
                packed_input_tokens,
                ..
            } => {
                self.shell.effective_model = Some(model);
                self.shell.model_source = Some("runtime.provider_attempt.model".to_string());
                // ProviderAttempt is a pre-request estimate delivered on a
                // separate event stream. It may arrive after the canonical
                // terminal projection, so it must never replace observed
                // provider usage for the same turn. TurnStarted resets these
                // fields before the next request can install a new estimate.
                if self.execution.context_usage_source.as_deref() != Some("provider_actual") {
                    self.execution.context_used_tokens = Some(packed_input_tokens);
                    self.execution.context_window_tokens = Some(context_window_tokens);
                    self.execution.context_remaining_tokens =
                        Some(context_window_tokens.saturating_sub(packed_input_tokens));
                    self.execution.context_usage_percent_bp =
                        (context_window_tokens > 0).then(|| {
                            packed_input_tokens
                                .saturating_mul(10_000)
                                .saturating_div(context_window_tokens)
                                .min(10_000) as u16
                        });
                    self.execution.context_usage_source = Some(format!(
                        "runtime.provider_attempt.request_budget:{context_window_source}"
                    ));
                    self.execution.context_window = context_window_tokens;
                }
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }
            CowdEvent::ContextEnvelope { envelope } => {
                self.execution.latest_context_envelope = Some(envelope);
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }
            CowdEvent::RuntimePolicyDecision { summary } => {
                self.execution.latest_runtime_policy = Some(summary);
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }
            CowdEvent::ExecutionGraphSummary { summary } => {
                self.execution.latest_execution_graph_summary = Some(summary);
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::PermissionRevisionChanged { .. } => {
                // This event is intentionally not a second policy authority.
                // Gateway's typed execution-policy projection owns the full
                // state; the revision only causes presentation invalidation.
                self.timeline.render_version = self.timeline.render_version.wrapping_add(1);
            }

            CowdEvent::TurnStarted => {
                self.execution.turn_interaction.submit_started();
                self.reset_live_execution_facts();
                self.execution.streaming_received = false;
                self.execution.latest_context_envelope = None;
                self.execution.latest_runtime_policy = None;
                self.execution.latest_execution_graph_summary = None;
                self.execution.latest_execution_projection = None;
                self.timeline.thinking_id_counter = 0;
                self.execution.pre_turn_input = self.history.input_tokens;
                self.execution.pre_turn_output = self.history.output_tokens;
                self.execution.turn_input_tokens = 0;
                self.execution.turn_output_tokens = 0;
                self.execution.turn_usage_known = false;
                self.execution.current_turn_tool_count = 0;
                self.execution.current_turn_thinking_count = 0;
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::ResourcesCommitted { ids } => {
                self.workbench
                    .pending_resources
                    .retain(|resource| !ids.contains(&resource.id));
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionInputProjection { projection } => {
                self.apply_session_input_projection(projection);
            }
            CowdEvent::SessionInputDispositionChanged { receipt } => {
                self.apply_session_input_disposition(receipt);
            }
            CowdEvent::ResourceUploaded { id, label, kind } => {
                if !self
                    .workbench
                    .pending_resources
                    .iter()
                    .any(|resource| resource.id == id)
                {
                    self.workbench.pending_resources.push(PendingResource {
                        id: id.clone(),
                        label: label.clone(),
                        kind: kind.clone(),
                    });
                }
                self.add_system_notice(
                    SystemNoticeKind::Info,
                    &format!("Attached resource {label} ({kind}) as resource://{id}"),
                );
            }
            CowdEvent::ResourceUploadFailed { path, error } => {
                self.add_system_notice(
                    SystemNoticeKind::Error,
                    &format!("Attach failed for {path}: {error}"),
                );
            }

            CowdEvent::TurnError { error } => {
                self.add_system_notice(SystemNoticeKind::Error, &format!("Error: {error}"));
            }

            CowdEvent::CompactionNotice { removed_count } => {
                self.shell.compaction_count += 1;
                self.add_system_notice(
                    SystemNoticeKind::Info,
                    &format!("Compacted {removed_count} earlier messages to save context."),
                );
            }

            CowdEvent::MemoryEntry { .. } => {
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::MemoryUpdate { .. } => {
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::MemoryStats {
                total_entries,
                vector_count,
                layers,
            } => {
                self.workbench.memory_total_entries = Some(total_entries);
                self.workbench.memory_vector_count = Some(vector_count);
                self.workbench.memory_layer_counts = memory_layer_counts_from_strings(&layers);
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionList { sessions } => {
                self.shell.sessions = sessions;
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionCreated { id, name } => {
                self.shell
                    .sessions
                    .push((id, name, App::format_timestamp()));
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionDeleted { id } => {
                self.shell.sessions.retain(|(sid, _, _)| sid != &id);
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionSwitched { id: _, name } => {
                self.shell.active_session_name = name;
                self.timeline.msg_version = self.timeline.msg_version.wrapping_add(1);
            }
            CowdEvent::Warning { message } => {
                self.show_notification(&message);
            }
            event => return Err(event),
        }
        Ok(())
    }
}

fn memory_layer_counts_from_strings(layers: &[String]) -> [usize; 5] {
    let mut counts = [0; 5];
    for (fallback_idx, layer) in layers.iter().enumerate() {
        let Some(count) = first_usize_after(layer, "entry_count")
            .or_else(|| first_usize_after(layer, "count"))
            .or_else(|| first_usize_after(layer, ":"))
            .or_else(|| layer.parse::<usize>().ok())
        else {
            continue;
        };
        let idx = layer
            .find('L')
            .and_then(|pos| layer[pos + 1..].chars().next())
            .and_then(|ch| ch.to_digit(10))
            .map(|value| value as usize)
            .unwrap_or(fallback_idx);
        if idx < counts.len() {
            counts[idx] = count;
        }
    }
    counts
}

fn first_usize_after(value: &str, marker: &str) -> Option<usize> {
    let start = value.find(marker)? + marker.len();
    let digits = value[start..]
        .chars()
        .skip_while(|ch| !ch.is_ascii_digit())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
}

/// Trait for tool registry integration with SkillsPanel.
pub trait ToolRegistry: Send + Sync {
    fn enable_tool(&self, name: &str);
    fn disable_tool(&self, name: &str);
}

#[cfg(test)]
#[path = "tests/reducer.rs"]
mod tests;
