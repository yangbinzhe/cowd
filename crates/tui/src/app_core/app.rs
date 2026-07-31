#![allow(dead_code)]
use crate::components::composer::model::ComposerModel;
use crate::components::turn_interaction::TurnInteractionState;
use crate::layout::{build_default_layout, LayoutState, LayoutTree};
use crate::runtime_control_store::{
    ApprovalSummary, ConnectorAccountSummary, ConnectorCapabilitySummary, ConnectorResourceSummary,
    CowdKernelSummary, FactFlowSummary, GatewayCapabilityContractSummary, MessageBindingSummary,
    MessageConnectorSummary, MessageEndpointSummary, MessageRouteSummary, MissionControlSummary,
    RealityCoreSummary, RuntimeActionReceiptSummary, StructuredDataSummary, SurfaceEventSummary,
    SurfaceHealthSummary, SurfaceSummary, TaskSummary,
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

#[derive(Debug, Clone)]
pub struct TimelinePage {
    pub entries: Vec<TimelineEntry>,
    pub start_index: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimelineEntry {
    Message {
        role: String,
        content: String,
        timestamp: String,
        identity: Option<MessageIdentity>,
    },
    Thinking {
        id: u64,
        causal_item_id: Option<String>,
        causality: Option<TimelineCausality>,
        content: String,
        complete: bool,
        expanded: bool,
    },
    ToolCall {
        id: String,
        name: String,
        preview: String,
        output: String,
        done: bool,
        expanded: bool,
        exit_code: Option<i32>,
        causality: Option<TimelineCausality>,
    },
    SlashOutput {
        command: String,
        output: String,
        expanded: bool,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimelineCausality {
    pub model_step_id: Option<String>,
    pub item_id: Option<String>,
    pub segment_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub causal_sequence: Option<u64>,
    pub delta_sequence: Option<u64>,
    pub causal_parent_ids: Vec<String>,
    pub wave: usize,
    pub lane: usize,
    pub lane_count: usize,
}

impl TimelineCausality {
    fn from_correlation(correlation: &crate::protocol::GatewayEventCorrelation) -> Self {
        Self {
            model_step_id: correlation.model_step_id.clone(),
            item_id: correlation.item_id.clone(),
            segment_id: correlation.segment_id.clone(),
            tool_call_id: correlation.tool_call_id.clone(),
            causal_sequence: correlation.causal_sequence,
            delta_sequence: correlation.delta_sequence,
            causal_parent_ids: correlation.causal_parent_ids.clone(),
            wave: 0,
            lane: 0,
            lane_count: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageIdentity {
    pub message_id: Option<String>,
    pub sequence: Option<usize>,
    pub execution_id: Option<String>,
    pub turn_id: Option<String>,
    pub part_id: Option<String>,
    pub source: MessageSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageSource {
    Local,
    DurableHistory,
    DurableIngress,
    Live,
    ReplayedTerminal,
    DurableTerminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LiveMessageKey {
    execution_id: Option<String>,
    turn_id: Option<String>,
    part_id: Option<String>,
}

/// Canonical identity for one tool invocation.
///
/// Provider-local tool ids are not globally unique (`dsml-tool-0` commonly
/// repeats every turn). All indexing therefore includes the owning
/// Session/execution/turn/part, or the durable message/block position while
/// hydrating history.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ToolInstanceIdentity {
    session_id: String,
    execution_id: Option<String>,
    turn_id: Option<String>,
    part_id: Option<String>,
    durable_message_id: Option<String>,
    durable_sequence: Option<usize>,
    block_index: Option<usize>,
    provider_tool_id: String,
}

impl ToolInstanceIdentity {
    fn stable_key(&self) -> String {
        fn segment(value: Option<&str>) -> String {
            value.map_or_else(
                || "-".to_string(),
                |value| format!("{}:{value}", value.len()),
            )
        }
        if self.provider_tool_id.contains("#cowd-")
            && self.execution_id.is_some()
            && self.turn_id.is_some()
        {
            return format!(
                "tool-instance-v2|{}|{}|{}|{}",
                segment(Some(&self.session_id)),
                segment(self.execution_id.as_deref()),
                segment(self.turn_id.as_deref()),
                segment(Some(&self.provider_tool_id)),
            );
        }
        format!(
            "tool-instance|{}|{}|{}|{}|{}|{}|{}|{}",
            segment(Some(&self.session_id)),
            segment(self.execution_id.as_deref()),
            segment(self.turn_id.as_deref()),
            segment(self.part_id.as_deref()),
            segment(self.durable_message_id.as_deref()),
            self.durable_sequence
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
            self.block_index
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
            segment(Some(&self.provider_tool_id)),
        )
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionActivityStats {
    pub thinking_count: usize,
    pub tool_count: usize,
    pub message_count: usize,
    pub event_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TuiTelemetry {
    pub history_hydration_duration_ms: Option<u64>,
    pub history_hydrated_messages: usize,
    pub history_hydration_pages: usize,
    pub session_sse_reconnect_count: u64,
    pub session_sse_last_cursor: Option<u64>,
    pub projection_sse_reconnect_count: u64,
    pub projection_sse_last_cursor: Option<u64>,
    pub replay_terminal_dedupe_count: u64,
    pub text_delta_dedupe_count: u64,
    pub orphan_event_count: u64,
    pub finalized_cache_hits: u64,
    pub finalized_cache_misses: u64,
    pub live_tail_rebuild_count: u64,
    pub full_timeline_rebuild_count: u64,
    pub model_mismatch_count: u64,
    pub model_mismatch_active: bool,
}

impl TimelineEntry {
    pub fn expanded_lines(&self) -> usize {
        match self {
            Self::Message { content, .. } => content.lines().count().max(1),
            Self::Thinking {
                content, expanded, ..
            } => {
                if *expanded {
                    content.lines().count().max(1) + 2
                } else {
                    1
                }
            }
            Self::ToolCall {
                output, expanded, ..
            } => {
                if *expanded && !output.is_empty() {
                    output.lines().count().max(1) + 2
                } else {
                    1
                }
            }
            Self::SlashOutput {
                output, expanded, ..
            } => {
                if *expanded && !output.is_empty() {
                    output.lines().count().max(1) + 2
                } else {
                    1
                }
            }
        }
    }

    pub fn is_collapsible(&self) -> bool {
        matches!(
            self,
            Self::Thinking { .. } | Self::ToolCall { .. } | Self::SlashOutput { .. }
        )
    }

    pub fn is_expanded(&self) -> bool {
        match self {
            Self::Thinking { expanded, .. } => *expanded,
            Self::ToolCall { expanded, .. } => *expanded,
            Self::SlashOutput { expanded, .. } => *expanded,
            _ => false,
        }
    }

    pub fn toggle(&mut self) {
        match self {
            Self::Thinking { expanded, .. } => *expanded = !*expanded,
            Self::ToolCall { expanded, .. } => *expanded = !*expanded,
            Self::SlashOutput { expanded, .. } => *expanded = !*expanded,
            _ => {}
        }
    }

    pub fn full_text(&self) -> String {
        match self {
            Self::Message { content, .. } => content.clone(),
            Self::Thinking { content, .. } => content.clone(),
            Self::ToolCall { output, .. } => output.clone(),
            Self::SlashOutput { output, .. } => output.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct ToolCard {
    pub id: String,
    pub name: String,
    pub output: String,
    pub done: bool,
    pub expanded: bool,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    pub id: String,
    pub title: Option<String>,
    pub path: String,
    pub updated_at_ms: u64,
    pub message_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingResource {
    pub id: String,
    pub label: String,
    pub kind: String,
}

/// Read-only TUI projection of a Runtime-owned SessionIngress record. Edits,
/// cancellation and routing still address the canonical `input_id` through
/// Gateway; this struct is never an executable local queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInputPreview {
    pub input_id: String,
    pub status: String,
    pub decision: String,
    pub content_preview: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemNoticeKind {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemNotice {
    pub kind: SystemNoticeKind,
    pub content: String,
    pub timestamp: String,
}

impl SystemNotice {
    pub fn label(&self) -> String {
        let prefix = match self.kind {
            SystemNoticeKind::Info => "notice",
            SystemNoticeKind::Warning => "warning",
            SystemNoticeKind::Error => "error",
        };
        format!("{prefix}: {}", self.content)
    }
}

pub struct App {
    pub model: String,
    /// Model requested by the caller/session record. This is not proof that a
    /// provider actually used it.
    pub requested_model: Option<String>,
    /// Provider/model observed in Runtime's canonical live projection.
    pub effective_model: Option<String>,
    /// Canonical origin of the effective model fact.
    pub model_source: Option<String>,
    pub session_id: String,
    pub yolo_mode: bool,
    pub current_task: Option<CurrentTaskSummary>,
    /// Canonical composer bytes and cursor. Visual rows are derived from the
    /// actual terminal rectangle by `components::composer::layout`.
    pub input: ComposerModel,
    pub spinner_idx: usize,
    pub should_quit: bool,

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

    pub token_count: u64,
    pub compaction_count: u32,
    pub cache_hits: u64,
    pub picker_active: bool,
    pub picker_sessions: Vec<SessionSummary>,
    pub picker_idx: usize,
    pub theme: Theme,
    pub approval: Option<ApprovalRequest>,
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
    pub scroll_offset: usize,
    pub auto_scroll: bool,

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

    pub msg_version: u64,
    /// Non-transcript UI revision (composer, focus, modal/search selection).
    pub render_version: u64,
    /// Changes only when consumers must replace their complete timeline
    /// projection (history reconciliation or front eviction).
    pub timeline_full_sync_revision: u64,
    /// Monotonic identity-aware mutation cursor consumed by ChatView. This is
    /// independent from append length and catches progress updates to entries
    /// far outside the visible tail.
    pub timeline_mutation_revision: u64,
    timeline_dirty_log: VecDeque<(u64, usize, String)>,
    pub last_drawn_version: u64,
    pub last_drawn_render_version: u64,
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
    pub telemetry: TuiTelemetry,
    pub stream_connection_state: crate::protocol::SessionStreamConnectionState,
    pub projection_connection_state: Option<crate::protocol::SessionStreamConnectionState>,
    pub live_output_snapshot_gap: bool,
    /// Last accepted provider stream revision for each causal assistant part.
    /// Byte offsets alone cannot reject a replay that carries a conflicting
    /// payload at the same range after reconnect.
    live_stream_revisions: HashMap<LiveMessageKey, u64>,
    seen_terminal_ids: BTreeSet<String>,
    hydrated_non_text_message_ids: BTreeSet<String>,
    pending_message_admissions: BTreeMap<String, u64>,
    pub latest_run_projection: Option<Value>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub durable_session_input_tokens: u64,
    pub durable_session_output_tokens: u64,
    /// Gateway-owned full-session totals. These remain distinct from
    /// `durable_session_*`, which only cover the currently hydrated window.
    pub authoritative_session_input_tokens: Option<u64>,
    pub authoritative_session_output_tokens: Option<u64>,
    durable_message_usage: BTreeMap<String, (u64, u64)>,

    pub turn_input_tokens: u64,
    pub turn_output_tokens: u64,
    /// Provider usage has been observed for the selected turn. Zero is a valid
    /// measured value; false means unknown and must render as an em dash.
    pub turn_usage_known: bool,
    pub current_turn_tool_count: usize,
    pub current_turn_thinking_count: usize,
    pre_turn_input: u64,
    pre_turn_output: u64,

    pub cached_chat_lines: Vec<ratatui::text::Line<'static>>,

    pub entry_line_counts: Vec<usize>,
    pub lines_dirty: bool,
    last_built_line_count: usize,

    pub input_history: Vec<String>,
    pub history_idx: Option<usize>,

    pub search_query: String,
    pub search_matches: Vec<usize>,
    pub search_current: usize,
    pub search_active: bool,
    searchable_content_revision: u64,
    search_index_revision: u64,
    search_text_index: Vec<String>,

    pub viewport_height: usize,

    pub help_visible: bool,

    pub available_models: Vec<String>,
    pub model_dirty: bool,

    pub notification: Option<String>,
    notification_ttl: u32,

    pub sessions: Vec<(String, String, String)>, // (id, name, created)
    pub active_session_name: String,

    pub layout_tree: LayoutTree,
    pub layout_state: LayoutState,

    pub compact_chat: bool,
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

#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub tool_name: String,
    pub input_preview: String,
    pub approved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Theme {
    Dark,
    Light,
}

impl Theme {
    pub fn bg(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Black,
            Self::Light => ratatui::style::Color::White,
        }
    }
    pub fn fg(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::White,
            Self::Light => ratatui::style::Color::Black,
        }
    }
    pub fn accent(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Cyan,
            Self::Light => ratatui::style::Color::Blue,
        }
    }
    pub fn user_color(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Green,
            Self::Light => ratatui::style::Color::DarkGray,
        }
    }
    /// Secondary / dimmed text (used for muted labels, timestamps, truncation notices).
    /// Higher contrast than DarkGray for readability on dark backgrounds.
    pub fn muted_color(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Rgb(150, 150, 150),
            Self::Light => ratatui::style::Color::Rgb(100, 100, 100),
        }
    }
    /// Warning / attention color.
    pub fn warn_color(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Yellow,
            Self::Light => ratatui::style::Color::Rgb(180, 130, 0),
        }
    }
    /// Success / positive color.
    pub fn success_color(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Green,
            Self::Light => ratatui::style::Color::Rgb(0, 130, 0),
        }
    }
    /// Error / negative color.
    pub fn error_color(&self) -> ratatui::style::Color {
        ratatui::style::Color::Red
    }
    /// Code block background color.
    pub fn code_bg_color(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Rgb(35, 35, 45),
            Self::Light => ratatui::style::Color::Rgb(235, 235, 240),
        }
    }
    /// Inline code color.
    pub fn inline_code_color(&self) -> ratatui::style::Color {
        self.warn_color()
    }
    /// Link color.
    pub fn link_color(&self) -> ratatui::style::Color {
        match self {
            Self::Dark => ratatui::style::Color::Cyan,
            Self::Light => ratatui::style::Color::Blue,
        }
    }
    pub fn toggle(&mut self) {
        *self = match self {
            Self::Dark => Self::Light,
            Self::Light => Self::Dark,
        };
    }
}

impl App {
    pub fn new(model: &str, session_id: &str) -> Self {
        Self {
            model: model.to_string(),
            requested_model: (!model.trim().is_empty()
                && model != "default"
                && model != "unresolved")
                .then(|| model.to_string()),
            effective_model: None,
            model_source: None,
            session_id: session_id.to_string(),
            yolo_mode: false,
            current_task: None,
            input: ComposerModel::default(),
            spinner_idx: 0,
            should_quit: false,

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

            token_count: 0,
            compaction_count: 0,
            cache_hits: 0,
            picker_active: false,
            picker_sessions: Vec::new(),
            picker_idx: 0,
            theme: Theme::Dark,
            approval: None,
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
            gateway_knowledge_candidates: Vec::new(),

            selected_agent_reputation: None,
            mcp_count: 0,
            lsp_available: 0,
            permission_count: 0,

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
            scroll_offset: 0,
            auto_scroll: true,

            turn_interaction: TurnInteractionState::default(),
            streaming_received: false,
            terminal_correlations: VecDeque::new(),
            committed_ingress_correlations: BTreeSet::new(),

            msg_version: 0,
            render_version: 0,
            timeline_full_sync_revision: 0,
            timeline_mutation_revision: 0,
            timeline_dirty_log: VecDeque::new(),
            last_drawn_version: u64::MAX,
            last_drawn_render_version: u64::MAX,
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
            telemetry: TuiTelemetry::default(),
            stream_connection_state: crate::protocol::SessionStreamConnectionState::Connecting,
            projection_connection_state: None,
            live_output_snapshot_gap: false,
            live_stream_revisions: HashMap::new(),
            seen_terminal_ids: BTreeSet::new(),
            hydrated_non_text_message_ids: BTreeSet::new(),
            pending_message_admissions: BTreeMap::new(),
            latest_run_projection: None,
            input_tokens: 0,
            output_tokens: 0,
            durable_session_input_tokens: 0,
            durable_session_output_tokens: 0,
            authoritative_session_input_tokens: None,
            authoritative_session_output_tokens: None,
            durable_message_usage: BTreeMap::new(),

            turn_input_tokens: 0,
            turn_output_tokens: 0,
            turn_usage_known: false,
            current_turn_tool_count: 0,
            current_turn_thinking_count: 0,
            pre_turn_input: 0,
            pre_turn_output: 0,

            cached_chat_lines: Vec::new(),

            entry_line_counts: Vec::new(),
            lines_dirty: true,
            last_built_line_count: 0,

            input_history: Vec::new(),
            history_idx: None,

            search_query: String::new(),
            search_matches: Vec::new(),
            search_current: 0,
            search_active: false,
            searchable_content_revision: 0,
            search_index_revision: u64::MAX,
            search_text_index: Vec::new(),

            viewport_height: 24,

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
        }
    }

    pub fn refresh_model_mismatch_telemetry(&mut self) {
        let mismatch = self
            .requested_model
            .as_deref()
            .zip(self.effective_model.as_deref())
            .is_some_and(|(requested, effective)| requested != effective);
        if mismatch && !self.telemetry.model_mismatch_active {
            self.telemetry.model_mismatch_count =
                self.telemetry.model_mismatch_count.saturating_add(1);
            tracing::warn!(
                session_id = %self.session_id,
                requested_model = self.requested_model.as_deref().unwrap_or("unknown"),
                effective_model = self.effective_model.as_deref().unwrap_or("unknown"),
                mismatch_count = self.telemetry.model_mismatch_count,
                "TUI observed a requested/effective model mismatch"
            );
        }
        self.telemetry.model_mismatch_active = mismatch;
    }

    pub fn apply_run_projection(&mut self, projection: Value) {
        if projection.get("kind").and_then(Value::as_str) != Some("session.run_projection") {
            return;
        }
        if let Some(tokens) = projection.pointer("/token_speed/stats/tokens") {
            self.authoritative_session_input_tokens = tokens.get("input").and_then(Value::as_u64);
            self.authoritative_session_output_tokens = tokens.get("output").and_then(Value::as_u64);
        }
        if let Some(total) = projection
            .pointer("/token_speed/stats/tokens/total")
            .and_then(Value::as_u64)
        {
            self.token_count = total;
        }
        if let Some(envelope) = projection.pointer("/memory_context/context_envelope") {
            if !envelope.is_null() {
                self.latest_context_envelope = Some(envelope.clone());
            }
        }
        self.latest_run_projection = Some(projection);
        self.msg_version = self.msg_version.wrapping_add(1);
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
        self.latest_execution_graph_summary = Some(crate::RuntimeExecutionGraphSummary {
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
        let preserved_model_telemetry = (self.current_execution_id.as_deref()
            == Some(projection.execution_id.as_str()))
        .then(|| self.latest_model_telemetry.clone())
        .flatten();
        if let Some(live) = projection.live.as_ref() {
            self.install_execution_live_facts(
                &projection.execution_id,
                live,
                preserved_model_telemetry,
            );
        } else {
            self.reset_live_execution_facts();
            self.latest_model_telemetry = preserved_model_telemetry;
            // The execution identity is canonical even before Runtime has
            // materialized live facts. Every other field remains unknown.
            self.current_execution_id = Some(projection.execution_id.clone());
        }
        self.latest_execution_projection = Some(projection);
        self.refresh_model_mismatch_telemetry();
        self.msg_version = self.msg_version.wrapping_add(1);
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
        let preserved_model_telemetry = (self.current_execution_id.as_deref()
            == Some(update.execution_id.as_str()))
        .then(|| self.latest_model_telemetry.clone())
        .flatten();
        if let Some(projection) = self.latest_execution_projection.as_mut() {
            projection.live = Some(update.live.clone());
        }
        self.install_execution_live_facts(
            &update.execution_id,
            &update.live,
            preserved_model_telemetry,
        );
        self.refresh_model_mismatch_telemetry();
        self.msg_version = self.msg_version.wrapping_add(1);
        true
    }

    fn install_execution_live_facts(
        &mut self,
        execution_id: &str,
        live: &harness_contract::projection::ExecutionLiveState,
        preserved_model_telemetry: Option<crate::protocol::RunModelTelemetryProjection>,
    ) {
        self.reconcile_live_output_parts(
            execution_id,
            live.turn_id.as_deref(),
            &live.output_parts,
            live.output_bytes,
        );
        self.reset_live_execution_facts();
        self.latest_model_telemetry = preserved_model_telemetry;
        self.current_execution_status = Some(live.status);
        self.current_execution_status_detail = live.status_detail.clone();
        self.current_execution_id = Some(execution_id.to_string());
        self.current_turn_id = live.turn_id.clone();
        self.execution_started_at_ms = Some(live.started_at_ms);
        self.last_progress_at_ms = Some(live.last_progress_at_ms);
        self.current_run_metrics = Some(live.metrics.clone());
        self.current_execution_latency = Some(live.latency.clone());
        self.turn_input_tokens = live.metrics.input_tokens;
        self.turn_output_tokens = live.metrics.output_tokens;
        self.turn_usage_known = live.context_usage.as_ref().is_some_and(|usage| {
            usage
                .input_source
                .as_deref()
                .is_some_and(|source| source != "runtime_request_budget_estimate")
        });
        self.input_tokens = live.metrics.input_tokens;
        self.output_tokens = live.metrics.output_tokens;
        self.token_count = live.metrics.total_tokens;
        if let Some(context) = live.context_usage.as_ref() {
            self.effective_model = context.model.clone();
            if self.effective_model.is_some() {
                self.model_source = Some("runtime.execution_live.context_usage.model".to_string());
            }
            self.context_used_tokens = context.input_tokens;
            self.context_window_tokens = context.window_tokens;
            self.context_remaining_tokens = context.remaining_tokens;
            self.context_usage_percent_bp = context.usage_percent_bp;
            self.context_usage_source = context.input_source.clone();
            self.context_window = context.window_tokens.unwrap_or_default();
        }
    }

    fn reconcile_live_output_parts(
        &mut self,
        execution_id: &str,
        turn_id: Option<&str>,
        parts: &[harness_contract::projection::ExecutionLiveOutputPart],
        output_bytes: u64,
    ) {
        self.live_output_snapshot_gap = false;
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
            self.live_output_snapshot_gap = output_bytes > 0;
            return;
        }
        let mut recovered_bytes = 0_u64;
        let mut ordered = parts.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|part| part.causal_sequence);
        for part in ordered {
            recovered_bytes = recovered_bytes.saturating_add(part.bytes);
            let Some(preview) = part.preview.as_deref() else {
                self.live_output_snapshot_gap |= part.bytes > 0;
                continue;
            };
            let Ok(preview_start) = usize::try_from(part.preview_start_bytes) else {
                self.live_output_snapshot_gap = true;
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
                    self.live_output_snapshot_gap = true;
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
                self.live_output_snapshot_gap = true;
            }
        }
        if recovered_bytes != output_bytes {
            self.live_output_snapshot_gap = true;
        }
        if self.live_output_snapshot_gap {
            self.add_system_notice(
                SystemNoticeKind::Warning,
                "Canonical live output has an incomplete per-item byte range; preserving verified segments until the durable terminal arrives",
            );
        }
    }

    fn reset_live_execution_facts(&mut self) {
        self.current_execution_status = None;
        self.current_execution_status_detail = None;
        self.current_execution_id = None;
        self.current_turn_id = None;
        self.execution_started_at_ms = None;
        self.last_progress_at_ms = None;
        self.current_run_metrics = None;
        self.current_execution_latency = None;
        self.latest_model_telemetry = None;
        self.effective_model = None;
        self.model_source = None;
        self.context_used_tokens = None;
        self.context_window_tokens = None;
        self.context_remaining_tokens = None;
        self.context_usage_percent_bp = None;
        self.context_usage_source = None;
        self.context_window = 0;
        self.live_stream_revisions.clear();
        self.turn_input_tokens = 0;
        self.turn_output_tokens = 0;
        self.turn_usage_known = false;
        self.input_tokens = 0;
        self.output_tokens = 0;
        self.token_count = 0;
    }

    /// Drop an execution projection as soon as Gateway revokes the caller or
    /// rejects its contract.  Retaining the last full snapshot would keep
    /// strategy, agent and evidence detail visible after the authority that
    /// produced it has expired.
    pub fn invalidate_execution_projection(&mut self, execution_id: &str) -> bool {
        let matches_projection = self
            .latest_execution_projection
            .as_ref()
            .is_some_and(|projection| projection.execution_id == execution_id);
        let matches_current_execution = self.current_execution_id.as_deref() == Some(execution_id);
        let matches_interaction =
            self.turn_interaction.execution.execution_id.as_deref() == Some(execution_id);
        if !matches_projection && !matches_current_execution && !matches_interaction {
            return false;
        }
        if matches_projection {
            self.latest_execution_projection = None;
        }
        if matches_projection || matches_current_execution {
            self.reset_live_execution_facts();
        }
        if self
            .latest_execution_graph_summary
            .as_ref()
            .is_some_and(|summary| summary.graph_id.as_deref() == Some(execution_id))
        {
            self.latest_execution_graph_summary = None;
        }
        self.turn_interaction
            .clear_execution_if_matches(execution_id);
        self.msg_version = self.msg_version.wrapping_add(1);
        true
    }

    /// Remove every session-derived projection immediately when Gateway
    /// revokes this observer. Reusing selected fields is intentionally
    /// avoided: transcript, evidence, model facts, drafts and cached panels
    /// all belonged to the expired authority.
    pub fn revoke_session_authorization(&mut self, reason: &str) {
        let session_id = self.session_id.clone();
        let skin = self.skin.clone();
        let theme = self.theme;
        let yolo_mode = self.yolo_mode;
        let mut clean = Self::new("unavailable", &session_id);
        clean.skin = skin;
        clean.theme = theme;
        clean.yolo_mode = yolo_mode;
        clean.history_hydration_error = Some("session authorization revoked".to_string());
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
        self.lines_dirty = true;
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    /// Request a frame without invalidating the width-aware transcript cache.
    /// Composer edits, focus movement, search selection and modal state do not
    /// change timeline content.
    pub fn request_redraw(&mut self) {
        self.render_version = self.render_version.wrapping_add(1);
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
        let absolute = *self.message_timeline_positions.get(message_id)?;
        self.logical_timeline_index(absolute)
    }

    fn logical_timeline_index(&self, absolute: u64) -> Option<usize> {
        let logical = absolute.checked_sub(self.timeline_base_position)?;
        usize::try_from(logical)
            .ok()
            .filter(|index| *index < self.total_entries)
    }

    fn timeline_message_by_id_mut(&mut self, message_id: &str) -> Option<&mut TimelineEntry> {
        let index = self.timeline_message_index(message_id)?;
        self.timeline_get_mut(index)
    }

    fn index_message_identity_at(&mut self, index: usize) {
        let absolute = self
            .timeline_base_position
            .saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
        self.live_timeline_positions
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
            self.message_timeline_positions.insert(message_id, absolute);
        }
        if let Some(live_key) = live_key {
            self.live_timeline_positions.insert(live_key, absolute);
        }
    }

    fn rebuild_timeline_positions(&mut self) {
        self.message_timeline_positions.clear();
        self.live_timeline_positions.clear();
        self.tool_timeline_positions.clear();
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
                .timeline_base_position
                .saturating_add(u64::try_from(index).unwrap_or(u64::MAX));
            if let Some(message_id) = message_id {
                self.message_timeline_positions.insert(message_id, absolute);
            }
            if let Some(live_key) = live_key {
                self.live_timeline_positions.insert(live_key, absolute);
            }
            if let Some(tool_id) = tool_id {
                self.tool_timeline_positions.insert(tool_id, absolute);
            }
        }
    }

    fn note_searchable_content_changed(&mut self) {
        self.searchable_content_revision = self.searchable_content_revision.wrapping_add(1);
    }

    fn ensure_search_text_index(&mut self) {
        if self.search_index_revision == self.searchable_content_revision
            && self.search_text_index.len() == self.total_entries
        {
            return;
        }
        self.search_text_index = self
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
        self.search_index_revision = self.searchable_content_revision;
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
        self.live_timeline_positions
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
        let mut matches = self
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
        if page.session_id != self.session_id {
            return;
        }
        if page.total > SOFT_CAP && !self.history_window_truncated {
            self.history_window_truncated = true;
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
            if let Some(usage) = message.token_usage.as_ref() {
                self.record_durable_message_usage(&message.id, usage);
            }
            if matches!(message.role.as_str(), "user" | "assistant") && !content.is_empty() {
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
            self.history_hydrated = true;
            self.history_hydration_error = None;
        }
        self.timeline_full_sync_revision = self.timeline_full_sync_revision.wrapping_add(1);
        if self.auto_scroll {
            self.timeline_cursor = self.timeline_len().saturating_sub(1);
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
        let non_text_owner = self.non_text_durable_owner.clone();
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
        let non_text_owner = self.non_text_durable_owner.clone();
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
                        .non_text_durable_owner
                        .contains_key(&format!("tool|{id}")),
                    TimelineEntry::Thinking { id, .. } => self
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
        self.hydrated_non_text_message_ids.clear();
        self.pending_history_tool_instances.clear();
        self.non_text_durable_owner.clear();
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
        self.non_text_durable_owner
            .retain(|key, _| retained_non_text_keys.contains(key));
        self.timeline_pages.clear();
        self.total_entries = 0;
        self.timeline_base_position = 0;
        self.message_timeline_positions.clear();
        self.live_timeline_positions.clear();
        self.tool_timeline_positions.clear();
        for chunk in entries.chunks(PAGE_SIZE) {
            let start_index = self.total_entries;
            self.timeline_pages.push_back(TimelinePage {
                entries: chunk.to_vec(),
                start_index,
            });
            self.total_entries = self.total_entries.saturating_add(chunk.len());
        }
        self.timeline_cursor = self
            .timeline_cursor
            .min(self.total_entries.saturating_sub(1));
        self.rebuild_timeline_positions();
        self.note_searchable_content_changed();
        self.timeline_dirty_log.clear();
        self.timeline_full_sync_revision = self.timeline_full_sync_revision.wrapping_add(1);
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
                    let id = self.thinking_id_counter;
                    self.thinking_id_counter = self.thinking_id_counter.saturating_add(1);
                    self.non_text_durable_owner
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
                        session_id: self.session_id.clone(),
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
                        .tool_timeline_positions
                        .get(&id)
                        .copied()
                        .and_then(|absolute| self.logical_timeline_index(absolute))
                        .is_some()
                    {
                        continue;
                    }
                    self.pending_history_tool_instances
                        .entry(provider_tool_id.to_string())
                        .or_default()
                        .push_back(id.clone());
                    self.non_text_durable_owner
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
                        .pending_history_tool_instances
                        .get_mut(tool_use_id)
                        .and_then(VecDeque::pop_front);
                    let tool_index = matched_instance
                        .as_ref()
                        .and_then(|id| self.tool_timeline_positions.get(id))
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
                            session_id: self.session_id.clone(),
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
                        self.non_text_durable_owner
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
            .durable_message_usage
            .insert(message_id.to_string(), (input, output))
        {
            self.durable_session_input_tokens = self
                .durable_session_input_tokens
                .saturating_sub(previous_input);
            self.durable_session_output_tokens = self
                .durable_session_output_tokens
                .saturating_sub(previous_output);
        }
        self.durable_session_input_tokens = self.durable_session_input_tokens.saturating_add(input);
        self.durable_session_output_tokens =
            self.durable_session_output_tokens.saturating_add(output);
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
        self.input_history = history;
        self.history_idx = None;
    }

    pub fn record_input_history(&mut self, input: String) {
        const INPUT_HISTORY_LIMIT: usize = 1_000;
        if input.is_empty()
            || self
                .input_history
                .last()
                .is_some_and(|previous| previous == &input)
        {
            self.history_idx = None;
            return;
        }
        self.input_history.push(input);
        if self.input_history.len() > INPUT_HISTORY_LIMIT {
            self.input_history.remove(0);
        }
        self.history_idx = None;
    }

    fn correlation_is_current(
        &self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> bool {
        if correlation.session_id != self.session_id
            || correlation.execution_id.is_none()
            || correlation.turn_id.is_none()
        {
            return false;
        }
        self.current_execution_id
            .as_deref()
            .is_none_or(|current| correlation.execution_id.as_deref() == Some(current))
            && self
                .current_turn_id
                .as_deref()
                .is_none_or(|current| correlation.turn_id.as_deref() == Some(current))
    }

    pub(crate) fn execution_is_terminalized(&self, execution_id: &str) -> bool {
        self.terminal_correlations
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
                self.terminal_correlations.iter().any(
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
            .terminal_correlations
            .iter()
            .any(|(known_execution_id, known_turn_id)| {
                known_execution_id == execution_id && known_turn_id == turn_id
            })
        {
            return;
        }
        self.terminal_correlations
            .push_back((execution_id.clone(), turn_id.clone()));
        while self.terminal_correlations.len() > TERMINAL_CORRELATION_CAPACITY {
            self.terminal_correlations.pop_front();
        }
    }

    fn adopt_live_correlation(
        &mut self,
        correlation: &crate::protocol::GatewayEventCorrelation,
    ) -> bool {
        if !self.correlation_is_current(correlation) {
            self.telemetry.orphan_event_count = self.telemetry.orphan_event_count.saturating_add(1);
            let orphan_count = self.telemetry.orphan_event_count;
            tracing::warn!(
                session_id = %self.session_id,
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
                let warning = format!(
                    "Ignored {orphan_count} event(s) outside the active session/execution/turn \
                     (current execution={}, turn={}, status={:?}; incoming execution={}, turn={}); \
                     canonical history and projection remain authoritative",
                    self.current_execution_id.as_deref().unwrap_or("none"),
                    self.current_turn_id.as_deref().unwrap_or("none"),
                    self.current_execution_status,
                    correlation.execution_id.as_deref().unwrap_or("missing"),
                    correlation.turn_id.as_deref().unwrap_or("missing"),
                );
                self.add_system_notice(SystemNoticeKind::Warning, &warning);
                self.show_notification(&warning);
            }
            return false;
        }
        self.current_execution_id = correlation.execution_id.clone();
        self.current_turn_id = correlation.turn_id.clone();
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
        if correlation.session_id != self.session_id
            || correlation.execution_id.is_none()
            || correlation.turn_id.is_none()
        {
            return self.adopt_live_correlation(correlation);
        }
        if self.current_execution_id.as_deref() != correlation.execution_id.as_deref()
            || self.current_turn_id.as_deref() != correlation.turn_id.as_deref()
        {
            if let Some(previous_execution_id) = self.current_execution_id.clone() {
                if let Some(previous_turn_id) = self.current_turn_id.clone() {
                    self.record_terminal_correlation(&crate::protocol::GatewayEventCorrelation {
                        session_id: self.session_id.clone(),
                        execution_id: Some(previous_execution_id.clone()),
                        turn_id: Some(previous_turn_id),
                        ..crate::protocol::GatewayEventCorrelation::default()
                    });
                }
                self.invalidate_execution_projection(&previous_execution_id);
            } else {
                self.reset_live_execution_facts();
            }
            self.current_execution_id = correlation.execution_id.clone();
            self.current_turn_id = correlation.turn_id.clone();
            if let Some(execution_id) = correlation.execution_id.as_deref() {
                self.turn_interaction.ingress_accepted(execution_id);
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
                self.committed_ingress_correlations
                    .contains(&(execution_id.to_string(), turn_id.to_string()))
            });
        let incoming_differs = self.current_execution_id.as_deref()
            != correlation.execution_id.as_deref()
            || self.current_turn_id.as_deref() != correlation.turn_id.as_deref();
        let current_is_terminal = self
            .current_execution_id
            .as_deref()
            .is_some_and(|execution_id| self.execution_is_terminalized(execution_id))
            || self
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
                self.lines_dirty = true;
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                return;
            }
        }
        let id = self.thinking_id_counter;
        self.thinking_id_counter = self.thinking_id_counter.saturating_add(1);
        self.timeline_push(TimelineEntry::Thinking {
            id,
            causal_item_id,
            causality: Some(TimelineCausality::from_correlation(correlation)),
            content: thinking,
            complete: false,
            expanded: false,
        });
        self.current_turn_thinking_count = self.current_turn_thinking_count.saturating_add(1);
        self.msg_version = self.msg_version.wrapping_add(1);
        self.timeline_cursor = self.timeline_len().saturating_sub(1);
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
                    self.msg_version = self.msg_version.wrapping_add(1);
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
        let Some(absolute) = self.tool_timeline_positions.get(tool_instance_id).copied() else {
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
        self.lines_dirty = true;
    }

    fn current_tool_instance_key(&self, provider_tool_id: &str) -> String {
        if provider_tool_id.starts_with("tool-instance|") {
            return provider_tool_id.to_string();
        }
        if self.current_execution_id.is_none() && self.current_turn_id.is_none() {
            return provider_tool_id.to_string();
        }
        ToolInstanceIdentity {
            session_id: self.session_id.clone(),
            execution_id: self.current_execution_id.clone(),
            turn_id: self.current_turn_id.clone(),
            part_id: None,
            durable_message_id: None,
            durable_sequence: None,
            block_index: None,
            provider_tool_id: provider_tool_id.to_string(),
        }
        .stable_key()
    }

    fn apply_gateway_session_event(&mut self, event: crate::protocol::GatewaySessionEvent) {
        use crate::protocol::GatewaySessionEvent;
        match event {
            GatewaySessionEvent::UserMessageCommitted {
                correlation,
                content,
                sequence,
                created_at_ms,
            } => {
                if correlation.session_id != self.session_id {
                    return;
                }
                let Some(message_id) = correlation.message_id else {
                    self.add_system_notice(
                        SystemNoticeKind::Warning,
                        "Ignored a committed user message without stable identity",
                    );
                    return;
                };
                let incoming_execution = correlation.execution_id.clone();
                let incoming_turn = correlation.turn_id.clone();
                if let Some(identity) = incoming_execution
                    .as_ref()
                    .zip(incoming_turn.as_ref())
                    .map(|(execution_id, turn_id)| (execution_id.clone(), turn_id.clone()))
                {
                    self.committed_ingress_correlations.insert(identity);
                }
                let selects_visible_execution = self.current_execution_id.is_none()
                    || self.current_execution_id.as_deref() == incoming_execution.as_deref()
                    || !self.turn_is_active()
                    || self.current_execution_status.is_some_and(
                        harness_contract::projection::ExecutionLiveStatus::is_terminal,
                    );
                if selects_visible_execution
                    && self.current_execution_id.as_deref() != incoming_execution.as_deref()
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
                    self.current_execution_id = incoming_execution;
                    self.current_turn_id = incoming_turn;
                    self.current_execution_status =
                        Some(harness_contract::projection::ExecutionLiveStatus::Queued);
                    self.current_execution_status_detail =
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
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.mark_dirty();
            }
            GatewaySessionEvent::TextDelta {
                correlation,
                text,
                start_bytes,
                end_bytes,
                stream_revision,
            } => {
                if !self.adopt_active_execution_correlation(&correlation) {
                    self.add_system_notice(
                        SystemNoticeKind::Warning,
                        "Ignored an assistant delta without the current session/execution/turn identity",
                    );
                    return;
                }
                let stream_key = LiveMessageKey {
                    execution_id: correlation.execution_id.clone(),
                    turn_id: correlation.turn_id.clone(),
                    part_id: correlation.part_id.clone(),
                };
                if self
                    .live_stream_revisions
                    .get(&stream_key)
                    .is_some_and(|accepted| stream_revision <= *accepted)
                {
                    self.telemetry.text_delta_dedupe_count =
                        self.telemetry.text_delta_dedupe_count.saturating_add(1);
                    return;
                }
                self.streaming_received = true;
                if let Some(TimelineEntry::Message { content, .. }) = self
                    .timeline_live_message_mut(
                        correlation.execution_id.as_deref(),
                        correlation.turn_id.as_deref(),
                        correlation.part_id.as_deref(),
                    )
                {
                    let accepted = content.len();
                    if end_bytes <= accepted {
                        self.telemetry.text_delta_dedupe_count =
                            self.telemetry.text_delta_dedupe_count.saturating_add(1);
                    } else if start_bytes <= accepted
                        && text.is_char_boundary(accepted.saturating_sub(start_bytes))
                    {
                        content.push_str(&text[accepted.saturating_sub(start_bytes)..]);
                        self.note_searchable_content_changed();
                    } else {
                        self.live_output_snapshot_gap = true;
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
                        // Durable terminal/history already owns this turn.
                        // A delayed live revision may advance monotonically but
                        // cannot recreate an obsolete assistant bubble.
                        self.telemetry.text_delta_dedupe_count =
                            self.telemetry.text_delta_dedupe_count.saturating_add(1);
                        self.live_stream_revisions
                            .insert(stream_key, stream_revision);
                        return;
                    }
                    if start_bytes != 0 {
                        self.live_output_snapshot_gap = true;
                        self.live_stream_revisions
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
                self.live_stream_revisions
                    .insert(stream_key, stream_revision);
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.mark_dirty();
            }
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
                    return;
                }
                let adopted = if status == harness_contract::projection::ExecutionLiveStatus::Queued
                {
                    self.adopt_live_correlation(&correlation)
                } else {
                    self.adopt_started_execution_correlation(&correlation)
                };
                if !adopted {
                    return;
                }
                self.current_execution_status = Some(status);
                self.current_execution_status_detail = detail;
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
            GatewaySessionEvent::TerminalCommitted {
                correlation,
                assistant_text,
                sequence,
                token_usage,
                ..
            } => {
                if correlation.session_id != self.session_id {
                    return;
                }
                let complete_identity = correlation.execution_id.is_some()
                    && correlation.turn_id.is_some()
                    && correlation.message_id.is_some()
                    && correlation.terminal_id.is_some();
                if !complete_identity {
                    self.telemetry.orphan_event_count =
                        self.telemetry.orphan_event_count.saturating_add(1);
                    let warning = format!(
                        "Rejected terminal without complete execution/turn/message/terminal identity (orphan #{})",
                        self.telemetry.orphan_event_count
                    );
                    self.add_system_notice(SystemNoticeKind::Warning, &warning);
                    self.show_notification(&warning);
                    return;
                }
                if correlation.replayed {
                    // Durable history is the only transcript authority for
                    // replay. A replayed commit is an ordering/cursor fact and
                    // must never append assistant prose on its own.
                    return;
                }
                let settles_current = self.adopt_live_correlation(&correlation);
                if !settles_current {
                    return;
                }
                if settles_current {
                    if self.current_execution_status
                        != Some(harness_contract::projection::ExecutionLiveStatus::Error)
                    {
                        self.current_execution_status =
                            Some(harness_contract::projection::ExecutionLiveStatus::Complete);
                        self.current_execution_status_detail =
                            Some("durable terminal committed".to_string());
                    }
                    self.current_execution_id = correlation.execution_id.clone();
                    self.current_turn_id = correlation.turn_id.clone();
                }
                if let Some(terminal_id) = correlation.terminal_id.as_ref() {
                    if !self.seen_terminal_ids.insert(terminal_id.clone()) {
                        self.telemetry.replay_terminal_dedupe_count = self
                            .telemetry
                            .replay_terminal_dedupe_count
                            .saturating_add(1);
                        return;
                    }
                }
                self.record_terminal_correlation(&correlation);
                if let Some(identity) = correlation
                    .execution_id
                    .as_ref()
                    .zip(correlation.turn_id.as_ref())
                    .map(|(execution_id, turn_id)| (execution_id.clone(), turn_id.clone()))
                {
                    self.committed_ingress_correlations.remove(&identity);
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
                    self.turn_interaction.terminal_observed();
                }
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.mark_dirty();
            }
            GatewaySessionEvent::TurnError { correlation, error } => {
                if self.adopt_live_correlation(&correlation) {
                    self.current_execution_status =
                        Some(harness_contract::projection::ExecutionLiveStatus::Error);
                    self.current_execution_status_detail = Some(error.clone());
                    self.current_execution_id = correlation.execution_id;
                    self.current_turn_id = correlation.turn_id;
                    self.turn_interaction.terminal_observed();
                    self.add_system_notice(SystemNoticeKind::Error, &format!("Error: {error}"));
                }
            }
        }
    }

    pub fn apply_session_input_projection(&mut self, projection: Value) {
        // A projection can be replayed after reconnect. Remember which queue
        // records were already announced so the TUI exposes the canonical id
        // exactly when it first becomes actionable, without creating a local
        // execution queue or repeating notices for the same snapshot.
        let announced_queued_ids = self
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
        self.pending_inputs = inputs;
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

    #[must_use]
    pub fn queued_follow_up_count(&self) -> usize {
        self.pending_inputs
            .iter()
            .filter(|input| input.status == "queued_next")
            .count()
    }

    #[must_use]
    pub fn queued_follow_up_preview(&self) -> Option<&PendingInputPreview> {
        self.pending_inputs
            .iter()
            .find(|input| input.status == "queued_next")
    }

    pub fn timeline_len(&self) -> usize {
        self.total_entries
    }

    pub fn timeline_is_empty(&self) -> bool {
        self.total_entries == 0
    }

    pub fn timeline_get(&self, idx: usize) -> Option<&TimelineEntry> {
        if idx >= self.total_entries {
            return None;
        }
        for page in &self.timeline_pages {
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
        if idx >= self.total_entries {
            return None;
        }
        let key = self.timeline_get(idx).map(Self::timeline_entry_key)?;
        self.note_timeline_entry_mutated(idx, key);
        for page in &mut self.timeline_pages {
            if idx >= page.start_index && idx < page.start_index + page.entries.len() {
                return page.entries.get_mut(idx - page.start_index);
            }
        }
        None
    }

    pub fn timeline_last_mut(&mut self) -> Option<&mut TimelineEntry> {
        self.total_entries
            .checked_sub(1)
            .and_then(|index| self.timeline_get_mut(index))
    }

    pub fn timeline_last(&self) -> Option<&TimelineEntry> {
        self.timeline_pages
            .back()
            .and_then(|page| page.entries.last())
    }

    pub fn timeline_push(&mut self, entry: TimelineEntry) {
        let absolute_position = self
            .timeline_base_position
            .saturating_add(u64::try_from(self.total_entries).unwrap_or(u64::MAX));
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
        if self.timeline_pages.is_empty()
            || self
                .timeline_pages
                .back()
                .is_none_or(|p| p.entries.len() >= PAGE_SIZE)
        {
            let start = self
                .timeline_pages
                .back()
                .map_or(0, |p| p.start_index + p.entries.len());
            self.timeline_pages.push_back(TimelinePage {
                entries: Vec::with_capacity(PAGE_SIZE),
                start_index: start,
            });
        }
        let Some(page) = self.timeline_pages.back_mut() else {
            return;
        };
        page.entries.push(entry);
        self.total_entries += 1;
        if let Some(message_id) = message_id {
            self.message_timeline_positions
                .insert(message_id, absolute_position);
        }
        if let Some(live_key) = live_key {
            self.live_timeline_positions
                .insert(live_key, absolute_position);
        }
        if let Some(tool_id) = tool_id {
            self.tool_timeline_positions
                .insert(tool_id, absolute_position);
        }
        self.note_searchable_content_changed();
        self.soft_evict();
        self.hard_evict();
    }

    pub fn timeline_iter(&self) -> impl Iterator<Item = (usize, &TimelineEntry)> + '_ {
        self.timeline_pages.iter().flat_map(|page| {
            let start = page.start_index;
            page.entries
                .iter()
                .enumerate()
                .map(move |(i, e)| (start + i, e))
        })
    }

    pub fn timeline_iter_mut(&mut self) -> impl Iterator<Item = &mut TimelineEntry> + '_ {
        self.timeline_full_sync_revision = self.timeline_full_sync_revision.wrapping_add(1);
        self.timeline_dirty_log.clear();
        self.timeline_pages
            .iter_mut()
            .flat_map(|page| page.entries.iter_mut())
    }

    pub fn timeline_clone_vec(&self) -> Vec<TimelineEntry> {
        let mut v = Vec::with_capacity(self.total_entries);
        for page in &self.timeline_pages {
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
        self.timeline_mutation_revision = self.timeline_mutation_revision.wrapping_add(1);
        self.timeline_dirty_log
            .push_back((self.timeline_mutation_revision, index, key));
        if self.timeline_dirty_log.len() > DIRTY_LOG_CAP {
            self.timeline_dirty_log.clear();
            self.timeline_full_sync_revision = self.timeline_full_sync_revision.wrapping_add(1);
        }
    }

    /// Return exact mutated entries since a consumer cursor. `None` means the
    /// bounded log was superseded and the consumer must perform a full sync.
    pub fn timeline_dirty_entries_since(
        &self,
        revision: u64,
    ) -> Option<(u64, Vec<(usize, TimelineEntry)>)> {
        if revision == self.timeline_mutation_revision {
            return Some((revision, Vec::new()));
        }
        let first_revision = self
            .timeline_dirty_log
            .front()
            .map(|(revision, _, _)| *revision)?;
        if revision.saturating_add(1) < first_revision {
            return None;
        }
        let mut dirty = BTreeMap::<usize, (&str, u64)>::new();
        for (mutation_revision, index, key) in self
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
        Some((self.timeline_mutation_revision, entries))
    }

    fn soft_evict(&mut self) {
        while self.total_entries > SOFT_CAP {
            let Some(front) = self.timeline_pages.front() else {
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

            let evicted_lines: usize = if !self.entry_line_counts.is_empty() {
                let count = evict_count.min(self.entry_line_counts.len());
                self.entry_line_counts
                    .iter()
                    .take(count)
                    .map(|&c| c + 1)
                    .sum()
            } else {
                0
            };

            let drain_count = evict_count.min(self.entry_line_counts.len());
            self.entry_line_counts.drain(0..drain_count);
            self.scroll_offset = self.scroll_offset.saturating_sub(evicted_lines);
            self.timeline_cursor = self.timeline_cursor.saturating_sub(evict_count);
            self.search_matches.retain(|&m| m >= evict_count);
            self.search_matches
                .iter_mut()
                .for_each(|m| *m -= evict_count);

            self.timeline_pages.pop_front();
            self.total_entries -= evict_count;
            self.timeline_base_position = self
                .timeline_base_position
                .saturating_add(u64::try_from(evict_count).unwrap_or(u64::MAX));
            for message_id in evicted_message_ids {
                self.message_timeline_positions.remove(&message_id);
            }
            self.live_timeline_positions
                .retain(|_, position| *position >= self.timeline_base_position);
            self.tool_timeline_positions
                .retain(|_, position| *position >= self.timeline_base_position);
            self.note_searchable_content_changed();
            self.timeline_full_sync_revision = self.timeline_full_sync_revision.wrapping_add(1);

            let mut next_start = 0usize;
            for page in &mut self.timeline_pages {
                page.start_index = next_start;
                next_start += page.entries.len();
            }
        }
    }

    fn hard_evict(&mut self) {
        while self.total_entries > HARD_CAP {
            let Some(front) = self.timeline_pages.front() else {
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

            let evicted_lines: usize = if !self.entry_line_counts.is_empty() {
                let count = evict_count.min(self.entry_line_counts.len());
                self.entry_line_counts
                    .iter()
                    .take(count)
                    .map(|&c| c + 1)
                    .sum()
            } else {
                0
            };

            let drain_count = evict_count.min(self.entry_line_counts.len());
            self.entry_line_counts.drain(0..drain_count);
            self.scroll_offset = self.scroll_offset.saturating_sub(evicted_lines);
            self.timeline_cursor = self.timeline_cursor.saturating_sub(evict_count);
            self.search_matches.retain(|&m| m >= evict_count);
            self.search_matches
                .iter_mut()
                .for_each(|m| *m -= evict_count);

            self.timeline_pages.pop_front();
            self.total_entries -= evict_count;
            self.timeline_base_position = self
                .timeline_base_position
                .saturating_add(u64::try_from(evict_count).unwrap_or(u64::MAX));
            for message_id in evicted_message_ids {
                self.message_timeline_positions.remove(&message_id);
            }
            self.live_timeline_positions
                .retain(|_, position| *position >= self.timeline_base_position);
            self.tool_timeline_positions
                .retain(|_, position| *position >= self.timeline_base_position);
            self.note_searchable_content_changed();
            self.timeline_full_sync_revision = self.timeline_full_sync_revision.wrapping_add(1);

            let mut next_start = 0usize;
            for page in &mut self.timeline_pages {
                page.start_index = next_start;
                next_start += page.entries.len();
            }
        }
    }

    pub fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        F[self.spinner_idx % F.len()]
    }

    pub fn tick(&mut self) {
        self.spinner_idx = self.spinner_idx.wrapping_add(1);
        if self.notification_ttl > 0 {
            self.notification_ttl -= 1;
            if self.notification_ttl == 0 {
                self.notification = None;
            }
        }
    }

    #[must_use]
    pub fn turn_is_active(&self) -> bool {
        self.turn_interaction.is_active()
    }

    pub fn next_model(&mut self) -> Option<String> {
        if self.available_models.len() <= 1 {
            return None;
        }
        if let Some(pos) = self.available_models.iter().position(|m| m == &self.model) {
            let idx = (pos + 1) % self.available_models.len();
            self.model = self.available_models[idx].clone();
            self.model_dirty = true;
            Some(self.model.clone())
        } else {
            self.model = self.available_models[0].clone();
            self.model_dirty = true;
            Some(self.model.clone())
        }
    }

    pub fn show_notification(&mut self, msg: &str) {
        self.notification = Some(msg.to_string());
        self.notification_ttl = 30;
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
        self.picker_sessions = sessions;
        self.picker_idx = 0;
        self.picker_active = true;
    }

    pub fn close_session_picker(&mut self) {
        self.picker_active = false;
        self.picker_sessions.clear();
        self.picker_idx = 0;
    }

    pub fn picker_up(&mut self) {
        if self.picker_idx > 0 {
            self.picker_idx -= 1;
        }
    }

    pub fn picker_down(&mut self) {
        if self.picker_idx + 1 < self.picker_sessions.len() {
            self.picker_idx += 1;
        }
    }

    pub fn picker_selected_id(&self) -> Option<&str> {
        self.picker_sessions
            .get(self.picker_idx)
            .map(|s| s.id.as_str())
    }

    pub fn cursor_up(&mut self) -> bool {
        if self.timeline_is_empty() {
            return false;
        }
        let mut idx = self.timeline_cursor;
        loop {
            if idx == 0 {
                break;
            }
            idx -= 1;
            if self.timeline_get(idx).is_some_and(|e| e.is_collapsible()) {
                self.timeline_cursor = idx;
                self.auto_scroll = false;
                return true;
            }
        }
        false
    }

    pub fn cursor_down(&mut self) -> bool {
        if self.timeline_is_empty() {
            return false;
        }
        let mut idx = self.timeline_cursor;
        while idx + 1 < self.timeline_len() {
            idx += 1;
            if self.timeline_get(idx).is_some_and(|e| e.is_collapsible()) {
                self.timeline_cursor = idx;
                self.auto_scroll = true;
                return true;
            }
        }
        false
    }

    pub fn toggle_expand_current(&mut self) {
        if let Some(entry) = self.timeline_get_mut(self.timeline_cursor) {
            entry.toggle();
            self.msg_version = self.msg_version.wrapping_add(1);
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
        self.timeline_cursor = self.timeline_len().saturating_sub(1);
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    pub fn begin_message_admission(
        &mut self,
        content: &str,
        client_message_id: String,
        submission_generation: u64,
        starts_new_turn: bool,
    ) {
        self.pending_message_admissions
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
        self.system_notices.push_back(SystemNotice {
            kind,
            content: trimmed.to_string(),
            timestamp: App::format_timestamp(),
        });
        const SYSTEM_NOTICE_CAP: usize = 500;
        while self.system_notices.len() > SYSTEM_NOTICE_CAP {
            self.system_notices.pop_front();
        }
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    pub fn recent_system_notice_labels(&self, limit: usize) -> Vec<String> {
        self.system_notices
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
        self.timeline_cursor = self.timeline_len().saturating_sub(1);
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    pub fn copy_focused_content(&self) -> bool {
        let Some(entry) = self.timeline_get(self.timeline_cursor) else {
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
        self.search_query = query.to_string();
        self.search_matches.clear();
        self.search_current = 0;

        let lower = query.to_lowercase();
        self.ensure_search_text_index();
        self.search_matches = self
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
        if self.search_matches.is_empty() {
            return;
        }
        let idx = if self.search_current + 1 < self.search_matches.len() {
            self.search_current + 1
        } else {
            0
        };
        self.go_search_match(idx);
    }

    pub fn search_prev(&mut self) {
        if self.search_matches.is_empty() {
            return;
        }
        let idx = if self.search_current > 0 {
            self.search_current - 1
        } else {
            self.search_matches.len() - 1
        };
        self.go_search_match(idx);
    }

    fn go_search_match(&mut self, match_idx: usize) {
        if let Some(&entry_idx) = self.search_matches.get(match_idx) {
            self.search_current = match_idx;
            self.timeline_cursor = entry_idx;
            self.auto_scroll = false;
            // ChatView owns the wrapped visual-row index and performs the
            // actual scroll after width-aware cache reconciliation.
            self.request_redraw();
        }
    }

    pub fn cancel_search(&mut self) {
        self.search_query.clear();
        self.search_matches.clear();
        self.search_current = 0;
        self.search_active = false;
    }

    pub fn scroll_to_entry(&mut self, entry_idx: usize) {
        let vh = self.viewport_height.max(1);
        let mut offset: usize = 0;
        for i in 0..entry_idx.min(self.entry_line_counts.len()) {
            offset += self.entry_line_counts[i] + 1;
        }
        let entry_h = self.entry_line_counts.get(entry_idx).copied().unwrap_or(1);

        let scroll = self.scroll_offset;
        if offset < scroll {
            self.scroll_offset = offset;
        } else if offset + entry_h > scroll + vh {
            self.scroll_offset = offset.saturating_sub(vh.saturating_sub(entry_h));
        }
    }

    pub fn scroll_page_up(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
    }

    pub fn scroll_page_down(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.scroll_offset = self.scroll_offset.saturating_add(amount);
    }

    pub fn history_prev(&mut self) -> Option<String> {
        if self.input_history.is_empty() {
            return None;
        }
        let idx = match self.history_idx {
            Some(0) => return None,
            Some(i) => i - 1,
            None => self.input_history.len().saturating_sub(1),
        };
        self.history_idx = Some(idx);
        self.input_history.get(idx).cloned()
    }

    pub fn history_next(&mut self) -> Option<String> {
        let idx = match self.history_idx {
            Some(i) if i + 1 < self.input_history.len() => i + 1,
            _ => {
                self.history_idx = None;
                return Some(String::new());
            }
        };
        self.history_idx = Some(idx);
        self.input_history.get(idx).cloned()
    }

    pub fn apply_event(&mut self, event: CowdEvent) {
        match event {
            CowdEvent::SessionScoped {
                session_id, event, ..
            } => {
                if session_id == self.session_id {
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
                if projection.session_id == self.session_id {
                    self.history_total_messages = projection.total_messages as usize;
                    self.history_has_older =
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
                    self.session_history_index = Some(projection);
                    self.msg_version = self.msg_version.wrapping_add(1);
                }
            }
            CowdEvent::SessionHistoryCatchupPage { page } => {
                let visible_at_tail = self.history_window_end_offset >= self.history_total_messages;
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
                if session_id == self.session_id {
                    self.history_hydrated = true;
                    self.history_hydration_error = None;
                    match kind {
                        crate::protocol::SessionHistoryHydrationKind::InitialWindow => {
                            self.history_oldest_offset = oldest_offset;
                            self.history_window_end_offset =
                                oldest_offset.saturating_add(message_count);
                            self.history_total_messages =
                                total_messages.max(self.history_window_end_offset);
                            self.history_has_older = has_older;
                        }
                        crate::protocol::SessionHistoryHydrationKind::IncrementalCatchup => {
                            let previous_total = self.history_total_messages;
                            let was_at_tail = self.history_window_end_offset >= previous_total;
                            self.history_total_messages =
                                self.history_total_messages.max(total_messages);
                            if was_at_tail {
                                self.history_window_end_offset = self.history_total_messages;
                                let visible_span = self
                                    .history_window_end_offset
                                    .saturating_sub(self.history_oldest_offset);
                                if visible_span > SOFT_CAP {
                                    self.history_oldest_offset =
                                        self.history_window_end_offset.saturating_sub(SOFT_CAP);
                                }
                                self.history_has_older = self.history_oldest_offset > 0;
                            }
                        }
                    }
                    self.history_window_truncated = self.history_has_older;
                    self.telemetry.history_hydration_duration_ms = Some(duration_ms);
                    self.telemetry.history_hydrated_messages = self
                        .telemetry
                        .history_hydrated_messages
                        .saturating_add(message_count);
                    self.telemetry.history_hydration_pages = self
                        .telemetry
                        .history_hydration_pages
                        .saturating_add(page_count);
                    if self.history_has_older {
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
                if page.session_id == self.session_id {
                    if self.turn_is_active() {
                        self.history_loading_older = false;
                        self.add_system_notice(
                            SystemNoticeKind::Warning,
                            "Older history was not installed because a live turn started while the page was loading",
                        );
                        return;
                    }
                    self.history_prepend_anchor_message_id = self
                        .timeline_get(self.timeline_cursor)
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
                    self.history_loading_older = false;
                    self.history_oldest_offset = oldest_offset;
                    self.history_window_end_offset = self
                        .history_window_end_offset
                        .saturating_sub(page.messages.len());
                    self.history_total_messages = self.history_total_messages.max(page.total);
                    self.history_has_older = has_older;
                    // API `has_more` points toward newer messages. This page
                    // is nevertheless complete for the older-window action.
                    page.has_more = false;
                    self.apply_history_page(page);
                    if let Some(anchor) = self.history_prepend_anchor_message_id.as_deref() {
                        if let Some(index) = self.timeline_message_index(anchor) {
                            self.timeline_cursor = index;
                            self.auto_scroll = false;
                        }
                    }
                    self.history_prepend_revision = self.history_prepend_revision.wrapping_add(1);
                }
            }
            CowdEvent::SessionHistoryNewerPage {
                mut page,
                window_end_offset,
                has_newer,
            } => {
                if page.session_id == self.session_id {
                    self.history_loading_newer = false;
                    if self.turn_is_active() {
                        self.add_system_notice(
                            SystemNoticeKind::Warning,
                            "Newer history was not installed because a live turn started while the page was loading",
                        );
                        return;
                    }
                    let loaded = page.messages.len();
                    self.make_room_for_newer_history(loaded);
                    self.history_oldest_offset = self.history_oldest_offset.saturating_add(loaded);
                    self.history_window_end_offset = window_end_offset;
                    self.history_total_messages = self.history_total_messages.max(page.total);
                    self.history_has_older = self.history_oldest_offset > 0;
                    page.has_more = false;
                    self.apply_history_page(page);
                    self.auto_scroll = !has_newer;
                    if self.auto_scroll {
                        self.timeline_cursor = self.timeline_len().saturating_sub(1);
                    }
                }
            }
            CowdEvent::SessionHistoryLatestPage {
                mut page,
                oldest_offset,
            } => {
                if page.session_id == self.session_id {
                    self.history_loading_newer = false;
                    if self.turn_is_active() {
                        self.add_system_notice(
                            SystemNoticeKind::Warning,
                            "Latest history was not installed because a live turn started while the page was loading",
                        );
                        return;
                    }
                    self.clear_durable_history_window();
                    self.history_oldest_offset = oldest_offset;
                    self.history_window_end_offset =
                        oldest_offset.saturating_add(page.messages.len());
                    self.history_total_messages = page.total;
                    self.history_has_older = oldest_offset > 0;
                    page.has_more = false;
                    self.apply_history_page(page);
                    self.auto_scroll = true;
                    self.timeline_cursor = self.timeline_len().saturating_sub(1);
                }
            }
            CowdEvent::SessionHistoryOlderFailed { session_id, error } => {
                if session_id == self.session_id {
                    self.history_loading_older = false;
                    self.history_loading_newer = false;
                    self.add_system_notice(
                        SystemNoticeKind::Error,
                        &format!("Loading older durable history failed: {error}"),
                    );
                }
            }
            CowdEvent::SessionHistoryHydrationFailed { session_id, error } => {
                if session_id == self.session_id {
                    self.history_hydrated = false;
                    self.history_hydration_error = Some(error.clone());
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
                if session_id == self.session_id
                    && self
                        .pending_message_admissions
                        .get(&client_message_id)
                        .is_some_and(|generation| *generation == submission_generation)
                {
                    self.pending_message_admissions.remove(&client_message_id);
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
                if session_id != self.session_id
                    || self
                        .pending_message_admissions
                        .get(&client_message_id)
                        .is_none_or(|generation| *generation != submission_generation)
                {
                    return;
                }
                self.pending_message_admissions.remove(&client_message_id);
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
                if self.input.text().is_empty() {
                    self.input.set_text(&original_text);
                }
                if started_new_turn
                    && self.pending_message_admissions.is_empty()
                    && matches!(
                        self.turn_interaction.transport,
                        crate::components::turn_interaction::TransportState::Submitting
                    )
                {
                    self.turn_interaction
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
                if session_id == self.session_id {
                    self.revoke_session_authorization(&reason);
                }
            }
            CowdEvent::SessionStreamConnection { session_id, state } => {
                if session_id == self.session_id {
                    self.stream_connection_state = state.clone();
                    match state {
                        crate::protocol::SessionStreamConnectionState::Connecting => {
                            self.turn_interaction.reconnecting();
                        }
                        crate::protocol::SessionStreamConnectionState::Connected => {}
                        crate::protocol::SessionStreamConnectionState::Reconnecting {
                            after_cursor,
                            ..
                        } => {
                            self.telemetry.session_sse_reconnect_count =
                                self.telemetry.session_sse_reconnect_count.saturating_add(1);
                            self.telemetry.session_sse_last_cursor = after_cursor;
                            self.turn_interaction.reconnecting();
                        }
                    }
                    self.mark_dirty();
                }
            }
            CowdEvent::ExecutionProjectionConnection { state, .. } => {
                self.projection_connection_state = Some(state.clone());
                match state {
                    crate::protocol::SessionStreamConnectionState::Connecting => {
                        self.turn_interaction.reconnecting();
                    }
                    crate::protocol::SessionStreamConnectionState::Reconnecting {
                        after_cursor,
                        ..
                    } => {
                        self.telemetry.projection_sse_reconnect_count = self
                            .telemetry
                            .projection_sse_reconnect_count
                            .saturating_add(1);
                        self.telemetry.projection_sse_last_cursor = after_cursor;
                        self.turn_interaction.reconnecting();
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
                    let id = self.thinking_id_counter;
                    self.thinking_id_counter += 1;
                    self.timeline_push(TimelineEntry::Thinking {
                        id,
                        causal_item_id: None,
                        causality: None,
                        content: summary,
                        complete: false,
                        expanded: false,
                    });
                    self.current_turn_thinking_count =
                        self.current_turn_thinking_count.saturating_add(1);
                    self.msg_version = self.msg_version.wrapping_add(1);
                } else {
                    self.lines_dirty = true;
                }
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
            }

            CowdEvent::ToolStart { id, name, preview } => {
                let id = self.current_tool_instance_key(&id);
                if self
                    .tool_timeline_positions
                    .get(&id)
                    .copied()
                    .and_then(|position| self.logical_timeline_index(position))
                    .is_some()
                {
                    return;
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
                self.current_turn_tool_count = self.current_turn_tool_count.saturating_add(1);
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::ToolProgress {
                id,
                name: _,
                progress,
            } => {
                let id = self.current_tool_instance_key(&id);
                let tool_index = self
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
                    self.lines_dirty = true;
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
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::TokenUsage {
                input,
                output,
                cache_create,
                cache_read,
            } => {
                self.input_tokens = input;
                self.output_tokens = output;
                self.token_count = input + output + cache_create + cache_read;
                self.turn_input_tokens = input.saturating_sub(self.pre_turn_input);
                self.turn_output_tokens = output.saturating_sub(self.pre_turn_output);
                self.turn_usage_known = true;
            }
            CowdEvent::RunModelTelemetry { telemetry } => {
                if let Some(model) = telemetry
                    .model
                    .as_ref()
                    .filter(|model| !model.trim().is_empty())
                {
                    self.effective_model = Some(model.clone());
                    self.model = model.clone();
                    self.model_source = Some("runtime.run_model_telemetry".to_string());
                }
                self.input_tokens = telemetry.input_tokens;
                self.output_tokens = telemetry.output_tokens;
                self.token_count = telemetry.total_tokens;
                self.turn_input_tokens = telemetry.input_tokens;
                self.turn_output_tokens = telemetry.output_tokens;
                self.turn_usage_known = !matches!(
                    telemetry.usage_source.as_str(),
                    "" | "unknown" | "pending" | "runtime_request_budget_estimate"
                );
                let metrics = self
                    .current_run_metrics
                    .get_or_insert_with(Default::default);
                metrics.input_tokens = telemetry.input_tokens;
                metrics.output_tokens = telemetry.output_tokens;
                metrics.total_tokens = telemetry.total_tokens;
                self.latest_model_telemetry = Some(telemetry);
                self.refresh_model_mismatch_telemetry();
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::ContextWindow(ctx) => {
                self.context_window = ctx;
                self.context_window_tokens = Some(ctx);
                self.msg_version = self.msg_version.wrapping_add(1);
            }
            CowdEvent::ProviderAttempt {
                model,
                context_window_tokens,
                context_window_source,
                packed_input_tokens,
                ..
            } => {
                self.effective_model = Some(model);
                self.model_source = Some("runtime.provider_attempt.model".to_string());
                // ProviderAttempt is a pre-request estimate delivered on a
                // separate event stream. It may arrive after the canonical
                // terminal projection, so it must never replace observed
                // provider usage for the same turn. TurnStarted resets these
                // fields before the next request can install a new estimate.
                if self.context_usage_source.as_deref() != Some("provider_actual") {
                    self.context_used_tokens = Some(packed_input_tokens);
                    self.context_window_tokens = Some(context_window_tokens);
                    self.context_remaining_tokens =
                        Some(context_window_tokens.saturating_sub(packed_input_tokens));
                    self.context_usage_percent_bp = (context_window_tokens > 0).then(|| {
                        packed_input_tokens
                            .saturating_mul(10_000)
                            .saturating_div(context_window_tokens)
                            .min(10_000) as u16
                    });
                    self.context_usage_source = Some(format!(
                        "runtime.provider_attempt.request_budget:{context_window_source}"
                    ));
                    self.context_window = context_window_tokens;
                }
                self.msg_version = self.msg_version.wrapping_add(1);
            }
            CowdEvent::ContextEnvelope { envelope } => {
                self.latest_context_envelope = Some(envelope);
                self.msg_version = self.msg_version.wrapping_add(1);
            }
            CowdEvent::RuntimePolicyDecision { summary } => {
                self.latest_runtime_policy = Some(summary);
                self.msg_version = self.msg_version.wrapping_add(1);
            }
            CowdEvent::ExecutionGraphSummary { summary } => {
                self.latest_execution_graph_summary = Some(summary);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::TurnStarted => {
                self.turn_interaction.submit_started();
                self.reset_live_execution_facts();
                self.streaming_received = false;
                self.latest_context_envelope = None;
                self.latest_runtime_policy = None;
                self.latest_execution_graph_summary = None;
                self.latest_execution_projection = None;
                self.latest_run_projection = None;
                self.thinking_id_counter = 0;
                self.pre_turn_input = self.input_tokens;
                self.pre_turn_output = self.output_tokens;
                self.turn_input_tokens = 0;
                self.turn_output_tokens = 0;
                self.turn_usage_known = false;
                self.current_turn_tool_count = 0;
                self.current_turn_thinking_count = 0;
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::ResourcesCommitted { ids } => {
                self.pending_resources
                    .retain(|resource| !ids.contains(&resource.id));
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionInputProjection { projection } => {
                self.apply_session_input_projection(projection);
            }
            CowdEvent::ResourceUploaded { id, label, kind } => {
                if !self
                    .pending_resources
                    .iter()
                    .any(|resource| resource.id == id)
                {
                    self.pending_resources.push(PendingResource {
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
                self.compaction_count += 1;
                self.add_system_notice(
                    SystemNoticeKind::Info,
                    &format!("Compacted {removed_count} earlier messages to save context."),
                );
            }

            CowdEvent::MemoryEntry { .. } => {
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::MemoryUpdate { .. } => {
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::MemoryStats {
                total_entries,
                vector_count,
                layers,
            } => {
                self.memory_total_entries = Some(total_entries);
                self.memory_vector_count = Some(vector_count);
                self.memory_layer_counts = memory_layer_counts_from_strings(&layers);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionList { sessions } => {
                self.sessions = sessions;
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionCreated { id, name } => {
                self.sessions.push((id, name, App::format_timestamp()));
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionDeleted { id } => {
                self.sessions.retain(|(sid, _, _)| sid != &id);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::SessionSwitched { id: _, name } => {
                self.active_session_name = name;
                self.msg_version = self.msg_version.wrapping_add(1);
            }
            CowdEvent::Warning { message } => {
                self.show_notification(&message);
            }
            // New CowdEvent variants not yet consumed by TUI
            _ => {}
        }
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
mod tests {
    use super::*;

    fn make_msg(content: &str) -> TimelineEntry {
        TimelineEntry::Message {
            role: "user".into(),
            content: content.into(),
            timestamp: "12:00".into(),
            identity: None,
        }
    }

    #[test]
    fn timeline_no_trim_at_3000() {
        let mut app = App::new("test", "sess");
        for i in 0..3500 {
            app.add_message("user", &format!("msg {i}"));
        }
        assert_eq!(app.timeline_len(), 3500);
        let first = app.timeline_get(0).unwrap();
        assert!(first.full_text().contains("msg 0"));
        let last = app.timeline_get(3499).unwrap();
        assert!(last.full_text().contains("msg 3499"));
    }

    #[test]
    fn scroll_up_loads_page() {
        let mut app = App::new("test", "sess");
        for i in 0..600 {
            app.add_message("user", &format!("msg {i}"));
        }
        assert_eq!(app.timeline_len(), 600);
        assert_eq!(app.timeline_pages.len(), 2);
        let at_500 = app.timeline_get(500).unwrap();
        assert!(at_500.full_text().contains("msg 500"));
        let at_0 = app.timeline_get(0).unwrap();
        assert!(at_0.full_text().contains("msg 0"));
    }

    #[test]
    fn context_envelope_event_updates_app_state() {
        let envelope = crate::test_utils::context_envelope_fixture();
        let expected_id = envelope
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_string();
        let mut app = App::new("test", "sess");

        app.apply_event(CowdEvent::ContextEnvelope { envelope });

        assert_eq!(
            app.latest_context_envelope
                .as_ref()
                .and_then(|env| env.get("id"))
                .and_then(serde_json::Value::as_str),
            Some(expected_id.as_str())
        );
    }

    #[test]
    fn turn_started_clears_previous_turn_runtime_evidence() {
        let mut app = App::new("test", "sess");
        app.latest_context_envelope = Some(serde_json::json!({"selected": [{"id": "old"}]}));
        app.latest_runtime_policy = Some(crate::RuntimePolicyDecisionSummary {
            level: "complex".into(),
            score: 80,
            recommended_profile: "deep".into(),
            agent_mode: "team".into(),
            requires_review: true,
            signal_count: 3,
        });
        app.latest_execution_graph_summary = Some(crate::RuntimeExecutionGraphSummary {
            graph_id: Some("g".into()),
            board_id: Some("b".into()),
            status: "done".into(),
            agent_tasks: 1,
            child_executions: 0,
            memory_candidates: 2,
            conflicts: 0,
            completion_rate: Some(1.0),
            synthesis_lift: None,
            complementarity_score: None,
        });
        app.latest_run_projection = Some(serde_json::json!({"kind": "session.run_projection"}));
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::Complete);
        app.effective_model = Some("old-effective-model".to_string());
        app.context_used_tokens = Some(8_000);
        app.context_window_tokens = Some(128_000);
        app.current_run_metrics = Some(Default::default());

        app.apply_event(CowdEvent::TurnStarted);

        assert!(app.latest_context_envelope.is_none());
        assert!(app.latest_runtime_policy.is_none());
        assert!(app.latest_execution_graph_summary.is_none());
        assert!(app.latest_run_projection.is_none());
        assert!(app.current_execution_status.is_none());
        assert!(app.effective_model.is_none());
        assert!(app.context_used_tokens.is_none());
        assert!(app.context_window_tokens.is_none());
        assert!(app.current_run_metrics.is_none());
        assert!(app.turn_is_active());
    }

    #[test]
    fn app_applies_gateway_session_run_projection() {
        let mut app = App::new("test", "sess");
        app.apply_run_projection(serde_json::json!({
            "kind": "session.run_projection",
            "token_speed": {
                "stats": {
                    "tokens": {
                        "total": 512
                    }
                }
            },
            "memory_context": {
                "context_envelope": {
                    "id": "ctx-v31",
                    "selected": [{"id": "mem-1"}],
                    "omitted": []
                }
            }
        }));

        assert_eq!(app.token_count, 512);
        assert_eq!(app.authoritative_session_input_tokens, None);
        assert_eq!(app.authoritative_session_output_tokens, None);
        assert_eq!(
            app.latest_context_envelope
                .as_ref()
                .and_then(|value| value.get("id"))
                .and_then(Value::as_str),
            Some("ctx-v31")
        );
        assert_eq!(
            app.latest_run_projection
                .as_ref()
                .and_then(|value| value.get("kind"))
                .and_then(Value::as_str),
            Some("session.run_projection")
        );
    }

    #[test]
    fn run_projection_owns_full_session_tokens_separately_from_the_visible_window() {
        let mut app = App::new("test", "sess");
        app.apply_run_projection(serde_json::json!({
            "kind": "session.run_projection",
            "token_speed": {
                "stats": {
                    "tokens": {
                        "input": 45_000,
                        "output": 5_000,
                        "total": 50_000
                    }
                }
            }
        }));
        app.record_durable_message_usage(
            "visible-message",
            &serde_json::json!({"input_tokens": 40, "output_tokens": 5}),
        );

        assert_eq!(app.authoritative_session_input_tokens, Some(45_000));
        assert_eq!(app.authoritative_session_output_tokens, Some(5_000));
        assert_eq!(app.durable_session_input_tokens, 40);
        assert_eq!(app.durable_session_output_tokens, 5);
    }

    #[test]
    fn execution_projection_owner_rejects_lower_revision_for_same_execution() {
        use harness_contract::execution_graph::ExecutionGraph;
        use harness_contract::projection::{ExecutionProjection, ProjectionCommandAvailability};

        let projection = |revision: u64, objective: &str| ExecutionProjection {
            schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
            execution_id: "execution-monotonic".to_string(),
            revision,
            cursor: revision,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
            authorization_revision: 1,
            redaction_revision: "redaction-1".to_string(),
            session_id: Some("session-monotonic".to_string()),
            mission_id: None,
            strategy: None,
            graph: harness_contract::execution_graph::project_execution_graph(
                &ExecutionGraph::new(objective),
            ),
            child_executions: Vec::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            admissions: Vec::new(),
            outcomes: Vec::new(),
            interventions: Vec::new(),
            usage: Vec::new(),
            context: Vec::new(),
            evidence: Vec::new(),
            health: Vec::new(),
            recovery: Vec::new(),
            live: None,
            available_commands: Vec::<ProjectionCommandAvailability>::new(),
        };

        let mut app = App::new("test", "session-monotonic");
        assert!(app.apply_execution_projection(projection(5, "revision five")));
        let graph_summary_id = app
            .latest_execution_graph_summary
            .as_ref()
            .and_then(|summary| summary.graph_id.clone());
        assert!(!app.apply_execution_projection(projection(4, "stale revision four")));

        assert_eq!(
            app.latest_execution_projection
                .as_ref()
                .map(|current| current.revision),
            Some(5)
        );
        assert_eq!(
            app.latest_execution_graph_summary
                .as_ref()
                .and_then(|summary| summary.graph_id.clone()),
            graph_summary_id
        );
        assert!(app
            .latest_execution_projection
            .as_ref()
            .is_some_and(|current| current.graph.objective == "revision five"));
    }

    #[test]
    fn execution_projection_without_live_facts_cannot_reuse_previous_execution_values() {
        use harness_contract::execution_graph::ExecutionGraph;
        use harness_contract::projection::{ExecutionProjection, ProjectionCommandAvailability};

        let mut app = App::new("requested-model", "session-live-missing");
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::Complete);
        app.current_execution_id = Some("execution-old".to_string());
        app.current_turn_id = Some("turn-old".to_string());
        app.effective_model = Some("old-effective-model".to_string());
        app.context_used_tokens = Some(64_000);
        app.context_window_tokens = Some(128_000);
        app.context_remaining_tokens = Some(64_000);
        app.context_usage_percent_bp = Some(5_000);
        app.current_run_metrics = Some(Default::default());
        app.input_tokens = 64_000;
        app.output_tokens = 2_000;
        app.token_count = 66_000;

        assert!(app.apply_execution_projection(ExecutionProjection {
            schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
            execution_id: "execution-new".to_string(),
            revision: 1,
            cursor: 1,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
            authorization_revision: 1,
            redaction_revision: "redaction-1".to_string(),
            session_id: Some("session-live-missing".to_string()),
            mission_id: None,
            strategy: None,
            graph: harness_contract::execution_graph::project_execution_graph(
                &ExecutionGraph::new("new execution"),
            ),
            child_executions: Vec::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            admissions: Vec::new(),
            outcomes: Vec::new(),
            interventions: Vec::new(),
            usage: Vec::new(),
            context: Vec::new(),
            evidence: Vec::new(),
            health: Vec::new(),
            recovery: Vec::new(),
            live: None,
            available_commands: Vec::<ProjectionCommandAvailability>::new(),
        }));

        assert_eq!(app.current_execution_id.as_deref(), Some("execution-new"));
        assert!(app.current_execution_status.is_none());
        assert!(app.current_turn_id.is_none());
        assert!(app.effective_model.is_none());
        assert!(app.context_used_tokens.is_none());
        assert!(app.context_window_tokens.is_none());
        assert!(app.current_run_metrics.is_none());
        assert_eq!(app.token_count, 0);
    }

    #[test]
    fn delayed_provider_attempt_cannot_replace_observed_projection_context_usage() {
        use harness_contract::projection::{
            ContextUsageProjection, ExecutionLiveState, ExecutionLiveStatus,
        };

        let mut app = App::new("requested-model", "session-context-authority");
        app.install_execution_live_facts(
            "execution-context-authority",
            &ExecutionLiveState {
                revision: 8,
                status: ExecutionLiveStatus::Complete,
                status_detail: None,
                turn_id: Some("turn-context-authority".to_string()),
                started_at_ms: 1,
                updated_at_ms: 2,
                last_progress_at_ms: 2,
                context_usage: Some(ContextUsageProjection {
                    model: Some("observed-model".to_string()),
                    window_tokens: Some(16_384),
                    window_source: Some("configured".to_string()),
                    input_tokens: Some(188),
                    input_source: Some("provider_actual".to_string()),
                    remaining_tokens: Some(16_196),
                    usage_percent_bp: Some(114),
                    request_sequence: Some(5),
                    components: Vec::new(),
                }),
                metrics: harness_contract::projection::RunMetricsProjection {
                    input_tokens: 188,
                    output_tokens: 19,
                    total_tokens: 207,
                    ..Default::default()
                },
                latency: Default::default(),
                output_preview: None,
                output_preview_start_bytes: 0,
                output_bytes: 0,
                output_parts: Vec::new(),
                terminal_ref: Some("terminal-context-authority".to_string()),
                error: None,
            },
            None,
        );

        app.apply_event(CowdEvent::ProviderAttempt {
            model: "observed-model".to_string(),
            models_tried: vec!["observed-model".to_string()],
            context_window_tokens: 16_384,
            context_window_source: "configured".to_string(),
            packed_input_tokens: 5_536,
        });

        assert_eq!(app.context_used_tokens, Some(188));
        assert_eq!(app.context_remaining_tokens, Some(16_196));
        assert_eq!(app.context_usage_percent_bp, Some(114));
        assert_eq!(app.context_usage_source.as_deref(), Some("provider_actual"));
        assert_eq!(app.current_run_metrics.as_ref().unwrap().total_tokens, 207);
    }

    #[test]
    fn invalidating_selected_execution_clears_identity_without_materialized_projection() {
        let mut app = App::new("requested-model", "session-selection");
        app.current_execution_id = Some("execution-old".to_string());
        app.current_turn_id = Some("turn-old".to_string());
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::Finalizing);
        app.effective_model = Some("stale-model".to_string());
        assert!(app.latest_execution_projection.is_none());

        assert!(app.invalidate_execution_projection("execution-old"));
        assert!(app.current_execution_id.is_none());
        assert!(app.current_turn_id.is_none());
        assert!(app.current_execution_status.is_none());
        assert!(app.effective_model.is_none());
    }

    #[test]
    fn page_boundary_seamless() {
        let mut app = App::new("test", "sess");
        for i in 0..PAGE_SIZE {
            app.add_message("user", &format!("msg {i}"));
        }
        assert_eq!(app.timeline_len(), PAGE_SIZE);
        assert_eq!(app.timeline_pages.len(), 1);

        app.add_message("user", "overflow");
        assert_eq!(app.timeline_len(), PAGE_SIZE + 1);
        assert_eq!(app.timeline_pages.len(), 2);

        assert!(app.timeline_get(0).unwrap().full_text().contains("msg 0"));
        assert!(app
            .timeline_get(PAGE_SIZE - 1)
            .unwrap()
            .full_text()
            .contains(&format!("msg {}", PAGE_SIZE - 1)));
        assert!(app
            .timeline_get(PAGE_SIZE)
            .unwrap()
            .full_text()
            .contains("overflow"));

        let count = app.timeline_iter().count();
        assert_eq!(count, PAGE_SIZE + 1);
    }

    #[test]
    fn memory_soft_cap() {
        let mut app = App::new("test", "sess");
        for i in 0..(SOFT_CAP + 500) {
            app.add_message("user", &format!("msg {i}"));
        }
        assert!(app.timeline_len() <= SOFT_CAP);
        let first_entry = app.timeline_get(0).unwrap();
        assert!(!first_entry.full_text().contains("msg 0"));
    }

    #[test]
    fn empty_timeline_handled() {
        let app = App::new("test", "sess");
        assert!(app.timeline_is_empty());
        assert_eq!(app.timeline_len(), 0);
        assert!(app.timeline_get(0).is_none());
        assert_eq!(app.timeline_iter().count(), 0);
    }

    #[test]
    fn unresolved_startup_model_is_not_claimed_as_requested_model() {
        let app = App::new("unresolved", "sess");
        assert_eq!(app.model, "unresolved");
        assert_eq!(app.requested_model, None);
        assert_eq!(app.effective_model, None);
    }

    #[test]
    fn oversized_durable_history_exposes_the_visible_window_limit() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: Vec::new(),
                total: SOFT_CAP + 1,
                offset: 0,
                from_seq: Some(0),
                next_seq: None,
                limit: PAGE_SIZE,
                has_more: false,
            },
        });

        assert!(app.history_window_truncated);
        assert!(app.system_notices.iter().any(|notice| {
            notice.kind == SystemNoticeKind::Warning
                && notice.content.contains("Compact or checkpoint")
                && notice.content.contains(&SOFT_CAP.to_string())
        }));
        assert!(app
            .notification
            .as_deref()
            .is_some_and(|notice| notice.contains("Durable history")));
    }

    #[test]
    fn body_free_history_index_drives_session_coverage_without_materializing_messages() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::SessionHistoryIndexLoaded {
            projection: crate::protocol::SessionHistoryIndexProjection {
                schema_version: 1,
                session_id: "sess".to_string(),
                projection_generation: 9,
                durable_cursor: 42,
                event_cursor: 41,
                history_revision: 7,
                total_messages: 100_000,
                total_bytes: 8_000_000,
                latest_checkpoint_sequence: Some(90_000),
                latest_checkpoint_event_id: Some("checkpoint-1".to_string()),
                index_generation: 4,
                indexed_through_sequence: Some(99_999),
                index_card_count: 250,
                index_complete: true,
                recovery_state: crate::protocol::SessionHistoryRecoveryState::Ready,
                recent_metadata: Vec::new(),
                cards: Vec::new(),
            },
        });

        assert_eq!(app.history_total_messages, 100_000);
        assert!(app.history_has_older);
        assert_eq!(
            app.session_history_index
                .as_ref()
                .map(|index| (index.projection_generation, index.durable_cursor)),
            Some((9, 42))
        );
        assert!(app.timeline_is_empty());
    }

    #[test]
    fn session_input_projection_is_a_bounded_runtime_owned_queue_view() {
        let mut app = App::new("test", "sess");
        app.apply_session_input_projection(serde_json::json!({
            "inputs": [
                {
                    "input_id": "queued-a",
                    "status": "queued_next",
                    "decision": "enqueue_next_step",
                    "content_preview": "follow up with tests"
                },
                {
                    "input_id": "done-b",
                    "status": "consumed",
                    "decision": "start_new_turn",
                    "content_preview": "already consumed"
                }
            ]
        }));

        assert_eq!(app.queued_follow_up_count(), 1);
        let preview = app.queued_follow_up_preview().expect("queued preview");
        assert_eq!(preview.input_id, "queued-a");
        assert_eq!(preview.content_preview, "follow up with tests");
        assert!(app.system_notices.iter().any(|notice| {
            notice.content.contains("/queue edit queued-a")
                && notice.content.contains("/queue cancel queued-a")
        }));
        assert!(app
            .pending_inputs
            .iter()
            .all(|input| input.input_id != "done-b"));

        app.apply_session_input_projection(serde_json::json!({
            "pending_count": 0,
            "inputs": [
                {
                    "input_id": "queued-a",
                    "status": "consumed",
                    "decision": "start_new_turn",
                    "content_preview": "follow up with tests"
                }
            ]
        }));
        assert_eq!(
            app.queued_follow_up_count(),
            0,
            "a consumed canonical projection must clear the composer queue"
        );
        assert!(app.queued_follow_up_preview().is_none());
    }

    #[test]
    fn incremental_history_hydration_preserves_and_advances_the_existing_window() {
        use crate::protocol::SessionHistoryHydrationKind;

        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::SessionHistoryHydrated {
            session_id: "sess".to_string(),
            kind: SessionHistoryHydrationKind::InitialWindow,
            duration_ms: 4,
            message_count: 26,
            page_count: 1,
            oldest_offset: 0,
            total_messages: 26,
            next_sequence: 26,
            has_older: false,
        });
        app.apply_event(CowdEvent::SessionHistoryHydrated {
            session_id: "sess".to_string(),
            kind: SessionHistoryHydrationKind::IncrementalCatchup,
            duration_ms: 2,
            message_count: 2,
            page_count: 1,
            oldest_offset: 0,
            total_messages: 28,
            next_sequence: 28,
            has_older: false,
        });

        assert_eq!(app.history_oldest_offset, 0);
        assert_eq!(app.history_window_end_offset, 28);
        assert_eq!(app.history_total_messages, 28);
        assert!(!app.history_has_older);

        app.history_oldest_offset = 5;
        app.history_window_end_offset = 15;
        app.history_total_messages = 28;
        app.apply_event(CowdEvent::SessionHistoryHydrated {
            session_id: "sess".to_string(),
            kind: SessionHistoryHydrationKind::IncrementalCatchup,
            duration_ms: 1,
            message_count: 2,
            page_count: 1,
            oldest_offset: 0,
            total_messages: 30,
            next_sequence: 30,
            has_older: false,
        });

        assert_eq!(
            (app.history_oldest_offset, app.history_window_end_offset),
            (5, 15),
            "catch-up while browsing a middle window must not invent a new pagination offset"
        );
        assert_eq!(app.history_total_messages, 30);

        app.history_oldest_offset = 10;
        app.history_window_end_offset = SOFT_CAP + 10;
        app.history_total_messages = SOFT_CAP + 10;
        app.history_has_older = true;
        app.apply_event(CowdEvent::SessionHistoryHydrated {
            session_id: "sess".to_string(),
            kind: SessionHistoryHydrationKind::IncrementalCatchup,
            duration_ms: 1,
            message_count: 2,
            page_count: 1,
            oldest_offset: 0,
            total_messages: SOFT_CAP + 12,
            next_sequence: SOFT_CAP + 12,
            has_older: false,
        });

        assert_eq!(app.history_oldest_offset, 12);
        assert_eq!(app.history_window_end_offset, SOFT_CAP + 12);
        assert!(app.history_has_older);
    }

    #[test]
    fn fifty_thousand_message_catchup_does_not_contaminate_a_middle_history_window() {
        let page = crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![crate::protocol::SessionMessageProjection {
                id: "new-message-50000".to_string(),
                session_id: "sess".to_string(),
                sequence: 50_000,
                role: "assistant".to_string(),
                blocks: vec![serde_json::json!({
                    "type": "text",
                    "text": "new tail answer"
                })],
                created_at_ms: 50_000,
                token_usage: None,
                tool_use_id: None,
                tool_name: None,
            }],
            total: 50_001,
            offset: 50_000,
            from_seq: Some(50_000),
            next_seq: Some(50_001),
            limit: 500,
            has_more: false,
        };
        let mut app = App::new("test", "sess");
        app.history_oldest_offset = 24_000;
        app.history_window_end_offset = 25_000;
        app.history_total_messages = 50_000;

        app.apply_event(CowdEvent::SessionHistoryCatchupPage { page: page.clone() });
        app.apply_event(CowdEvent::SessionHistoryHydrated {
            session_id: "sess".to_string(),
            kind: crate::protocol::SessionHistoryHydrationKind::IncrementalCatchup,
            duration_ms: 1,
            message_count: 1,
            page_count: 1,
            oldest_offset: 50_000,
            total_messages: 50_001,
            next_sequence: 50_001,
            has_older: true,
        });

        assert!(
            app.timeline_iter()
                .all(|(_, entry)| !entry.full_text().contains("new tail answer")),
            "a reconnect catch-up must not splice the newest message into a browsed middle window"
        );
        assert_eq!(app.history_oldest_offset, 24_000);
        assert_eq!(app.history_window_end_offset, 25_000);
        assert_eq!(app.history_total_messages, 50_001);

        app.history_oldest_offset = 49_000;
        app.history_window_end_offset = 50_000;
        app.history_total_messages = 50_000;
        app.apply_event(CowdEvent::SessionHistoryCatchupPage { page });
        assert!(app
            .timeline_iter()
            .any(|(_, entry)| entry.full_text().contains("new tail answer")));
    }

    #[test]
    fn terminal_without_complete_causal_identity_is_visible_and_fail_closed() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-live", "turn-live"),
                text: "partial".to_string(),
                start_bytes: 0,
                end_bytes: 7,
                stream_revision: 7,
            },
        });
        let mut incomplete = correlation("execution-live", "turn-live");
        incomplete.message_id = Some("assistant-live".to_string());
        incomplete.terminal_id = None;
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: incomplete,
                assistant_text: "must not commit".to_string(),
                sequence: Some(1),
                iterations: 1,
                token_usage: None,
            },
        });

        assert_eq!(app.telemetry.orphan_event_count, 1);
        assert!(app
            .notification
            .as_deref()
            .is_some_and(|value| value.contains("Rejected terminal")));
        assert!(app
            .timeline_iter()
            .all(|(_, entry)| !entry.full_text().contains("must not commit")));
    }

    #[test]
    fn e10_history_failure_is_visible_without_polluting_the_transcript() {
        let mut app = App::new("test", "sess");
        app.add_message("assistant", "durable answer");
        let timeline_before = app.timeline_clone_vec();

        app.apply_event(CowdEvent::SessionHistoryHydrationFailed {
            session_id: "sess".to_string(),
            error: "HTTP 500 malformed stored message".to_string(),
        });

        assert!(!app.history_hydrated);
        assert_eq!(
            app.history_hydration_error.as_deref(),
            Some("HTTP 500 malformed stored message")
        );
        assert_eq!(app.timeline_clone_vec(), timeline_before);
        assert!(app.system_notices.iter().any(|notice| {
            notice.kind == SystemNoticeKind::Error
                && notice.content.contains("Session history unavailable")
                && notice.content.contains("malformed stored message")
        }));
    }

    #[test]
    fn e10_closed_session_admission_restores_the_draft_without_ghost_messages() {
        let mut app = App::new("test", "sess");
        let message_id = "tui:e10-message".to_string();
        app.begin_message_admission("must remain editable", message_id.clone(), 11, true);
        assert!(app
            .timeline_iter()
            .any(|(_, entry)| entry.full_text().contains("must remain editable")));

        app.apply_event(CowdEvent::MessageAdmissionFailed {
            session_id: "sess".to_string(),
            client_message_id: message_id,
            submission_generation: 11,
            original_text: "must remain editable".to_string(),
            started_new_turn: true,
            error: "session is closed".to_string(),
        });

        assert_eq!(app.input.text(), "must remain editable");
        assert!(app.pending_message_admissions.is_empty());
        assert!(app
            .timeline_iter()
            .all(|(_, entry)| !entry.full_text().contains("must remain editable")));
        assert!(app.system_notices.iter().any(|notice| {
            notice.kind == SystemNoticeKind::Error
                && notice.content.contains("draft was restored")
                && notice.content.contains("session is closed")
        }));
    }

    #[test]
    fn e10_session_authorization_revocation_clears_all_session_derived_state() {
        let mut app = App::new("private-model", "sess");
        app.add_message("assistant", "private transcript");
        app.input.set_text("private draft");
        app.effective_model = Some("private-effective-model".to_string());
        app.latest_context_envelope = Some(serde_json::json!({"secret": true}));

        app.apply_event(CowdEvent::SessionAuthorizationRevoked {
            session_id: "sess".to_string(),
            reason: "credential epoch changed".to_string(),
        });

        assert!(app.timeline_is_empty());
        assert!(app.input.text().is_empty());
        assert!(app.effective_model.is_none());
        assert!(app.latest_context_envelope.is_none());
        assert_eq!(app.model, "unavailable");
        assert!(app.system_notices.iter().any(|notice| {
            notice.kind == SystemNoticeKind::Error
                && notice.content.contains("Session authorization revoked")
        }));
    }

    #[test]
    fn session_activity_stats_cover_current_conversation() {
        let mut app = App::new("test", "sess");
        app.add_message("user", "hi");
        app.add_message("system", "memory update");
        app.timeline_push(TimelineEntry::Thinking {
            id: 1,
            causal_item_id: None,
            causality: None,
            content: "reasoning".to_string(),
            complete: true,
            expanded: false,
        });
        app.timeline_push(TimelineEntry::ToolCall {
            id: "tool-1".to_string(),
            name: "bash".to_string(),
            preview: "echo ok".to_string(),
            output: "ok".to_string(),
            done: true,
            expanded: false,
            exit_code: Some(0),
            causality: None,
        });
        app.add_message("assistant", "done");

        let stats = app.session_activity_stats();
        assert_eq!(stats.thinking_count, 1);
        assert_eq!(stats.tool_count, 1);
        assert_eq!(stats.message_count, 2);
        assert_eq!(stats.event_count, 4);
    }

    #[test]
    fn add_entry_appends_to_last_page() {
        let mut app = App::new("test", "sess");
        for i in 0..300 {
            app.timeline_push(make_msg(&format!("entry {i}")));
        }
        assert_eq!(app.timeline_len(), 300);
        assert_eq!(app.timeline_pages.len(), 1);
        assert_eq!(app.timeline_pages[0].entries.len(), 300);
        assert_eq!(app.timeline_pages[0].start_index, 0);
    }

    #[test]
    fn get_entry_cross_page() {
        let mut app = App::new("test", "sess");
        for i in 0..(PAGE_SIZE * 3 + 200) {
            app.timeline_push(make_msg(&format!("entry {i}")));
        }
        assert_eq!(app.timeline_len(), PAGE_SIZE * 3 + 200);
        assert!(app.timeline_get(0).unwrap().full_text().contains("entry 0"));
        assert!(app
            .timeline_get(PAGE_SIZE)
            .unwrap()
            .full_text()
            .contains(&format!("entry {}", PAGE_SIZE)));
        assert!(app
            .timeline_get(PAGE_SIZE * 2 + 50)
            .unwrap()
            .full_text()
            .contains(&format!("entry {}", PAGE_SIZE * 2 + 50)));
    }

    #[test]
    fn cursor_up_down_works_across_pages() {
        let mut app = App::new("test", "sess");
        for i in 0..600 {
            app.timeline_push(TimelineEntry::Thinking {
                id: i,
                causal_item_id: None,
                causality: None,
                content: format!("think {i}"),
                complete: true,
                expanded: false,
            });
        }
        app.timeline_cursor = 599;
        let moved = app.cursor_up();
        assert!(moved);
        assert!(app.timeline_cursor < 599);
    }

    fn correlation(execution_id: &str, turn_id: &str) -> crate::protocol::GatewayEventCorrelation {
        crate::protocol::GatewayEventCorrelation {
            session_id: "sess".to_string(),
            execution_id: Some(execution_id.to_string()),
            turn_id: Some(turn_id.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn causal_reasoning_items_remain_distinct_in_the_tui_timeline() {
        let mut app = App::new("test", "sess");
        app.current_execution_id = Some("execution-causal".to_string());
        app.current_turn_id = Some("turn-causal".to_string());
        for (item_id, text) in [("reasoning-a", "inspect"), ("reasoning-b", "decide")] {
            let mut item = correlation("execution-causal", "turn-causal");
            item.model_step_id = Some("step-causal".to_string());
            item.item_id = Some(item_id.to_string());
            item.segment_id = Some(format!("{item_id}:reasoning-summary:0"));
            app.apply_gateway_session_event(
                crate::protocol::GatewaySessionEvent::ReasoningSummaryDelta {
                    correlation: item.clone(),
                    summary: text.to_string(),
                },
            );
            app.apply_gateway_session_event(crate::protocol::GatewaySessionEvent::ItemCompleted {
                correlation: item,
                kind: "public_reasoning".to_string(),
            });
        }
        let items = app
            .timeline_clone_vec()
            .into_iter()
            .filter_map(|entry| match entry {
                TimelineEntry::Thinking {
                    causal_item_id,
                    content,
                    complete,
                    ..
                } => Some((causal_item_id, content, complete)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0],
            (
                Some("reasoning-a:reasoning-summary:0".to_string()),
                "inspect".to_string(),
                true
            )
        );
        assert_eq!(
            items[1],
            (
                Some("reasoning-b:reasoning-summary:0".to_string()),
                "decide".to_string(),
                true
            )
        );
    }

    #[test]
    fn canonical_cross_surface_fixture_keeps_causal_order_and_parallel_tool_waves() {
        let fixture: serde_json::Value =
            serde_json::from_str(harness_contract::live::CAUSAL_SURFACE_TIMELINE_V1_FIXTURE_JSON)
                .expect("canonical causal fixture");
        let session_id = fixture["session_id"].as_str().expect("fixture session");
        let mut app = App::new("fixture-model", session_id);

        for payload in fixture["events"].as_array().expect("fixture events") {
            let event = crate::gateway_client::gateway_sse_json_to_cowd_event_for_session(
                payload,
                Some(session_id),
            )
            .expect("fixture event must map to the TUI protocol");
            app.apply_event(event);
        }

        let rows = app
            .timeline_iter()
            .filter_map(|(_, entry)| match entry {
                TimelineEntry::Thinking {
                    causality: Some(causality),
                    ..
                } => causality.item_id.clone(),
                TimelineEntry::ToolCall {
                    causality: Some(causality),
                    ..
                } => causality.tool_call_id.clone(),
                _ => None,
            })
            .collect::<Vec<_>>();
        let expected = fixture["expected_activity"]
            .as_array()
            .expect("expected activity")
            .iter()
            .filter_map(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        assert_eq!(rows, expected);

        let tools = app
            .timeline_iter()
            .filter_map(|(_, entry)| match entry {
                TimelineEntry::ToolCall {
                    causality: Some(causality),
                    ..
                } => Some((
                    causality.tool_call_id.clone().unwrap_or_default(),
                    causality.wave,
                    causality.lane,
                    causality.lane_count,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            tools,
            vec![
                ("tool-a".to_string(), 0, 0, 2),
                ("tool-b".to_string(), 0, 1, 2),
                ("tool-c".to_string(), 1, 0, 1),
            ]
        );
        assert!(app.timeline_iter().any(|(_, entry)| matches!(
            entry,
            TimelineEntry::Message {
                role,
                content,
                ..
            } if role == "assistant" && content == "完成"
        )));
    }

    #[test]
    fn durable_history_hydrates_full_transcript_and_deduplicates_replayed_terminal() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: vec![
                    crate::protocol::SessionMessageProjection {
                        id: "user-1".to_string(),
                        session_id: "sess".to_string(),
                        sequence: 0,
                        role: "user".to_string(),
                        blocks: vec![serde_json::json!({
                            "type": "text",
                            "text": "historical question"
                        })],
                        created_at_ms: 1_000,
                        token_usage: None,
                        tool_use_id: None,
                        tool_name: None,
                    },
                    crate::protocol::SessionMessageProjection {
                        id: "assistant-1".to_string(),
                        session_id: "sess".to_string(),
                        sequence: 1,
                        role: "assistant".to_string(),
                        blocks: vec![serde_json::json!({
                            "type": "text",
                            "text": "historical answer"
                        })],
                        created_at_ms: 2_000,
                        token_usage: Some(serde_json::json!({
                            "input_tokens": 12,
                            "output_tokens": 3
                        })),
                        tool_use_id: None,
                        tool_name: None,
                    },
                ],
                total: 2,
                offset: 0,
                from_seq: Some(0),
                next_seq: Some(2),
                limit: 500,
                has_more: false,
            },
        });

        let mut terminal = correlation("execution-old", "turn-old");
        terminal.message_id = Some("assistant-1".to_string());
        terminal.terminal_id = Some("terminal-old".to_string());
        terminal.replayed = true;
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: terminal,
                assistant_text: "historical answer".to_string(),
                sequence: Some(1),
                iterations: 1,
                token_usage: None,
            },
        });

        assert!(app.history_hydrated);
        assert_eq!(app.timeline_len(), 2);
        assert_eq!(app.durable_session_input_tokens, 12);
        assert_eq!(app.durable_session_output_tokens, 3);
        assert_eq!(
            app.timeline_get(0).unwrap().full_text(),
            "historical question"
        );
        assert_eq!(
            app.timeline_get(1).unwrap().full_text(),
            "historical answer"
        );
        assert_eq!(app.input_history, vec!["historical question".to_string()]);
    }

    #[test]
    fn durable_history_restores_tool_use_and_result_as_one_deduplicated_card() {
        let page = crate::protocol::SessionMessagesPage {
            session_id: "sess".to_string(),
            messages: vec![
                crate::protocol::SessionMessageProjection {
                    id: "assistant-tool-use".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 0,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "read_file",
                        "input": "{\"path\":\"Cargo.toml\"}"
                    })],
                    created_at_ms: 1_000,
                    token_usage: None,
                    tool_use_id: Some("tool-1".to_string()),
                    tool_name: Some("read_file".to_string()),
                },
                crate::protocol::SessionMessageProjection {
                    id: "tool-result".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 1,
                    role: "tool".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "tool_result",
                        "tool_use_id": "tool-1",
                        "tool_name": "read_file",
                        "output": "workspace manifest",
                        "is_error": false
                    })],
                    created_at_ms: 2_000,
                    token_usage: None,
                    tool_use_id: Some("tool-1".to_string()),
                    tool_name: Some("read_file".to_string()),
                },
            ],
            total: 2,
            offset: 0,
            from_seq: Some(0),
            next_seq: Some(2),
            limit: 500,
            has_more: false,
        };
        let mut app = App::new("test", "sess");

        app.apply_event(CowdEvent::SessionHistoryPage { page: page.clone() });
        app.apply_event(CowdEvent::SessionHistoryPage { page });

        let tools = app
            .timeline_iter()
            .filter_map(|(_, entry)| match entry {
                TimelineEntry::ToolCall {
                    id,
                    name,
                    output,
                    done,
                    exit_code,
                    ..
                } => Some((id, name, output, done, exit_code)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(tools.len(), 1);
        assert!(
            tools[0].0.starts_with("tool-instance|"),
            "history tool cards must use their collision-safe canonical instance identity"
        );
        assert_eq!(tools[0].1, "read_file");
        assert_eq!(tools[0].2, "workspace manifest");
        assert!(*tools[0].3);
        assert_eq!(*tools[0].4, Some(0));
        assert!(
            app.timeline_iter()
                .all(|(_, entry)| !matches!(entry, TimelineEntry::Message { content, .. } if content.is_empty())),
            "tool-only assistant messages must not become empty chat bubbles"
        );
    }

    #[test]
    fn stable_turn_identity_prevents_cross_turn_delta_and_terminal_corruption() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-1", "turn-1"),
                text: "same prefix first".to_string(),
                start_bytes: 0,
                end_bytes: 17,
                stream_revision: 17,
            },
        });
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
                correlation: {
                    let mut correlation = correlation("execution-2", "turn-2");
                    correlation.message_id = Some("user-second".to_string());
                    correlation
                },
                content: "second question".to_string(),
                sequence: 1,
                created_at_ms: 2_000,
            },
        });
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-2", "turn-2"),
                text: "same prefix second".to_string(),
                start_bytes: 0,
                end_bytes: 18,
                stream_revision: 18,
            },
        });
        let mut first_terminal = correlation("execution-1", "turn-1");
        first_terminal.message_id = Some("assistant-first".to_string());
        first_terminal.terminal_id = Some("terminal-first".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: first_terminal,
                assistant_text: "first terminal".to_string(),
                sequence: Some(2),
                iterations: 1,
                token_usage: None,
            },
        });

        let messages = app
            .timeline_iter()
            .filter_map(|(_, entry)| match entry {
                TimelineEntry::Message {
                    content, identity, ..
                } => Some((content.as_str(), identity.as_ref())),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 3);
        assert!(
            messages
                .iter()
                .all(|(content, _)| *content != "first terminal"),
            "a stale terminal must not materialize prose into the active turn"
        );
        assert!(messages
            .iter()
            .any(|(content, _)| *content == "same prefix second"));
        assert_eq!(
            app.current_execution_status,
            Some(harness_contract::projection::ExecutionLiveStatus::Queued)
        );
        assert_eq!(app.current_execution_id.as_deref(), Some("execution-2"));
        assert_eq!(app.telemetry.orphan_event_count, 1);
    }

    #[test]
    fn durable_history_race_reconciles_live_reply_before_terminal_commit() {
        let mut app = App::new("test", "sess");
        let mut live = correlation("execution-race", "turn-race");
        live.part_id = Some("item-text-1:text:0".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: live,
                text: "streamed answer".to_string(),
                start_bytes: 0,
                end_bytes: 15,
                stream_revision: 15,
            },
        });
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: vec![crate::protocol::SessionMessageProjection {
                    id: "assistant-race".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 1,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "text",
                        "text": "streamed answer",
                        "cowd_turn_id": "turn-race"
                    })],
                    created_at_ms: 2_000,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                }],
                total: 2,
                offset: 0,
                from_seq: Some(0),
                next_seq: Some(2),
                limit: 500,
                has_more: false,
            },
        });

        let mut terminal = correlation("execution-race", "turn-race");
        terminal.part_id = Some("item-text-1:text:0".to_string());
        terminal.message_id = Some("assistant-race".to_string());
        terminal.terminal_id = Some("terminal-race".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: terminal,
                assistant_text: "streamed answer".to_string(),
                sequence: Some(1),
                iterations: 1,
                token_usage: None,
            },
        });

        let assistant = app
            .timeline_iter()
            .filter(|(_, entry)| {
                matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            assistant.len(),
            1,
            "history hydration and terminal delivery must reconcile the live bubble"
        );
        assert!(matches!(
            assistant[0].1,
            TimelineEntry::Message {
                content,
                identity: Some(MessageIdentity {
                    message_id: Some(message_id),
                    source: MessageSource::DurableHistory,
                    ..
                }),
                ..
            } if content == "streamed answer" && message_id == "assistant-race"
        ));
    }

    #[test]
    fn late_live_snapshot_cannot_recreate_a_committed_assistant_bubble() {
        let mut app = App::new("test", "sess");
        let mut live = correlation("execution-late", "turn-late");
        live.part_id = Some("item-text-1:text:0".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: live,
                text: "one answer".to_string(),
                start_bytes: 0,
                end_bytes: 10,
                stream_revision: 10,
            },
        });

        let mut terminal = correlation("execution-late", "turn-late");
        terminal.part_id = Some("item-text-1:text:0".to_string());
        terminal.message_id = Some("assistant-late".to_string());
        terminal.terminal_id = Some("terminal-late".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: terminal,
                assistant_text: "one answer".to_string(),
                sequence: Some(1),
                iterations: 1,
                token_usage: None,
            },
        });

        app.reconcile_live_output_parts(
            "execution-late",
            Some("turn-late"),
            &[harness_contract::projection::ExecutionLiveOutputPart {
                model_step_id: "step-late".to_string(),
                item_id: "item-late".to_string(),
                part_id: "item-text-1:text:0".to_string(),
                causal_sequence: 1,
                completed: true,
                preview: Some("one answer".to_string()),
                preview_start_bytes: 0,
                bytes: 10,
            }],
            10,
        );

        let assistant_count = app
            .timeline_iter()
            .filter(|(_, entry)| {
                matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant")
            })
            .count();
        assert_eq!(
            assistant_count, 1,
            "a delayed canonical preview must not duplicate its committed terminal"
        );
    }

    #[test]
    fn identical_typed_text_deltas_are_appended_without_snapshot_guessing() {
        let mut app = App::new("test", "sess");
        for (start_bytes, text) in [(0, "ha"), (2, "ha")] {
            app.apply_event(CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TextDelta {
                    correlation: correlation("execution-repeat", "turn-repeat"),
                    text: text.to_string(),
                    start_bytes,
                    end_bytes: start_bytes + text.len(),
                    stream_revision: (start_bytes + text.len()) as u64,
                },
            });
        }

        assert!(matches!(
            app.timeline_get(0),
            Some(TimelineEntry::Message { content, .. }) if content == "haha"
        ));
    }

    #[test]
    fn text_delta_revision_is_monotonic_within_one_causal_part() {
        let mut app = App::new("test", "sess");
        let apply = |app: &mut App, text: &str, start_bytes, end_bytes, stream_revision| {
            app.apply_event(CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TextDelta {
                    correlation: correlation("execution-revision", "turn-revision"),
                    text: text.to_string(),
                    start_bytes,
                    end_bytes,
                    stream_revision,
                },
            });
        };

        apply(&mut app, "first", 0, 5, 10);
        apply(&mut app, "stale-conflict", 0, 14, 9);
        apply(&mut app, "replayed-conflict", 0, 17, 10);
        apply(&mut app, " tail", 5, 10, 11);

        assert!(matches!(
            app.timeline_get(0),
            Some(TimelineEntry::Message { content, .. }) if content == "first tail"
        ));
        assert_eq!(
            app.telemetry.text_delta_dedupe_count, 2,
            "older and equal revisions must not mutate visible text"
        );
    }

    #[test]
    fn durable_terminal_rejects_late_non_terminal_phase_for_the_same_execution() {
        let mut app = App::new("test", "sess");
        let mut terminal = correlation("execution-terminal", "turn-terminal");
        terminal.message_id = Some("assistant-terminal".to_string());
        terminal.terminal_id = Some("terminal-1".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: terminal.clone(),
                assistant_text: "done".to_string(),
                sequence: Some(1),
                iterations: 1,
                token_usage: None,
            },
        });
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
                correlation: terminal,
                status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
                detail: Some("late projection".to_string()),
            },
        });

        assert_eq!(
            app.current_execution_status,
            Some(harness_contract::projection::ExecutionLiveStatus::Complete)
        );
        assert_eq!(
            app.current_execution_status_detail.as_deref(),
            Some("durable terminal committed")
        );
        assert!(app.execution_is_terminalized("execution-terminal"));
        assert_eq!(
            app.telemetry.orphan_event_count, 0,
            "a known late phase is discarded as an ordering fact, not misreported as a causal orphan"
        );
    }

    #[test]
    fn queued_followup_does_not_replace_the_running_execution_status() {
        let mut app = App::new("test", "sess");
        app.turn_interaction.ingress_accepted("execution-running");
        app.current_execution_id = Some("execution-running".to_string());
        app.current_turn_id = Some("turn-running".to_string());
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::CallingModel);

        let mut queued = correlation("execution-queued", "turn-queued");
        queued.message_id = Some("message-queued".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
                correlation: queued,
                content: "follow up".to_string(),
                sequence: 4,
                created_at_ms: 5,
            },
        });

        assert_eq!(
            app.current_execution_id.as_deref(),
            Some("execution-running")
        );
        assert_eq!(
            app.current_execution_status,
            Some(harness_contract::projection::ExecutionLiveStatus::CallingModel)
        );
        assert!(app.timeline_iter().any(|(_, entry)| matches!(
            entry,
            TimelineEntry::Message {
                identity: Some(MessageIdentity {
                    message_id: Some(message_id),
                    ..
                }),
                ..
            } if message_id == "message-queued"
        )));
    }

    #[test]
    fn started_followup_replaces_stale_finalizing_correlation_for_observers() {
        let mut app = App::new("test", "sess");
        app.turn_interaction.ingress_accepted("execution-old");
        app.current_execution_id = Some("execution-old".to_string());
        app.current_turn_id = Some("turn-old".to_string());
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::Finalizing);

        let mut admitted = correlation("execution-new", "turn-new");
        admitted.message_id = Some("message-new".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
                correlation: admitted,
                content: "new observer turn".to_string(),
                sequence: 8,
                created_at_ms: 9,
            },
        });
        assert_eq!(
            app.current_execution_id.as_deref(),
            Some("execution-old"),
            "durable admission alone may still be queued and cannot steal an active turn"
        );

        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
                correlation: correlation("execution-new", "turn-new"),
                status: harness_contract::projection::ExecutionLiveStatus::PreparingContext,
                detail: Some("started by Runtime".to_string()),
            },
        });
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-new", "turn-new"),
                text: "first live delta".to_string(),
                start_bytes: 0,
                end_bytes: 16,
                stream_revision: 16,
            },
        });

        assert_eq!(app.current_execution_id.as_deref(), Some("execution-new"));
        assert_eq!(app.current_turn_id.as_deref(), Some("turn-new"));
        assert_eq!(
            app.current_execution_status,
            Some(harness_contract::projection::ExecutionLiveStatus::PreparingContext)
        );
        assert_eq!(app.telemetry.orphan_event_count, 0);
        assert!(app.timeline_iter().any(|(_, entry)| matches!(
            entry,
            TimelineEntry::Message {
                role,
                content,
                identity: Some(MessageIdentity {
                    source: MessageSource::Live,
                    execution_id: Some(execution_id),
                    turn_id: Some(turn_id),
                    ..
                }),
                ..
            } if role == "assistant"
                && content == "first live delta"
                && execution_id == "execution-new"
                && turn_id == "turn-new"
        )));

        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
                correlation: correlation("execution-old", "turn-old"),
                status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
                detail: Some("delayed old phase".to_string()),
            },
        });
        assert_eq!(
            app.current_execution_id.as_deref(),
            Some("execution-new"),
            "a superseded execution cannot reclaim the observer after the new Runtime phase"
        );
    }

    #[test]
    fn first_live_delta_activates_a_committed_followup_when_phase_was_coalesced() {
        let mut app = App::new("test", "sess");
        app.turn_interaction.ingress_accepted("execution-old");
        app.current_execution_id = Some("execution-old".to_string());
        app.current_turn_id = Some("turn-old".to_string());
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::Finalizing);

        let mut admitted = correlation("execution-new", "turn-new");
        admitted.message_id = Some("message-new".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
                correlation: admitted,
                content: "queued until Runtime starts it".to_string(),
                sequence: 8,
                created_at_ms: 9,
            },
        });
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-new", "turn-new"),
                text: "visible before terminal".to_string(),
                start_bytes: 0,
                end_bytes: 23,
                stream_revision: 23,
            },
        });

        assert_eq!(app.current_execution_id.as_deref(), Some("execution-new"));
        assert_eq!(app.current_turn_id.as_deref(), Some("turn-new"));
        assert_eq!(app.telemetry.orphan_event_count, 0);
        assert!(app.timeline_iter().any(|(_, entry)| matches!(
            entry,
            TimelineEntry::Message {
                role,
                content,
                identity: Some(MessageIdentity {
                    source: MessageSource::Live,
                    execution_id: Some(execution_id),
                    ..
                }),
                ..
            } if role == "assistant"
                && content == "visible before terminal"
                && execution_id == "execution-new"
        )));

        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-old", "turn-old"),
                text: "late old output".to_string(),
                start_bytes: 0,
                end_bytes: 15,
                stream_revision: 15,
            },
        });
        assert_eq!(app.current_execution_id.as_deref(), Some("execution-new"));
        assert_eq!(
            app.telemetry.orphan_event_count, 1,
            "the causal tombstone must reject delayed output from the superseded execution"
        );
    }

    #[test]
    fn first_live_delta_activates_new_turn_after_terminal_when_admission_was_missed() {
        let mut app = App::new("test", "sess");
        app.current_execution_id = Some("execution-old".to_string());
        app.current_turn_id = Some("turn-old".to_string());
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::Complete);
        app.terminal_correlations
            .push_back(("execution-old".to_string(), "turn-old".to_string()));
        app.turn_interaction.terminal_observed();

        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-new", "turn-new"),
                text: "visible before terminal".to_string(),
                start_bytes: 0,
                end_bytes: 23,
                stream_revision: 23,
            },
        });

        assert_eq!(app.current_execution_id.as_deref(), Some("execution-new"));
        assert_eq!(app.current_turn_id.as_deref(), Some("turn-new"));
        assert_eq!(app.telemetry.orphan_event_count, 0);
        assert!(app.timeline_iter().any(|(_, entry)| matches!(
            entry,
            TimelineEntry::Message {
                role,
                content,
                identity: Some(MessageIdentity {
                    source: MessageSource::Live,
                    execution_id: Some(execution_id),
                    ..
                }),
                ..
            } if role == "assistant"
                && content == "visible before terminal"
                && execution_id == "execution-new"
        )));

        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TextDelta {
                correlation: correlation("execution-old", "turn-old"),
                text: "late old output".to_string(),
                start_bytes: 0,
                end_bytes: 15,
                stream_revision: 15,
            },
        });
        assert_eq!(app.current_execution_id.as_deref(), Some("execution-new"));
        assert_eq!(app.telemetry.orphan_event_count, 1);
    }

    #[test]
    fn committed_cross_surface_user_message_reconciles_optimistic_identity() {
        let mut app = App::new("test", "sess");
        app.add_message_with_id(
            "user",
            "cross-surface prompt",
            Some("client-message-1".to_string()),
        );
        let mut committed = correlation("execution-1", "turn-1");
        committed.message_id = Some("client-message-1".to_string());
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted {
                correlation: committed,
                content: "cross-surface prompt".to_string(),
                sequence: 7,
                created_at_ms: 9_000,
            },
        });

        assert_eq!(app.timeline_len(), 1);
        assert!(matches!(
            app.timeline_get(0),
            Some(TimelineEntry::Message {
                identity: Some(MessageIdentity {
                    sequence: Some(7),
                    source: MessageSource::DurableIngress,
                    ..
                }),
                ..
            })
        ));
    }

    #[test]
    fn reconnect_history_repairs_cross_surface_message_order_by_durable_sequence() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: vec![
                    crate::protocol::SessionMessageProjection {
                        id: "user-0".to_string(),
                        session_id: "sess".to_string(),
                        sequence: 0,
                        role: "user".to_string(),
                        blocks: vec![serde_json::json!({"type": "text", "text": "zero"})],
                        created_at_ms: 1,
                        token_usage: None,
                        tool_use_id: None,
                        tool_name: None,
                    },
                    crate::protocol::SessionMessageProjection {
                        id: "assistant-2".to_string(),
                        session_id: "sess".to_string(),
                        sequence: 2,
                        role: "assistant".to_string(),
                        blocks: vec![serde_json::json!({"type": "text", "text": "two"})],
                        created_at_ms: 3,
                        token_usage: None,
                        tool_use_id: None,
                        tool_name: None,
                    },
                ],
                total: 2,
                offset: 0,
                from_seq: Some(0),
                next_seq: Some(3),
                limit: 500,
                has_more: false,
            },
        });
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: vec![
                    crate::protocol::SessionMessageProjection {
                        id: "user-0".to_string(),
                        session_id: "sess".to_string(),
                        sequence: 0,
                        role: "user".to_string(),
                        blocks: vec![serde_json::json!({"type": "text", "text": "zero"})],
                        created_at_ms: 1,
                        token_usage: None,
                        tool_use_id: None,
                        tool_name: None,
                    },
                    crate::protocol::SessionMessageProjection {
                        id: "user-1".to_string(),
                        session_id: "sess".to_string(),
                        sequence: 1,
                        role: "user".to_string(),
                        blocks: vec![serde_json::json!({"type": "text", "text": "one"})],
                        created_at_ms: 2,
                        token_usage: None,
                        tool_use_id: None,
                        tool_name: None,
                    },
                    crate::protocol::SessionMessageProjection {
                        id: "assistant-2".to_string(),
                        session_id: "sess".to_string(),
                        sequence: 2,
                        role: "assistant".to_string(),
                        blocks: vec![serde_json::json!({"type": "text", "text": "two"})],
                        created_at_ms: 3,
                        token_usage: None,
                        tool_use_id: None,
                        tool_name: None,
                    },
                ],
                total: 3,
                offset: 0,
                from_seq: Some(0),
                next_seq: Some(3),
                limit: 500,
                has_more: false,
            },
        });

        assert_eq!(
            app.timeline_iter()
                .map(|(_, entry)| entry.full_text())
                .collect::<Vec<_>>(),
            vec!["zero", "one", "two"]
        );
    }

    #[test]
    fn correlated_turn_error_stops_activity_and_exposes_terminal_status() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::TurnStarted);
        app.apply_event(CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TurnError {
                correlation: correlation("execution-failed", "turn-failed"),
                error: "provider unavailable".to_string(),
            },
        });

        assert!(!app.turn_is_active());
        assert_eq!(
            app.current_execution_status,
            Some(harness_contract::projection::ExecutionLiveStatus::Error)
        );
        assert_eq!(
            app.current_execution_status_detail.as_deref(),
            Some("provider unavailable")
        );
        assert_eq!(
            app.current_execution_id.as_deref(),
            Some("execution-failed")
        );
    }

    #[test]
    fn causal_history_places_late_terminal_before_the_next_ingress() {
        let mut app = App::new("test", "sess");
        let projection =
            |id: &str, sequence: usize, role: &str, text: &str, turn_id: &str, ingress_id: &str| {
                crate::protocol::SessionMessageProjection {
                    id: id.to_string(),
                    session_id: "sess".to_string(),
                    sequence,
                    role: role.to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "text",
                        "text": text,
                        "cowd_turn_id": turn_id,
                        "cowd_turn_ingress_message_id": ingress_id,
                    })],
                    created_at_ms: sequence as u64 + 1,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                }
            };
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: vec![
                    projection("user-1", 0, "user", "first", "turn-1", "user-1"),
                    projection("user-2", 1, "user", "second", "turn-2", "user-2"),
                    projection(
                        "assistant-1",
                        2,
                        "assistant",
                        "first answer",
                        "turn-1",
                        "user-1",
                    ),
                ],
                total: 3,
                offset: 0,
                from_seq: Some(0),
                next_seq: Some(3),
                limit: 500,
                has_more: false,
            },
        });

        assert_eq!(
            app.timeline_iter()
                .map(|(_, entry)| entry.full_text())
                .collect::<Vec<_>>(),
            vec!["first", "first answer", "second"]
        );
        assert_eq!(
            app.timeline_iter()
                .filter_map(|(_, entry)| match entry {
                    TimelineEntry::Message {
                        identity: Some(identity),
                        ..
                    } => identity.sequence,
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![0, 2, 1],
            "logical presentation order must not rewrite the immutable physical cursor"
        );
    }

    #[test]
    fn repeated_provider_tool_ids_pair_history_results_fifo() {
        let tool_use = |message_id: &str, sequence: usize, name: &str| {
            crate::protocol::SessionMessageProjection {
                id: message_id.to_string(),
                session_id: "sess".to_string(),
                sequence,
                role: "assistant".to_string(),
                blocks: vec![serde_json::json!({
                    "type": "tool_use",
                    "id": "provider-reused-id",
                    "name": name,
                    "input": "{}"
                })],
                created_at_ms: sequence as u64,
                token_usage: None,
                tool_use_id: Some("provider-reused-id".to_string()),
                tool_name: Some(name.to_string()),
            }
        };
        let tool_result = |message_id: &str, sequence: usize, output: &str| {
            crate::protocol::SessionMessageProjection {
                id: message_id.to_string(),
                session_id: "sess".to_string(),
                sequence,
                role: "tool".to_string(),
                blocks: vec![serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": "provider-reused-id",
                    "tool_name": "tool",
                    "output": output,
                    "is_error": false
                })],
                created_at_ms: sequence as u64,
                token_usage: None,
                tool_use_id: Some("provider-reused-id".to_string()),
                tool_name: Some("tool".to_string()),
            }
        };
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: vec![
                    tool_use("use-1", 0, "first-tool"),
                    tool_use("use-2", 1, "second-tool"),
                    tool_result("result-1", 2, "first-output"),
                    tool_result("result-2", 3, "second-output"),
                ],
                total: 4,
                offset: 0,
                from_seq: Some(0),
                next_seq: Some(4),
                limit: 500,
                has_more: false,
            },
        });
        assert_eq!(
            app.timeline_iter()
                .filter_map(|(_, entry)| match entry {
                    TimelineEntry::ToolCall { name, output, .. } =>
                        Some((name.as_str(), output.as_str())),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                ("first-tool", "first-output"),
                ("second-tool", "second-output")
            ]
        );
    }

    #[test]
    fn current_turn_thinking_counter_ignores_history_and_counts_one_live_stream() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::SessionHistoryPage {
            page: crate::protocol::SessionMessagesPage {
                session_id: "sess".to_string(),
                messages: vec![crate::protocol::SessionMessageProjection {
                    id: "historical-thinking".to_string(),
                    session_id: "sess".to_string(),
                    sequence: 0,
                    role: "assistant".to_string(),
                    blocks: vec![serde_json::json!({
                        "type": "thinking",
                        "thinking": "old reasoning"
                    })],
                    created_at_ms: 1,
                    token_usage: None,
                    tool_use_id: None,
                    tool_name: None,
                }],
                total: 1,
                offset: 0,
                from_seq: Some(0),
                next_seq: Some(1),
                limit: 500,
                has_more: false,
            },
        });
        assert_eq!(app.current_turn_thinking_count, 0);
        app.apply_event(CowdEvent::TurnStarted);
        app.apply_event(CowdEvent::ReasoningSummaryDelta {
            summary: "new".to_string(),
        });
        app.apply_event(CowdEvent::ReasoningSummaryDelta {
            summary: " reasoning".to_string(),
        });
        assert_eq!(app.current_turn_thinking_count, 1);
    }

    #[test]
    fn unicode_tool_progress_is_bounded_without_splitting_utf8() {
        let mut app = App::new("test", "sess");
        app.apply_event(CowdEvent::ToolStart {
            id: "tool-unicode".to_string(),
            name: "logger".to_string(),
            preview: String::new(),
        });
        app.apply_event(CowdEvent::ToolProgress {
            id: "tool-unicode".to_string(),
            name: "logger".to_string(),
            progress: "你好🙂".repeat(1200),
        });
        let output = app
            .timeline_iter()
            .find_map(|(_, entry)| match entry {
                TimelineEntry::ToolCall { output, .. } => Some(output),
                _ => None,
            })
            .expect("tool output");
        assert!(output.len() <= 4096);
        assert!(std::str::from_utf8(output.as_bytes()).is_ok());
    }
}
