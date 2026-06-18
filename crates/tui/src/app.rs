#![allow(dead_code)]
use crate::layout::{build_default_layout, LayoutState, LayoutTree};
use crate::runtime_control_store::{
    ApprovalSummary, ConnectorAccountSummary, ConnectorCapabilitySummary, ConnectorResourceSummary,
    CowdKernelSummary, RuntimeActionReceiptSummary, StructuredDataSummary, TaskSummary,
};
use crate::CowdEvent;
use ratatui::widgets::{Block, Borders};
use serde_json::Value;
use std::collections::VecDeque;
use tui_textarea::TextArea;

const PAGE_SIZE: usize = 500;
const SOFT_CAP: usize = 10000;
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
    },
    Thinking {
        id: u64,
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
    },
    SlashOutput {
        command: String,
        output: String,
        expanded: bool,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionActivityStats {
    pub thinking_count: usize,
    pub tool_count: usize,
    pub message_count: usize,
    pub event_count: usize,
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

#[derive(Debug, Clone)]
pub struct SessionSummary {
    pub id: String,
    pub path: String,
    pub updated_at_ms: u64,
    pub message_count: usize,
}

pub struct App {
    pub model: String,
    pub session_id: String,
    pub yolo_mode: bool,
    pub current_task: Option<CurrentTaskSummary>,
    pub input: TextArea<'static>,
    pub is_loading: bool,
    pub spinner_idx: usize,
    pub should_quit: bool,

    pub timeline_pages: VecDeque<TimelinePage>,
    pub total_entries: usize,
    pub timeline_cursor: usize,
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
    pub skin: crate::skin::SkinConfig,
    pub memory_status: Option<String>,
    pub memory_total_entries: Option<usize>,
    pub memory_vector_count: Option<usize>,
    pub memory_layer_counts: [usize; 5],

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
    /// Cowd kernel capability and release-gate summary observed through projection API.
    pub gateway_cowd_kernel: Option<CowdKernelSummary>,
    /// Structured data-plane summary observed through projection API.
    pub gateway_structured_data: Option<StructuredDataSummary>,
    /// Connector-specific degraded reasons observed through the Gateway API API.
    pub gateway_connector_degraded_reasons: Vec<String>,
    /// Degraded Gateway API/control reasons collected during snapshot refresh.
    pub gateway_degraded_reasons: Vec<String>,
    /// Current runtime session lease owner for the attached TUI session.
    pub gateway_lease_owner: Option<String>,
    /// Current runtime session lease mode for the attached TUI session.
    pub gateway_lease_mode: Option<String>,

    pub scroll_offset: u16,
    pub auto_scroll: bool,

    pub turn_active: bool,
    streaming_received: bool,

    pub msg_version: u64,
    pub last_drawn_version: u64,
    pub context_window: u64,
    pub latest_context_envelope: Option<Value>,
    pub latest_runtime_policy: Option<crate::RuntimePolicyDecisionSummary>,
    pub latest_workgraph_summary: Option<crate::RuntimeWorkGraphSummary>,
    pub input_tokens: u64,
    pub output_tokens: u64,

    pub turn_input_tokens: u64,
    pub turn_output_tokens: u64,
    pre_turn_input: u64,
    pre_turn_output: u64,

    pub cached_chat_lines: Vec<ratatui::text::Line<'static>>,

    pub entry_line_counts: Vec<u16>,
    pub lines_dirty: bool,
    last_built_line_count: usize,

    pub input_history: Vec<String>,
    pub history_idx: Option<usize>,

    pub search_query: String,
    pub search_matches: Vec<usize>,
    pub search_current: usize,
    pub search_active: bool,

    pub viewport_height: u16,

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
        let mut input = TextArea::default();
        input.set_block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Input (Enter=send, Esc=quit, Alt+Enter/Ctrl+J=newline) "),
        );
        input.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));

        Self {
            model: model.to_string(),
            session_id: session_id.to_string(),
            yolo_mode: false,
            current_task: None,
            input,
            is_loading: false,
            spinner_idx: 0,
            should_quit: false,

            timeline_pages: VecDeque::new(),
            total_entries: 0,
            timeline_cursor: 0,
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
            skin: crate::skin::SkinConfig::default(),
            memory_status: None,
            memory_total_entries: None,
            memory_vector_count: None,
            memory_layer_counts: [0; 5],

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
            gateway_cowd_kernel: None,
            gateway_structured_data: None,
            gateway_connector_degraded_reasons: Vec::new(),
            gateway_degraded_reasons: Vec::new(),
            gateway_lease_owner: None,
            gateway_lease_mode: None,

            scroll_offset: 0,
            auto_scroll: true,

            turn_active: false,
            streaming_received: false,

            msg_version: 0,
            last_drawn_version: u64::MAX,
            context_window: 0,
            latest_context_envelope: None,
            latest_runtime_policy: None,
            latest_workgraph_summary: None,
            input_tokens: 0,
            output_tokens: 0,

            turn_input_tokens: 0,
            turn_output_tokens: 0,
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

    pub fn mark_dirty(&mut self) {
        self.lines_dirty = true;
        self.msg_version = self.msg_version.wrapping_add(1);
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
        for page in &mut self.timeline_pages {
            if idx >= page.start_index && idx < page.start_index + page.entries.len() {
                return page.entries.get_mut(idx - page.start_index);
            }
        }
        None
    }

    pub fn timeline_last_mut(&mut self) -> Option<&mut TimelineEntry> {
        self.timeline_pages
            .back_mut()
            .and_then(|page| page.entries.last_mut())
    }

    pub fn timeline_push(&mut self, entry: TimelineEntry) {
        if self.timeline_pages.is_empty()
            || self
                .timeline_pages
                .back()
                .map_or(true, |p| p.entries.len() >= PAGE_SIZE)
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
        self.timeline_pages.back_mut().unwrap().entries.push(entry);
        self.total_entries += 1;
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

    fn soft_evict(&mut self) {
        while self.total_entries > SOFT_CAP {
            let Some(front) = self.timeline_pages.front() else {
                break;
            };
            let evict_count = front.entries.len();

            let evicted_lines: u16 = if !self.entry_line_counts.is_empty() {
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

            let evicted_lines: u16 = if !self.entry_line_counts.is_empty() {
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
            if self.timeline_get(idx).map_or(false, |e| e.is_collapsible()) {
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
            if self.timeline_get(idx).map_or(false, |e| e.is_collapsible()) {
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
        self.timeline_push(TimelineEntry::Message {
            role: role.to_string(),
            content: content.to_string(),
            timestamp: App::format_timestamp(),
        });
        self.timeline_cursor = self.timeline_len().saturating_sub(1);
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    pub fn add_slash_output(&mut self, command: &str, output: &str) {
        let trimmed = output.trim();
        if trimmed.is_empty() {
            return;
        }
        let line_count = trimmed.lines().count();
        if line_count <= 3 {
            self.add_message("system", &format!("/{command}:"));
            self.add_message("system", trimmed);
        } else {
            self.timeline_push(TimelineEntry::SlashOutput {
                command: command.to_string(),
                output: trimmed.to_string(),
                expanded: false,
            });
            self.timeline_cursor = self.timeline_len().saturating_sub(1);
            self.msg_version = self.msg_version.wrapping_add(1);
        }
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
        let mut matches = Vec::new();
        for (idx, entry) in self.timeline_iter() {
            if entry.full_text().to_lowercase().contains(&lower) {
                matches.push(idx);
            }
        }
        self.search_matches = matches;

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
            self.scroll_to_entry(entry_idx);
        }
    }

    pub fn cancel_search(&mut self) {
        self.search_query.clear();
        self.search_matches.clear();
        self.search_current = 0;
        self.search_active = false;
    }

    pub fn scroll_to_entry(&mut self, entry_idx: usize) {
        let vh = self.viewport_height.max(1) as usize;
        let mut offset: usize = 0;
        for i in 0..entry_idx.min(self.entry_line_counts.len()) {
            offset += self.entry_line_counts[i] as usize + 1;
        }
        let entry_h = self.entry_line_counts.get(entry_idx).copied().unwrap_or(1) as usize;

        let scroll = self.scroll_offset as usize;
        if offset < scroll {
            self.scroll_offset = offset as u16;
        } else if offset + entry_h > scroll + vh {
            self.scroll_offset = offset.saturating_sub(vh.saturating_sub(entry_h)) as u16;
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
            CowdEvent::TextDelta { text } => {
                self.streaming_received = true;
                self.auto_scroll = true;
                let mut found = false;
                if let Some(TimelineEntry::Message { role, content, .. }) = self.timeline_last_mut()
                {
                    if role == "assistant" && content != "✓ Done" {
                        if text.starts_with(content.as_str()) {
                            content.clear();
                            content.push_str(&text);
                        } else {
                            content.push_str(&text);
                        }
                        found = true;
                    }
                }
                if !found {
                    self.timeline_push(TimelineEntry::Message {
                        role: "assistant".into(),
                        content: text,
                        timestamp: App::format_timestamp(),
                    });
                    self.msg_version = self.msg_version.wrapping_add(1);
                } else {
                    self.lines_dirty = true;
                }
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
            }

            CowdEvent::ThinkingDelta { thinking } => {
                let mut found = false;
                if let Some(TimelineEntry::Thinking {
                    content, complete, ..
                }) = self.timeline_last_mut()
                {
                    if !*complete {
                        if thinking.starts_with(content.as_str()) {
                            content.clear();
                            content.push_str(&thinking);
                        } else {
                            content.push_str(&thinking);
                        }
                        found = true;
                    }
                }
                if !found {
                    let id = self.thinking_id_counter;
                    self.thinking_id_counter += 1;
                    self.timeline_push(TimelineEntry::Thinking {
                        id,
                        content: thinking,
                        complete: false,
                        expanded: false,
                    });
                    self.msg_version = self.msg_version.wrapping_add(1);
                } else {
                    self.lines_dirty = true;
                }
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
            }

            CowdEvent::ThinkingComplete => {
                if let Some(TimelineEntry::Thinking {
                    complete, expanded, ..
                }) = self.timeline_last_mut()
                {
                    *complete = true;
                    *expanded = false;
                }
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::ToolStart { id, name, preview } => {
                self.auto_scroll = true;
                self.timeline_push(TimelineEntry::ToolCall {
                    id,
                    name,
                    preview,
                    output: String::new(),
                    done: false,
                    expanded: true,
                    exit_code: None,
                });
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::ToolProgress {
                id,
                name: _,
                progress,
            } => {
                let mut found_output: Option<&mut String> = None;
                for entry in self.timeline_iter_mut() {
                    if let TimelineEntry::ToolCall {
                        id: tid, output, ..
                    } = entry
                    {
                        if tid == &id {
                            found_output = Some(output);
                        }
                    }
                }
                if let Some(output) = found_output {
                    output.push_str(&progress);
                    if output.len() > 4096 {
                        *output = output[output.len() - 4096..].to_string();
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
                let mut found: Option<(&mut String, &mut bool, &mut bool, &mut Option<i32>)> = None;
                for entry in self.timeline_iter_mut() {
                    if let TimelineEntry::ToolCall {
                        id: tid,
                        output,
                        done,
                        expanded,
                        exit_code: ec,
                        ..
                    } = entry
                    {
                        if tid == &id {
                            found = Some((output, done, expanded, ec));
                        }
                    }
                }
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
            }

            CowdEvent::ContextWindow(ctx) => {
                self.context_window = ctx;
            }
            CowdEvent::ContextEnvelope { envelope } => {
                self.latest_context_envelope = Some(envelope);
                self.msg_version = self.msg_version.wrapping_add(1);
            }
            CowdEvent::RuntimePolicyDecision { summary } => {
                self.latest_runtime_policy = Some(summary);
                self.msg_version = self.msg_version.wrapping_add(1);
            }
            CowdEvent::WorkGraphSummary { summary } => {
                self.latest_workgraph_summary = Some(summary);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::TurnStarted => {
                self.is_loading = true;
                self.turn_active = true;
                self.streaming_received = false;
                self.thinking_id_counter = 0;
                self.pre_turn_input = self.input_tokens;
                self.pre_turn_output = self.output_tokens;
                self.turn_input_tokens = 0;
                self.turn_output_tokens = 0;
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::TurnComplete {
                assistant_text,
                iterations: _,
            } => {
                self.is_loading = false;
                self.turn_active = false;
                for entry in self.timeline_iter_mut() {
                    match entry {
                        TimelineEntry::Thinking { expanded, .. } => *expanded = false,
                        TimelineEntry::ToolCall { expanded, .. } => *expanded = false,
                        _ => {}
                    }
                }
                if !assistant_text.is_empty() {
                    if self.streaming_received {
                        if let Some(TimelineEntry::Message { role, content, .. }) =
                            self.timeline_last_mut()
                        {
                            if role == "assistant" && assistant_text.starts_with(content.as_str()) {
                                *content = assistant_text;
                            }
                        }
                    } else {
                        self.timeline_push(TimelineEntry::Message {
                            role: "assistant".into(),
                            content: assistant_text,
                            timestamp: App::format_timestamp(),
                        });
                    }
                }
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::TurnError { error } => {
                self.is_loading = false;
                self.turn_active = false;
                self.timeline_push(TimelineEntry::Message {
                    role: "system".into(),
                    content: format!("Error: {error}"),
                    timestamp: App::format_timestamp(),
                });
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            CowdEvent::CompactionNotice { removed_count } => {
                self.compaction_count += 1;
                self.timeline_push(TimelineEntry::Message {
                    role: "system".into(),
                    content: format!("Compacted {removed_count} earlier messages to save context."),
                    timestamp: App::format_timestamp(),
                });
                self.timeline_cursor = self.timeline_len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(content: &str) -> TimelineEntry {
        TimelineEntry::Message {
            role: "user".into(),
            content: content.into(),
            timestamp: "12:00".into(),
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
    fn session_activity_stats_cover_current_conversation() {
        let mut app = App::new("test", "sess");
        app.add_message("user", "hi");
        app.add_message("system", "memory update");
        app.timeline_push(TimelineEntry::Thinking {
            id: 1,
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
        });
        app.add_message("assistant", "done");

        let stats = app.session_activity_stats();
        assert_eq!(stats.thinking_count, 1);
        assert_eq!(stats.tool_count, 1);
        assert_eq!(stats.message_count, 2);
        assert_eq!(stats.event_count, 5);
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
}

/// Trait for tool registry integration with SkillsPanel.
pub trait ToolRegistry: Send + Sync {
    fn enable_tool(&self, name: &str);
    fn disable_tool(&self, name: &str);
}
