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
    pub(crate) fn from_correlation(correlation: &crate::protocol::GatewayEventCorrelation) -> Self {
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
pub(crate) struct LiveMessageKey {
    pub(crate) execution_id: Option<String>,
    pub(crate) turn_id: Option<String>,
    pub(crate) part_id: Option<String>,
}

/// Canonical identity for one tool invocation.
///
/// Provider-local tool ids are not globally unique (`dsml-tool-0` commonly
/// repeats every turn). All indexing therefore includes the owning
/// Session/execution/turn/part, or the durable message/block position while
/// hydrating history.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ToolInstanceIdentity {
    pub(crate) session_id: String,
    pub(crate) execution_id: Option<String>,
    pub(crate) turn_id: Option<String>,
    pub(crate) part_id: Option<String>,
    pub(crate) durable_message_id: Option<String>,
    pub(crate) durable_sequence: Option<usize>,
    pub(crate) block_index: Option<usize>,
    pub(crate) provider_tool_id: String,
}

impl ToolInstanceIdentity {
    pub(crate) fn stable_key(&self) -> String {
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
