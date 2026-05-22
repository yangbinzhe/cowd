#![allow(dead_code)]
use tui_textarea::TextArea;
use ratatui::widgets::{Block, Borders};
use crate::tui::TuiEvent;

/// A single entry in the time-ordered conversation timeline.
/// Replaces the old split model of Vec<ChatMessage> + Vec<ToolCard> + streaming_thinking.
#[derive(Debug, Clone)]
pub enum TimelineEntry {
    /// A text message (user or assistant or system).
    Message {
        role: String,
        content: String,
        timestamp: String,
    },
    /// A thinking/reasoning block that can be collapsed.
    Thinking {
        id: u64,
        content: String,
        complete: bool,
        expanded: bool,
    },
    /// A tool call that can be collapsed.
    ToolCall {
        id: String,
        name: String,
        preview: String,
        output: String,
        done: bool,
        expanded: bool,
        exit_code: Option<i32>,
    },
    /// Slash command output (grouped into a single collapsible entry).
    /// Prevents flooding the timeline with per-line system messages.
    SlashOutput {
        /// Command name (e.g. "session", "status", "cost")
        command: String,
        /// Full captured output text
        output: String,
        /// Whether the output is currently expanded
        expanded: bool,
    },
}

impl TimelineEntry {
    /// Number of display lines when fully expanded (approx).
    pub fn expanded_lines(&self) -> usize {
        match self {
            Self::Message { content, .. } => content.lines().count().max(1),
            Self::Thinking { content, expanded, .. } => {
                if *expanded { content.lines().count().max(1) + 2 } else { 1 }
            }
            Self::ToolCall { output, expanded, .. } => {
                if *expanded && !output.is_empty() { output.lines().count().max(1) + 2 } else { 1 }
            }
            Self::SlashOutput { output, expanded, .. } => {
                if *expanded && !output.is_empty() { output.lines().count().max(1) + 2 } else { 1 }
            }
        }
    }

    /// Whether this entry can be toggled (expanded/collapsed).
    pub fn is_collapsible(&self) -> bool {
        matches!(self, Self::Thinking { .. } | Self::ToolCall { .. } | Self::SlashOutput { .. })
    }

    /// Whether this entry is currently expanded.
    pub fn is_expanded(&self) -> bool {
        match self {
            Self::Thinking { expanded, .. } => *expanded,
            Self::ToolCall { expanded, .. } => *expanded,
            Self::SlashOutput { expanded, .. } => *expanded,
            _ => false,
        }
    }

    /// Toggle expanded state.
    pub fn toggle(&mut self) {
        match self {
            Self::Thinking { expanded, .. } => *expanded = !*expanded,
            Self::ToolCall { expanded, .. } => *expanded = !*expanded,
            Self::SlashOutput { expanded, .. } => *expanded = !*expanded,
            _ => {}
        }
    }

    /// Get the full text content of this entry (for copy to clipboard).
    pub fn full_text(&self) -> String {
        match self {
            Self::Message { content, .. } => content.clone(),
            Self::Thinking { content, .. } => content.clone(),
            Self::ToolCall { output, .. } => output.clone(),
            Self::SlashOutput { output, .. } => output.clone(),
        }
    }
}

// ── Legacy types kept for App API compatibility ──

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Panel { Chat, Gateway, Files, Delegate, Memory, Skills }

pub struct App {
    pub model: String,
    pub session_id: String,
    pub input: TextArea<'static>,
    pub is_loading: bool,
    pub spinner_idx: usize,
    pub should_quit: bool,

    // ── Timeline (replaces messages + tool_cards + streaming_thinking) ──
    pub timeline: Vec<TimelineEntry>,
    /// Index of the currently focused timeline entry (for expand/collapse).
    pub timeline_cursor: usize,
    /// Counter for generating unique thinking IDs.
    thinking_id_counter: u64,

    pub token_count: u64,
    pub compaction_count: u32,
    pub cache_hits: u64,
    pub picker_active: bool,
    pub picker_sessions: Vec<SessionSummary>,
    pub picker_idx: usize,
    pub theme: Theme,
    pub approval: Option<ApprovalRequest>,
    pub current_panel: Panel,
    pub gateway_sessions: Vec<GatewaySession>,
    pub gateway_platform: String,
    pub file_entries: Vec<FileEntry>,
    pub delegate_tasks: Vec<DelegateTask>,
    pub memory_entries: Vec<MemoryEntry>,
    pub skill_list: Vec<SkillSummary>,
    pub skin: crate::tui::skin::SkinConfig,
    pub memory_status: Option<String>,

    // ── Scrolling ──
    pub scroll_offset: u16,
    pub auto_scroll: bool,

    // ── Turn state ──
    pub turn_active: bool,
    /// Whether we've received any TextDelta this turn (for TurnComplete fallback).
    streaming_received: bool,

    pub msg_version: u64,
    pub last_drawn_version: u64,
    pub context_window: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,

    // ── Per-turn token tracking ──
    pub turn_input_tokens: u64,
    pub turn_output_tokens: u64,
    /// Snapshot of cumulative totals at turn start (for computing deltas).
    pre_turn_input: u64,
    pre_turn_output: u64,

    // ── Render cache ──
    pub cached_chat_lines: Vec<ratatui::text::Line<'static>>,

    // ── Virtual scrolling ──
    /// Per-entry line count in cached_chat_lines (excluding separator blank lines).
    /// Used to compute viewport-visible entries without building all lines.
    pub entry_line_counts: Vec<u16>,
    /// True when cached_chat_lines is stale and needs rebuilding.
    pub lines_dirty: bool,
    /// For incremental line building: last index into cached_chat_lines that was built.
    last_built_line_count: usize,

    // ── Input history ──
    pub input_history: Vec<String>,
    pub history_idx: Option<usize>,

    // ── Search ──
    pub search_query: String,
    pub search_matches: Vec<usize>,
    pub search_current: usize,
    pub search_active: bool,

    // ── Viewport (updated by chat widget) ──
    pub viewport_height: u16,

    // ── Help panel ──
    pub help_visible: bool,

    // ── Model switching ──
    pub available_models: Vec<String>,
    pub model_dirty: bool,

    // ── Notification ──
    pub notification: Option<String>,
    notification_ttl: u32,
}

#[derive(Debug, Clone)]
pub struct MemoryEntry {
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

#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub tool_name: String,
    pub input_preview: String,
    pub approved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Theme { Dark, Light }

impl Theme {
    pub fn bg(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::Black, Self::Light => ratatui::style::Color::White }
    }
    pub fn fg(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::White, Self::Light => ratatui::style::Color::Black }
    }
    pub fn accent(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::Cyan, Self::Light => ratatui::style::Color::Blue }
    }
    pub fn user_color(&self) -> ratatui::style::Color {
        match self { Self::Dark => ratatui::style::Color::Green, Self::Light => ratatui::style::Color::DarkGray }
    }
    pub fn toggle(&mut self) { *self = match self { Self::Dark => Self::Light, Self::Light => Self::Dark }; }
}

impl App {
    pub fn new(model: &str, session_id: &str) -> Self {
        let mut input = TextArea::default();
        input.set_block(Block::default().borders(Borders::ALL).title(" Input (Enter=send, Esc=quit, Shift+Enter=newline) "));
        input.set_style(ratatui::style::Style::default().fg(ratatui::style::Color::White));

        Self {
            model: model.to_string(),
            session_id: session_id.to_string(),
            input,
            is_loading: false,
            spinner_idx: 0,
            should_quit: false,

            timeline: Vec::new(),
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
            current_panel: Panel::Chat,
            gateway_sessions: Vec::new(),
            gateway_platform: String::new(),
            file_entries: Vec::new(),
            delegate_tasks: Vec::new(),
            memory_entries: Vec::new(),
            skill_list: Vec::new(),
            skin: crate::tui::skin::SkinConfig::default(),
            memory_status: None,

            scroll_offset: 0,
            auto_scroll: true,

            turn_active: false,
            streaming_received: false,

            msg_version: 0,
            last_drawn_version: u64::MAX,
            context_window: 0,
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

            available_models: Vec::new(),
            model_dirty: false,

            notification: None,
            notification_ttl: 0,
        }
    }

    /// Mark render cache as dirty (force rebuild next draw).
    pub fn mark_dirty(&mut self) {
        self.lines_dirty = true;
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    pub fn next_panel(&mut self) {
        self.current_panel = match self.current_panel {
            Panel::Chat => Panel::Gateway,
            Panel::Gateway => Panel::Files,
            Panel::Files => Panel::Memory,
            Panel::Memory => Panel::Skills,
            Panel::Skills => Panel::Delegate,
            Panel::Delegate => Panel::Chat,
        };
    }

    pub fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"];
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

    /// Cycle to the next available model name.
    /// Returns the new model name, or None if only one model is available.
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
            // Current model not in list — pick first
            self.model = self.available_models[0].clone();
            self.model_dirty = true;
            Some(self.model.clone())
        }
    }

    /// Show a temporary notification that auto-dismisses after ~3 seconds.
    pub fn show_notification(&mut self, msg: &str) {
        self.notification = Some(msg.to_string());
        self.notification_ttl = 30; // ~3s at 10fps tick
    }

    /// Format a timestamp for display in the chat.
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

    // ── Session picker ──

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
        if self.picker_idx > 0 { self.picker_idx -= 1; }
    }

    pub fn picker_down(&mut self) {
        if self.picker_idx + 1 < self.picker_sessions.len() { self.picker_idx += 1; }
    }

    pub fn picker_selected_id(&self) -> Option<&str> {
        self.picker_sessions.get(self.picker_idx).map(|s| s.id.as_str())
    }

    // ── Navigation ──

    /// Move timeline cursor up by one collapsible entry.
    /// Returns true if the cursor actually moved.
    pub fn cursor_up(&mut self) -> bool {
        if self.timeline.is_empty() { return false; }
        let mut idx = self.timeline_cursor;
        loop {
            if idx == 0 { break; }
            idx -= 1;
            if self.timeline[idx].is_collapsible() {
                self.timeline_cursor = idx;
                self.auto_scroll = false;
                return true;
            }
        }
        false
    }

    /// Move timeline cursor down by one collapsible entry.
    /// Returns true if the cursor actually moved.
    pub fn cursor_down(&mut self) -> bool {
        if self.timeline.is_empty() { return false; }
        let mut idx = self.timeline_cursor;
        while idx + 1 < self.timeline.len() {
            idx += 1;
            if self.timeline[idx].is_collapsible() {
                self.timeline_cursor = idx;
                self.auto_scroll = true; // re-enable auto-scroll when moving to bottom
                return true;
            }
        }
        false
    }

    /// Toggle expand/collapse on the currently focused timeline entry.
    pub fn toggle_expand_current(&mut self) {
        if let Some(entry) = self.timeline.get_mut(self.timeline_cursor) {
            entry.toggle();
            self.msg_version = self.msg_version.wrapping_add(1);
        }
    }

    // ── Trim ──

    fn trim_timeline(&mut self) {
        const MAX: usize = 3000;
        if self.timeline.len() > MAX {
            let excess = self.timeline.len() - MAX;
            self.timeline.drain(0..excess);
            self.timeline_cursor = self.timeline_cursor.saturating_sub(excess);
            self.scroll_offset = self.scroll_offset.saturating_sub(excess as u16);
        }
    }

    // ── Public API (used by runner to push user/system messages) ──

    /// Add a user or system message to the timeline.
    pub fn add_message(&mut self, role: &str, content: &str) {
        self.timeline.push(TimelineEntry::Message {
            role: role.to_string(),
            content: content.to_string(),
            timestamp: App::format_timestamp(),
        });
        self.timeline_cursor = self.timeline.len().saturating_sub(1);
        self.msg_version = self.msg_version.wrapping_add(1);
        self.trim_timeline();
    }

    /// Add slash command output as a single collapsible entry.
    /// Short output (<=3 lines) is added as a simple system message.
    /// Longer output gets grouped into a SlashOutput entry.
    pub fn add_slash_output(&mut self, command: &str, output: &str) {
        let trimmed = output.trim();
        if trimmed.is_empty() { return; }
        let line_count = trimmed.lines().count();
        if line_count <= 3 {
            self.add_message("system", &format!("/{command}:"));
            self.add_message("system", trimmed);
        } else {
            self.timeline.push(TimelineEntry::SlashOutput {
                command: command.to_string(),
                output: trimmed.to_string(),
                expanded: false, // collapsed by default to avoid dominating the view
            });
            self.timeline_cursor = self.timeline.len().saturating_sub(1);
            self.msg_version = self.msg_version.wrapping_add(1);
            self.trim_timeline();
        }
    }

    /// Copy the content of the currently focused timeline entry to system clipboard.
    /// Returns true if copy succeeded, false otherwise.
    pub fn copy_focused_content(&self) -> bool {
        let Some(entry) = self.timeline.get(self.timeline_cursor) else {
            return false;
        };
        let text = entry.full_text();
        if text.is_empty() {
            return false;
        }
        crate::tui::osc52::write_osc52_clipboard(&text)
    }

    // ── Search ──

    /// Execute a search: find all timeline entries whose full_text contains the query.
    /// Jumps to the first match if any found.
    pub fn execute_search(&mut self, query: &str) {
        self.search_query = query.to_string();
        self.search_matches.clear();
        self.search_current = 0;

        let lower = query.to_lowercase();
        for (idx, entry) in self.timeline.iter().enumerate() {
            if entry.full_text().to_lowercase().contains(&lower) {
                self.search_matches.push(idx);
            }
        }

        // Jump to first match
        self.go_search_match(0);
    }

    /// Navigate to the next search match.
    pub fn search_next(&mut self) {
        if self.search_matches.is_empty() { return; }
        let idx = if self.search_current + 1 < self.search_matches.len() {
            self.search_current + 1
        } else {
            0
        };
        self.go_search_match(idx);
    }

    /// Navigate to the previous search match.
    pub fn search_prev(&mut self) {
        if self.search_matches.is_empty() { return; }
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

    /// Clear search state.
    pub fn cancel_search(&mut self) {
        self.search_query.clear();
        self.search_matches.clear();
        self.search_current = 0;
        self.search_active = false;
    }

    // ── Scrolling helpers ──

    /// Scroll so the entry at the given index is visible in the viewport.
    pub fn scroll_to_entry(&mut self, entry_idx: usize) {
        let vh = self.viewport_height.max(1) as usize;
        // Compute the line offset where this entry starts
        let mut offset: usize = 0;
        for i in 0..entry_idx.min(self.entry_line_counts.len()) {
            offset += self.entry_line_counts[i] as usize + 1; // +1 for separator
        }
        let entry_h = self.entry_line_counts
            .get(entry_idx)
            .copied()
            .unwrap_or(1) as usize;

        // If the entry is above the viewport, scroll to it
        let scroll = self.scroll_offset as usize;
        if offset < scroll {
            self.scroll_offset = offset as u16;
        }
        // If the entry is below the viewport, scroll so it's visible at the bottom
        else if offset + entry_h > scroll + vh {
            self.scroll_offset = offset.saturating_sub(vh.saturating_sub(entry_h)) as u16;
        }
    }

    /// Scroll up by one viewport worth of lines.
    pub fn scroll_page_up(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.scroll_offset = self.scroll_offset.saturating_sub(amount);
    }

    /// Scroll down by one viewport worth of lines.
    pub fn scroll_page_down(&mut self) {
        let amount = self.viewport_height.max(1).saturating_sub(1);
        self.scroll_offset = self.scroll_offset.saturating_add(amount);
    }

    // ── Input history ──

    /// Navigate to an older (previous) input history entry.
    /// Returns the history text to set in the input box, if any.
    pub fn history_prev(&mut self) -> Option<String> {
        if self.input_history.is_empty() { return None; }
        let idx = match self.history_idx {
            Some(0) => return None,
            Some(i) => i - 1,
            None => self.input_history.len().saturating_sub(1),
        };
        self.history_idx = Some(idx);
        self.input_history.get(idx).cloned()
    }

    /// Navigate to a newer (next) input history entry.
    /// Returns the history text to set in the input box, or empty string to clear.
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

    // ── Event handling ──

    /// Apply a TuiEvent from the background turn runner to the display state.
    pub fn apply_event(&mut self, event: TuiEvent) {
        match event {
            TuiEvent::TextDelta { text } => {
                self.streaming_received = true;
                self.auto_scroll = true;
                // Find last incomplete assistant Message, or create new one
                let mut found = false;
                if let Some(TimelineEntry::Message { role, content, .. }) = self.timeline.last_mut() {
                    if role == "assistant" && content != "✓ Done" {
                        content.push_str(&text);
                        found = true;
                    }
                }
                if !found {
                    self.timeline.push(TimelineEntry::Message {
                        role: "assistant".into(),
                        content: text,
                        timestamp: App::format_timestamp(),
                    });
                    self.msg_version = self.msg_version.wrapping_add(1); // structural change
                } else {
                    // Streaming update: mark dirty without full version bump
                    self.lines_dirty = true;
                }
                self.timeline_cursor = self.timeline.len().saturating_sub(1);
                self.trim_timeline();
            }

            TuiEvent::ThinkingDelta { thinking } => {
                // Find last incomplete Thinking entry, or create new one
                let mut found = false;
                if let Some(TimelineEntry::Thinking { content, complete, .. }) = self.timeline.last_mut() {
                    if !*complete {
                        content.push_str(&thinking);
                        found = true;
                    }
                }
                if !found {
                    let id = self.thinking_id_counter;
                    self.thinking_id_counter += 1;
                    self.timeline.push(TimelineEntry::Thinking {
                        id,
                        content: thinking,
                        complete: false,
                        expanded: false,
                    });
                    self.msg_version = self.msg_version.wrapping_add(1); // structural change
                } else {
                    // Streaming update: mark dirty without full version bump
                    self.lines_dirty = true;
                }
                self.timeline_cursor = self.timeline.len().saturating_sub(1);
                self.trim_timeline();
            }

            TuiEvent::ThinkingComplete => {
                // Mark the last incomplete Thinking as complete and collapse it
                if let Some(TimelineEntry::Thinking { complete, expanded, .. }) = self.timeline.last_mut() {
                    *complete = true;
                    *expanded = false; // auto-collapse when done
                }
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            TuiEvent::ToolStart { id, name, preview } => {
                self.auto_scroll = true;
                self.timeline.push(TimelineEntry::ToolCall {
                    id,
                    name,
                    preview,
                    output: String::new(),
                    done: false,
                    expanded: true, // expanded while running so user can see progress
                    exit_code: None,
                });
                self.timeline_cursor = self.timeline.len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
                self.trim_timeline();
            }

            TuiEvent::ToolProgress { id, name: _, progress } => {
                if let Some(TimelineEntry::ToolCall { output, .. }) = self.timeline.iter_mut()
                    .rev()
                    .find(|e| matches!(e, TimelineEntry::ToolCall { id: tid, .. } if tid == &id))
                {
                    output.push_str(&progress);
                    if output.len() > 4096 {
                        *output = output[output.len() - 4096..].to_string();
                    }
                    // Mark dirty for incremental rebuild (streaming tool output)
                    self.lines_dirty = true;
                }
            }

            TuiEvent::ToolComplete { id, name: _, summary, exit_code } => {
                if let Some(entry) = self.timeline.iter_mut()
                    .rev()
                    .find(|e| matches!(e, TimelineEntry::ToolCall { id: tid, .. } if tid == &id))
                {
                    if let TimelineEntry::ToolCall { output, done, expanded, exit_code: ec, .. } = entry {
                        *output = summary;
                        *done = true;
                        *expanded = false; // auto-collapse when done
                        *ec = exit_code;
                    }
                }
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            TuiEvent::TokenUsage { input, output, cache_create, cache_read } => {
                self.input_tokens = input;
                self.output_tokens = output;
                self.token_count = input + output + cache_create + cache_read;
                // Compute per-turn deltas from pre-turn snapshots
                self.turn_input_tokens = input.saturating_sub(self.pre_turn_input);
                self.turn_output_tokens = output.saturating_sub(self.pre_turn_output);
            }

            TuiEvent::TurnStarted => {
                self.is_loading = true;
                self.turn_active = true;
                self.streaming_received = false;
                self.thinking_id_counter = 0;
                // Capture pre-turn snapshots for delta computation
                self.pre_turn_input = self.input_tokens;
                self.pre_turn_output = self.output_tokens;
                self.turn_input_tokens = 0;
                self.turn_output_tokens = 0;
                self.msg_version = self.msg_version.wrapping_add(1);
            }

            TuiEvent::TurnComplete { assistant_text, iterations: _ } => {
                self.is_loading = false;
                self.turn_active = false;
                // Collapse all thinking and tool entries from this turn
                for entry in &mut self.timeline {
                    match entry {
                        TimelineEntry::Thinking { expanded, .. } => *expanded = false,
                        TimelineEntry::ToolCall { expanded, .. } => *expanded = false,
                        _ => {}
                    }
                }
                // If we didn't receive any streaming text, use the fallback
                if !assistant_text.is_empty() && !self.streaming_received {
                    self.timeline.push(TimelineEntry::Message {
                        role: "assistant".into(),
                        content: assistant_text,
                        timestamp: App::format_timestamp(),
                    });
                }
                // Add the "✓ Done" marker
                self.timeline.push(TimelineEntry::Message {
                    role: "assistant".into(),
                    content: "✓ Done".into(),
                    timestamp: App::format_timestamp(),
                });
                self.timeline_cursor = self.timeline.len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
                self.trim_timeline();
            }

            TuiEvent::TurnError { error } => {
                self.is_loading = false;
                self.turn_active = false;
                self.timeline.push(TimelineEntry::Message {
                    role: "system".into(),
                    content: format!("Error: {error}"),
                    timestamp: App::format_timestamp(),
                });
                self.timeline_cursor = self.timeline.len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
                self.trim_timeline();
            }

            TuiEvent::CompactionNotice { removed_count } => {
                self.compaction_count += 1;
                self.timeline.push(TimelineEntry::Message {
                    role: "system".into(),
                    content: format!("Compacted {removed_count} earlier messages to save context."),
                    timestamp: App::format_timestamp(),
                });
                self.timeline_cursor = self.timeline.len().saturating_sub(1);
                self.msg_version = self.msg_version.wrapping_add(1);
                self.trim_timeline();
            }
        }
    }
}
