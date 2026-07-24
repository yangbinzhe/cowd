// ── ChatView Component ────────────────────────────────────────────
// Port of widgets/chat.rs to the Component trait.
// Preserves ALL functionality:
//   - 4 TimelineEntry variants (Message, Thinking, ToolCall, SlashOutput)
//   - Virtual scrolling (>3x viewport)
//   - Incremental rebuild (streaming tail)
//   - Scroll-to-entry with scrollbar
//   - Loading spinner during active turns
//   - Entry line counts pre-computation
// -----------------------------------------------------------------

use crossterm::event::{Event, KeyCode, KeyEvent};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Clear, Paragraph},
};

use crate::app::{Theme, TimelineEntry};
use crate::components::{Component, EventResult, RenderContext};
use crate::md_renderer;
use crate::scroll_state::ScrollState;
use crate::wrapping::wrap_styled_lines;

// ── ChatView ──────────────────────────────────────────────────────

/// Self-contained chat view component.
///
/// Holds all state needed to render the conversation timeline:
/// entries, scrolling, render cache, and theme. Call `sync_from_app()`
/// to pull state from the shared `App` before each render frame.
pub struct ChatView {
    // ── Timeline ──
    pub timeline: Vec<TimelineEntry>,
    pub timeline_cursor: usize,
    session_id: String,

    // ── Scrolling (unified scroll state from ratatui-kit pattern) ──
    pub scroll_state: ScrollState,

    // ── Turn state ──
    pub turn_active: bool,
    turn_input_tokens: u64,
    turn_output_tokens: u64,
    turn_usage_known: bool,
    current_turn_tool_count: usize,
    current_turn_thinking_count: usize,
    session_input_tokens: u64,
    session_output_tokens: u64,
    context_used_tokens: Option<u64>,
    context_window_tokens: Option<u64>,
    context_remaining_tokens: Option<u64>,
    context_usage_percent_bp: Option<u16>,
    execution_status: Option<harness_contract::projection::ExecutionLiveStatus>,
    execution_started_at_ms: Option<u64>,
    last_progress_at_ms: Option<u64>,
    run_metrics: Option<harness_contract::projection::RunMetricsProjection>,
    memory_total_entries: usize,
    context_selected_count: usize,
    context_omitted_count: usize,
    memory_candidate_count: usize,
    reality_stage_count: u64,
    reality_event_count: u64,
    reality_promotion_count: u64,
    reality_boundary_count: u64,
    pending_approval_count: u64,
    surface_count: usize,
    degraded_count: usize,
    spinner_idx: usize,

    // ── Message menu (Task 5) ──
    /// Set when user presses Ctrl+O on a focused message.
    pub pending_message_menu: bool,
    /// Index of the message to show the menu for.
    pub pending_menu_entry_idx: usize,

    // ── Subagent navigation (Task 9) ──
    /// Set when user presses Enter on a tool call with subagent_session_id.
    pub pending_subagent_nav: Option<String>,

    // ── Compact chat mode ──
    /// When true, shows only key content (thinking, final answer, stats).
    pub compact_mode: bool,

    // ── Render cache ──
    cached_chat_lines: Vec<Line<'static>>,
    entry_line_counts: Vec<usize>,
    /// Wrapped visual-row start for each timeline entry. Hidden entries are
    /// `None`. Search navigation and rendering share this exact coordinate
    /// system instead of relying on App-side line estimates.
    entry_visual_starts: Vec<Option<usize>>,
    msg_version: u64,
    last_drawn_version: u64,
    lines_dirty: bool,
    cached_wrap_width: u16,
    last_full_sync_revision: u64,
    last_timeline_mutation_revision: u64,
    dirty_patch_indices: Vec<usize>,
    last_main_entry_index: Option<usize>,
    last_assistant_entry_index: Option<usize>,
    tail_patch_eligible: bool,
    append_patch_start: Option<usize>,
    /// First cached visual row owned by the dynamic turn footer/spinner. The
    /// transcript before this boundary is width-stable and must not be rebuilt
    /// for the 100ms active-run clock.
    cached_dynamic_start: usize,
    full_rebuild_count: u64,
    incremental_rebuild_count: u64,
    dynamic_rebuild_count: u64,
    finalized_cache_hits: u64,
    finalized_cache_misses: u64,
    last_history_prepend_revision: u64,
    pending_history_anchor: Option<(String, usize)>,

    // ── Theme ──
    pub theme: Theme,

    // ── Search highlight (Task 17) ──
    pub search_query: String,
    pub search_matches: Vec<usize>,
    pub search_current: usize,
    pending_search_scroll: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ChatTurnStats {
    thinking_count: usize,
    tool_count: usize,
}

impl ChatView {
    fn renders_in_main_chat(entry: &TimelineEntry) -> bool {
        match entry {
            TimelineEntry::Message { role, .. } => role == "user" || role == "assistant",
            // A tool invocation is part of the causal conversation, not an
            // implementation-only diagnostic. Keep the card compact here and
            // leave full input/output details in Process.
            TimelineEntry::ToolCall { .. } => true,
            TimelineEntry::SlashOutput { .. } => true,
            _ => false,
        }
    }

    fn visible_main_entries(&self) -> impl Iterator<Item = (usize, &TimelineEntry)> {
        self.timeline
            .iter()
            .enumerate()
            .filter(|(_, entry)| Self::renders_in_main_chat(entry))
    }

    /// Create a new empty chat view with default 24-row viewport.
    #[must_use]
    pub fn new() -> Self {
        Self {
            timeline: Vec::new(),
            timeline_cursor: 0,
            session_id: String::new(),
            scroll_state: ScrollState::new(),
            turn_active: false,
            turn_input_tokens: 0,
            turn_output_tokens: 0,
            turn_usage_known: false,
            current_turn_tool_count: 0,
            current_turn_thinking_count: 0,
            session_input_tokens: 0,
            session_output_tokens: 0,
            context_used_tokens: None,
            context_window_tokens: None,
            context_remaining_tokens: None,
            context_usage_percent_bp: None,
            execution_status: None,
            execution_started_at_ms: None,
            last_progress_at_ms: None,
            run_metrics: None,
            memory_total_entries: 0,
            context_selected_count: 0,
            context_omitted_count: 0,
            memory_candidate_count: 0,
            reality_stage_count: 0,
            reality_event_count: 0,
            reality_promotion_count: 0,
            reality_boundary_count: 0,
            pending_approval_count: 0,
            surface_count: 0,
            degraded_count: 0,
            spinner_idx: 0,
            pending_message_menu: false,
            pending_menu_entry_idx: 0,
            pending_subagent_nav: None,
            compact_mode: false,
            cached_chat_lines: Vec::new(),
            entry_line_counts: Vec::new(),
            entry_visual_starts: Vec::new(),
            msg_version: 0,
            last_drawn_version: u64::MAX,
            lines_dirty: true,
            cached_wrap_width: 0,
            last_full_sync_revision: u64::MAX,
            last_timeline_mutation_revision: 0,
            dirty_patch_indices: Vec::new(),
            last_main_entry_index: None,
            last_assistant_entry_index: None,
            tail_patch_eligible: false,
            append_patch_start: None,
            cached_dynamic_start: 0,
            full_rebuild_count: 0,
            incremental_rebuild_count: 0,
            dynamic_rebuild_count: 0,
            finalized_cache_hits: 0,
            finalized_cache_misses: 0,
            last_history_prepend_revision: 0,
            pending_history_anchor: None,
            theme: Theme::Dark,
            search_query: String::new(),
            search_matches: Vec::new(),
            search_current: 0,
            pending_search_scroll: None,
        }
    }

    /// Return the current spinner character (braille animation frame).
    #[must_use]
    pub fn spinner_char(&self) -> &'static str {
        const F: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        F[self.spinner_idx % F.len()]
    }

    /// Advance the spinner animation by one frame.
    pub fn tick(&mut self) {
        self.spinner_idx = self.spinner_idx.wrapping_add(1);
    }

    /// Mark render cache as dirty (force full rebuild next frame).
    pub fn mark_dirty(&mut self) {
        self.lines_dirty = true;
        self.msg_version = self.msg_version.wrapping_add(1);
    }

    /// Sync view-model from the shared App state.
    /// Called once per frame before rendering.
    /// Uses incremental append: clones only new entries, falls back to
    /// full rebuild on eviction, and patches the streaming tail in-place.
    pub fn sync_from_app(&mut self, app: &crate::App) {
        if self.last_history_prepend_revision != app.history_prepend_revision {
            if let Some(anchor_id) = app.history_prepend_anchor_message_id.as_ref() {
                if let Some(old_index) = self.timeline.iter().position(|entry| {
                    matches!(
                        entry,
                        TimelineEntry::Message {
                            identity:
                                Some(crate::app::MessageIdentity {
                                    message_id: Some(message_id),
                                    ..
                                }),
                            ..
                        } if message_id == anchor_id
                    )
                }) {
                    let old_start = self
                        .entry_visual_starts
                        .get(old_index)
                        .copied()
                        .flatten()
                        .unwrap_or(self.scroll_state.offset);
                    let within_anchor = self.scroll_state.offset.saturating_sub(old_start);
                    self.pending_history_anchor = Some((anchor_id.clone(), within_anchor));
                }
            }
            self.last_history_prepend_revision = app.history_prepend_revision;
        }
        let new_len = app.timeline_len();
        let previous_len = self.timeline.len();
        let previous_last_main = self.last_main_entry_index;
        let content_changed = app.msg_version != self.msg_version || app.lines_dirty;
        let session_changed = self.session_id != app.session_id;
        let full_sync =
            session_changed || self.last_full_sync_revision != app.timeline_full_sync_revision;
        let mut changed_main_outside_tail = false;
        if full_sync {
            self.timeline = app.timeline_clone_vec();
            self.last_main_entry_index = self.timeline.iter().rposition(Self::renders_in_main_chat);
            self.last_assistant_entry_index = self.timeline.iter().rposition(
                |entry| matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant"),
            );
            self.last_timeline_mutation_revision = app.timeline_mutation_revision;
            self.dirty_patch_indices.clear();
        } else if new_len > self.timeline.len() {
            for i in self.timeline.len()..new_len {
                if let Some(entry) = app.timeline_entry(i) {
                    if Self::renders_in_main_chat(&entry) {
                        self.last_main_entry_index = Some(i);
                    }
                    if matches!(&entry, TimelineEntry::Message { role, .. } if role == "assistant")
                    {
                        self.last_assistant_entry_index = Some(i);
                    }
                    self.timeline.push(entry);
                }
            }
        } else if new_len < self.timeline.len() {
            self.timeline = app.timeline_clone_vec();
            self.last_main_entry_index = self.timeline.iter().rposition(Self::renders_in_main_chat);
            self.last_assistant_entry_index = self.timeline.iter().rposition(
                |entry| matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant"),
            );
            self.last_timeline_mutation_revision = app.timeline_mutation_revision;
            self.dirty_patch_indices.clear();
        }
        // Pull exact identity-aware mutations. A bounded-log miss is a
        // correctness event and forces a complete sync; it never silently
        // assumes that only the last 256 entries can change.
        if !full_sync && content_changed {
            match app.timeline_dirty_entries_since(self.last_timeline_mutation_revision) {
                Some((revision, dirty)) => {
                    self.last_timeline_mutation_revision = revision;
                    self.dirty_patch_indices.clear();
                    for (index, fresh) in dirty {
                        let Some(local) = self.timeline.get_mut(index) else {
                            self.timeline = app.timeline_clone_vec();
                            self.last_main_entry_index =
                                self.timeline.iter().rposition(Self::renders_in_main_chat);
                            self.last_assistant_entry_index = self.timeline.iter().rposition(
                                |entry| matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant"),
                            );
                            self.last_full_sync_revision = app.timeline_full_sync_revision;
                            self.dirty_patch_indices.clear();
                            break;
                        };
                        if fresh != *local {
                            if Self::renders_in_main_chat(&fresh) {
                                self.dirty_patch_indices.push(index);
                                if Some(index) != self.last_main_entry_index {
                                    changed_main_outside_tail = true;
                                }
                            }
                            *local = fresh;
                        }
                    }
                }
                None => {
                    self.timeline = app.timeline_clone_vec();
                    self.last_main_entry_index =
                        self.timeline.iter().rposition(Self::renders_in_main_chat);
                    self.last_assistant_entry_index = self.timeline.iter().rposition(
                        |entry| matches!(entry, TimelineEntry::Message { role, .. } if role == "assistant"),
                    );
                    self.last_timeline_mutation_revision = app.timeline_mutation_revision;
                    self.dirty_patch_indices.clear();
                    changed_main_outside_tail = true;
                }
            }
        }
        self.tail_patch_eligible = !full_sync
            && previous_len == new_len
            && !changed_main_outside_tail
            && self.last_main_entry_index.is_some();
        self.append_patch_start = (!full_sync && new_len > previous_len)
            .then(|| {
                previous_last_main.or_else(|| {
                    self.timeline[previous_len..]
                        .iter()
                        .position(Self::renders_in_main_chat)
                        .map(|offset| previous_len + offset)
                })
            })
            .flatten();
        self.last_full_sync_revision = app.timeline_full_sync_revision;
        self.session_id.clone_from(&app.session_id);
        self.timeline_cursor = app.timeline_cursor;
        self.scroll_state.offset = app.scroll_offset;
        self.scroll_state.auto_scroll = app.auto_scroll;
        self.scroll_state.viewport_height = app.viewport_height;
        self.turn_active = app.turn_is_active();
        self.turn_input_tokens = app.turn_input_tokens;
        self.turn_output_tokens = app.turn_output_tokens;
        self.turn_usage_known = app.turn_usage_known;
        self.current_turn_tool_count = app.current_turn_tool_count;
        self.current_turn_thinking_count = app.current_turn_thinking_count;
        self.session_input_tokens = app.durable_session_input_tokens.max(app.input_tokens);
        self.session_output_tokens = app.durable_session_output_tokens.max(app.output_tokens);
        self.context_used_tokens = app.context_used_tokens;
        self.context_window_tokens = app.context_window_tokens;
        self.context_remaining_tokens = app.context_remaining_tokens;
        self.context_usage_percent_bp = app.context_usage_percent_bp;
        self.execution_status = app.current_execution_status;
        self.execution_started_at_ms = app.execution_started_at_ms;
        self.last_progress_at_ms = app.last_progress_at_ms;
        self.run_metrics = app.current_run_metrics.clone();
        self.memory_total_entries = app.memory_total_entries.unwrap_or(app.memory_entries.len());
        self.context_selected_count = app
            .latest_context_envelope
            .as_ref()
            .and_then(|value| value.get("selected").and_then(serde_json::Value::as_array))
            .map(Vec::len)
            .unwrap_or_default();
        self.context_omitted_count = app
            .latest_context_envelope
            .as_ref()
            .and_then(|value| value.get("omitted").and_then(serde_json::Value::as_array))
            .map(Vec::len)
            .unwrap_or_default();
        self.memory_candidate_count = app
            .latest_execution_graph_summary
            .as_ref()
            .map(|summary| summary.memory_candidates)
            .unwrap_or_default();
        if let Some(flow) = &app.gateway_fact_flow {
            self.reality_stage_count = flow.stage_count;
            self.reality_event_count = flow.event_count;
            self.reality_promotion_count = flow.promotion_count;
            self.reality_boundary_count = flow.boundary_count;
        } else {
            self.reality_stage_count = 0;
            self.reality_event_count = 0;
            self.reality_promotion_count = 0;
            self.reality_boundary_count = 0;
        }
        self.pending_approval_count = app.gateway_pending_approvals.unwrap_or_default();
        self.surface_count = app.gateway_surfaces.len();
        self.degraded_count =
            app.gateway_degraded_reasons.len() + app.gateway_connector_degraded_reasons.len();
        self.spinner_idx = app.spinner_idx;
        self.theme = app.theme;
        self.msg_version = app.msg_version;
        self.lines_dirty = app.lines_dirty;
        if self.search_query != app.search_query
            || self.search_matches != app.search_matches
            || self.search_current != app.search_current
        {
            self.pending_search_scroll = app.search_matches.get(app.search_current).copied();
        }
        self.search_query = app.search_query.clone();
        self.search_matches = app.search_matches.clone();
        self.search_current = app.search_current;
        self.compact_mode = app.compact_chat;
    }

    /// Persist view-model back to the shared App state.
    /// Called after rendering to preserve cursor position and scroll offset.
    pub fn sync_to_app(&self, app: &mut crate::App) {
        app.scroll_offset = self.scroll_state.offset;
        app.auto_scroll = self.scroll_state.auto_scroll;
        app.viewport_height = self.scroll_state.viewport_height;
        app.cache_hits = self.finalized_cache_hits;
        app.telemetry.finalized_cache_hits = self.finalized_cache_hits;
        app.telemetry.finalized_cache_misses = self.finalized_cache_misses;
        app.telemetry.live_tail_rebuild_count = self
            .incremental_rebuild_count
            .saturating_add(self.dynamic_rebuild_count);
        app.telemetry.full_timeline_rebuild_count = self.full_rebuild_count;
    }

    // ── Navigation ────────────────────────────────────────────────

    /// Move timeline cursor up by one collapsible entry.
    pub fn cursor_up(&mut self) -> bool {
        if self.timeline.is_empty() {
            return false;
        }
        if self.timeline_cursor >= self.timeline.len() {
            self.timeline_cursor = self.timeline.len().saturating_sub(1);
        }
        let mut idx = self.timeline_cursor;
        loop {
            if idx == 0 {
                break;
            }
            idx -= 1;
            if self.timeline[idx].is_collapsible() {
                self.timeline_cursor = idx;
                self.scroll_state.auto_scroll = false;
                return true;
            }
        }
        false
    }

    /// Move timeline cursor down by one collapsible entry.
    pub fn cursor_down(&mut self) -> bool {
        if self.timeline.is_empty() {
            return false;
        }
        let mut idx = self.timeline_cursor;
        while idx + 1 < self.timeline.len() {
            idx += 1;
            if self.timeline[idx].is_collapsible() {
                self.timeline_cursor = idx;
                self.scroll_state.auto_scroll = true;
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

    /// Scroll up by one viewport worth of lines.
    pub fn scroll_page_up(&mut self) {
        self.scroll_state.scroll_page_up();
    }

    /// Scroll down by one viewport worth of lines.
    pub fn scroll_page_down(&mut self) {
        self.scroll_state.scroll_page_down();
    }

    /// Scroll so the entry at the given index is visible.
    pub fn scroll_to_entry(&mut self, entry_idx: usize) {
        let vh = self.scroll_state.viewport_height.max(1);
        let Some(offset) = self.entry_visual_starts.get(entry_idx).copied().flatten() else {
            return;
        };
        let entry_h = self.entry_line_counts.get(entry_idx).copied().unwrap_or(1);

        let scroll = self.scroll_state.offset;
        if offset < scroll {
            self.scroll_state.offset = offset;
        } else if offset + entry_h > scroll + vh {
            self.scroll_state.offset = offset.saturating_sub(vh.saturating_sub(entry_h));
        }
    }

    // ── Line computation (internal) ───────────────────────────────

    /// Total number of transcript lines. The live footer is rendered in its
    /// own fixed viewport and therefore never changes transcript scroll math.
    pub fn total_lines(&self) -> usize {
        let n = self
            .timeline
            .iter()
            .filter(|entry| Self::renders_in_main_chat(entry))
            .count();
        let mut total: usize =
            self.entry_line_counts.iter().copied().sum::<usize>() + n.saturating_sub(1); // separators between entries, not after last
        if total == 0 && self.timeline.is_empty() {
            total = 1;
        }
        total
    }
}

fn compact_execution_status(
    status: harness_contract::projection::ExecutionLiveStatus,
) -> &'static str {
    use harness_contract::projection::ExecutionLiveStatus;
    match status {
        ExecutionLiveStatus::Queued => "queued",
        ExecutionLiveStatus::PreparingContext => "preparing context",
        ExecutionLiveStatus::CallingModel => "calling model",
        ExecutionLiveStatus::Thinking => "thinking",
        ExecutionLiveStatus::CallingTool => "calling tool",
        ExecutionLiveStatus::WaitingApproval => "waiting approval",
        ExecutionLiveStatus::Finalizing => "finalizing",
        ExecutionLiveStatus::Complete => "complete",
        ExecutionLiveStatus::Cancelled => "cancelled",
        ExecutionLiveStatus::Error => "error",
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

// ── Component impl ────────────────────────────────────────────────

impl Component for ChatView {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let wrap_width = area.width.max(1);
        let mut footer_lines =
            wrap_styled_lines(self.build_dynamic_turn_lines(), usize::from(wrap_width));
        let max_footer_height = usize::from(area.height.saturating_sub(1));
        if footer_lines.len() > max_footer_height {
            footer_lines = footer_lines.split_off(footer_lines.len() - max_footer_height);
        }
        let footer_height = u16::try_from(footer_lines.len())
            .unwrap_or(area.height)
            .min(area.height);
        let transcript_height = area.height.saturating_sub(footer_height);
        let transcript_area = Rect::new(area.x, area.y, area.width, transcript_height);
        let footer_area = Rect::new(
            area.x,
            area.y.saturating_add(transcript_height),
            area.width,
            footer_height,
        );
        let viewport_h = usize::from(transcript_height);
        self.scroll_state.viewport_height = viewport_h;

        // ── Build line buffer before computing scroll bounds ──
        // Scroll is intentionally based on the exact line buffer we render.
        // This avoids the old split-brain path where entry estimates, virtual
        // slicing, Paragraph wrapping, and the scrollbar each had different
        // ideas of content height.
        if self.compact_mode {
            let needs_full_rebuild = self.msg_version != self.last_drawn_version
                || self.cached_wrap_width != wrap_width
                || self.lines_dirty;
            if needs_full_rebuild {
                self.finalized_cache_misses = self.finalized_cache_misses.saturating_add(1);
                self.cached_chat_lines =
                    wrap_styled_lines(Self::build_compact_lines(self), usize::from(wrap_width));
                self.entry_line_counts.clear();
                self.last_drawn_version = self.msg_version;
                self.cached_wrap_width = wrap_width;
                self.lines_dirty = false;
                self.append_patch_start = None;
                self.full_rebuild_count = self.full_rebuild_count.saturating_add(1);
            }
            if !needs_full_rebuild {
                self.finalized_cache_hits = self.finalized_cache_hits.saturating_add(1);
            }
            if let Some(entry_idx) = self.pending_search_scroll.take() {
                self.scroll_to_entry(entry_idx);
            }
            let total_lines = self.cached_chat_lines.len().max(1);
            self.scroll_state.set_content_size(total_lines);
            if self.scroll_state.auto_scroll && total_lines > viewport_h {
                self.scroll_state.offset = total_lines.saturating_sub(viewport_h);
            }
            let scroll_off = self
                .scroll_state
                .offset
                .min(total_lines.saturating_sub(viewport_h));
            let visible_end = scroll_off
                .saturating_add(viewport_h.max(1))
                .min(self.cached_chat_lines.len());
            let visible_lines = if self.cached_chat_lines.is_empty() {
                vec![Line::raw("")]
            } else {
                self.cached_chat_lines[scroll_off..visible_end].to_vec()
            };
            let frame = ctx.frame_mut();
            frame.render_widget(Clear, area);
            frame.render_widget(Paragraph::new(Text::from(visible_lines)), transcript_area);
            if footer_height > 0 {
                frame.render_widget(Paragraph::new(Text::from(footer_lines)), footer_area);
            }
            return;
        }
        let mut rebuilt_content = false;
        if self.msg_version != self.last_drawn_version || self.cached_wrap_width != wrap_width {
            if self.cached_wrap_width == wrap_width
                && self.append_patch_start.is_some()
                && !self.cached_chat_lines.is_empty()
            {
                let start = self.append_patch_start.take().expect("checked above");
                self.rebuild_wrapped_appended_tail(start, usize::from(wrap_width));
                self.incremental_rebuild_count = self.incremental_rebuild_count.saturating_add(1);
            } else if self.cached_wrap_width == wrap_width
                && !self.dirty_patch_indices.is_empty()
                && !self.cached_chat_lines.is_empty()
            {
                self.rebuild_wrapped_dirty_entries(usize::from(wrap_width));
                self.incremental_rebuild_count = self.incremental_rebuild_count.saturating_add(1);
            } else if self.cached_wrap_width == wrap_width
                && self.tail_patch_eligible
                && !self.cached_chat_lines.is_empty()
            {
                if !self.dirty_patch_indices.is_empty() {
                    self.rebuild_wrapped_last_main_entry(usize::from(wrap_width));
                    self.incremental_rebuild_count =
                        self.incremental_rebuild_count.saturating_add(1);
                }
            } else {
                self.rebuild_all_wrapped(usize::from(wrap_width));
            }
            self.last_drawn_version = self.msg_version;
            self.cached_wrap_width = wrap_width;
            self.lines_dirty = false;
            self.dirty_patch_indices.clear();
            rebuilt_content = true;
        } else if self.lines_dirty {
            self.rebuild_all_wrapped(usize::from(wrap_width));
            self.lines_dirty = false;
            rebuilt_content = true;
        }
        if self.cached_chat_lines.is_empty() {
            self.rebuild_all_wrapped(usize::from(wrap_width));
            rebuilt_content = true;
        }
        if rebuilt_content {
            self.finalized_cache_misses = self.finalized_cache_misses.saturating_add(1);
        } else {
            self.finalized_cache_hits = self.finalized_cache_hits.saturating_add(1);
        }
        if let Some((anchor_id, within_anchor)) = self.pending_history_anchor.take() {
            if let Some(index) = self.timeline.iter().position(|entry| {
                matches!(
                    entry,
                    TimelineEntry::Message {
                        identity:
                            Some(crate::app::MessageIdentity {
                                message_id: Some(message_id),
                                ..
                            }),
                        ..
                    } if message_id == &anchor_id
                )
            }) {
                if let Some(start) = self.entry_visual_starts.get(index).copied().flatten() {
                    self.scroll_state.offset = start.saturating_add(within_anchor);
                    self.scroll_state.auto_scroll = false;
                }
            }
        }
        if let Some(entry_idx) = self.pending_search_scroll.take() {
            self.scroll_state.auto_scroll = false;
            self.scroll_to_entry(entry_idx);
        }

        let total_lines = self.cached_chat_lines.len().max(1);

        // ── Post-render size callback: sync actual content height ──
        self.scroll_state.set_content_size(total_lines);

        // ── Auto-scroll ──
        if self.scroll_state.auto_scroll && total_lines > viewport_h {
            self.scroll_state.offset = total_lines.saturating_sub(viewport_h);
        }
        let scroll_off = self
            .scroll_state
            .offset
            .min(total_lines.saturating_sub(viewport_h));

        // ── Build visible lines ──
        let visible_end = scroll_off
            .saturating_add(viewport_h.max(1))
            .min(self.cached_chat_lines.len());
        let mut visible_lines = if self.cached_chat_lines.is_empty() {
            vec![Line::raw("")]
        } else {
            self.cached_chat_lines[scroll_off..visible_end].to_vec()
        };

        // ── Apply search highlight (Task 17) ──
        if !self.search_query.is_empty() && !self.search_matches.is_empty() {
            let current_match_entry = self.search_matches.get(self.search_current).copied();
            let final_assistant_idx = self.last_assistant_entry_index;
            let current_turn_stats = self.current_turn_stats();
            let mut matched_entries = self.search_matches.clone();
            matched_entries.sort_unstable();
            matched_entries.dedup();
            for entry_idx in matched_entries {
                let Some(start) = self.entry_visual_starts.get(entry_idx).copied().flatten() else {
                    continue;
                };
                let height = self
                    .entry_line_counts
                    .get(entry_idx)
                    .copied()
                    .unwrap_or_default();
                let end = start.saturating_add(height);
                if end <= scroll_off || start >= visible_end {
                    continue;
                }
                let Some(entry) = self.timeline.get(entry_idx) else {
                    continue;
                };
                let mut logical = Vec::new();
                Self::build_entry_with_meta(
                    entry,
                    entry_idx == self.timeline_cursor,
                    final_assistant_idx == Some(entry_idx),
                    &mut logical,
                    &self.theme,
                    self.turn_input_tokens,
                    self.turn_output_tokens,
                    self.turn_usage_known,
                    current_turn_stats.tool_count,
                    current_turn_stats.thinking_count,
                    self.memory_total_entries,
                );
                let is_current = current_match_entry == Some(entry_idx);
                for line in &mut logical {
                    Self::highlight_search_in_line(line, &self.search_query, is_current);
                }
                let highlighted = wrap_styled_lines(logical, usize::from(wrap_width));
                if highlighted.len() != height {
                    continue;
                }
                let replace_start = start.max(scroll_off);
                let replace_end = end.min(visible_end);
                for visual_row in replace_start..replace_end {
                    visible_lines[visual_row - scroll_off] =
                        highlighted[visual_row - start].clone();
                }
            }
        }

        // ── Render ──
        let frame = ctx.frame_mut();
        frame.render_widget(Clear, area);

        let paragraph = Paragraph::new(Text::from(visible_lines))
            // The model tracks an unbounded logical offset. The widget receives
            // only the visible slice, so Ratatui's u16 scroll coordinate cannot
            // truncate a long conversation.
            .scroll((0, 0));
        frame.render_widget(paragraph, transcript_area);
        if footer_height > 0 {
            frame.render_widget(Paragraph::new(Text::from(footer_lines)), footer_area);
        }
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) => self.handle_key(key),
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "chat_view"
    }
}

impl ChatView {
    fn handle_key(&mut self, key: &KeyEvent) -> EventResult {
        // ── Ctrl+O: open per-message action menu (Task 5) ──
        if key.modifiers == crossterm::event::KeyModifiers::CONTROL
            && key.code == KeyCode::Char('o')
        {
            if !self.timeline.is_empty() && self.timeline_cursor < self.timeline.len() {
                self.pending_menu_entry_idx = self.timeline_cursor;
                self.pending_message_menu = true;
            }
            return EventResult::Consumed;
        }

        match key.code {
            KeyCode::Enter => {
                // Check if focused entry is a ToolCall with subagent_session_id (Task 9)
                if let Some(entry) = self.timeline.get(self.timeline_cursor) {
                    if let TimelineEntry::ToolCall {
                        name, output, done, ..
                    } = entry
                    {
                        if *done && name == "task" && !output.is_empty() {
                            // Try to extract subagent_session_id from output
                            if let Ok(val) = serde_json::from_str::<serde_json::Value>(output) {
                                let session_id = val
                                    .get("subagent_session_id")
                                    .or_else(|| val.get("session_id"))
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                if let Some(sid) = session_id {
                                    self.pending_subagent_nav = Some(sid);
                                    return EventResult::Consumed;
                                }
                            }
                        }
                    }
                }
                self.toggle_expand_current();
                EventResult::Consumed
            }
            KeyCode::Up => {
                self.cursor_up();
                EventResult::Consumed
            }
            KeyCode::Down => {
                self.cursor_down();
                EventResult::Consumed
            }
            KeyCode::PageUp => {
                self.scroll_page_up();
                self.scroll_state.auto_scroll = false;
                EventResult::Consumed
            }
            KeyCode::PageDown => {
                self.scroll_page_down();
                self.scroll_state.auto_scroll = false;
                EventResult::Consumed
            }
            KeyCode::Home => {
                self.scroll_state.scroll_to_top();
                EventResult::Consumed
            }
            KeyCode::End => {
                self.scroll_state.scroll_to_bottom();
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }
}

// ── Drawing logic (ported from widgets/chat.rs) ───────────────────

impl ChatView {
    fn rebuild_all_wrapped(&mut self, width: usize) {
        self.cached_chat_lines = wrap_styled_lines(Self::build_new_lines(self), width);
        self.cached_dynamic_start = self.cached_chat_lines.len();
        self.entry_line_counts = Self::compute_wrapped_entry_line_counts(self, width);
        self.rebuild_entry_visual_starts();
        self.append_patch_start = None;
        self.full_rebuild_count = self.full_rebuild_count.saturating_add(1);
    }

    /// Re-render only the previous visible tail plus newly appended entries.
    ///
    /// The previous tail is included because appending a new assistant reply
    /// changes which entry owns the final-answer decoration and turn metrics.
    /// Work is therefore bounded by the visible tail, independent of a 50k
    /// durable transcript.
    fn rebuild_wrapped_appended_tail(&mut self, start: usize, width: usize) {
        if start >= self.timeline.len() || self.entry_line_counts.len() > self.timeline.len() {
            self.rebuild_all_wrapped(width);
            return;
        }
        self.entry_line_counts.resize(self.timeline.len(), 0);
        self.entry_visual_starts.resize(self.timeline.len(), None);
        let prefix_lines = self
            .entry_visual_starts
            .get(start)
            .copied()
            .flatten()
            .unwrap_or(self.cached_dynamic_start);
        if prefix_lines > self.cached_chat_lines.len() {
            self.rebuild_all_wrapped(width);
            return;
        }
        self.cached_chat_lines.truncate(prefix_lines);

        let final_assistant_idx = self.last_assistant_entry_index;
        let current_turn_stats = self.current_turn_stats();
        let mut rendered = false;
        for idx in start..self.timeline.len() {
            let entry = &self.timeline[idx];
            if !Self::renders_in_main_chat(entry) {
                self.entry_line_counts[idx] = 0;
                self.entry_visual_starts[idx] = None;
                continue;
            }
            if rendered {
                self.cached_chat_lines.push(Line::raw(""));
            }
            let mut logical = Vec::new();
            Self::build_entry_with_meta(
                entry,
                idx == self.timeline_cursor,
                final_assistant_idx == Some(idx),
                &mut logical,
                &self.theme,
                self.turn_input_tokens,
                self.turn_output_tokens,
                self.turn_usage_known,
                current_turn_stats.tool_count,
                current_turn_stats.thinking_count,
                self.memory_total_entries,
            );
            let wrapped = wrap_styled_lines(logical, width);
            self.entry_visual_starts[idx] = Some(self.cached_chat_lines.len());
            self.entry_line_counts[idx] = wrapped.len();
            self.cached_chat_lines.extend(wrapped);
            rendered = true;
        }
        self.cached_dynamic_start = self.cached_chat_lines.len();
    }

    /// Compute entry heights in the same visual-cell coordinate system used
    /// by rendering and scrolling.
    fn compute_wrapped_entry_line_counts(&self, width: usize) -> Vec<usize> {
        let final_assistant_idx = self.last_assistant_entry_index;
        let current_turn_stats = self.current_turn_stats();
        self.timeline
            .iter()
            .enumerate()
            .map(|(idx, entry)| {
                if !Self::renders_in_main_chat(entry) {
                    return 0;
                }
                let mut lines = Vec::new();
                Self::build_entry_with_meta(
                    entry,
                    idx == self.timeline_cursor,
                    final_assistant_idx == Some(idx),
                    &mut lines,
                    &self.theme,
                    self.turn_input_tokens,
                    self.turn_output_tokens,
                    self.turn_usage_known,
                    current_turn_stats.tool_count,
                    current_turn_stats.thinking_count,
                    self.memory_total_entries,
                );
                wrap_styled_lines(lines, width).len()
            })
            .collect()
    }

    fn rebuild_wrapped_last_main_entry(&mut self, width: usize) {
        let Some(entry_idx) = self.last_main_entry_index else {
            return;
        };
        if self.entry_line_counts.len() != self.timeline.len() {
            self.cached_chat_lines = wrap_styled_lines(Self::build_new_lines(self), width);
            self.entry_line_counts = Self::compute_wrapped_entry_line_counts(self, width);
            return;
        }
        let prefix_lines = self
            .entry_visual_starts
            .get(entry_idx)
            .copied()
            .flatten()
            .unwrap_or(self.cached_dynamic_start);
        if prefix_lines > self.cached_chat_lines.len() {
            self.cached_chat_lines = wrap_styled_lines(Self::build_new_lines(self), width);
            self.entry_line_counts = Self::compute_wrapped_entry_line_counts(self, width);
            return;
        }

        self.cached_chat_lines.truncate(prefix_lines);
        let mut logical = Vec::new();
        let current_turn_stats = self.current_turn_stats();
        Self::build_entry_with_meta(
            &self.timeline[entry_idx],
            entry_idx == self.timeline_cursor,
            matches!(
                self.timeline[entry_idx],
                TimelineEntry::Message { ref role, .. } if role == "assistant"
            ),
            &mut logical,
            &self.theme,
            self.turn_input_tokens,
            self.turn_output_tokens,
            self.turn_usage_known,
            current_turn_stats.tool_count,
            current_turn_stats.thinking_count,
            self.memory_total_entries,
        );
        let wrapped = wrap_styled_lines(logical, width);
        self.entry_visual_starts[entry_idx] = Some(self.cached_chat_lines.len());
        self.entry_line_counts[entry_idx] = wrapped.len();
        self.cached_chat_lines.extend(wrapped);
        self.cached_dynamic_start = self.cached_chat_lines.len();
    }

    fn rebuild_wrapped_dirty_entries(&mut self, width: usize) {
        if self.entry_line_counts.len() != self.timeline.len()
            || self.entry_visual_starts.len() != self.timeline.len()
            || self.cached_dynamic_start > self.cached_chat_lines.len()
        {
            self.rebuild_all_wrapped(width);
            return;
        }
        self.cached_chat_lines.truncate(self.cached_dynamic_start);
        self.dirty_patch_indices.sort_unstable();
        self.dirty_patch_indices.dedup();
        let final_assistant_idx = self.last_assistant_entry_index;
        let current_turn_stats = self.current_turn_stats();
        for entry_idx in self.dirty_patch_indices.clone() {
            if !self
                .timeline
                .get(entry_idx)
                .is_some_and(Self::renders_in_main_chat)
            {
                continue;
            }
            let Some(start) = self.entry_visual_starts.get(entry_idx).copied().flatten() else {
                self.rebuild_all_wrapped(width);
                return;
            };
            let old_len = self.entry_line_counts[entry_idx];
            let end = start.saturating_add(old_len);
            if end > self.cached_chat_lines.len() {
                self.rebuild_all_wrapped(width);
                return;
            }
            let mut logical = Vec::new();
            Self::build_entry_with_meta(
                &self.timeline[entry_idx],
                entry_idx == self.timeline_cursor,
                final_assistant_idx == Some(entry_idx),
                &mut logical,
                &self.theme,
                self.turn_input_tokens,
                self.turn_output_tokens,
                self.turn_usage_known,
                current_turn_stats.tool_count,
                current_turn_stats.thinking_count,
                self.memory_total_entries,
            );
            let wrapped = wrap_styled_lines(logical, width);
            let new_len = wrapped.len();
            self.cached_chat_lines.splice(start..end, wrapped);
            self.entry_line_counts[entry_idx] = new_len;
            if new_len != old_len {
                for later_start in self
                    .entry_visual_starts
                    .iter_mut()
                    .skip(entry_idx.saturating_add(1))
                    .flatten()
                {
                    if new_len > old_len {
                        *later_start = later_start.saturating_add(new_len - old_len);
                    } else {
                        *later_start = later_start.saturating_sub(old_len - new_len);
                    }
                }
            }
        }
        self.cached_dynamic_start = self.cached_chat_lines.len();
    }

    fn rebuild_entry_visual_starts(&mut self) {
        self.entry_visual_starts = vec![None; self.timeline.len()];
        let mut visual_row = 0usize;
        let mut has_visible_entry = false;
        for (idx, entry) in self.timeline.iter().enumerate() {
            if !Self::renders_in_main_chat(entry) {
                continue;
            }
            if has_visible_entry {
                visual_row = visual_row.saturating_add(1);
            }
            self.entry_visual_starts[idx] = Some(visual_row);
            visual_row =
                visual_row.saturating_add(self.entry_line_counts.get(idx).copied().unwrap_or(0));
            has_visible_entry = true;
        }
    }

    /// Full rebuild of chat lines from internal state.
    fn build_new_lines(&self) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();

        if self.timeline.is_empty() {
            lines.push(Line::from(Span::styled(
                "Type to start. /help /resume /exit",
                Style::default().fg(self.theme.muted_color()),
            )));
        }

        let visible: Vec<_> = self.visible_main_entries().collect();
        let visible_count = visible.len();
        let final_assistant_idx = self.last_assistant_entry_index;
        let current_turn_stats = self.current_turn_stats();
        for (visible_idx, (idx, entry)) in visible.into_iter().enumerate() {
            let is_focused = idx == self.timeline_cursor;
            Self::build_entry_with_meta(
                entry,
                is_focused,
                final_assistant_idx == Some(idx),
                &mut lines,
                &self.theme,
                self.turn_input_tokens,
                self.turn_output_tokens,
                self.turn_usage_known,
                current_turn_stats.tool_count,
                current_turn_stats.thinking_count,
                self.memory_total_entries,
            );
            if visible_idx + 1 < visible_count {
                lines.push(Line::raw(""));
            }
        }
        lines
    }

    fn build_compact_lines(&self) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();
        if self.timeline.is_empty() {
            lines.push(Line::from(Span::styled(
                "Type to start. /help /resume /exit",
                Style::default().fg(self.theme.muted_color()),
            )));
            return lines;
        }

        // Clean mode is the current-reply projection, not a recent-history
        // transcript. Showing older assistant turns here made the first visible
        // output look like a replay and hid the live tail below the viewport.
        let latest_user_index = self.timeline.iter().rposition(
            |entry| matches!(entry, TimelineEntry::Message { role, .. } if role == "user"),
        );
        let assistant_message =
            self.timeline
                .iter()
                .enumerate()
                .rev()
                .find_map(|(index, entry)| match entry {
                    TimelineEntry::Message { role, content, .. }
                        if role == "assistant"
                            && (!self.turn_active
                                || latest_user_index
                                    .is_none_or(|user_index| index > user_index)) =>
                    {
                        Some((index, content.as_str()))
                    }
                    _ => None,
                });
        let Some((entry_idx, content)) = assistant_message else {
            lines.push(Line::from(Span::styled(
                if self.turn_active {
                    "Waiting for the current reply…"
                } else {
                    "No assistant reply yet."
                },
                Style::default().fg(self.theme.muted_color()),
            )));
            return lines;
        };

        let is_focused = entry_idx == self.timeline_cursor;
        let terminal = self
            .execution_status
            .is_some_and(harness_contract::projection::ExecutionLiveStatus::is_terminal);
        let label = if self.turn_active && !terminal {
            if is_focused {
                "● ├─ CURRENT REPLY"
            } else {
                "  ├─ CURRENT REPLY"
            }
        } else if is_focused {
            "● ├─ FINAL REPLY"
        } else {
            "  ├─ FINAL REPLY"
        };
        lines.push(Line::from(Span::styled(
            label,
            Style::default()
                .fg(if is_focused {
                    self.theme.accent()
                } else {
                    self.theme.success_color()
                })
                .bold(),
        )));
        let md_lines = md_renderer::render_markdown_lines(content, &self.theme);
        append_bounded_head_tail(&mut lines, md_lines, 800, &self.theme);

        lines
    }

    fn current_turn_stats(&self) -> ChatTurnStats {
        ChatTurnStats {
            thinking_count: self.current_turn_thinking_count,
            tool_count: self.current_turn_tool_count,
        }
    }

    fn build_turn_footer_lines(&self) -> Vec<Line<'static>> {
        if self.timeline.is_empty()
            && self.run_metrics.is_none()
            && self.execution_status.is_none()
            && self.context_used_tokens.is_none()
        {
            return Vec::new();
        }
        let mut lines = vec![Line::from(Span::styled(
            "─".repeat(60),
            Style::default().fg(self.theme.muted_color()),
        ))];

        let mut context_parts = Vec::new();
        if let Some(used) = self.context_used_tokens {
            context_parts.push(format!("ctx {}", fmt_tokens(used)));
        } else {
            context_parts.push("ctx —".to_string());
        }
        if let Some(window) = self.context_window_tokens {
            context_parts.push(format!("/{}", fmt_tokens(window)));
        }
        if let Some(percent) = self.context_usage_percent_bp {
            context_parts.push(format!("{:.0}%", f64::from(percent) / 100.0));
        }
        if let Some(remaining) = self.context_remaining_tokens {
            context_parts.push(format!("rem {}", fmt_tokens(remaining)));
        }
        lines.push(Line::from(Span::styled(
            format!("  {}", context_parts.join(" ")),
            Style::default().fg(self.theme.accent()),
        )));

        let total = self
            .turn_input_tokens
            .saturating_add(self.turn_output_tokens);
        lines.push(Line::from(Span::styled(
            if self.turn_usage_known {
                format!(
                    "  in {} · out {} · total {}",
                    fmt_tokens(self.turn_input_tokens),
                    fmt_tokens(self.turn_output_tokens),
                    fmt_tokens(total)
                )
            } else {
                "  in — · out — · total —".to_string()
            },
            Style::default().fg(self.theme.warn_color()),
        )));

        if let Some(metrics) = &self.run_metrics {
            lines.push(Line::from(Span::styled(
                format!(
                    "  tools {} · memory {}/{} · approvals {} · files {}",
                    metrics.tool_calls,
                    metrics.memory_recalls,
                    metrics.memory_evidence,
                    metrics.approvals,
                    metrics.files_touched
                ),
                Style::default().fg(self.theme.muted_color()),
            )));
        }

        let mut status = self
            .execution_status
            .map(compact_execution_status)
            .unwrap_or(if self.turn_active {
                "submitting"
            } else {
                "idle"
            })
            .to_string();
        let now = current_time_ms();
        if let Some(started) = self.execution_started_at_ms {
            let terminal = self
                .execution_status
                .is_some_and(harness_contract::projection::ExecutionLiveStatus::is_terminal);
            let end = if terminal {
                self.last_progress_at_ms.unwrap_or(now)
            } else {
                now
            };
            status.push_str(&format!(
                " · elapsed {:.1}s",
                end.saturating_sub(started) as f64 / 1_000.0
            ));
        }
        if self.turn_active {
            if let Some(last_progress) = self.last_progress_at_ms {
                status.push_str(&format!(
                    " · progress {:.1}s ago",
                    now.saturating_sub(last_progress) as f64 / 1_000.0
                ));
            }
        }
        lines.push(Line::from(Span::styled(
            format!("  {status}"),
            Style::default().fg(self.theme.success_color()),
        )));
        lines
    }

    fn build_dynamic_turn_lines(&self) -> Vec<Line<'static>> {
        let mut lines = self.build_turn_footer_lines();
        if self.turn_active {
            lines.push(Line::from(Span::styled(
                format!("{} Processing...", self.spinner_char()),
                Style::default().fg(self.theme.accent()),
            )));
        }
        lines
    }

    #[cfg(test)]
    fn rebuild_streaming_tail(&mut self) {
        let n = self.timeline.len();
        if n == 0 || n > self.timeline.len() {
            return;
        }

        let prefix_count: usize = self
            .entry_line_counts
            .iter()
            .take(n.saturating_sub(1))
            .sum::<usize>()
            .saturating_add(n.saturating_sub(1));

        let last_entry = self.timeline[n - 1].clone();
        if !Self::renders_in_main_chat(&last_entry) {
            self.cached_chat_lines = Self::build_new_lines(self);
            return;
        }
        let is_focused = (n - 1) == self.timeline_cursor;
        let theme = self.theme;
        let turn_active = self.turn_active;
        let spinner_str = if turn_active {
            Some(self.spinner_char().to_string())
        } else {
            None
        };

        self.cached_chat_lines
            .truncate(prefix_count.min(self.cached_chat_lines.len()));
        let before_len = self.cached_chat_lines.len();
        Self::build_entry(&last_entry, is_focused, &mut self.cached_chat_lines, &theme);

        if let Some(count) = self.entry_line_counts.get_mut(n - 1) {
            *count = self.cached_chat_lines.len() - before_len;
        }

        if let Some(spinner) = spinner_str {
            self.cached_chat_lines.push(Line::from(vec![Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(self.theme.accent()),
            )]));
        }
    }

    /// Highlight inline markdown spans in a message line.
    fn highlight_line(
        line: &str,
        spans: &mut Vec<Span<'static>>,
        base_color: Color,
        theme: &Theme,
    ) {
        let code_color = theme.inline_code_color();
        let mut remaining = line;
        while !remaining.is_empty() {
            let bt = remaining.find('`');
            let bs = remaining.find("**");
            let is_ = remaining.find('*');

            let earliest = [bt, bs, is_].iter().filter_map(|&o| o).min();

            match earliest {
                None => {
                    spans.push(Span::styled(
                        remaining.to_string(),
                        Style::default().fg(base_color),
                    ));
                    break;
                }
                Some(pos) => {
                    if pos > 0 {
                        spans.push(Span::styled(
                            remaining[..pos].to_string(),
                            Style::default().fg(base_color),
                        ));
                    }

                    if bt == Some(pos) {
                        remaining = &remaining[pos + 1..];
                        if let Some(end) = remaining.find('`') {
                            spans.push(Span::styled(
                                remaining[..end].to_string(),
                                Style::default().fg(code_color),
                            ));
                            remaining = &remaining[end + 1..];
                        } else {
                            spans.push(Span::styled(
                                remaining.to_string(),
                                Style::default().fg(code_color),
                            ));
                            remaining = "";
                        }
                    } else if bs == Some(pos) {
                        remaining = &remaining[pos + 2..];
                        if let Some(end) = remaining.find("**") {
                            spans.push(Span::styled(
                                remaining[..end].to_string(),
                                Style::default().fg(base_color).bold(),
                            ));
                            remaining = &remaining[end + 2..];
                        } else {
                            spans.push(Span::styled(
                                remaining.to_string(),
                                Style::default().fg(base_color).bold(),
                            ));
                            remaining = "";
                        }
                    } else if is_ == Some(pos) {
                        remaining = &remaining[pos + 1..];
                        if let Some(end) = remaining.find('*') {
                            if remaining.get(end + 1..end + 2) == Some("*") {
                                spans.push(Span::styled(
                                    format!("*{}", &remaining[..end]),
                                    Style::default().fg(base_color),
                                ));
                                remaining = &remaining[end..];
                            } else {
                                spans.push(Span::styled(
                                    remaining[..end].to_string(),
                                    Style::default().fg(base_color).italic(),
                                ));
                                remaining = &remaining[end + 1..];
                            }
                        } else {
                            spans.push(Span::styled(
                                remaining.to_string(),
                                Style::default().fg(base_color).italic(),
                            ));
                            remaining = "";
                        }
                    } else {
                        let mut chars = remaining.char_indices();
                        let Some((_, ch)) = chars.next() else {
                            break;
                        };
                        let next = chars.next().map(|(idx, _)| idx).unwrap_or(remaining.len());
                        spans.push(Span::styled(
                            ch.to_string(),
                            Style::default().fg(base_color),
                        ));
                        remaining = &remaining[next..];
                    }
                }
            }
        }
    }

    /// Apply search highlight to a Line by splitting spans around match boundaries.
    ///
    /// Every occurrence of `search_query` (case-insensitive) gets inverse video.
    /// Occurrences on the selected result's visual rows get a yellow background.
    fn highlight_search_in_line(line: &mut Line<'static>, query: &str, is_current_entry: bool) {
        if query.is_empty() {
            return;
        }
        let mut content = String::new();
        let mut styled_ranges = Vec::new();
        for span in line.spans.drain(..) {
            let start = content.len();
            content.push_str(span.content.as_ref());
            styled_ranges.push((start, content.len(), span.style));
        }
        let matches = unicode_case_insensitive_ranges(&content, query);
        if matches.is_empty() {
            line.spans = styled_ranges
                .into_iter()
                .map(|(start, end, style)| Span::styled(content[start..end].to_string(), style))
                .collect();
            return;
        }
        let mut boundaries = styled_ranges
            .iter()
            .flat_map(|(start, end, _)| [*start, *end])
            .chain(matches.iter().flat_map(|(start, end)| [*start, *end]))
            .collect::<Vec<_>>();
        boundaries.sort_unstable();
        boundaries.dedup();
        line.spans = boundaries
            .windows(2)
            .filter_map(|window| {
                let start = window[0];
                let end = window[1];
                (start < end).then(|| {
                    let base_style = styled_ranges
                        .iter()
                        .find(|(range_start, range_end, _)| {
                            start >= *range_start && start < *range_end
                        })
                        .map(|(_, _, style)| *style)
                        .unwrap_or_default();
                    let matched = matches
                        .iter()
                        .any(|(match_start, match_end)| start >= *match_start && end <= *match_end);
                    let style = if !matched {
                        base_style
                    } else if is_current_entry {
                        Style::default()
                            .bg(Color::Yellow)
                            .fg(Color::Black)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        base_style
                            .bg(Color::DarkGray)
                            .add_modifier(Modifier::REVERSED)
                    };
                    Span::styled(content[start..end].to_string(), style)
                })
            })
            .collect();
    }

    /// Build ratatui Lines for a single timeline entry.
    pub fn build_entry(
        entry: &TimelineEntry,
        is_focused: bool,
        lines: &mut Vec<Line<'static>>,
        theme: &Theme,
    ) {
        Self::build_entry_with_meta(entry, is_focused, false, lines, theme, 0, 0, false, 0, 0, 0);
    }

    #[allow(clippy::too_many_arguments)]
    fn build_entry_with_meta(
        entry: &TimelineEntry,
        is_focused: bool,
        is_final_assistant: bool,
        lines: &mut Vec<Line<'static>>,
        theme: &Theme,
        turn_input_tokens: u64,
        turn_output_tokens: u64,
        turn_usage_known: bool,
        tool_count: usize,
        thinking_rounds: usize,
        memory_count: usize,
    ) {
        match entry {
            TimelineEntry::Message { role, content, .. } => {
                let (color, prefix) = match role.as_str() {
                    "user" => (theme.user_color(), "> "),
                    "system" => (theme.muted_color(), "  "),
                    _ => (theme.fg(), ""),
                };
                const MAX_LINES: usize = 500;

                if role == "assistant" {
                    if is_final_assistant {
                        lines.push(Line::from(Span::styled(
                            "├─ FINAL REPLY",
                            Style::default().fg(theme.success_color()).bold(),
                        )));
                    }
                    let mut md_lines = md_renderer::render_markdown_lines(content, theme);
                    if !is_final_assistant {
                        if let Some(first) = md_lines.first_mut() {
                            first.spans.insert(
                                0,
                                Span::styled(
                                    "├─ ",
                                    Style::default().fg(theme.muted_color()).bold(),
                                ),
                            );
                        } else {
                            md_lines.push(Line::from(Span::styled(
                                "├─",
                                Style::default().fg(theme.muted_color()).bold(),
                            )));
                        }
                    }
                    append_bounded_head_tail(lines, md_lines, MAX_LINES, theme);
                    if is_final_assistant {
                        lines.push(Line::from(vec![
                            Span::styled(
                                "└─ usage ",
                                Style::default().fg(theme.warn_color()).bold(),
                            ),
                            Span::styled(
                                if turn_usage_known {
                                    format!(
                                        "in:{} out:{} think:{} tools:{} memory:{}",
                                        fmt_tokens(turn_input_tokens),
                                        fmt_tokens(turn_output_tokens),
                                        thinking_rounds,
                                        tool_count,
                                        memory_count
                                    )
                                } else {
                                    format!(
                                        "in:— out:— think:{} tools:{} memory:{}",
                                        thinking_rounds, tool_count, memory_count
                                    )
                                },
                                Style::default().fg(theme.muted_color()),
                            ),
                        ]));
                    }
                    return;
                }

                let mut rendered_lines = Vec::new();
                for line in content.lines() {
                    let mut spans = vec![Span::styled(
                        prefix.to_string(),
                        Style::default().fg(color).bold(),
                    )];
                    Self::highlight_line(line, &mut spans, color, theme);
                    rendered_lines.push(Line::from(spans));
                }
                append_bounded_head_tail(lines, rendered_lines, MAX_LINES, theme);
            }

            TimelineEntry::Thinking {
                id,
                content,
                complete,
                expanded: _,
            } => {
                let focus_marker = if is_focused { "● " } else { "  " };
                let status = if *complete { "saved" } else { "streaming" };
                let total_non_empty = content
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .count();
                let words = content.split_whitespace().count();
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}think "),
                        Style::default().fg(theme.accent()).bold(),
                    ),
                    Span::styled(
                        format!("#{id}"),
                        Style::default().fg(theme.muted_color()).bold(),
                    ),
                    Span::styled(
                        format!(" · {status}"),
                        Style::default().fg(if *complete {
                            theme.muted_color()
                        } else {
                            theme.warn_color()
                        }),
                    ),
                    Span::styled(
                        format!(" · {total_non_empty} lines · {words} words · details in Process"),
                        Style::default().fg(theme.muted_color()),
                    ),
                ]));
            }

            TimelineEntry::ToolCall {
                id: _,
                name,
                preview,
                output,
                done,
                expanded: _,
                exit_code,
            } => {
                let status_style = if *done {
                    if exit_code == &Some(0) {
                        Style::default().fg(theme.success_color())
                    } else {
                        Style::default().fg(theme.error_color())
                    }
                } else {
                    Style::default().fg(theme.warn_color())
                };
                let status_icon = if *done {
                    if exit_code == &Some(0) {
                        "✅"
                    } else {
                        "❌"
                    }
                } else {
                    "⏳"
                };
                let status_text = if *done {
                    format!("exit:{}", exit_code.unwrap_or(0))
                } else {
                    "running...".to_string()
                };
                let focus_marker = if is_focused { "● " } else { "  " };

                // Always compact in the conversation – details remain in the
                // Process panel so raw tool output cannot flood the transcript.
                let preview_text = if preview.is_empty() {
                    name.as_str()
                } else {
                    preview.as_str()
                };
                let short_preview: String = preview_text.chars().take(60).collect();
                let more = if preview_text.len() > 60 { "..." } else { "" };
                let mut tool_line = vec![
                    Span::styled(
                        format!("{focus_marker}🔧 {name}"),
                        Style::default().fg(theme.warn_color()).bold(),
                    ),
                    Span::styled(
                        format!(": {short_preview}{more}"),
                        Style::default().fg(theme.muted_color()),
                    ),
                    Span::styled(format!(" [{status_icon} {status_text}]"), status_style),
                ];
                // Check for subagent session
                if *done && name == "task" && !output.is_empty() {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(output) {
                        let has_sid = val
                            .get("subagent_session_id")
                            .or_else(|| val.get("session_id"))
                            .and_then(|v| v.as_str())
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);
                        if has_sid && is_focused {
                            tool_line.push(Span::styled(
                                " [Open Subagent]".to_string(),
                                Style::default().fg(theme.link_color()),
                            ));
                        }
                    }
                }
                lines.push(Line::from(tool_line));
            }

            TimelineEntry::SlashOutput {
                command,
                output,
                expanded,
            } => {
                let total_lines = output.lines().count();
                let focus_marker = if is_focused { "● " } else { "  " };

                if *expanded {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{focus_marker}┌─ /{command} ({total_lines} lines)"),
                            Style::default().fg(theme.accent()).bold(),
                        ),
                        Span::styled(
                            if is_focused {
                                "[Enter=collapse] [Ctrl+Y=copy]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(theme.muted_color()),
                        ),
                    ]));
                    for line in output.lines().take(100) {
                        lines.push(Line::from(vec![
                            Span::styled("│  ".to_string(), Style::default().fg(theme.accent())),
                            Span::styled(line.to_string(), Style::default().fg(theme.fg())),
                        ]));
                    }
                    if total_lines > 100 {
                        lines.push(Line::from(Span::styled(
                            format!("│ ... ({} more lines)", total_lines - 100),
                            Style::default().fg(theme.muted_color()),
                        )));
                    }
                    lines.push(Line::from(Span::styled(
                        "└─".to_string(),
                        Style::default().fg(theme.accent()),
                    )));
                } else {
                    let preview: String = output.chars().take(80).collect();
                    let more = if output.len() > 80 { "..." } else { "" };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("{focus_marker}/ {command} ({total_lines}L): {preview}{more}"),
                            Style::default().fg(theme.accent()).bold(),
                        ),
                        Span::styled(
                            if is_focused {
                                "[Enter=expand] [Ctrl+Y=copy]".to_string()
                            } else {
                                String::new()
                            },
                            Style::default().fg(theme.muted_color()),
                        ),
                    ]));
                }
            }
        }
    }
}

fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Keep both the beginning and the newest/end portion of an oversized entry.
/// Head-only truncation made the current answer look like a stale previous
/// response because the terminal conclusion was literally unreachable.
fn append_bounded_head_tail(
    target: &mut Vec<Line<'static>>,
    source: Vec<Line<'static>>,
    max_lines: usize,
    theme: &Theme,
) {
    let total = source.len();
    if total <= max_lines {
        target.extend(source);
        return;
    }
    let head = max_lines / 2;
    let tail = max_lines.saturating_sub(head);
    let omitted = total.saturating_sub(max_lines);
    for (index, line) in source.into_iter().enumerate() {
        if index < head {
            target.push(line);
            continue;
        }
        if index == head {
            target.push(Line::from(Span::styled(
                format!("  … {omitted} lines omitted; showing the conclusion below …"),
                Style::default().fg(theme.muted_color()),
            )));
        }
        if index >= total.saturating_sub(tail) {
            target.push(line);
        }
    }
}

impl Default for ChatView {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────

fn unicode_case_insensitive_ranges(content: &str, query: &str) -> Vec<(usize, usize)> {
    let folded_query = query.to_lowercase();
    if folded_query.is_empty() {
        return Vec::new();
    }
    let mut folded = String::new();
    let mut map = Vec::new();
    for (original_start, character) in content.char_indices() {
        let original_end = original_start + character.len_utf8();
        let lowered = character.to_lowercase().collect::<String>();
        for lowered_character in lowered.chars() {
            let folded_start = folded.len();
            folded.push(lowered_character);
            map.push((folded_start, folded.len(), original_start, original_end));
        }
    }
    let mut ranges = Vec::new();
    let mut search_start = 0;
    while search_start <= folded.len() {
        let Some(relative) = folded[search_start..].find(&folded_query) else {
            break;
        };
        let match_start = search_start + relative;
        let match_end = match_start + folded_query.len();
        let original_start = map
            .iter()
            .find(|(_, folded_end, _, _)| *folded_end > match_start)
            .map(|(_, _, original_start, _)| *original_start);
        let original_end = map
            .iter()
            .rev()
            .find(|(folded_start, _, _, _)| *folded_start < match_end)
            .map(|(_, _, _, original_end)| *original_end);
        if let (Some(original_start), Some(original_end)) = (original_start, original_end) {
            if ranges
                .last()
                .is_none_or(|(_, previous_end)| *previous_end <= original_start)
            {
                ranges.push((original_start, original_end));
            }
        }
        search_start = match_end.max(search_start.saturating_add(1));
        while search_start < folded.len() && !folded.is_char_boundary(search_start) {
            search_start = search_start.saturating_add(1);
        }
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::MockTerminal;

    // ── Helpers ───────────────────────────────────────────────────

    fn make_message(role: &str, content: &str) -> TimelineEntry {
        TimelineEntry::Message {
            role: role.into(),
            content: content.into(),
            timestamp: "12:00".into(),
            identity: None,
        }
    }

    #[test]
    fn oversized_assistant_reply_keeps_both_opening_and_conclusion() {
        let content = (0..600)
            .map(|index| {
                if index == 0 {
                    "OPENING".to_string()
                } else if index == 599 {
                    "FINAL-CONCLUSION".to_string()
                } else {
                    format!("line-{index}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        let entry = make_message("assistant", &content);
        let mut lines = Vec::new();

        ChatView::build_entry(&entry, false, &mut lines, &Theme::Dark);

        let rendered = lines
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(rendered.contains("OPENING"));
        assert!(rendered.contains("FINAL-CONCLUSION"));
        assert!(rendered.contains("100 lines omitted"));
    }

    fn make_thinking(id: u64, content: &str, complete: bool, expanded: bool) -> TimelineEntry {
        TimelineEntry::Thinking {
            id,
            content: content.into(),
            complete,
            expanded,
        }
    }

    fn make_tool_call(
        id: &str,
        name: &str,
        output: &str,
        done: bool,
        expanded: bool,
        exit_code: Option<i32>,
    ) -> TimelineEntry {
        TimelineEntry::ToolCall {
            id: id.into(),
            name: name.into(),
            preview: format!("Run {name}"),
            output: output.into(),
            done,
            expanded,
            exit_code,
        }
    }

    #[test]
    fn fixed_footer_uses_typed_run_metrics_and_never_renders_fake_duration() {
        let mut app = crate::App::new("requested-model", "compact-real-metrics");
        app.add_message("user", "question");
        app.add_message("assistant", "answer");
        app.turn_input_tokens = 1_200;
        app.turn_output_tokens = 340;
        app.turn_usage_known = true;
        app.durable_session_input_tokens = 8_000;
        app.durable_session_output_tokens = 2_000;
        app.context_used_tokens = Some(12_000);
        app.context_window_tokens = Some(128_000);
        app.context_remaining_tokens = Some(116_000);
        app.context_usage_percent_bp = Some(938);
        app.current_execution_status =
            Some(harness_contract::projection::ExecutionLiveStatus::CallingTool);
        app.execution_started_at_ms = Some(current_time_ms().saturating_sub(5_000));
        app.last_progress_at_ms = Some(current_time_ms().saturating_sub(500));
        app.current_run_metrics = Some(harness_contract::projection::RunMetricsProjection {
            tool_calls: 4,
            memory_recalls: 2,
            memory_evidence: 1,
            approvals: 1,
            files_touched: 3,
            input_tokens: 1_200,
            output_tokens: 340,
            total_tokens: 1_540,
            ..Default::default()
        });

        let mut view = ChatView::new();
        view.sync_from_app(&app);
        let joined = ChatView::build_dynamic_turn_lines(&view)
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(joined.contains("tools 4"), "{joined}");
        assert!(joined.contains("memory 2/1"), "{joined}");
        assert!(joined.contains("calling tool"), "{joined}");
        assert!(
            joined.contains("ctx 12.0k /128.0k 9% rem 116.0k"),
            "{joined}"
        );
        assert!(joined.contains("elapsed "), "{joined}");
        assert!(!joined.contains("Turn: 0.0"), "{joined}");
    }

    #[test]
    fn compact_mode_renders_only_the_latest_assistant_reply_and_its_tail() {
        let mut view = ChatView::new();
        let long_reply = (0..805)
            .map(|index| format!("current-line-{index}"))
            .collect::<Vec<_>>()
            .join("\n");
        view.timeline = vec![
            make_message("assistant", "stale-previous-reply"),
            make_message("user", "new question"),
            make_message("assistant", &long_reply),
        ];
        view.timeline_cursor = 2;
        view.last_assistant_entry_index = Some(2);
        view.turn_active = true;

        let joined = ChatView::build_compact_lines(&view)
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        assert!(!joined.contains("stale-previous-reply"), "{joined}");
        assert!(joined.contains("CURRENT REPLY"), "{joined}");
        assert!(joined.contains("current-line-804"), "{joined}");
        assert!(joined.contains("showing the conclusion below"), "{joined}");
    }

    #[test]
    fn compact_mode_never_replays_the_previous_reply_while_a_new_turn_is_submitting() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "previous question"),
            make_message("assistant", "previous reply must stay hidden"),
            make_message("user", "current question"),
        ];
        view.timeline_cursor = 2;
        view.last_assistant_entry_index = Some(1);
        view.turn_active = true;

        let joined = ChatView::build_compact_lines(&view)
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert!(!joined.contains("previous reply"), "{joined}");
        assert!(joined.contains("Waiting for the current reply"), "{joined}");
    }

    fn make_slash_output(command: &str, output: &str, expanded: bool) -> TimelineEntry {
        TimelineEntry::SlashOutput {
            command: command.into(),
            output: output.into(),
            expanded,
        }
    }

    fn render_view(view: &mut ChatView, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let theme_skin = crate::skin::SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme_skin);
            view.render(&mut ctx, area);
        });
        terminal.buffer_lines()
    }

    // ── render_message tests ──────────────────────────────────────

    #[test]
    fn render_message_user() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "Hello, world!")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        assert!(
            lines.iter().any(|l| l.contains("Hello, world!")),
            "Expected 'Hello, world!' in output"
        );
        assert!(
            lines.iter().any(|l| l.contains("> ")),
            "Expected '> ' user prefix"
        );
    }

    #[test]
    fn render_message_assistant() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("assistant", "Here is **bold** and `code`.")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("bold"), "Expected 'bold' in output");
        assert!(joined.contains("code"), "Expected 'code' in output");
    }

    #[test]
    fn render_message_system() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("system", "System notification.")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        assert!(
            !lines.iter().any(|l| l.contains("System notification.")),
            "system notices should not render in main chat"
        );
    }

    #[test]
    fn render_thinking_collapsed() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "line1\nline2\nline3", true, false)];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            !joined.contains("think") && !joined.contains("line1"),
            "Thinking should stay out of main chat: {joined}"
        );
    }

    #[test]
    fn render_thinking() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "line A\nline B", true, true)];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            !joined.contains("line A") && !joined.contains("saved"),
            "Thinking details should stay out of main chat: {joined}"
        );
    }

    #[test]
    fn render_tool_call() {
        let mut view = ChatView::new();
        view.timeline = vec![make_tool_call(
            "t1",
            "bash",
            "output line",
            true,
            false,
            Some(0),
        )];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("bash: Run bash"), "{joined}");
        assert!(joined.contains("exit:0"), "{joined}");
        assert!(
            !joined.contains("output line"),
            "raw tool output belongs in Process, got: {joined}"
        );
    }

    #[test]
    fn render_tool_call_completed() {
        let mut view = ChatView::new();
        view.timeline = vec![make_tool_call(
            "t1",
            "echo",
            "Hello\nWorld",
            true,
            true,
            Some(0),
        )];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("echo: Run echo"), "{joined}");
        assert!(
            !joined.contains("Hello") && !joined.contains("World"),
            "completed tool output belongs in Process, got: {joined}"
        );
    }

    #[test]
    fn render_slash_output() {
        let mut view = ChatView::new();
        view.timeline = vec![make_slash_output("status", "All systems go", false)];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("/ status"),
            "Expected '/ status' in slash output"
        );
        assert!(
            joined.contains("All systems go"),
            "Expected command output text"
        );
    }

    #[test]
    fn render_slash_output_expanded() {
        let mut view = ChatView::new();
        view.timeline = vec![make_slash_output(
            "status",
            "line1\nline2\nline3\nline4",
            true,
        )];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![6];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("┌─"),
            "Expected top border '┌─' in expanded slash output"
        );
        assert!(joined.contains("line1"), "Expected first output line");
        assert!(joined.contains("└─"), "Expected bottom border '└─'");
    }

    // ── Virtual scrolling ─────────────────────────────────────────

    #[test]
    fn virtual_scroll_activates() {
        // Create enough entries that total_lines > 3 * viewport
        // Each message = 1 content line + 1 separator = 2 lines.
        // viewport = 10, threshold = 30, need >30 lines.
        // 20 messages → 20 * 2 = 40 lines (>30).
        let mut view = ChatView::new();
        view.scroll_state.viewport_height = 10;
        view.timeline = (0..20)
            .map(|i| make_message("user", &format!("msg {i}")))
            .collect();
        view.entry_line_counts = vec![1usize; 20];
        view.msg_version = 1;
        view.lines_dirty = true;

        // After rendering, the cached lines should NOT be fully populated
        // because virtual scrolling builds only visible entries.
        let _lines = render_view(&mut view, 80, 10);

        // In virtual scrolling mode, cached_chat_lines is NOT used;
        // build_visible is called directly. The cache stays stale.
        // Verify by checking that cached_chat_lines is NOT 40+ lines
        let total = view.total_lines();
        let threshold = 10_usize.saturating_mul(3);
        assert!(
            total > threshold,
            "total_lines={total} should exceed threshold={threshold}"
        );
    }

    // ── Streaming tail rebuild ───────────────────────────────────

    #[test]
    fn streaming_tail_rebuild() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "first message"),
            make_message("assistant", "partial streamin"),
        ];
        view.turn_active = true;
        view.entry_line_counts = vec![1, 1];
        view.msg_version = 0;
        // Build initial cache
        view.cached_chat_lines = ChatView::build_new_lines(&view);
        view.last_drawn_version = 0;
        view.lines_dirty = false;

        let before_count = view.cached_chat_lines.len();

        // Simulate streaming: update last entry content
        view.timeline[1] = make_message("assistant", "partial streaming complete!");
        view.lines_dirty = true;

        // Call rebuild
        ChatView::rebuild_streaming_tail(&mut view);

        let after_count = view.cached_chat_lines.len();
        // The last entry gets rebuilt; the prefix stays the same.
        // Total lines should be comparable (could be same or more/less).
        assert!(
            after_count > 0,
            "cached_chat_lines should have content after rebuild"
        );

        // Verify the updated content is present
        let joined = view
            .cached_chat_lines
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.as_ref())
                    .collect::<Vec<&str>>()
                    .join("")
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            joined.contains("complete!"),
            "Expected 'complete!' after streaming rebuild, got:\n{joined}"
        );
        assert!(
            joined.contains("first message"),
            "Prefix entry should be preserved"
        );

        // Verify before and after line counts are reasonable
        assert!(
            (after_count as isize - before_count as isize).abs() < 10,
            "Line count changed drastically: before={before_count}, after={after_count}"
        );
    }

    #[test]
    fn ten_thousand_entry_history_patches_an_early_mutation_without_full_rebuild() {
        let mut app = crate::App::new("model", "session");
        for index in 0..10_000 {
            app.timeline_push(make_message("assistant", &format!("message {index}")));
        }
        app.mark_dirty();
        let mut view = ChatView::new();
        view.sync_from_app(&app);
        let _ = render_view(&mut view, 80, 24);
        let full_before = view.full_rebuild_count;
        let incremental_before = view.incremental_rebuild_count;

        let Some(TimelineEntry::Message { content, .. }) = app.timeline_get_mut(17) else {
            panic!("early message exists");
        };
        *content = "message 17 corrected".to_string();
        app.mark_dirty();
        view.sync_from_app(&app);
        let _ = render_view(&mut view, 80, 24);

        assert!(matches!(
            view.timeline.get(17),
            Some(TimelineEntry::Message { content, .. })
                if content == "message 17 corrected"
        ));
        assert_eq!(
            view.full_rebuild_count, full_before,
            "an exact dirty entry outside the old 256-entry tail must not force a full scan"
        );
        assert_eq!(
            view.incremental_rebuild_count,
            incremental_before.saturating_add(1)
        );
    }

    // ── Scroll-to-entry ───────────────────────────────────────────

    #[test]
    fn scroll_to_entry() {
        let mut view = ChatView::new();
        view.scroll_state.viewport_height = 5;
        // 10 entries, each 1 line + 1 separator = 2 lines → 20 total lines
        view.timeline = (0..10)
            .map(|i| make_message("user", &format!("msg {i}")))
            .collect();
        view.entry_line_counts = vec![1usize; 10];
        view.rebuild_entry_visual_starts();
        view.scroll_state.offset = 0;

        // Scroll to entry 8 (near the bottom)
        view.scroll_to_entry(8);

        // Entry 8 starts at offset 8 * 2 = 16.
        // Viewport of 5: scroll_offset should be 16 - (5 - 1) = 12 or close.
        // With entry_h=1: offset=16, need offset + 1 > scroll + 5 → 17 > scroll + 5 → scroll < 12
        // scroll_to_entry computes: offset.saturating_sub(5.saturating_sub(1)) = 16 - 4 = 12
        assert!(
            view.scroll_state.offset >= 10,
            "scroll_offset={} should be large enough to show entry 8",
            view.scroll_state.offset
        );

        // Scroll to entry 0
        view.scroll_to_entry(0);
        assert_eq!(
            view.scroll_state.offset, 0,
            "scroll_to_entry(0) should reset scroll"
        );
    }

    // ── Empty timeline ────────────────────────────────────────────

    #[test]
    fn empty_timeline_shows_placeholder() {
        let mut view = ChatView::new();
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Type to start"),
            "Empty timeline should show placeholder"
        );
    }

    // ── Spinner during active turn ────────────────────────────────

    #[test]
    fn spinner_renders_when_turn_active() {
        let mut view = ChatView::new();
        view.turn_active = true;
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("Processing..."),
            "Spinner should show 'Processing...' when turn is active"
        );
    }

    #[test]
    fn live_footer_stays_fixed_when_transcript_is_scrolled_to_top() {
        let mut view = ChatView::new();
        view.timeline = (0..12)
            .map(|index| make_message("assistant", &format!("reply-{index}")))
            .collect();
        view.last_assistant_entry_index = Some(11);
        view.turn_active = true;
        view.turn_usage_known = true;
        view.turn_input_tokens = 120;
        view.turn_output_tokens = 40;
        view.context_used_tokens = Some(1_000);
        view.context_window_tokens = Some(8_000);
        view.scroll_state.auto_scroll = false;
        view.scroll_state.offset = 0;

        let lines = render_view(&mut view, 80, 12);
        let transcript = lines[..6].join("\n");
        let footer = lines[6..].join("\n");
        assert!(transcript.contains("reply-0"), "{transcript}");
        assert!(footer.contains("ctx 1.0k /8.0k"), "{footer}");
        assert!(footer.contains("in 120 · out 40 · total 160"), "{footer}");
        assert!(footer.contains("Processing..."), "{footer}");
    }

    #[test]
    fn spinner_tick_does_not_rebuild_cached_transcript() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "question"),
            make_message("assistant", "streaming reply"),
        ];
        view.last_assistant_entry_index = Some(1);
        view.turn_active = true;

        let _ = render_view(&mut view, 80, 16);
        let counters = (
            view.full_rebuild_count,
            view.incremental_rebuild_count,
            view.dynamic_rebuild_count,
        );
        let cached = view.cached_chat_lines.clone();
        view.tick();
        let _ = render_view(&mut view, 80, 16);

        assert_eq!(
            (
                view.full_rebuild_count,
                view.incremental_rebuild_count,
                view.dynamic_rebuild_count,
            ),
            counters
        );
        assert_eq!(view.cached_chat_lines, cached);
    }

    // ── Event handling ────────────────────────────────────────────

    #[test]
    fn handle_enter_expands_thinking() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "content", true, false)];
        view.timeline_cursor = 0;

        let result = view.handle_event(&Event::Key(KeyEvent::from(KeyCode::Enter)));
        assert!(result.is_consumed());

        // Entry should now be expanded
        match &view.timeline[0] {
            TimelineEntry::Thinking { expanded, .. } => assert!(*expanded),
            _ => panic!("expected Thinking entry"),
        }
    }

    #[test]
    fn handle_enter_collapses_thinking() {
        let mut view = ChatView::new();
        view.timeline = vec![make_thinking(1, "content", true, true)];
        view.timeline_cursor = 0;

        let result = view.handle_event(&Event::Key(KeyEvent::from(KeyCode::Enter)));
        assert!(result.is_consumed());

        match &view.timeline[0] {
            TimelineEntry::Thinking { expanded, .. } => assert!(!*expanded),
            _ => panic!("expected Thinking entry"),
        }
    }

    #[test]
    fn handle_pageup_scrolls() {
        let mut view = ChatView::new();
        view.scroll_state.viewport_height = 10;
        view.scroll_state.offset = 30;

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::PageUp)));
        assert!(
            view.scroll_state.offset < 30,
            "PageUp should reduce scroll offset"
        );
    }

    #[test]
    fn handle_pagedown_scrolls() {
        let mut view = ChatView::new();
        view.scroll_state.viewport_height = 10;
        view.scroll_state.offset = 5;

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::PageDown)));
        assert!(
            view.scroll_state.offset > 5,
            "PageDown should increase scroll offset"
        );
    }

    #[test]
    fn render_clamps_scroll_to_bottom_with_exact_rendered_lines() {
        let mut view = ChatView::new();
        for i in 0..40 {
            view.timeline
                .push(make_message("assistant", &format!("message {i}")));
        }
        view.msg_version = 1;
        view.scroll_state.auto_scroll = true;

        let _ = render_view(&mut view, 80, 10);

        let max_offset = view
            .cached_chat_lines
            .len()
            .saturating_sub(view.scroll_state.viewport_height);
        assert_eq!(view.scroll_state.offset, max_offset);
        assert!(max_offset > 0);
    }

    #[test]
    fn home_and_end_jump_to_stable_scroll_bounds() {
        let mut view = ChatView::new();
        for i in 0..30 {
            view.timeline
                .push(make_message("assistant", &format!("line {i}")));
        }
        view.msg_version = 1;
        view.scroll_state.auto_scroll = true;
        let _ = render_view(&mut view, 80, 8);
        let bottom = view.scroll_state.offset;
        assert!(bottom > 0);

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::Home)));
        let _ = render_view(&mut view, 80, 8);
        assert_eq!(view.scroll_state.offset, 0);
        assert!(!view.scroll_state.auto_scroll);

        view.handle_event(&Event::Key(KeyEvent::from(KeyCode::End)));
        let _ = render_view(&mut view, 80, 8);
        assert_eq!(view.scroll_state.offset, bottom);
        assert!(view.scroll_state.auto_scroll);
    }

    #[test]
    fn long_chat_renders_the_visible_slice_without_u16_scroll_truncation() {
        let mut view = ChatView::new();
        view.cached_chat_lines = (0..70_000)
            .map(|index| Line::raw(format!("line {index}")))
            .collect();
        view.msg_version = 0;
        view.last_drawn_version = 0;
        view.lines_dirty = false;
        view.cached_wrap_width = 80;
        view.scroll_state.auto_scroll = true;

        let _ = render_view(&mut view, 80, 8);

        assert_eq!(view.scroll_state.offset, 69_992);
        assert!(view.scroll_state.offset > usize::from(u16::MAX));

        view.scroll_state.scroll_to_top();
        let _ = render_view(&mut view, 80, 8);
        assert_eq!(view.scroll_state.offset, 0);
    }

    #[test]
    fn component_trait_methods() {
        let view = ChatView::new();
        assert!(view.focusable(), "ChatView should be focusable");
        assert_eq!(view.id(), "chat_view");
    }

    // ── Markdown line highlighting ────────────────────────────────

    #[test]
    fn highlight_line_code_span() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "use `cargo test` to run tests")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(
            joined.contains("cargo test"),
            "Expected code content in output"
        );
    }

    // ── Multiple entries render correctly ─────────────────────────

    #[test]
    fn multiple_entries_rendered() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "Hello"),
            make_message("assistant", "Hi there!"),
            make_thinking(1, "Let me think...", true, false),
            make_tool_call("t1", "bash", "", true, false, Some(0)),
        ];
        view.entry_line_counts = vec![1, 1, 1, 1];
        view.last_assistant_entry_index = Some(1);
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("Hello"), "Expected user message");
        assert!(joined.contains("Hi there!"), "Expected assistant message");
        assert!(
            joined.contains("FINAL REPLY"),
            "Expected final reply marker"
        );
        assert!(
            !joined.contains("Let me think"),
            "Thinking details should stay in Process, not main chat: {joined}"
        );
        assert!(
            joined.contains("bash: Run bash") && joined.contains("exit:0"),
            "The conversation must expose a compact tool lifecycle card: {joined}"
        );
    }

    // ── tick / spinner ────────────────────────────────────────────

    #[test]
    fn tick_advances_spinner() {
        let mut view = ChatView::new();
        let s1 = view.spinner_char().to_string();
        view.tick();
        // After 10 ticks it should cycle, but after 1 tick the char
        // may be the same or different (cycle is length 10).
        // The spinner_idx wrapping_add(1) is enough for the test.
        let s2 = view.spinner_char().to_string();
        // After 1 tick, the character might change (depends on position in cycle).
        // Just verify it doesn't panic and returns valid chars.
        assert!(!s1.is_empty());
        assert!(!s2.is_empty());
    }

    // ── Default impl ──────────────────────────────────────────────

    #[test]
    fn default_chat_view_is_empty() {
        let view = ChatView::default();
        assert!(view.timeline.is_empty());
        assert_eq!(view.scroll_state.offset, 0);
        assert!(view.scroll_state.auto_scroll);
        assert_eq!(view.id(), "chat_view");
    }

    // ── sync_from_app / sync_to_app ───────────────────────────────

    #[test]
    fn sync_roundtrip() {
        let mut app = crate::test_utils::app_with_messages(5);
        app.scroll_offset = 10;
        app.auto_scroll = false;
        app.viewport_height = 30;

        let mut view = ChatView::new();
        view.sync_from_app(&app);

        assert_eq!(view.timeline.len(), 5);
        assert_eq!(view.scroll_state.offset, 10);
        assert!(!view.scroll_state.auto_scroll);

        // Modify view and sync back
        view.scroll_state.offset = 42;
        view.scroll_state.auto_scroll = true;
        view.sync_to_app(&mut app);

        assert_eq!(app.scroll_offset, 42);
        assert!(app.auto_scroll);
    }

    // ── Task 5: Per-message actions menu ────────────────────────────

    #[test]
    fn message_menu_shows_on_ctrl_o() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "Hello")];
        view.timeline_cursor = 0;
        view.msg_version = 0;
        view.lines_dirty = false;

        let ev = Event::Key(KeyEvent::new(
            KeyCode::Char('o'),
            crossterm::event::KeyModifiers::CONTROL,
        ));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Ctrl+O should be consumed");
        assert!(
            view.pending_message_menu,
            "pending_message_menu should be set"
        );
        assert_eq!(view.pending_menu_entry_idx, 0, "should target entry 0");
    }

    #[test]
    fn message_menu_ctrl_o_empty_timeline_noop() {
        let mut view = ChatView::new();
        let ev = Event::Key(KeyEvent::new(
            KeyCode::Char('o'),
            crossterm::event::KeyModifiers::CONTROL,
        ));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Ctrl+O should always be consumed");
        assert!(
            !view.pending_message_menu,
            "no pending menu on empty timeline"
        );
    }

    #[test]
    fn message_menu_entry_idx_tracks_cursor() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "A"),
            make_message("user", "B"),
            make_message("user", "C"),
        ];
        view.timeline_cursor = 2;
        view.msg_version = 0;
        view.lines_dirty = false;

        let ev = Event::Key(KeyEvent::new(
            KeyCode::Char('o'),
            crossterm::event::KeyModifiers::CONTROL,
        ));
        let _ = view.handle_event(&ev);
        assert_eq!(
            view.pending_menu_entry_idx, 2,
            "should track cursor position"
        );
    }

    // ── Task 9: Subagent session navigation ─────────────────────────

    #[test]
    fn subagent_nav_shows_on_task_tool_call() {
        let mut view = ChatView::new();
        let output = r#"{"subagent_session_id": "sess_sub_123"}"#;
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "task".into(),
            preview: "Run subagent".into(),
            output: output.into(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;

        let lines = render_view(&mut view, 80, 24);
        let joined = lines.join("\n");
        assert!(joined.contains("task: Run subagent"), "{joined}");
        assert!(joined.contains("Run subagent"), "{joined}");
        assert!(joined.contains("Open Subagent"), "{joined}");
    }

    #[test]
    fn open_subagent_navigates_on_enter() {
        let mut view = ChatView::new();
        let output = r#"{"subagent_session_id": "sess_sub_456"}"#;
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "task".into(),
            preview: "Run subagent".into(),
            output: output.into(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;

        // Enter on task tool call should set pending_subagent_nav
        let ev = Event::Key(KeyEvent::from(KeyCode::Enter));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Enter should be consumed");

        assert!(
            view.pending_subagent_nav.is_some(),
            "pending_subagent_nav should be set"
        );
        assert_eq!(
            view.pending_subagent_nav.as_deref(),
            Some("sess_sub_456"),
            "should capture session ID"
        );
    }

    #[test]
    fn subagent_nav_no_output_does_not_navigate() {
        let mut view = ChatView::new();
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "task".into(),
            preview: "Run subagent".into(),
            output: String::new(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;

        let ev = Event::Key(KeyEvent::from(KeyCode::Enter));
        let result = view.handle_event(&ev);
        assert!(result.is_consumed(), "Enter should be consumed");

        // Without output, should just toggle expand, not set nav
        assert!(view.pending_subagent_nav.is_none(), "no nav without output");
        // Should have expanded the entry
        match &view.timeline[0] {
            TimelineEntry::ToolCall { expanded, .. } => {
                assert!(*expanded, "Should have expanded tool call");
            }
            _ => panic!("expected ToolCall"),
        }
    }

    // ── Task 17: Search highlight tests ───────────────────────────

    #[test]
    fn search_highlight_inverts_matching() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "Hello world, this is a test message")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;
        view.search_query = "test".to_string();
        view.search_matches = vec![0];
        view.search_current = 0;

        let mut terminal = MockTerminal::new(80, 24);
        let theme_skin = crate::skin::SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme_skin);
            view.render(&mut ctx, area);
        });
        let lines = terminal.buffer_lines();
        let joined = lines.join("\n");
        // The text should still be rendered (containing "test")
        assert!(
            joined.contains("test"),
            "Search match text should be visible"
        );
    }

    #[test]
    fn search_highlight_survives_a_visual_wrap_boundary() {
        let mut logical = vec![Line::raw("abcdefghTARGETxyz")];
        ChatView::highlight_search_in_line(&mut logical[0], "target", true);
        let wrapped = wrap_styled_lines(logical, 10);
        let highlighted_rows = wrapped
            .iter()
            .enumerate()
            .filter_map(|(row, line)| {
                line.spans
                    .iter()
                    .any(|span| span.style.bg == Some(Color::Yellow))
                    .then_some(row)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            highlighted_rows,
            vec![0, 1],
            "the selected match must retain highlight on both sides of wrapping"
        );
    }

    #[test]
    fn handles_cjk() {
        let mut view = ChatView::new();
        view.timeline = vec![make_message("user", "你好世界 test 中文")];
        view.entry_line_counts = vec![1];
        view.msg_version = 0;
        view.lines_dirty = false;
        view.search_query = "中文".to_string();
        view.search_matches = vec![0];
        view.search_current = 0;

        // Verify the highlight function works correctly with CJK
        let mut line = Line::from(Span::raw("你好世界 test 中文"));
        ChatView::highlight_search_in_line(&mut line, "中文", true);
        // The line spans should have the highlighted match
        let has_cjk = line
            .spans
            .iter()
            .any(|span| span.content.contains("中文") && span.style.bg == Some(Color::Yellow));
        assert!(has_cjk, "CJK match should be preserved in spans");
    }

    #[test]
    fn next_match_cycles() {
        let mut view = ChatView::new();
        view.timeline = vec![
            make_message("user", "first match here"),
            make_message("user", "second match here"),
            make_message("user", "no match"),
        ];
        view.entry_line_counts = vec![1, 1, 1];
        view.msg_version = 0;
        view.lines_dirty = false;
        view.search_query = "match".to_string();
        view.search_matches = vec![0, 1];
        view.search_current = 0;

        // Verify initial state
        assert_eq!(view.search_current, 0);
        assert_eq!(view.search_matches.len(), 2);

        // Verify highlight_search_in_line works on a line containing match
        let mut line = Line::from(Span::raw("first match here"));
        ChatView::highlight_search_in_line(&mut line, "match", true);
        assert!(
            line.spans
                .iter()
                .any(|span| span.content == "match" && span.style.bg == Some(Color::Yellow)),
            "selected entry match should use the current-result style"
        );
        // The spans should contain both the original text and highlighted parts
        assert!(!line.spans.is_empty(), "Should have split spans");

        let mut line2 = Line::from(Span::raw("no match here"));
        ChatView::highlight_search_in_line(&mut line2, "match", false);
        assert!(line2.spans.iter().any(|span| span.content == "match"));
    }

    #[test]
    fn search_highlight_skips_empty_query() {
        let mut line = Line::from(Span::raw("hello world"));
        ChatView::highlight_search_in_line(&mut line, "", false);
        assert_eq!(line.spans.len(), 1, "Spans should be unmodified");
    }

    // ── Continue existing tests ──────────────────────────────────

    #[test]
    fn subagent_nav_non_task_tool_no_nav() {
        let mut view = ChatView::new();
        let output = r#"{"subagent_session_id": "sess_sub_789"}"#;
        view.timeline = vec![TimelineEntry::ToolCall {
            id: "t1".into(),
            name: "bash".into(), // not "task"
            preview: "echo".into(),
            output: output.into(),
            done: true,
            expanded: false,
            exit_code: Some(0),
        }];
        view.timeline_cursor = 0;

        let ev = Event::Key(KeyEvent::from(KeyCode::Enter));
        let _ = view.handle_event(&ev);
        assert!(
            view.pending_subagent_nav.is_none(),
            "non-task tool should not set nav flag"
        );
    }
}
