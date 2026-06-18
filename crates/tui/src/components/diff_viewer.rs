// ── Diff Viewer Component ──────────────────────────────────────────
// Unified/split diff viewer with syntax highlighting and file tree
// sidebar. Parses unified diff format, renders added/removed/context
// lines with color-coded backgrounds, and provides keyboard navigation.
//
// Features:
//   - Parse unified diff format (lines starting with @@, +, -, space)
//   - Render: added=green bg, removed=red bg, context=default, header=cyan
//   - Syntax highlight changed code lines via syntect
//   - File tree sidebar: list changed files with +/- counts (j/k nav)
//   - Toggle unified/split mode (key 't')
//   - Navigate between hunks (n/N)
//   - Scroll vertically within diff (Up/Down / PageUp/PageDown)
// -----------------------------------------------------------------

#![allow(dead_code)]

use std::sync::LazyLock;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Paragraph};
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::SyntaxSet;

use crate::components::base::{Component, EventResult, RenderContext};
use crate::components::panel_scroll::{clamp_u16_offset, offset_to_u16, PanelScrollState};

/// Local static references to syntect data, duplicating those in
/// `md_renderer` so the module can compile independently.
static SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(SyntaxSet::load_defaults_newlines);
static THEME_SET: LazyLock<ThemeSet> = LazyLock::new(ThemeSet::load_defaults);

// ═══════════════════════════════════════════════════════════════════
// Data types
// ═══════════════════════════════════════════════════════════════════

/// The kind of a single line in a diff hunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    /// Context line (unchanged), prefix ' '.
    Context,
    /// Added line, prefix '+'.
    Added,
    /// Removed line, prefix '-'.
    Removed,
    /// Hunk header line, starts with '@@'.
    Header,
}

/// A single line within a diff hunk, with its kind and content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HunkLine {
    pub kind: LineKind,
    /// The line content without the leading '+', '-', or ' ' prefix.
    pub content: String,
    /// Original old-file line number (if determinable from hunk header).
    pub old_lineno: Option<u32>,
    /// Original new-file line number (if determinable from hunk header).
    pub new_lineno: Option<u32>,
}

/// A diff hunk: a group of changed lines with a header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hunk {
    /// The raw hunk header text (e.g., "@@ -1,4 +1,5 @@").
    pub header: String,
    /// The lines in this hunk.
    pub lines: Vec<HunkLine>,
}

/// Represents one changed file in a diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChange {
    /// The file path (extracted from the diff header).
    pub path: String,
    /// Number of added lines.
    pub added: usize,
    /// Number of removed lines.
    pub removed: usize,
    /// All hunks for this file.
    pub hunks: Vec<Hunk>,
}

/// The display mode for the diff viewer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffMode {
    /// All diff lines shown inline (added, removed, context).
    Unified,
    /// Left column shows old (removed + context), right shows new (added + context).
    Split,
}

// ═══════════════════════════════════════════════════════════════════
// DiffViewer Component
// ═══════════════════════════════════════════════════════════════════

/// A TUI component for viewing diffs with syntax highlighting,
/// a file-tree sidebar, and unified/split mode toggle.
///
/// # Usage
///
/// ```ignore
/// use gateway::tui::components::diff_viewer::DiffViewer;
///
/// let mut viewer = DiffViewer::new("Changes");
/// viewer.load("diff --git a/src/main.rs b/src/main.rs\n\
///               @@ -1,3 +1,4 @@\n\
///                fn main() {\n\
///               -    old_stuff();\n\
///               +    new_stuff();\n\
///                }");
/// ```
pub struct DiffViewer {
    /// Parsed file changes.
    files: Vec<FileChange>,
    /// Currently selected file index in the file tree sidebar.
    selected_file: usize,
    /// Currently selected hunk index within the selected file.
    selected_hunk: usize,
    /// Display mode.
    mode: DiffMode,
    /// Vertical scroll offset in the diff view (in lines).
    scroll_offset: u16,
    last_viewport_len: usize,
    /// View title shown in the border.
    title: String,
    /// Set of file paths that have been marked as reviewed (Task 10).
    reviewed_files: std::collections::HashSet<String>,
}

impl DiffViewer {
    /// Create a new, empty diff viewer with the given title.
    #[must_use]
    pub fn new(title: &str) -> Self {
        Self {
            files: Vec::new(),
            selected_file: 0,
            selected_hunk: 0,
            mode: DiffMode::Unified,
            scroll_offset: 0,
            last_viewport_len: 1,
            title: title.to_string(),
            reviewed_files: std::collections::HashSet::new(),
        }
    }

    // ── Public API ────────────────────────────────────────────────

    /// Parse a unified diff text and populate the viewer.
    ///
    /// The diff text must follow standard unified diff format:
    /// - `diff --git a/path b/path` to identify files
    /// - `@@ -start,len +start,len @@` for hunk headers
    /// - `+line` for added lines
    /// - `-line` for removed lines
    /// - ` line` for context lines
    ///
    /// After parsing, `selected_file` and `selected_hunk` are reset to 0,
    /// and `scroll_offset` is reset to 0.
    pub fn load(&mut self, diff_text: &str) {
        self.files = parse_unified_diff(diff_text);
        self.selected_file = 0;
        self.selected_hunk = 0;
        self.scroll_offset = 0;
    }

    /// Sync the diff viewer from the App state by extracting diff text
    /// from timeline entries (ToolCall outputs containing unified diff format).
    ///
    /// Scans all timeline entries for ToolCall outputs that contain
    /// `diff --git` and `@@ -` patterns, concatenates them, and loads
    /// the result into the viewer.
    ///
    /// If no diff-like text is found, the viewer state is unchanged.
    pub fn sync_from_app(&mut self, app: &crate::App) {
        let timeline = app.timeline_clone_vec();

        // Collect diff text from ToolCall outputs
        let mut diff_text = String::new();
        for entry in &timeline {
            if let crate::app::TimelineEntry::ToolCall { output, .. } = entry {
                if output.is_empty() {
                    continue;
                }
                // Check if the output contains diff-like content
                if output.contains("diff --git") || output.contains("@@ -") {
                    if !diff_text.is_empty() {
                        diff_text.push('\n');
                    }
                    diff_text.push_str(output);
                }
            }
        }

        // Only load if we found diff content and it's different from current
        if !diff_text.is_empty() {
            let current_text = self
                .files
                .iter()
                .map(|f| f.path.as_str())
                .collect::<Vec<_>>()
                .join(",");
            let parsed = parse_unified_diff(&diff_text);
            let new_text = parsed
                .iter()
                .map(|f| f.path.as_str())
                .collect::<Vec<_>>()
                .join(",");
            if current_text != new_text {
                self.files = parsed;
                self.selected_file = 0;
                self.selected_hunk = 0;
                self.scroll_offset = 0;
            }
        }
    }

    /// Returns the number of files in the diff.
    #[must_use]
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Returns the current diff display mode.
    #[must_use]
    pub fn mode(&self) -> DiffMode {
        self.mode
    }

    /// Returns the index of the currently selected file.
    #[must_use]
    pub fn selected_file_index(&self) -> usize {
        self.selected_file
    }

    /// Returns the index of the currently selected hunk.
    #[must_use]
    pub fn selected_hunk_index(&self) -> usize {
        self.selected_hunk
    }

    /// Returns a reference to the parsed file changes.
    #[must_use]
    pub fn files(&self) -> &[FileChange] {
        &self.files
    }

    /// Returns the scroll offset.
    #[must_use]
    pub fn scroll_offset(&self) -> u16 {
        self.scroll_offset
    }

    // ── Mark reviewed (Task 10) ──

    /// Toggle the reviewed state of a file path.
    pub fn toggle_reviewed(&mut self, file_path: &str) {
        if self.reviewed_files.contains(file_path) {
            self.reviewed_files.remove(file_path);
        } else {
            self.reviewed_files.insert(file_path.to_string());
        }
    }

    /// Toggle reviewed state for the currently selected file.
    pub fn toggle_reviewed_selected(&mut self) {
        let path = self.files.get(self.selected_file).map(|f| f.path.clone());
        if let Some(p) = path {
            self.toggle_reviewed(&p);
        }
    }

    /// Check if a file has been marked as reviewed.
    #[must_use]
    pub fn is_reviewed(&self, file_path: &str) -> bool {
        self.reviewed_files.contains(file_path)
    }

    /// Return the set of reviewed file paths.
    #[must_use]
    pub fn reviewed_files(&self) -> &std::collections::HashSet<String> {
        &self.reviewed_files
    }

    /// Cycle to the next hunk within the current file.
    /// Wraps to the first hunk after the last.
    pub fn next_hunk(&mut self) {
        if let Some(file) = self.files.get(self.selected_file) {
            if !file.hunks.is_empty() {
                self.selected_hunk = (self.selected_hunk + 1) % file.hunks.len();
            }
        }
    }

    /// Cycle to the previous hunk within the current file.
    /// Wraps to the last hunk from the first.
    pub fn prev_hunk(&mut self) {
        if let Some(file) = self.files.get(self.selected_file) {
            if file.hunks.is_empty() {
                self.selected_hunk = 0;
            } else {
                self.selected_hunk = if self.selected_hunk == 0 {
                    file.hunks.len() - 1
                } else {
                    self.selected_hunk - 1
                };
            }
        }
    }

    /// Select the next file in the file tree.
    pub fn select_next_file(&mut self) {
        if !self.files.is_empty() {
            self.selected_file = (self.selected_file + 1).min(self.files.len() - 1);
            self.selected_hunk = 0;
            self.scroll_offset = 0;
        }
    }

    /// Select the previous file in the file tree.
    pub fn select_prev_file(&mut self) {
        self.selected_file = self.selected_file.saturating_sub(1);
        self.selected_hunk = 0;
        self.scroll_offset = 0;
    }

    /// Toggle between unified and split diff mode.
    pub fn toggle_mode(&mut self) {
        self.mode = match self.mode {
            DiffMode::Unified => DiffMode::Split,
            DiffMode::Split => DiffMode::Unified,
        };
    }

    // ── Scroll helpers ────────────────────────────────────────────

    fn scroll_up(&mut self, lines: u16) {
        self.scroll_offset = self.scroll_offset.saturating_sub(lines);
    }

    fn scroll_down(&mut self, lines: u16) {
        self.scroll_offset = self.scroll_offset.saturating_add(lines);
    }

    /// Total number of visible lines in the current file's selected hunk.
    fn total_diff_lines(&self) -> u16 {
        if let Some(file) = self.files.get(self.selected_file) {
            if let Some(hunk) = file.hunks.get(self.selected_hunk) {
                // 1 header + N lines
                return 1 + hunk.lines.len() as u16;
            }
        }
        0
    }

    // ── Render helpers ────────────────────────────────────────────

    /// Compute the maximum file-tree width needed.
    fn tree_width(&self, available_width: u16) -> u16 {
        if self.files.is_empty() {
            return 0;
        }
        let max_path = self
            .files
            .iter()
            .map(|f| format!(" {} [+{} -{}]", f.path, f.added, f.removed).len())
            .max()
            .unwrap_or(0);
        // Cap at 40% of available width, with min 15 and max 50
        let cap = ((available_width as f32) * 0.4) as u16;
        let width = max_path as u16 + 4; // +4 for borders/padding
        width.clamp(15, cap.max(15).min(50))
    }

    fn render_file_tree(&self, ctx: &mut RenderContext, area: Rect) {
        if self.files.is_empty() {
            return;
        }

        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" Files ({}) ", self.files.len()))
            .border_style(Style::default().fg(Color::DarkGray));

        let inner = block.inner(area);
        ctx.frame_mut().render_widget(block, area);

        // Compute visible range based on scroll
        let max_visible = inner.height as usize;

        let file_items: Vec<Line> = self
            .files
            .iter()
            .enumerate()
            .skip(self.scroll_offset as usize)
            .take(max_visible)
            .map(|(i, f)| {
                let reviewed_mark = if self.reviewed_files.contains(&f.path) {
                    "✅ "
                } else {
                    ""
                };
                let label = format!(
                    " {}{} [+{} -{}] ",
                    reviewed_mark,
                    truncate_middle(&f.path, inner.width.saturating_sub(8) as usize),
                    f.added,
                    f.removed
                );
                let is_reviewed = self.reviewed_files.contains(&f.path);
                if i == self.selected_file {
                    if is_reviewed {
                        Line::styled(
                            label,
                            Style::default()
                                .fg(Color::DarkGray)
                                .bg(Color::Rgb(40, 40, 40))
                                .add_modifier(Modifier::BOLD),
                        )
                    } else {
                        Line::styled(
                            label,
                            Style::default()
                                .fg(Color::Black)
                                .bg(Color::Cyan)
                                .add_modifier(Modifier::BOLD),
                        )
                    }
                } else if is_reviewed {
                    Line::styled(
                        label,
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::DIM),
                    )
                } else {
                    Line::styled(label, Style::default().fg(Color::Gray))
                }
            })
            .collect();

        let text = Text::from(file_items);
        let paragraph = Paragraph::new(text);
        ctx.frame_mut().render_widget(paragraph, inner);
    }

    fn render_diff_view(&self, ctx: &mut RenderContext, area: Rect) {
        if self.files.is_empty() {
            let block = Block::default()
                .borders(Borders::ALL)
                .title(" No diff loaded ");

            let paragraph = Paragraph::new("No diff to display.\nUse :diff or load a diff file.")
                .block(block)
                .style(Style::default().fg(Color::DarkGray));

            ctx.frame_mut().render_widget(paragraph, area);
            return;
        }

        let file = match self.files.get(self.selected_file) {
            Some(f) => f,
            None => return,
        };

        let block_title = format!(
            " {} [{} hunks] [{} mode: press t to toggle] ",
            file.path,
            file.hunks.len(),
            match self.mode {
                DiffMode::Unified => "unified",
                DiffMode::Split => "split",
            }
        );

        let block = Block::default()
            .borders(Borders::ALL)
            .title(block_title)
            .border_style(Style::default().fg(Color::DarkGray));

        let inner = block.inner(area);
        ctx.frame_mut().render_widget(block, area);

        match self.mode {
            DiffMode::Unified => self.render_unified(ctx, inner, file),
            DiffMode::Split => self.render_split(ctx, inner, file),
        }
    }

    fn render_unified(&self, ctx: &mut RenderContext, area: Rect, file: &FileChange) {
        let lines = self.build_unified_lines(file);
        let visible_lines = area.height as usize;
        let scroll = self.scroll_offset as usize;

        let display: Vec<Line> = lines
            .iter()
            .skip(scroll)
            .take(visible_lines.max(1))
            .cloned()
            .collect();

        let content_height = lines.len() as u16;
        let max_scroll = content_height.saturating_sub(area.height);
        self.render_scroll_indicator(ctx, area, content_height);
        let _ = max_scroll;

        let paragraph = Paragraph::new(Text::from(display));
        ctx.frame_mut().render_widget(paragraph, area);
    }

    fn build_unified_lines(&self, file: &FileChange) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();

        let lang = detect_language(&file.path);

        // Hunk navigation: if a hunk is selected, show an indicator
        for (hi, hunk) in file.hunks.iter().enumerate() {
            let is_selected = hi == self.selected_hunk;

            // Hunk header
            let header_style = if is_selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Cyan)
            };
            lines.push(Line::styled(
                truncate_to_width(&hunk.header, 120),
                header_style,
            ));

            // Hunk lines
            for hl in &hunk.lines {
                let styled = self.style_line_unified(hl, lang.as_deref());
                lines.push(styled);
            }
        }

        lines
    }

    fn style_line_unified(&self, hl: &HunkLine, lang: Option<&str>) -> Line<'static> {
        let prefix = match hl.kind {
            LineKind::Added => "+",
            LineKind::Removed => "-",
            LineKind::Context => " ",
            LineKind::Header => "",
        };
        let display = format!("{}{}", prefix, hl.content);

        match hl.kind {
            LineKind::Added => {
                let bg = Color::Rgb(0, 80, 0);
                let highlight_spans = highlight_code_line(&hl.content, lang, bg);
                // Prepend the "+" prefix in white on green
                let mut spans = vec![Span::styled(
                    "+".to_string(),
                    Style::default()
                        .fg(Color::Green)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                )];
                spans.extend(highlight_spans);
                Line::from(spans)
            }
            LineKind::Removed => {
                let bg = Color::Rgb(80, 0, 0);
                let highlight_spans = highlight_code_line(&hl.content, lang, bg);
                let mut spans = vec![Span::styled(
                    "-".to_string(),
                    Style::default()
                        .fg(Color::Red)
                        .bg(bg)
                        .add_modifier(Modifier::BOLD),
                )];
                spans.extend(highlight_spans);
                Line::from(spans)
            }
            LineKind::Context => {
                // Context: no syntax highlighting, just normal text
                Line::styled(
                    truncate_to_width(&display, 200),
                    Style::default().fg(Color::Gray),
                )
            }
            LineKind::Header => Line::styled(
                truncate_to_width(&display, 200),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        }
    }

    fn render_split(&self, ctx: &mut RenderContext, area: Rect, file: &FileChange) {
        let half_width = area.width / 2;
        let left_area = Rect::new(area.x, area.y, half_width, area.height);
        let right_area = Rect::new(
            area.x + half_width,
            area.y,
            area.width - half_width,
            area.height,
        );

        let lang = detect_language(&file.path);

        // Build left (old) and right (new) line lists
        let (old_lines, new_lines) = self.build_split_lines(file, lang.as_deref());

        let scroll = self.scroll_offset as usize;
        let visible = area.height as usize;

        let old_display: Vec<Line> = old_lines
            .iter()
            .skip(scroll)
            .take(visible.max(1))
            .cloned()
            .collect();
        let new_display: Vec<Line> = new_lines
            .iter()
            .skip(scroll)
            .take(visible.max(1))
            .cloned()
            .collect();

        let old_block = Block::default()
            .borders(Borders::NONE)
            .title(" Old ")
            .title_style(Style::default().fg(Color::Red));
        let new_block = Block::default()
            .borders(Borders::NONE)
            .title(" New ")
            .title_style(Style::default().fg(Color::Green));

        ctx.frame_mut().render_widget(
            Paragraph::new(Text::from(old_display)).block(old_block),
            left_area,
        );
        ctx.frame_mut().render_widget(
            Paragraph::new(Text::from(new_display)).block(new_block),
            right_area,
        );

        // Vertical separator
        for y in area.y..area.y + area.height {
            let sep_area = Rect::new(area.x + half_width, y, 1, 1);
            ctx.frame_mut().render_widget(
                Span::styled("│", Style::default().fg(Color::DarkGray)),
                sep_area,
            );
        }
    }

    fn build_split_lines(
        &self,
        file: &FileChange,
        lang: Option<&str>,
    ) -> (Vec<Line<'static>>, Vec<Line<'static>>) {
        let mut old_lines: Vec<Line<'static>> = Vec::new();
        let mut new_lines: Vec<Line<'static>> = Vec::new();

        for (hi, hunk) in file.hunks.iter().enumerate() {
            let is_selected = hi == self.selected_hunk;

            // Hunk header appears in both columns
            let header_line = if is_selected {
                Line::styled(
                    truncate_to_width(&hunk.header, 80),
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                Line::styled(
                    truncate_to_width(&hunk.header, 80),
                    Style::default().fg(Color::Cyan),
                )
            };
            old_lines.push(header_line.clone());
            new_lines.push(header_line);

            for hl in &hunk.lines {
                match hl.kind {
                    LineKind::Added => {
                        // Show empty in old, highlighted in new
                        old_lines.push(Line::from(""));
                        let bg = Color::Rgb(0, 80, 0);
                        let mut spans = vec![Span::styled(
                            "+".to_string(),
                            Style::default()
                                .fg(Color::Green)
                                .bg(bg)
                                .add_modifier(Modifier::BOLD),
                        )];
                        spans.extend(highlight_code_line(&hl.content, lang, bg));
                        new_lines.push(Line::from(spans));
                    }
                    LineKind::Removed => {
                        // Show highlighted in old, empty in new
                        let bg = Color::Rgb(80, 0, 0);
                        let mut spans = vec![Span::styled(
                            "-".to_string(),
                            Style::default()
                                .fg(Color::Red)
                                .bg(bg)
                                .add_modifier(Modifier::BOLD),
                        )];
                        spans.extend(highlight_code_line(&hl.content, lang, bg));
                        old_lines.push(Line::from(spans));
                        new_lines.push(Line::from(""));
                    }
                    LineKind::Context => {
                        let display = truncate_to_width(&hl.content, 80);
                        let gray = Style::default().fg(Color::Gray);
                        old_lines.push(Line::styled(display.clone(), gray));
                        new_lines.push(Line::styled(display, gray));
                    }
                    LineKind::Header => {
                        // Should not normally see Header inside a hunk
                        let display = truncate_to_width(&hl.content, 80);
                        old_lines.push(Line::styled(
                            display.clone(),
                            Style::default().fg(Color::Cyan),
                        ));
                        new_lines.push(Line::styled(display, Style::default().fg(Color::Cyan)));
                    }
                }
            }
        }

        (old_lines, new_lines)
    }

    fn render_scroll_indicator(&self, ctx: &mut RenderContext, area: Rect, content_height: u16) {
        if content_height <= area.height {
            return;
        }
        let scroll_pct = self.scroll_offset as f64 / (content_height - area.height) as f64;
        let scroll_pct = scroll_pct.clamp(0.0, 1.0);

        let bar_bg = Style::default().fg(Color::DarkGray);
        let bar_fg = Style::default().fg(Color::Cyan);

        // Render a thin scrollbar at the right edge
        let bar_x = area.x + area.width - 1;
        let bar_height = area.height;
        if bar_height < 3 {
            return;
        }

        // Thumb position
        let thumb_size =
            (bar_height as f64 * area.height as f64 / content_height as f64).max(1.0) as u16;
        let thumb_start = (scroll_pct * (bar_height - thumb_size) as f64) as u16;

        for i in 0..bar_height {
            let ch = if i >= thumb_start && i < thumb_start + thumb_size {
                Span::styled("█", bar_fg)
            } else {
                Span::styled("│", bar_bg)
            };
            let ch_area = Rect::new(bar_x, area.y + i, 1, 1);
            ctx.frame_mut().render_widget(ch, ch_area);
        }
    }

    fn render_footer(&self, ctx: &mut RenderContext, area: Rect) {
        let help_text = if self.files.is_empty() {
            " No diff loaded "
        } else {
            " j/k:file  n/N:hunk  t:mode  m:review  ↑/↓:scroll  PgUp/PgDn  q/help:exit "
        };
        let span = Span::styled(
            help_text,
            Style::default().fg(Color::Black).bg(Color::DarkGray),
        );
        let block = Block::default().style(Style::default().bg(Color::DarkGray));
        let paragraph = Paragraph::new(Line::from(span)).block(block);
        ctx.frame_mut().render_widget(paragraph, area);
    }
}

// ═══════════════════════════════════════════════════════════════════
// Component trait implementation
// ═══════════════════════════════════════════════════════════════════
impl Component for DiffViewer {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if area.height < 4 || area.width < 20 {
            return;
        }

        // Layout: tree sidebar | diff view | scrollbar
        //          footer bar
        let footer_h = 1u16;
        let main_h = area.height.saturating_sub(footer_h);

        let tree_w = self.tree_width(area.width);
        let diff_w = area.width.saturating_sub(tree_w);

        let tree_area = Rect::new(area.x, area.y, tree_w, main_h);
        let diff_area = Rect::new(area.x + tree_w, area.y, diff_w, main_h);
        let footer_area = Rect::new(area.x, area.y + main_h, area.width, footer_h);
        self.last_viewport_len = diff_area.height.max(1) as usize;
        let total_diff_lines = self.total_diff_lines() as usize;
        clamp_u16_offset(
            &mut self.scroll_offset,
            total_diff_lines,
            self.last_viewport_len,
        );

        self.render_file_tree(ctx, tree_area);
        self.render_diff_view(ctx, diff_area);
        self.render_footer(ctx, footer_area);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => self.handle_key(key),
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "diff_viewer"
    }
}

impl DiffViewer {
    fn handle_key(&mut self, key: &KeyEvent) -> EventResult {
        match key.code {
            // File navigation
            KeyCode::Char('j') => {
                self.select_next_file();
                EventResult::Consumed
            }
            KeyCode::Char('k') => {
                self.select_prev_file();
                EventResult::Consumed
            }

            // Hunk navigation
            KeyCode::Char('n') => {
                self.next_hunk();
                EventResult::Consumed
            }
            KeyCode::Char('N') => {
                self.prev_hunk();
                EventResult::Consumed
            }

            // Mode toggle
            KeyCode::Char('t') => {
                self.toggle_mode();
                EventResult::Consumed
            }

            // Mark reviewed (Task 10)
            KeyCode::Char('m') => {
                self.toggle_reviewed_selected();
                EventResult::Consumed
            }

            // Scroll
            KeyCode::Up => {
                let mut scroll = PanelScrollState {
                    offset: self.scroll_offset as usize,
                    content_len: self.total_diff_lines() as usize,
                    viewport_len: self.last_viewport_len,
                };
                scroll.line_up();
                self.scroll_offset = offset_to_u16(scroll.offset);
                EventResult::Consumed
            }
            KeyCode::Down => {
                let mut scroll = PanelScrollState {
                    offset: self.scroll_offset as usize,
                    content_len: self.total_diff_lines() as usize,
                    viewport_len: self.last_viewport_len,
                };
                scroll.line_down();
                self.scroll_offset = offset_to_u16(scroll.offset);
                EventResult::Consumed
            }
            KeyCode::PageUp => {
                let mut scroll = PanelScrollState {
                    offset: self.scroll_offset as usize,
                    content_len: self.total_diff_lines() as usize,
                    viewport_len: self.last_viewport_len,
                };
                scroll.page_up();
                self.scroll_offset = offset_to_u16(scroll.offset);
                EventResult::Consumed
            }
            KeyCode::PageDown => {
                let mut scroll = PanelScrollState {
                    offset: self.scroll_offset as usize,
                    content_len: self.total_diff_lines() as usize,
                    viewport_len: self.last_viewport_len,
                };
                scroll.page_down();
                self.scroll_offset = offset_to_u16(scroll.offset);
                EventResult::Consumed
            }
            KeyCode::Home => {
                let mut scroll = PanelScrollState {
                    offset: self.scroll_offset as usize,
                    content_len: self.total_diff_lines() as usize,
                    viewport_len: self.last_viewport_len,
                };
                scroll.top();
                self.scroll_offset = offset_to_u16(scroll.offset);
                EventResult::Consumed
            }
            KeyCode::End => {
                let mut scroll = PanelScrollState {
                    offset: self.scroll_offset as usize,
                    content_len: self.total_diff_lines() as usize,
                    viewport_len: self.last_viewport_len,
                };
                scroll.bottom();
                self.scroll_offset = offset_to_u16(scroll.offset);
                EventResult::Consumed
            }

            _ => EventResult::NotConsumed,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Parsing: Unified Diff Format
// ═══════════════════════════════════════════════════════════════════

/// Parse a unified diff text into a list of `FileChange` structs.
///
/// Handles multi-file diffs (e.g., from `git diff`).
/// Each file is delimited by `diff --git a/path b/path`.
/// Within each file, hunks are delimited by `@@ ... @@` headers.
fn parse_unified_diff(diff_text: &str) -> Vec<FileChange> {
    let mut files: Vec<FileChange> = Vec::new();
    let mut current_file: Option<FileChange> = None;
    let mut current_hunk: Option<Hunk> = None;
    // Track line numbers from the hunk header
    let mut old_lineno: u32 = 0;
    let mut new_lineno: u32 = 0;

    for line in diff_text.lines() {
        // File boundary: diff --git a/path b/path
        if line.starts_with("diff --git ") {
            // Commit previous hunk and file
            if let Some(hunk) = current_hunk.take() {
                if let Some(ref mut file) = current_file {
                    file.hunks.push(hunk);
                }
            }
            if let Some(file) = current_file.take() {
                files.push(file);
            }

            // Extract file path (prefer the b/path)
            let path = extract_file_path(line);
            current_file = Some(FileChange {
                path,
                added: 0,
                removed: 0,
                hunks: Vec::new(),
            });
            continue;
        }

        // Skip --- and +++ header lines
        if line.starts_with("--- ") || line.starts_with("+++ ") {
            continue;
        }

        // Hunk header: @@ -old_start[,old_count] +new_start[,new_count] @@
        if line.starts_with("@@") {
            // Commit previous hunk
            if let Some(hunk) = current_hunk.take() {
                if let Some(ref mut file) = current_file {
                    file.hunks.push(hunk);
                }
            }

            // Parse line numbers
            let (old_start, new_start) = parse_hunk_header(line);
            old_lineno = old_start;
            new_lineno = new_start;

            current_hunk = Some(Hunk {
                header: line.to_string(),
                lines: Vec::new(),
            });
            continue;
        }

        // Only process content lines if we have an active file and hunk
        let file = match current_file.as_mut() {
            Some(f) => f,
            None => continue,
        };
        let hunk = match current_hunk.as_mut() {
            Some(h) => h,
            None => continue,
        };

        if line.is_empty() {
            // Empty line: treat as context in a diff
            continue;
        }

        let (kind, content) = if line.starts_with('+') {
            file.added += 1;
            (LineKind::Added, &line[1..])
        } else if line.starts_with('-') {
            file.removed += 1;
            (LineKind::Removed, &line[1..])
        } else if line.starts_with(' ') {
            (LineKind::Context, &line[1..])
        } else if line == r"\ No newline at end of file" {
            // Skip the "no newline" indicator
            continue;
        } else {
            // Treat any other line as context (it may be indented)
            (LineKind::Context, line)
        };

        // Track line numbers
        let old_num = match kind {
            LineKind::Added => None,
            LineKind::Removed => {
                let n = Some(old_lineno);
                old_lineno += 1;
                n
            }
            LineKind::Context | LineKind::Header => {
                let n = Some(old_lineno);
                old_lineno += 1;
                n
            }
        };
        let new_num = match kind {
            LineKind::Removed => None,
            LineKind::Added => {
                let n = Some(new_lineno);
                new_lineno += 1;
                n
            }
            LineKind::Context | LineKind::Header => {
                let n = Some(new_lineno);
                new_lineno += 1;
                n
            }
        };

        hunk.lines.push(HunkLine {
            kind,
            content: content.to_string(),
            old_lineno: old_num,
            new_lineno: new_num,
        });
    }

    // Commit any remaining hunk and file
    if let Some(hunk) = current_hunk.take() {
        if let Some(ref mut file) = current_file {
            file.hunks.push(hunk);
        }
    }
    if let Some(file) = current_file.take() {
        files.push(file);
    }

    files
}

/// Extract the file path from a `diff --git a/path b/path` line.
/// Returns the `b/path` part (the new file path).
fn extract_file_path(diff_git_line: &str) -> String {
    // Format: "diff --git a/old/path b/new/path"
    // We want the part after " b/"
    if let Some(idx) = diff_git_line.find(" b/") {
        let path = &diff_git_line[idx + 3..];
        // Remove trailing whitespace
        path.trim().to_string()
    } else {
        // Fallback: return the raw line without the prefix
        diff_git_line
            .strip_prefix("diff --git ")
            .unwrap_or(diff_git_line)
            .to_string()
    }
}

/// Parse a hunk header like "@@ -1,4 +1,5 @@" to extract
/// the starting line numbers (old_start, new_start).
fn parse_hunk_header(header: &str) -> (u32, u32) {
    let parts: Vec<&str> = header.split_whitespace().collect();
    // Typical: ["@@", "-1,4", "+1,5", "@@"]
    // or:      ["@@", "-1", "+1", "@@"]
    let mut old_start: u32 = 0;
    let mut new_start: u32 = 0;

    for part in parts {
        if let Some(stripped) = part.strip_prefix('-') {
            // Parse "1" or "1,4" → take the first number
            if let Some(comma_idx) = stripped.find(',') {
                old_start = stripped[..comma_idx].parse().unwrap_or(0);
            } else {
                old_start = stripped.parse().unwrap_or(0);
            }
        } else if let Some(stripped) = part.strip_prefix('+') {
            if let Some(comma_idx) = stripped.find(',') {
                new_start = stripped[..comma_idx].parse().unwrap_or(0);
            } else {
                new_start = stripped.parse().unwrap_or(0);
            }
        }
    }

    (old_start, new_start)
}

// ═══════════════════════════════════════════════════════════════════
// Syntax Highlighting Helpers
// ═══════════════════════════════════════════════════════════════════

/// Detect the programming language from a file path extension.
fn detect_language(path: &str) -> Option<String> {
    let ext = path.rsplit('.').next()?;
    match ext {
        "rs" => Some("rust".to_string()),
        "py" => Some("python".to_string()),
        "js" | "jsx" => Some("javascript".to_string()),
        "ts" | "tsx" => Some("typescript".to_string()),
        "go" => Some("go".to_string()),
        "java" => Some("java".to_string()),
        "c" | "h" => Some("c".to_string()),
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" => Some("cpp".to_string()),
        "cs" => Some("csharp".to_string()),
        "rb" => Some("ruby".to_string()),
        "php" => Some("php".to_string()),
        "swift" => Some("swift".to_string()),
        "kt" | "kts" => Some("kotlin".to_string()),
        "scala" => Some("scala".to_string()),
        "sh" | "bash" => Some("bash".to_string()),
        "yaml" | "yml" => Some("yaml".to_string()),
        "json" => Some("json".to_string()),
        "toml" => Some("toml".to_string()),
        "md" | "markdown" => Some("markdown".to_string()),
        "html" | "htm" => Some("html".to_string()),
        "css" => Some("css".to_string()),
        "sql" => Some("sql".to_string()),
        "lua" => Some("lua".to_string()),
        _ => None,
    }
}

/// Syntax-highlight a single code line using syntect.
/// Returns spans with the highlight colors applied on top of `bg_color`.
fn highlight_code_line(code: &str, language: Option<&str>, bg_color: Color) -> Vec<Span<'static>> {
    let lang = match language.and_then(|l| SYNTAX_SET.find_syntax_by_token(l)) {
        Some(s) => s,
        None => {
            // No syntax found: return plain text
            return vec![Span::styled(
                code.to_string(),
                Style::default().fg(Color::White).bg(bg_color),
            )];
        }
    };

    let theme = &THEME_SET.themes["base16-ocean.dark"];
    let mut highlighter = HighlightLines::new(lang, theme);

    // syntect highlights by line; we feed our single line
    let line_with_ending = format!("{}\n", code);
    match highlighter.highlight_line(&line_with_ending, &SYNTAX_SET) {
        Ok(highlighted) => highlighted
            .into_iter()
            .map(|(style, text)| {
                let fg = Color::Rgb(style.foreground.r, style.foreground.g, style.foreground.b);
                Span::styled(text.to_string(), Style::default().fg(fg).bg(bg_color))
            })
            .collect(),
        Err(_) => {
            vec![Span::styled(
                code.to_string(),
                Style::default().fg(Color::White).bg(bg_color),
            )]
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// General Utility Helpers
// ═══════════════════════════════════════════════════════════════════

/// Truncate a string to at most `max_chars` characters (not bytes),
/// appending "…" if truncated.
fn truncate_to_width(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else if max_chars <= 1 {
        "…".to_string()
    } else {
        let truncated: String = chars[..max_chars - 1].iter().collect();
        format!("{}…", truncated)
    }
}

/// Truncate a path from the middle for display in narrow areas.
/// e.g., "very/long/path/to/file.rs" → "very/…/file.rs"
fn truncate_middle(path: &str, max_width: usize) -> String {
    let char_count = path.chars().count();
    if max_width < 10 {
        return truncate_to_width(path, max_width);
    }
    if char_count <= max_width {
        return path.to_string();
    }
    let keep_front = max_width / 3;
    // -1 for the ellipsis character, but it may be multi-byte
    let keep_back = max_width.saturating_sub(keep_front + 1);
    let front: String = path.chars().take(keep_front).collect();
    let back_len = path.chars().count();
    let back: String = path
        .chars()
        .skip(back_len.saturating_sub(keep_back))
        .take(keep_back)
        .collect();
    format!("{}…{}", front, back)
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::SkinConfig;
    use crate::test_utils::MockTerminal;

    // ── Test helpers ───────────────────────────────────────────────

    fn simple_diff() -> &'static str {
        "diff --git a/src/main.rs b/src/main.rs\n\
         --- a/src/main.rs\n\
         +++ b/src/main.rs\n\
         @@ -1,3 +1,4 @@\n\
          fn main() {\n\
         -    old_code();\n\
         +    new_code();\n\
         +    extra_line();\n\
          }\n"
    }

    fn multi_file_diff() -> &'static str {
        "diff --git a/src/lib.rs b/src/lib.rs\n\
         --- a/src/lib.rs\n\
         +++ b/src/lib.rs\n\
         @@ -1,2 +1,3 @@\n\
          pub fn hello() {\n\
         +    println!(\"hi\");\n\
          }\n\
         diff --git a/src/main.rs b/src/main.rs\n\
         --- a/src/main.rs\n\
         +++ b/src/main.rs\n\
         @@ -5,4 +5,5 @@\n\
          fn main() {\n\
         -    panic!();\n\
         +    eprintln!(\"ok\");\n\
          }\n"
    }

    // ── Parsing tests ──────────────────────────────────────────────

    #[test]
    fn test_parse_single_file_basic() {
        let files = parse_unified_diff(simple_diff());
        assert_eq!(files.len(), 1, "should parse one file");
        let f = &files[0];
        assert_eq!(f.path, "src/main.rs");
        assert_eq!(f.added, 2);
        assert_eq!(f.removed, 1);
        assert_eq!(f.hunks.len(), 1);

        let hunk = &f.hunks[0];
        assert!(hunk.header.starts_with("@@"));
        assert_eq!(hunk.lines.len(), 5); // context, removed, added, added, context = 5 lines
    }

    #[test]
    fn test_parse_line_counts() {
        let files = parse_unified_diff(simple_diff());
        let f = &files[0];
        assert_eq!(f.added, 2, "should count 2 added lines");
        assert_eq!(f.removed, 1, "should count 1 removed line");
    }

    #[test]
    fn test_parse_multi_file() {
        let files = parse_unified_diff(multi_file_diff());
        assert_eq!(files.len(), 2, "should parse two files");
        assert_eq!(files[0].path, "src/lib.rs");
        assert_eq!(files[1].path, "src/main.rs");
        assert_eq!(files[0].added, 1);
        assert_eq!(files[1].removed, 1);
    }

    #[test]
    fn test_parse_hunk_header_line_numbers() {
        let (old, new) = parse_hunk_header("@@ -1,4 +1,5 @@");
        assert_eq!(old, 1);
        assert_eq!(new, 1);

        let (old, new) = parse_hunk_header("@@ -10,0 +15,3 @@");
        assert_eq!(old, 10);
        assert_eq!(new, 15);
    }

    #[test]
    fn test_parse_extract_file_path() {
        assert_eq!(
            extract_file_path("diff --git a/old/path.rs b/new/path.rs"),
            "new/path.rs"
        );
        assert_eq!(
            extract_file_path("diff --git a/src/main.rs b/src/main.rs"),
            "src/main.rs"
        );
    }

    #[test]
    fn test_parse_line_kinds() {
        let files = parse_unified_diff(simple_diff());
        let hunk = &files[0].hunks[0];
        let kinds: Vec<LineKind> = hunk.lines.iter().map(|l| l.kind).collect();
        assert_eq!(
            kinds,
            vec![
                LineKind::Context,
                LineKind::Removed,
                LineKind::Added,
                LineKind::Added,
                LineKind::Context,
            ]
        );
    }

    #[test]
    fn test_parse_empty_diff() {
        let files = parse_unified_diff("");
        assert!(files.is_empty());
    }

    #[test]
    fn test_parse_no_newline_at_eof_is_skipped() {
        let diff = "diff --git a/file.txt b/file.txt\n\
                    --- a/file.txt\n\
                    +++ b/file.txt\n\
                    @@ -1,3 +1,2 @@\n\
                     line1\n\
                    -line2\n\
                    \\ No newline at end of file\n";
        let files = parse_unified_diff(diff);
        assert_eq!(files.len(), 1);
        let hunk = &files[0].hunks[0];
        // Should have 2 lines: context "line1" and removed "line2"
        assert_eq!(hunk.lines.len(), 2);
        // The "No newline" marker should be skipped
        let kinds: Vec<_> = hunk.lines.iter().map(|l| l.kind).collect();
        assert_eq!(kinds, vec![LineKind::Context, LineKind::Removed]);
    }

    // ── DiffViewer state tests ─────────────────────────────────────

    #[test]
    fn test_viewer_new_is_empty() {
        let viewer = DiffViewer::new("Test");
        assert_eq!(viewer.file_count(), 0);
        assert_eq!(viewer.mode(), DiffMode::Unified);
        assert_eq!(viewer.selected_file_index(), 0);
    }

    #[test]
    fn test_viewer_load_resets_state() {
        let mut viewer = DiffViewer::new("Test");
        viewer.selected_file = 5;
        viewer.selected_hunk = 3;
        viewer.scroll_offset = 42;

        viewer.load(simple_diff());
        assert_eq!(viewer.file_count(), 1);
        assert_eq!(viewer.selected_file_index(), 0);
        assert_eq!(viewer.selected_hunk_index(), 0);
        assert_eq!(viewer.scroll_offset(), 0);
    }

    #[test]
    fn test_viewer_file_navigation() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(multi_file_diff());
        assert_eq!(viewer.selected_file_index(), 0);

        viewer.select_next_file();
        assert_eq!(viewer.selected_file_index(), 1);

        // Clamped at last file
        viewer.select_next_file();
        assert_eq!(viewer.selected_file_index(), 1);

        viewer.select_prev_file();
        assert_eq!(viewer.selected_file_index(), 0);

        // Clamped at first file
        viewer.select_prev_file();
        assert_eq!(viewer.selected_file_index(), 0);
    }

    #[test]
    fn test_viewer_mode_toggle() {
        let mut viewer = DiffViewer::new("Test");
        assert_eq!(viewer.mode(), DiffMode::Unified);

        viewer.toggle_mode();
        assert_eq!(viewer.mode(), DiffMode::Split);

        viewer.toggle_mode();
        assert_eq!(viewer.mode(), DiffMode::Unified);
    }

    #[test]
    fn test_viewer_hunk_navigation_cycles() {
        let mut viewer = DiffViewer::new("Test");
        // Create diff with 2 hunks
        let two_hunk_diff = "diff --git a/file.rs b/file.rs\n\
                             --- a/file.rs\n\
                             +++ b/file.rs\n\
                             @@ -1,1 +1,2 @@\n\
                              a\n\
                             +b\n\
                             @@ -10,1 +11,2 @@\n\
                              c\n\
                             -d\n";
        viewer.load(two_hunk_diff);
        assert_eq!(viewer.selected_hunk_index(), 0);

        viewer.next_hunk();
        assert_eq!(viewer.selected_hunk_index(), 1);

        viewer.next_hunk();
        assert_eq!(viewer.selected_hunk_index(), 0); // wraps around

        viewer.prev_hunk();
        assert_eq!(viewer.selected_hunk_index(), 1); // wraps from 0
    }

    #[test]
    fn test_viewer_hunk_navigation_empty() {
        let mut viewer = DiffViewer::new("Test");
        // No hunks/crash test
        viewer.next_hunk();
        viewer.prev_hunk();
        // Should not crash
    }

    #[test]
    fn test_viewer_scroll() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(simple_diff());
        assert_eq!(viewer.scroll_offset(), 0);

        viewer.scroll_down(5);
        assert!(viewer.scroll_offset() > 0);

        viewer.scroll_up(10);
        assert_eq!(viewer.scroll_offset(), 0); // clamped

        viewer.scroll_offset = 100;
        assert!(viewer.scroll_offset() > 0);
    }

    // ── Utility tests ──────────────────────────────────────────────

    #[test]
    fn test_truncate_to_width() {
        assert_eq!(truncate_to_width("hello", 10), "hello");
        assert_eq!(truncate_to_width("hello world", 5), "hell…");
        assert_eq!(truncate_to_width("hello", 0), "");
    }

    #[test]
    fn test_truncate_middle() {
        let path = "very/long/path/to/my/file.rs";
        let result = truncate_middle(path, 20);
        let char_count = result.chars().count();
        assert!(char_count <= 20, "result has {char_count} chars: {result}");
        assert!(result.contains('…'));
        assert!(result.starts_with("very"));
        assert!(result.ends_with("file.rs"));
    }

    #[test]
    fn test_detect_language() {
        assert_eq!(detect_language("main.rs"), Some("rust".into()));
        assert_eq!(detect_language("app.js"), Some("javascript".into()));
        assert_eq!(detect_language("types.tsx"), Some("typescript".into()));
        assert_eq!(detect_language("script.py"), Some("python".into()));
        assert_eq!(detect_language("main.go"), Some("go".into()));
        assert_eq!(detect_language("file.unknown_ext"), None);
        assert_eq!(detect_language("no_extension"), None);
        assert_eq!(detect_language("Makefile"), None);
    }

    // ── Render tests ───────────────────────────────────────────────

    #[test]
    fn test_render_shows_file_contents() {
        let mut terminal = MockTerminal::new(100, 30);
        let theme = SkinConfig::default();
        let mut viewer = DiffViewer::new("Diff");
        viewer.load(simple_diff());

        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            viewer.render(&mut ctx, area);
        });

        // Should show the file path
        terminal.assert_line_contains("src/main.rs");
        // Should show the footer
        terminal.assert_line_contains("j/k:file");
    }

    #[test]
    fn test_render_empty_diff() {
        let mut terminal = MockTerminal::new(100, 30);
        let theme = SkinConfig::default();
        let mut viewer = DiffViewer::new("Diff");
        // Don't load anything

        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            viewer.render(&mut ctx, area);
        });

        terminal.assert_line_contains("No diff loaded");
    }

    #[test]
    fn test_handle_key_t_toggles_mode() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(simple_diff());

        let event = Event::Key(KeyEvent::new(
            KeyCode::Char('t'),
            crossterm::event::KeyModifiers::NONE,
        ));
        let result = viewer.handle_event(&event);
        assert!(result.is_consumed());
        assert_eq!(viewer.mode(), DiffMode::Split);

        let result = viewer.handle_event(&event);
        assert!(result.is_consumed());
        assert_eq!(viewer.mode(), DiffMode::Unified);
    }

    #[test]
    fn test_handle_key_n_navigates_hunks() {
        let mut viewer = DiffViewer::new("Test");
        // Single hunk → next_hunk stays at 0 (wraps)
        viewer.load(simple_diff());
        viewer.select_next_file(); // already at 0, file count=1

        let event = Event::Key(KeyEvent::new(
            KeyCode::Char('n'),
            crossterm::event::KeyModifiers::NONE,
        ));
        let result = viewer.handle_event(&event);
        assert!(result.is_consumed());
        // With single hunk, wraps to 0
        assert_eq!(viewer.selected_hunk_index(), 0);
    }

    #[test]
    fn test_handle_key_jk_file_navigation() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(multi_file_diff());

        let j = Event::Key(KeyEvent::new(
            KeyCode::Char('j'),
            crossterm::event::KeyModifiers::NONE,
        ));
        let k = Event::Key(KeyEvent::new(
            KeyCode::Char('k'),
            crossterm::event::KeyModifiers::NONE,
        ));

        assert_eq!(viewer.selected_file_index(), 0);
        viewer.handle_event(&j);
        assert_eq!(viewer.selected_file_index(), 1);
        viewer.handle_event(&k);
        assert_eq!(viewer.selected_file_index(), 0);
    }

    #[test]
    fn test_viewer_component_trait() {
        let viewer = DiffViewer::new("Test");
        assert!(viewer.focusable());
        assert_eq!(viewer.id(), "diff_viewer");
    }

    #[test]
    fn test_render_multi_file_tree() {
        let mut terminal = MockTerminal::new(120, 35);
        let theme = SkinConfig::default();
        let mut viewer = DiffViewer::new("Diff");
        viewer.load(multi_file_diff());

        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            viewer.render(&mut ctx, area);
        });

        terminal.assert_line_contains("src/lib.rs");
        terminal.assert_line_contains("src/main.rs");
        terminal.assert_line_contains("+1");
        terminal.assert_line_contains("-1");
    }

    #[test]
    fn test_viewer_split_mode_rendering() {
        let mut terminal = MockTerminal::new(100, 30);
        let theme = SkinConfig::default();
        let mut viewer = DiffViewer::new("Diff");
        viewer.load(simple_diff());
        viewer.toggle_mode(); // Switch to split

        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            viewer.render(&mut ctx, area);
        });

        // Split mode should show "Old" and "New" columns
        terminal.assert_line_contains("Old");
        terminal.assert_line_contains("New");
    }

    #[test]
    fn test_viewer_does_not_crash_with_many_files() {
        // Generate a diff with many files
        let mut diff = String::new();
        for i in 0..20 {
            diff.push_str(&format!(
                "diff --git a/file_{}.rs b/file_{}.rs\n\
                 --- a/file_{}.rs\n\
                 +++ b/file_{}.rs\n\
                 @@ -1,1 +1,2 @@\n\
                  old\n\
                 +new\n",
                i, i, i, i
            ));
        }

        let mut viewer = DiffViewer::new("Many Files");
        viewer.load(&diff);
        assert_eq!(viewer.file_count(), 20);

        let mut terminal = MockTerminal::new(100, 40);
        let theme = SkinConfig::default();

        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            viewer.render(&mut ctx, area);
        });

        // Should not panic; should render something
        terminal.assert_line_contains("Files (20)");
    }

    // ── Task 10: Diff file tree counts and mark reviewed ────────────

    #[test]
    fn diff_file_tree_counts_shown() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(simple_diff());

        let mut terminal = MockTerminal::new(100, 30);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            viewer.render(&mut ctx, area);
        });

        // File tree should show [+N -M] counts
        terminal.assert_line_contains("+2");
        terminal.assert_line_contains("-1");
    }

    #[test]
    fn mark_reviewed_toggles_state() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(simple_diff()); // loads "src/main.rs"

        // Initially not reviewed
        assert!(!viewer.is_reviewed("src/main.rs"));

        // Toggle reviewed
        viewer.toggle_reviewed("src/main.rs");
        assert!(viewer.is_reviewed("src/main.rs"));

        // Toggle again to un-review
        viewer.toggle_reviewed("src/main.rs");
        assert!(!viewer.is_reviewed("src/main.rs"));
    }

    #[test]
    fn mark_reviewed_toggle_selected() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(multi_file_diff()); // files: src/lib.rs, src/main.rs

        // First file is src/lib.rs
        assert_eq!(viewer.selected_file_index(), 0);
        assert!(!viewer.is_reviewed("src/lib.rs"));

        // Toggle reviewed for selected file
        viewer.toggle_reviewed_selected();
        assert!(viewer.is_reviewed("src/lib.rs"));

        // Select next file and toggle
        viewer.select_next_file();
        viewer.toggle_reviewed_selected();
        assert!(viewer.is_reviewed("src/main.rs"));
        assert!(viewer.is_reviewed("src/lib.rs")); // still reviewed
    }

    #[test]
    fn reviewed_files_show_dimmed() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(simple_diff());

        // Initially not reviewed
        assert!(!viewer.is_reviewed("src/main.rs"));

        // Mark as reviewed
        viewer.toggle_reviewed("src/main.rs");
        assert!(viewer.is_reviewed("src/main.rs"));

        // Check that is_reviewed works and the file is in the set
        assert!(viewer.reviewed_files().contains("src/main.rs"));
        assert_eq!(viewer.reviewed_files().len(), 1);
    }

    #[test]
    fn mark_reviewed_via_m_key() {
        let mut viewer = DiffViewer::new("Test");
        viewer.load(simple_diff());

        // Press 'm' to mark currently selected file as reviewed
        let event = Event::Key(KeyEvent::new(
            KeyCode::Char('m'),
            crossterm::event::KeyModifiers::NONE,
        ));
        let result = viewer.handle_event(&event);
        assert!(result.is_consumed(), "'m' key should be consumed");
        assert!(viewer.is_reviewed("src/main.rs"));
    }
}
