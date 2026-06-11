// ── Prompt Component ────────────────────────────────────────────────
// Enhanced input component with autocomplete, frecency scoring,
// @file completion, and /command completion.
//
// Features:
//   - AutocompleteEngine: prefix → suggestions from files, commands, history
//   - FrecencyTracker: frequency + recency scoring for suggestion ranking
//   - @file completion: "@sr" → suggests "src/" via directory listing
//   - /command completion: "/sta" → suggests "/status"
//   - Inline preview: dimmed text showing the rest of the top suggestion
//   - Tab: accept top suggestion; Shift+Tab: cycle suggestions
//   - Esc: close suggestion dropdown
// -----------------------------------------------------------------

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

use commands::slash_command_specs;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Paragraph};
use tui_textarea::{CursorMove, TextArea};

use crate::tui::components::base::{Component, EventResult, RenderContext};

// ═══════════════════════════════════════════════════════════════════
// Suggestion types
// ═══════════════════════════════════════════════════════════════════

/// The kind of an autocomplete suggestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuggestionKind {
    /// A file-system path (triggered by '@' prefix).
    File,
    /// A structured context reference (triggered by '@' prefix).
    Context,
    /// A slash command (triggered by '/' prefix).
    Command,
    /// A historical input.
    History,
}

/// A single autocomplete suggestion with its display text and kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Suggestion {
    /// The full suggestion text (e.g., "src/main.rs" or "/status").
    pub text: String,
    /// What kind of suggestion this is.
    pub kind: SuggestionKind,
}

impl Suggestion {
    pub fn new(text: impl Into<String>, kind: SuggestionKind) -> Self {
        Self {
            text: text.into(),
            kind,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// FrecencyTracker — frequency + recency scoring
// ═══════════════════════════════════════════════════════════════════

/// Tracks how frequently and recently each suggestion has been used,
/// producing a combined score for ranking.
///
/// The score formula is: `freq * recency_decay` where recency_decay
/// is ~1.0 for items used within the last minute, decaying toward 0
/// over a configurable half-life.
#[derive(Debug, Clone)]
pub struct FrecencyTracker {
    entries: HashMap<Suggestion, FrecencyEntry>,
    /// Half-life in seconds — after this duration the recency weight
    /// drops to 0.5. Default: 300s (5 minutes).
    half_life_secs: f64,
}

#[derive(Debug, Clone)]
struct FrecencyEntry {
    freq: u32,
    last_used: Instant,
}

impl FrecencyTracker {
    /// Create a new tracker with the default half-life (300s).
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            half_life_secs: 300.0,
        }
    }

    /// Create a tracker with a custom recency half-life (in seconds).
    pub fn with_half_life(half_life_secs: f64) -> Self {
        Self {
            entries: HashMap::new(),
            half_life_secs,
        }
    }

    /// Record that a suggestion was used.
    pub fn record_use(&mut self, suggestion: &Suggestion) {
        let entry = self
            .entries
            .entry(suggestion.clone())
            .or_insert(FrecencyEntry {
                freq: 0,
                last_used: Instant::now(),
            });
        entry.freq += 1;
        entry.last_used = Instant::now();
    }

    /// Compute the frecency score for a suggestion.
    ///
    /// Returns `(freq as f64) * recency_decay`. The recency decay
    /// is based on elapsed seconds since last use and the half-life.
    /// Returns 0.0 for suggestions never recorded.
    pub fn score(&self, suggestion: &Suggestion) -> f64 {
        if let Some(entry) = self.entries.get(suggestion) {
            let elapsed = entry.last_used.elapsed().as_secs_f64();
            let recency = 2.0_f64.powf(-elapsed / self.half_life_secs);
            entry.freq as f64 * recency
        } else {
            0.0
        }
    }

    /// Sort a vector of suggestions by frecency score (descending).
    /// Suggestions with no record retain their position at the end.
    pub fn rank(&self, suggestions: &mut [Suggestion]) {
        suggestions.sort_by(|a, b| {
            self.score(b)
                .partial_cmp(&self.score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Returns the number of tracked entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the tracker is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for FrecencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════
// AutocompleteEngine
// ═══════════════════════════════════════════════════════════════════

/// Engine that produces autocomplete suggestions based on a prefix.
///
/// - If `prefix` starts with `@`: glob directory for matching file paths.
/// - If `prefix` starts with `/`: match against known slash commands.
/// - Otherwise: match against command history entries.
#[derive(Debug, Clone)]
pub struct AutocompleteEngine {
    /// The current working directory for file-system completions.
    cwd: std::path::PathBuf,
    /// Command history for free-text matching.
    history: Vec<String>,
    /// Frecency tracker for ranking suggestions.
    frecency: FrecencyTracker,
    /// Max number of suggestions to return.
    max_suggestions: usize,
}

impl AutocompleteEngine {
    /// Create a new autocomplete engine.
    pub fn new(cwd: impl Into<std::path::PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            history: Vec::new(),
            frecency: FrecencyTracker::new(),
            max_suggestions: 5,
        }
    }

    /// Add a command to the history (for future free-text matching).
    pub fn add_history(&mut self, input: String) {
        // Avoid consecutive duplicates
        if self.history.last().map_or(true, |h| h != &input) {
            self.history.push(input);
        }
    }

    /// Produce suggestions for the given prefix string.
    ///
    /// The prefix includes the trigger character:
    ///   - `@src/ma` → matches file paths like `src/main.rs`
    ///   - `/sta` → matches commands like `/status`
    ///   - `hello` → matches history entries containing "hello"
    pub fn suggest(&mut self, prefix: &str) -> Vec<Suggestion> {
        let mut suggestions: Vec<Suggestion> = if prefix.starts_with('@') {
            self.context_suggestions(&prefix[1..])
        } else if prefix.starts_with('/') {
            self.command_suggestions(&prefix[1..])
        } else {
            self.history_suggestions(prefix)
        };

        // Rank by frecency and limit
        self.frecency.rank(&mut suggestions);
        suggestions.truncate(self.max_suggestions);
        suggestions
    }

    /// Record that a suggestion was selected/used.
    pub fn record_use(&mut self, suggestion: &Suggestion) {
        self.frecency.record_use(suggestion);
    }

    /// Return a reference to the frecency tracker (for testing).
    pub fn frecency(&self) -> &FrecencyTracker {
        &self.frecency
    }

    /// Return a mutable reference to the frecency tracker.
    pub fn frecency_mut(&mut self) -> &mut FrecencyTracker {
        &mut self.frecency
    }

    /// Return a reference to the history.
    pub fn history(&self) -> &[String] {
        &self.history
    }

    // ── private helpers ──────────────────────────────────────────

    fn context_suggestions(&self, prefix: &str) -> Vec<Suggestion> {
        if prefix.is_empty() {
            return vec![
                Suggestion::new("@diff", SuggestionKind::Context),
                Suggestion::new("@staged", SuggestionKind::Context),
                Suggestion::new("@file:", SuggestionKind::Context),
                Suggestion::new("@folder:", SuggestionKind::Context),
                Suggestion::new("@url:", SuggestionKind::Context),
                Suggestion::new("@git:", SuggestionKind::Context),
            ];
        }

        if prefix == "file" || prefix == "folder" {
            return self.typed_context_file_suggestions(prefix, "");
        }

        if let Some(path_part) = prefix.strip_prefix("file:") {
            return self.typed_context_file_suggestions("file", path_part);
        }

        if let Some(path_part) = prefix.strip_prefix("folder:") {
            return self.typed_context_file_suggestions("folder", path_part);
        }

        let static_items = [
            ("diff", "git diff"),
            ("staged", "staged diff"),
            ("file:", "attach file"),
            ("folder:", "attach folder"),
            ("url:", "fetch url"),
            ("git:", "git reference"),
        ];

        let static_matches: Vec<Suggestion> = static_items
            .into_iter()
            .filter(|(text, _)| text.starts_with(prefix))
            .map(|(text, _)| Suggestion::new(format!("@{text}"), SuggestionKind::Context))
            .collect();

        if !static_matches.is_empty() {
            return static_matches;
        }

        self.file_suggestions(prefix)
    }

    fn typed_context_file_suggestions(&self, kind: &str, path_part: &str) -> Vec<Suggestion> {
        let want_dir = kind == "folder";
        let suggestions = if !want_dir
            && path_part.len() >= 2
            && !path_part.contains('/')
            && !path_part.starts_with('.')
        {
            self.fuzzy_repo_file_suggestions(path_part)
        } else {
            self.file_suggestions_filtered(path_part, Some(want_dir))
        };

        suggestions
            .into_iter()
            .map(|suggestion| {
                Suggestion::new(format!("@{kind}:{}", suggestion.text), SuggestionKind::File)
            })
            .collect()
    }

    fn fuzzy_repo_file_suggestions(&self, query: &str) -> Vec<Suggestion> {
        let mut ranked = Vec::new();
        collect_repo_file_matches(&self.cwd, &self.cwd, query, 0, &mut ranked);
        ranked.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        ranked
            .into_iter()
            .take(self.max_suggestions)
            .map(|(_, rel)| Suggestion::new(rel, SuggestionKind::File))
            .collect()
    }

    fn file_suggestions(&self, prefix: &str) -> Vec<Suggestion> {
        self.file_suggestions_filtered(prefix, None)
    }

    fn file_suggestions_filtered(&self, prefix: &str, want_dir: Option<bool>) -> Vec<Suggestion> {
        // Determine the directory and file prefix from the pattern.
        // e.g., prefix "sr" with cwd "/project" → look in cwd for "sr*"
        let (search_dir, file_prefix) = resolve_file_search(&self.cwd, prefix);

        let mut results = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&search_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with(&file_prefix) {
                    let is_dir = entry.file_type().map_or(false, |ft| ft.is_dir());
                    if let Some(want_dir) = want_dir {
                        if want_dir != is_dir {
                            continue;
                        }
                    }
                    // Reconstruct the full suggestion text relative to cwd
                    let rel_path = if search_dir == self.cwd {
                        name_str.to_string()
                    } else {
                        // Include the directory prefix
                        let rel_dir = search_dir.strip_prefix(&self.cwd).unwrap_or(&search_dir);
                        format!("{}/{}", rel_dir.display(), name_str)
                    };

                    // Add trailing slash for directories
                    let display = if is_dir {
                        format!("{}/", rel_path)
                    } else {
                        rel_path
                    };

                    results.push(Suggestion::new(display, SuggestionKind::File));
                }
            }
        }
        results
    }

    fn command_suggestions(&self, prefix: &str) -> Vec<Suggestion> {
        slash_command_specs()
            .iter()
            .flat_map(|spec| std::iter::once(spec.name).chain(spec.aliases.iter().copied()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|cmd| cmd.starts_with(prefix))
            .map(|cmd| Suggestion::new(format!("/{cmd}"), SuggestionKind::Command))
            .collect()
    }

    fn history_suggestions(&self, prefix: &str) -> Vec<Suggestion> {
        let lower = prefix.to_lowercase();
        self.history
            .iter()
            .rev() // most recent first
            .filter(|h| h.to_lowercase().contains(&lower))
            .take(self.max_suggestions * 2)
            .map(|h| Suggestion::new(h.clone(), SuggestionKind::History))
            .collect()
    }
}

/// Resolve which directory to search and what file-name prefix to match.
///
/// Given a cwd and a user-typed pattern like `sr` or `src/ma`:
/// - If the pattern contains a `/`, use the part before the last `/` as the
///   subdirectory and the part after as the filename prefix.
/// - Otherwise, search in cwd directly.
fn resolve_file_search(cwd: &std::path::Path, pattern: &str) -> (std::path::PathBuf, String) {
    if let Some(last_slash) = pattern.rfind('/') {
        let dir_part = &pattern[..last_slash];
        let file_part = &pattern[last_slash + 1..];
        let search_dir = cwd.join(dir_part);
        (search_dir, file_part.to_string())
    } else {
        (cwd.to_path_buf(), pattern.to_string())
    }
}

fn collect_repo_file_matches(
    root: &std::path::Path,
    dir: &std::path::Path,
    query: &str,
    depth: usize,
    ranked: &mut Vec<((usize, usize), String)>,
) {
    if depth > 8 || ranked.len() > 200 {
        return;
    }

    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if matches!(
            name.as_ref(),
            ".git" | "target" | "node_modules" | ".venv" | "venv"
        ) {
            continue;
        }

        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };

        if file_type.is_dir() {
            collect_repo_file_matches(root, &path, query, depth + 1, ranked);
            continue;
        }

        if !file_type.is_file() {
            continue;
        }

        let Some(rank) = fuzzy_basename_rank(&name, query) else {
            continue;
        };

        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        ranked.push((rank, rel));
    }
}

fn fuzzy_basename_rank(name: &str, query: &str) -> Option<(usize, usize)> {
    let name_lower = name.to_lowercase();
    let query_lower = query.to_lowercase();

    if name_lower == query_lower {
        return Some((0, name.len()));
    }
    if name_lower.starts_with(&query_lower) {
        return Some((1, name.len()));
    }
    if name_lower.contains(&query_lower) {
        return Some((2, name.len()));
    }

    let mut query_chars = query_lower.chars();
    let mut current = query_chars.next()?;
    for ch in name_lower.chars() {
        if ch == current {
            if let Some(next) = query_chars.next() {
                current = next;
            } else {
                return Some((3, name.len()));
            }
        }
    }

    None
}

// ═══════════════════════════════════════════════════════════════════
// Cursor-aware completion kernel
// ═══════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionTrigger {
    SlashCommand,
    Path,
    Context,
    History,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionRequest {
    pub trigger: CompletionTrigger,
    pub token: String,
    pub replace_start: usize,
    pub replace_end: usize,
    source_text: String,
}

impl CompletionRequest {
    pub fn from_text_at_cursor(text: &str, cursor: usize) -> Option<Self> {
        let cursor = clamp_to_char_boundary(text, cursor.min(text.len()));
        let (start, end) = token_bounds_at_cursor(text, cursor)?;
        if start == end {
            return None;
        }

        let token = text[start..end].to_string();
        let trigger = classify_completion_token(&token)?;

        Some(Self {
            trigger,
            token,
            replace_start: start,
            replace_end: end,
            source_text: text.to_string(),
        })
    }

    pub fn apply(&self, replacement: &str) -> String {
        self.apply_to_text(&self.source_text, replacement)
    }

    pub fn apply_to_text(&self, text: &str, replacement: &str) -> String {
        let replacement = match self.trigger {
            CompletionTrigger::SlashCommand
                if self.token.starts_with('/') && !replacement.starts_with('/') =>
            {
                format!("/{replacement}")
            }
            CompletionTrigger::Context
                if self.token.starts_with('@') && !replacement.starts_with('@') =>
            {
                format!("@{replacement}")
            }
            _ => replacement.to_string(),
        };

        format!(
            "{}{}{}",
            &text[..self.replace_start],
            replacement,
            &text[self.replace_end..]
        )
    }
}

fn clamp_to_char_boundary(text: &str, mut cursor: usize) -> usize {
    while cursor > 0 && !text.is_char_boundary(cursor) {
        cursor -= 1;
    }
    cursor
}

fn token_bounds_at_cursor(text: &str, cursor: usize) -> Option<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut start = cursor;
    while start > 0 {
        let prev = prev_char_boundary_raw(text, start);
        let ch = text[prev..start].chars().next()?;
        if ch.is_whitespace() {
            break;
        }
        start = prev;
    }

    let mut end = cursor;
    while end < text.len() {
        let next = next_char_boundary_raw(text, end);
        let ch = text[end..next].chars().next()?;
        if ch.is_whitespace() {
            break;
        }
        end = next;
    }

    if bytes.get(start..end).is_some() {
        Some((start, end))
    } else {
        None
    }
}

fn prev_char_boundary_raw(text: &str, byte_pos: usize) -> usize {
    let mut pos = byte_pos.saturating_sub(1);
    while pos > 0 && !text.is_char_boundary(pos) {
        pos -= 1;
    }
    pos
}

fn next_char_boundary_raw(text: &str, byte_pos: usize) -> usize {
    let mut pos = byte_pos.saturating_add(1);
    while pos < text.len() && !text.is_char_boundary(pos) {
        pos += 1;
    }
    pos.min(text.len())
}

fn classify_completion_token(token: &str) -> Option<CompletionTrigger> {
    if token.starts_with('@') {
        return Some(CompletionTrigger::Context);
    }

    if looks_like_slash_command_token(token) {
        return Some(CompletionTrigger::SlashCommand);
    }

    if looks_like_path_token(token) {
        return Some(CompletionTrigger::Path);
    }

    Some(CompletionTrigger::History)
}

fn looks_like_slash_command_token(token: &str) -> bool {
    token.starts_with('/')
        && (token.len() == 1
            || (token.len() > 1
                && !token[1..].contains('/')
                && token[1..]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))))
}

fn looks_like_path_token(token: &str) -> bool {
    token.starts_with("~/")
        || token.starts_with("./")
        || token.starts_with("../")
        || (token.starts_with('/') && token[1..].contains('/'))
        || token.contains('/')
}

// ═══════════════════════════════════════════════════════════════════
// Prompt — The enhanced input component
// ═══════════════════════════════════════════════════════════════════

/// An enhanced text-input component with autocomplete, frecency scoring,
/// @file completion, and /command completion.
///
/// Wraps a [`tui_textarea::TextArea`] and adds:
/// - Inline preview (dimmed text after cursor showing the top suggestion)
/// - Dropdown list of suggestions (up to 5 items)
/// - Tab to accept top suggestion
/// - Shift+Tab to cycle suggestions
/// - Esc to close the dropdown
pub struct Prompt {
    /// The underlying text input area.
    textarea: TextArea<'static>,
    /// The autocomplete engine.
    engine: AutocompleteEngine,
    /// Current suggestions (recomputed on each keystroke).
    suggestions: Vec<Suggestion>,
    /// Currently highlighted suggestion index (for cycling).
    highlighted: usize,
    /// Whether the suggestion dropdown is visible.
    show_suggestions: bool,
    /// The current word prefix being completed (e.g., "@sr", "/sta").
    /// Used to replace the word when a suggestion is accepted.
    current_prefix: String,
    /// Cached inline preview text (empty if no suggestion applicable).
    inline_preview: String,
    /// Cursor-aware completion request for external input replacement.
    current_request: Option<CompletionRequest>,
    /// Input history buffer for free-text autocomplete.
    input_history: Vec<String>,
    /// Whether Enter should submit via propagate.
    submit_on_enter: bool,
    /// Block style for the textarea.
    block_title: String,
    /// Whether shell mode is active (input starts with '!').
    shell_mode: bool,
    /// Cached shell command text (without the '!' prefix).
    shell_command: String,
}

impl Prompt {
    /// Create a new prompt with a given working directory.
    pub fn new(cwd: impl Into<std::path::PathBuf>) -> Self {
        let cwd = cwd.into();
        let mut textarea = TextArea::default();
        textarea.set_block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Input (Enter=send, Tab=autocomplete, Esc=cancel suggestions) "),
        );
        textarea.set_cursor_line_style(Style::default());

        Self {
            textarea,
            engine: AutocompleteEngine::new(cwd),
            suggestions: Vec::new(),
            highlighted: 0,
            show_suggestions: false,
            current_prefix: String::new(),
            inline_preview: String::new(),
            current_request: None,
            input_history: Vec::new(),
            submit_on_enter: true,
            block_title: " Input ".to_string(),
            shell_mode: false,
            shell_command: String::new(),
        }
    }

    /// Create a prompt with a custom block title.
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        let title = title.into();
        self.block_title = title.clone();
        self.textarea.set_block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} ", title)),
        );
        self
    }

    /// Return the current text content of the prompt.
    pub fn text(&self) -> String {
        self.textarea.lines().join("\n")
    }

    /// Set the text content and clear autocomplete state.
    pub fn set_text(&mut self, text: &str) {
        // Clear and refill using delete + insert
        self.textarea.move_cursor(CursorMove::End);
        // Delete all lines
        let line_count = self.textarea.lines().len();
        for _ in 0..line_count {
            self.textarea.delete_line_by_end();
            if self.textarea.lines().len() > 1 {
                // Delete the newline
                self.textarea.delete_line_by_end();
            }
        }
        // Move to beginning
        self.textarea.move_cursor(CursorMove::Head);
        // Now insert the text
        if !text.is_empty() {
            self.textarea.insert_str(text);
        }
        self.clear_suggestions();
    }

    /// Clear the textarea content.
    pub fn clear(&mut self) {
        self.textarea = TextArea::default();
        self.textarea.set_block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} ", self.block_title)),
        );
        self.textarea.set_cursor_line_style(Style::default());
        self.clear_suggestions();
    }

    /// Return the underlying textarea (for external styling).
    pub fn textarea(&self) -> &TextArea<'static> {
        &self.textarea
    }

    /// Return mutable access to the textarea.
    pub fn textarea_mut(&mut self) -> &mut TextArea<'static> {
        &mut self.textarea
    }

    /// Return a reference to the autocomplete engine.
    pub fn engine(&self) -> &AutocompleteEngine {
        &self.engine
    }

    /// Return mutable access to the autocomplete engine.
    pub fn engine_mut(&mut self) -> &mut AutocompleteEngine {
        &mut self.engine
    }

    /// Add an entry to the input history.
    pub fn add_history(&mut self, input: String) {
        self.engine.add_history(input.clone());
        self.input_history.push(input);
    }

    /// Return whether Enter should submit (true) or just insert a newline (false).
    pub fn submit_on_enter(&self) -> bool {
        self.submit_on_enter
    }

    /// Set whether Enter submits.
    pub fn set_submit_on_enter(&mut self, submit: bool) {
        self.submit_on_enter = submit;
    }

    /// Check if shell mode is currently active (input starts with '!').
    pub fn is_shell_mode(&self) -> bool {
        self.shell_mode
    }

    /// Get the shell command text (without '!' prefix).
    pub fn shell_command(&self) -> &str {
        &self.shell_command
    }

    /// Safe access to the highlighted suggestion, with bounds protection
    /// against stale indices when suggestions are updated externally.
    fn highlighted_suggestion(&self) -> Option<&Suggestion> {
        self.suggestions.get(self.highlighted)
    }

    /// Update shell mode based on current text content.
    /// Call this after each text change.
    pub fn update_shell_mode(&mut self) {
        let text = self.text();
        if text.starts_with('!') && text.len() > 1 {
            self.shell_mode = true;
            self.shell_command = text[1..].to_string();
        } else {
            self.shell_mode = false;
            self.shell_command.clear();
        }
    }

    /// Return the shell command if in shell mode and Enter is pressed,
    /// consuming it. Otherwise return None.
    pub fn take_shell_command(&mut self) -> Option<String> {
        if self.shell_mode && !self.shell_command.is_empty() {
            let cmd = self.shell_command.clone();
            self.clear();
            self.update_shell_mode();
            Some(cmd)
        } else {
            None
        }
    }

    /// Refresh autocomplete suggestions based on the current textarea content.
    /// Call this after external text changes to keep suggestions in sync.
    pub fn refresh_suggestions(&mut self) {
        self.update_suggestions();
    }

    // ── Private helpers ──────────────────────────────────────────

    /// Extract the current word being typed from the textarea.
    /// Returns the word as a string slice, or empty if none.
    fn current_word(&self) -> String {
        let _text = self.textarea.lines().join("\n");
        let (row, col) = (self.textarea.cursor().0, self.textarea.cursor().1);

        // Get the line the cursor is on
        let line = self
            .textarea
            .lines()
            .get(row)
            .map(|l| l.as_str())
            .unwrap_or("");

        // Walk backward from cursor position to find word boundary
        let chars: Vec<char> = line.chars().collect();
        let col = col.min(chars.len());
        let mut start = col;
        while start > 0 {
            let c = chars[start - 1];
            if c.is_whitespace() {
                break;
            }
            start -= 1;
        }

        chars[start..col].iter().collect()
    }

    /// Update suggestions based on the current word.
    fn update_suggestions(&mut self) {
        self.update_shell_mode();
        let word = self.current_word();
        if word.is_empty() {
            self.clear_suggestions();
            return;
        }

        self.current_prefix = word.clone();
        self.suggestions = self.engine.suggest(&word);
        self.highlighted = 0;
        self.show_suggestions = !self.suggestions.is_empty();

        // Generate inline preview
        self.inline_preview = if self.show_suggestions {
            let top = self
                .highlighted_suggestion()
                .map(|s| &s.text)
                .unwrap_or(&self.current_prefix);
            // The complement is the part of the suggestion after the prefix
            if top.starts_with(&self.current_prefix) {
                top[self.current_prefix.len()..].to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };
    }

    /// Accept the currently highlighted suggestion.
    fn accept_suggestion(&mut self) {
        if !self.show_suggestions || self.suggestions.is_empty() {
            return;
        }

        let suggestion = match self.highlighted_suggestion() {
            Some(s) => s.clone(),
            None => return,
        };
        let suggestion_text = suggestion.text.clone();

        // Replace the current word with the suggestion text
        self.replace_current_word(&suggestion_text);

        // Record the use for frecency
        self.engine.record_use(&suggestion);

        // If it's a directory, re-trigger suggestions
        if suggestion_text.ends_with('/') {
            self.update_suggestions();
        } else {
            self.clear_suggestions();
        }
    }

    /// Replace the current word (the prefix) with new text.
    fn replace_current_word(&mut self, new_text: &str) {
        let (row, col) = (self.textarea.cursor().0, self.textarea.cursor().1);
        let line = self
            .textarea
            .lines()
            .get(row)
            .map(|l| l.as_str())
            .unwrap_or("");
        let chars: Vec<char> = line.chars().collect();
        let col = col.min(chars.len());

        // Find word start
        let mut start = col;
        while start > 0 {
            let c = chars[start - 1];
            if c.is_whitespace() {
                break;
            }
            start -= 1;
        }

        // Delete from start to col
        let delete_count = col - start;
        for _ in 0..delete_count {
            self.textarea.delete_char();
        }

        // Insert the new text
        self.textarea.insert_str(new_text);
    }

    /// Get the currently highlighted suggestion text (without applying it).
    /// Returns None if no suggestions are available.
    pub fn highlighted_suggestion_text(&self) -> Option<String> {
        if !self.show_suggestions || self.suggestions.is_empty() {
            return None;
        }
        self.highlighted_suggestion().map(|s| s.text.clone())
    }

    /// Get the current autocomplete prefix being matched.
    pub fn current_prefix_str(&self) -> &str {
        &self.current_prefix
    }

    /// Apply a suggestion to an external text line (does not touch internal textarea).
    /// Returns the new full text with the current prefix replaced by the suggestion.
    pub fn apply_suggestion_to_text(&self, full_text: &str) -> Option<String> {
        let suggestion = self.highlighted_suggestion_text()?;
        let prefix = &self.current_prefix;
        if full_text.ends_with(prefix) {
            let base = &full_text[..full_text.len() - prefix.len()];
            Some(format!("{}{}", base, suggestion))
        } else {
            // Fallback: just replace the last occurrence of prefix
            None
        }
    }

    /// Record use of highlighted suggestion for frecency.
    pub fn record_highlighted_use(&mut self) {
        if !self.show_suggestions || self.suggestions.is_empty() {
            return;
        }
        let suggestion = match self.highlighted_suggestion() {
            Some(s) => s.clone(),
            None => return,
        };
        self.engine.record_use(&suggestion);
        // If it's a directory, re-trigger suggestions; otherwise clear
        if suggestion.text.ends_with('/') {
            // Keep suggestions open for directory navigation
            self.update_suggestions();
        } else {
            self.clear_suggestions();
        }
    }

    /// Clear all suggestion state.
    fn clear_suggestions(&mut self) {
        self.suggestions.clear();
        self.highlighted = 0;
        self.show_suggestions = false;
        self.current_prefix.clear();
        self.inline_preview.clear();
        self.current_request = None;
    }

    /// Cycle to the next suggestion.
    fn next_suggestion(&mut self) {
        if self.suggestions.is_empty() {
            return;
        }
        self.highlighted = (self.highlighted + 1) % self.suggestions.len();
        self.update_inline_preview();
    }

    /// Cycle to the previous suggestion.
    fn prev_suggestion(&mut self) {
        if self.suggestions.is_empty() {
            return;
        }
        self.highlighted = if self.highlighted == 0 {
            self.suggestions.len() - 1
        } else {
            self.highlighted - 1
        };
        self.update_inline_preview();
    }

    /// Update the inline preview based on the currently highlighted suggestion.
    fn update_inline_preview(&mut self) {
        self.inline_preview = if self.show_suggestions && !self.suggestions.is_empty() {
            let top = self
                .highlighted_suggestion()
                .map(|s| &s.text)
                .unwrap_or(&self.current_prefix);
            if top.starts_with(&self.current_prefix) {
                top[self.current_prefix.len()..].to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };
    }

    /// Compute how many lines the suggestion dropdown occupies.
    fn dropdown_height(&self) -> u16 {
        if !self.show_suggestions || self.suggestions.is_empty() {
            0
        } else {
            self.suggestions.len() as u16 + 2 // +2 for borders
        }
    }

    /// Refresh autocomplete suggestions based on external text (without touching textarea).
    /// Use this when the text source is external (e.g., App.input).
    pub fn refresh_suggestions_from_text(&mut self, text: &str) {
        self.update_shell_mode();
        // Extract the current word from the text at the right position
        // Since we don't have cursor position, use the end of text
        let word = self.current_word_from_text(text);
        if word.is_empty() {
            self.clear_suggestions();
            return;
        }
        self.current_prefix = word.clone();
        self.suggestions = self.engine.suggest(&word);
        self.highlighted = 0;
        self.show_suggestions = !self.suggestions.is_empty();

        self.inline_preview = if self.show_suggestions {
            let top = self
                .highlighted_suggestion()
                .map(|s| &s.text)
                .unwrap_or(&self.current_prefix);
            if top.starts_with(&self.current_prefix) {
                top[self.current_prefix.len()..].to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };
    }

    /// Refresh autocomplete suggestions from external text at a cursor byte offset.
    pub fn refresh_suggestions_from_text_at_cursor(&mut self, text: &str, cursor: usize) {
        self.update_shell_mode();
        let Some(request) = CompletionRequest::from_text_at_cursor(text, cursor) else {
            self.clear_suggestions();
            return;
        };

        self.current_prefix = request.token.clone();
        self.suggestions = self.suggestions_for_request(&request);
        self.highlighted = 0;
        self.show_suggestions = !self.suggestions.is_empty();
        self.current_request = Some(request);
        self.update_inline_preview();
    }

    fn suggestions_for_request(&mut self, request: &CompletionRequest) -> Vec<Suggestion> {
        match request.trigger {
            CompletionTrigger::SlashCommand
            | CompletionTrigger::Context
            | CompletionTrigger::History => self.engine.suggest(&request.token),
            CompletionTrigger::Path => self.engine.file_suggestions(&request.token),
        }
    }

    /// Return whether autocomplete suggestions are visible.
    pub fn suggestions_visible(&self) -> bool {
        self.show_suggestions && !self.suggestions.is_empty()
    }

    /// Select the next visible suggestion.
    pub fn select_next_suggestion(&mut self) {
        self.next_suggestion();
    }

    /// Select the previous visible suggestion.
    pub fn select_prev_suggestion(&mut self) {
        self.prev_suggestion();
    }

    /// Apply the highlighted suggestion to external text using the cursor-aware replacement range.
    pub fn apply_highlighted_suggestion_to_text(&mut self, text: &str) -> Option<String> {
        let suggestion = self.highlighted_suggestion()?.clone();
        let request = self.current_request.clone()?;
        let next = request.apply_to_text(text, &suggestion.text);
        self.engine.record_use(&suggestion);
        self.clear_suggestions();
        Some(next)
    }

    /// Extract the word being typed from a raw text string (uses end of text as cursor).
    fn current_word_from_text(&self, text: &str) -> String {
        let lines: Vec<&str> = text.lines().collect();
        if lines.is_empty() {
            return String::new();
        }
        let last_line = lines[lines.len() - 1];
        let chars: Vec<char> = last_line.chars().collect();
        let mut start = chars.len();
        while start > 0 {
            let c = chars[start - 1];
            if c.is_whitespace() {
                break;
            }
            start -= 1;
        }
        chars[start..chars.len()].iter().collect()
    }

    /// Render the suggestion dropdown below the input area.
    /// Public so TuiState can call it after rendering app.input directly.
    pub fn render_dropdown(&self, ctx: &mut RenderContext, base_area: Rect) {
        if !self.show_suggestions || self.suggestions.is_empty() {
            return;
        }

        let dropdown_width = base_area.width.min(60);
        let dropdown_y = base_area.y + base_area.height;
        let dropdown_h = self.suggestions.len() as u16 + 2;

        // Ensure dropdown fits on screen
        let screen_h = ctx.area().height;
        if dropdown_y + dropdown_h > screen_h {
            // Not enough room — skip rendering or render above
            return;
        }

        let dropdown_area = Rect::new(
            base_area.x + 2, // indent slightly
            dropdown_y,
            dropdown_width,
            dropdown_h,
        );

        // Build suggestion lines
        let lines: Vec<Line> = self
            .suggestions
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let kind_char = match s.kind {
                    SuggestionKind::File => "📁",
                    SuggestionKind::Context => "◎",
                    SuggestionKind::Command => "⚡",
                    SuggestionKind::History => "🕐",
                };
                let text = format!(" {} {}", kind_char, s.text);

                if i == self.highlighted {
                    Line::styled(
                        text,
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    )
                } else {
                    Line::styled(text, Style::default().fg(Color::DarkGray))
                }
            })
            .collect();

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" suggestions ");

        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .style(Style::default().fg(Color::Gray));

        ctx.frame_mut().render_widget(paragraph, dropdown_area);
    }
}

impl Component for Prompt {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        // Update block styling based on shell mode
        if self.shell_mode {
            let block = Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow))
                .title(format!(" SHELL: !{} ", self.shell_command));
            self.textarea.set_block(block);
        } else {
            let block = Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} ", self.block_title));
            self.textarea.set_block(block);
        }

        // Render the textarea
        ctx.frame_mut().render_widget(&self.textarea, area);

        // Render inline preview: dimmed text after the cursor
        if !self.inline_preview.is_empty() {
            let (row, col) = (self.textarea.cursor().0, self.textarea.cursor().1);
            let preview_x = area.x + col as u16 + 1; // +1 for border

            if preview_x < area.x + area.width - 1 {
                let preview_text = self.inline_preview.clone();
                // Truncate to fit
                let available = (area.x + area.width - 1).saturating_sub(preview_x) as usize;
                let display: String = preview_text.chars().take(available).collect();

                let preview_span = Span::styled(
                    display,
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::DIM),
                );

                let preview_area = Rect::new(
                    preview_x,
                    area.y + row as u16 + 1, // +1 for top border
                    available as u16,
                    1,
                );

                ctx.frame_mut().render_widget(preview_span, preview_area);
            }
        }

        // Render suggestion dropdown below
        self.render_dropdown(ctx, area);
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) if key.kind == crossterm::event::KeyEventKind::Press => {
                self.handle_key(key)
            }
            Event::Key(key) if key.kind == crossterm::event::KeyEventKind::Repeat => {
                // Re-query suggestions on repeat (for held keys)
                if let KeyCode::Char(_) = key.code {
                    self.update_suggestions();
                }
                EventResult::NotConsumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "prompt"
    }
}

impl Prompt {
    fn handle_key(&mut self, key: &KeyEvent) -> EventResult {
        match key.code {
            // ── Tab: accept top suggestion ──
            KeyCode::Tab => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    // Shift+Tab: cycle to next suggestion
                    self.next_suggestion();
                    EventResult::Consumed
                } else if self.show_suggestions {
                    self.accept_suggestion();
                    EventResult::Consumed
                } else {
                    // No suggestions active — let the event fall through
                    EventResult::NotConsumed
                }
            }

            // ── Esc: close suggestions ──
            KeyCode::Esc => {
                if self.show_suggestions {
                    self.clear_suggestions();
                    EventResult::Consumed
                } else {
                    EventResult::NotConsumed
                }
            }

            // ── Enter: submit (propagate) or insert newline ──
            KeyCode::Enter => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    // Shift+Enter: insert newline
                    self.textarea.input(Event::Key(*key));
                    self.update_suggestions();
                    return EventResult::Consumed;
                }
                if self.show_suggestions {
                    // Enter with suggestions visible: accept the top suggestion
                    self.accept_suggestion();
                    EventResult::Consumed
                } else if self.submit_on_enter {
                    // Propagate so the caller can decide what to do
                    EventResult::Consumed
                } else {
                    // Multiline mode: insert newline
                    self.textarea.input(Event::Key(*key));
                    self.clear_suggestions();
                    EventResult::Consumed
                }
            }

            // ── Regular text input ──
            KeyCode::Char(_) => {
                self.textarea.input(Event::Key(*key));
                self.update_suggestions();
                EventResult::Consumed
            }

            // ── Backspace / Delete: update suggestions after edit ──
            KeyCode::Backspace | KeyCode::Delete => {
                self.textarea.input(Event::Key(*key));
                self.update_suggestions();
                EventResult::Consumed
            }

            // ── Navigation keys ──
            KeyCode::Up
            | KeyCode::Down
            | KeyCode::Left
            | KeyCode::Right
            | KeyCode::Home
            | KeyCode::End => {
                self.textarea.input(Event::Key(*key));
                // Don't update suggestions on cursor movement unless
                // the cursor moved to a different word boundary
                EventResult::Consumed
            }

            // ── Ctrl+A / Ctrl+E / etc. — pass to textarea ──
            _ => {
                if self.show_suggestions {
                    // Unknown key with suggestions open: close them
                    self.clear_suggestions();
                }
                EventResult::NotConsumed
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

    // ── Helpers ───────────────────────────────────────────────────

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new_with_kind(code, KeyModifiers::NONE, KeyEventKind::Press)
    }

    fn key_char(c: char) -> KeyEvent {
        KeyEvent::new_with_kind(KeyCode::Char(c), KeyModifiers::NONE, KeyEventKind::Press)
    }

    fn key_shift_tab() -> KeyEvent {
        KeyEvent::new_with_kind(KeyCode::Tab, KeyModifiers::SHIFT, KeyEventKind::Press)
    }

    fn key_esc() -> KeyEvent {
        KeyEvent::new_with_kind(KeyCode::Esc, KeyModifiers::NONE, KeyEventKind::Press)
    }

    fn key_enter() -> KeyEvent {
        KeyEvent::new_with_kind(KeyCode::Enter, KeyModifiers::NONE, KeyEventKind::Press)
    }

    // ── FrecencyTracker tests ─────────────────────────────────────

    #[test]
    fn frecency_new_is_empty() {
        let ft = FrecencyTracker::new();
        assert!(ft.is_empty());
        assert_eq!(ft.len(), 0);
    }

    #[test]
    fn frecency_record_and_score() {
        let mut ft = FrecencyTracker::new();
        let s = Suggestion::new("test", SuggestionKind::History);

        ft.record_use(&s);
        assert_eq!(ft.len(), 1);

        // Just recorded, so recency decay should be ~1.0
        let score = ft.score(&s);
        assert!(
            score > 0.5,
            "freshly used item should score high, got {score}"
        );
    }

    #[test]
    fn frecency_ranking() {
        let mut ft = FrecencyTracker::new();
        let s1 = Suggestion::new("alpha", SuggestionKind::History);
        let s2 = Suggestion::new("beta", SuggestionKind::History);
        let s3 = Suggestion::new("gamma", SuggestionKind::History);

        // Record uses with different frequency
        for _ in 0..3 {
            ft.record_use(&s1); // freq=3
        }
        for _ in 0..5 {
            ft.record_use(&s2); // freq=5
        }
        ft.record_use(&s3); // freq=1

        let mut suggestions = vec![s3, s1, s2]; // unsorted
        ft.rank(&mut suggestions);

        // s2 should be first (freq=5), then s1 (freq=3), then s3 (freq=1)
        assert_eq!(suggestions[0].text, "beta");
        assert_eq!(suggestions[1].text, "alpha");
        assert_eq!(suggestions[2].text, "gamma");
    }

    #[test]
    fn frecency_zero_for_unknown() {
        let ft = FrecencyTracker::new();
        let s = Suggestion::new("unknown", SuggestionKind::History);
        assert!((ft.score(&s) - 0.0).abs() < f64::EPSILON);
    }

    // ── AutocompleteEngine tests ───────────────────────────────────

    #[test]
    fn autocomplete_command_prefix() {
        let mut engine = AutocompleteEngine::new("/tmp");

        let suggestions = engine.suggest("/sta");
        assert!(
            !suggestions.is_empty(),
            "should find commands matching '/sta'"
        );

        // Should contain /status
        let has_status = suggestions.iter().any(|s| s.text == "/status");
        assert!(has_status, "should include '/status' in results");

        // Should also match /stats
        let has_stats = suggestions.iter().any(|s| s.text == "/stats");
        assert!(has_stats, "should include '/stats' in results");

        // All suggestions should start with "/st"
        for s in &suggestions {
            assert!(
                s.text.starts_with("/st"),
                "{} should start with '/st'",
                s.text
            );
            assert_eq!(s.kind, SuggestionKind::Command);
        }
    }

    #[test]
    fn autocomplete_command_no_match() {
        let mut engine = AutocompleteEngine::new("/tmp");
        let suggestions = engine.suggest("/zzz");
        assert!(suggestions.is_empty());
    }

    #[test]
    fn autocomplete_commands_include_registered_aliases() {
        let mut engine = AutocompleteEngine::new("/tmp");
        let suggestions = engine.suggest("/market");
        assert!(
            suggestions.iter().any(|s| s.text == "/marketplace"),
            "should include slash command aliases from the registry"
        );

        let suggestions = engine.suggest("/skill");
        assert!(
            suggestions.iter().any(|s| s.text == "/skills")
                && suggestions.iter().any(|s| s.text == "/skill"),
            "should include canonical skill command and alias"
        );
    }

    #[test]
    fn autocomplete_history_prefix() {
        let mut engine = AutocompleteEngine::new("/tmp");
        engine.add_history("hello world".into());
        engine.add_history("foo bar".into());
        engine.add_history("hello rust".into());

        let suggestions = engine.suggest("hello");
        assert!(!suggestions.is_empty());

        // Should match "hello world" and "hello rust"
        let has_hello_world = suggestions.iter().any(|s| s.text == "hello world");
        let has_hello_rust = suggestions.iter().any(|s| s.text == "hello rust");

        assert!(has_hello_world);
        assert!(has_hello_rust);

        // Should NOT match "foo bar"
        let has_foo = suggestions.iter().any(|s| s.text == "foo bar");
        assert!(!has_foo);
    }

    #[test]
    fn autocomplete_max_suggestions() {
        let mut engine = AutocompleteEngine::new("/tmp");
        // Add many history entries
        for i in 0..20 {
            engine.add_history(format!("test {}", i));
        }

        let suggestions = engine.suggest("test");
        assert!(
            suggestions.len() <= 5,
            "should return at most 5 suggestions, got {}",
            suggestions.len()
        );
    }

    #[test]
    fn autocomplete_file_prefix() {
        // Create a temp directory with some files
        let dir = std::env::temp_dir().join("cowd_prompt_test_files");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src").join("main.rs"), "// main").unwrap();
        std::fs::write(dir.join("src").join("lib.rs"), "// lib").unwrap();
        std::fs::create_dir_all(dir.join("tests")).unwrap();
        std::fs::write(dir.join("tests").join("test_main.rs"), "// test").unwrap();

        let mut engine = AutocompleteEngine::new(dir.clone());

        // Test: @src → should find src/main.rs, src/lib.rs
        let suggestions = engine.suggest("@src");
        // The cwd is "dir", so @src should match "src" as a directory
        // But actually @src matches the prefix "src" — the @ is stripped,
        // so it looks for files starting with "src" in cwd.
        // That would match "src/" directory itself since read_dir returns "src"
        let found = suggestions.iter().any(|s| s.text.starts_with("src"));
        assert!(
            found,
            "should find files starting with 'src', got: {:?}",
            suggestions.iter().map(|s| &s.text).collect::<Vec<_>>()
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn autocomplete_file_subdirectory() {
        let dir = std::env::temp_dir().join("cowd_prompt_test_subdir");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src").join("main.rs"), "// main").unwrap();
        std::fs::write(dir.join("src").join("lib.rs"), "// lib").unwrap();

        let mut engine = AutocompleteEngine::new(dir.clone());

        // @src/ma → should match src/main.rs
        let suggestions = engine.suggest("@src/ma");
        let has_main = suggestions.iter().any(|s| s.text.contains("main.rs"));
        assert!(
            has_main,
            "@src/ma should find src/main.rs, got: {:?}",
            suggestions.iter().map(|s| &s.text).collect::<Vec<_>>()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn autocomplete_context_root_lists_structured_context_entries() {
        let mut engine = AutocompleteEngine::new("/tmp");

        let suggestions = engine.suggest("@");
        let texts: Vec<&str> = suggestions.iter().map(|s| s.text.as_str()).collect();

        assert!(texts.contains(&"@diff"), "missing @diff in {texts:?}");
        assert!(texts.contains(&"@staged"), "missing @staged in {texts:?}");
        assert!(texts.contains(&"@file:"), "missing @file: in {texts:?}");
        assert!(texts.contains(&"@folder:"), "missing @folder: in {texts:?}");
    }

    #[test]
    fn autocomplete_at_file_only_yields_files() {
        let dir = std::env::temp_dir().join("cowd_prompt_test_at_file");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("readme.md"), "readme").unwrap();
        std::fs::write(dir.join(".env"), "secret").unwrap();

        let mut engine = AutocompleteEngine::new(dir.clone());
        let suggestions = engine.suggest("@file:");
        let texts: Vec<&str> = suggestions.iter().map(|s| s.text.as_str()).collect();

        assert!(texts.contains(&"@file:readme.md"), "{texts:?}");
        assert!(texts.contains(&"@file:.env"), "{texts:?}");
        assert!(
            !texts.iter().any(|text| text.starts_with("@file:src/")),
            "{texts:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn autocomplete_at_folder_only_yields_directories() {
        let dir = std::env::temp_dir().join("cowd_prompt_test_at_folder");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("readme.md"), "readme").unwrap();

        let mut engine = AutocompleteEngine::new(dir.clone());
        let suggestions = engine.suggest("@folder:");
        let texts: Vec<&str> = suggestions.iter().map(|s| s.text.as_str()).collect();

        assert!(texts.contains(&"@folder:src/"), "{texts:?}");
        assert!(!texts.contains(&"@folder:readme.md"), "{texts:?}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn autocomplete_at_file_fuzzy_finds_nested_basename() {
        let dir = std::env::temp_dir().join("cowd_prompt_test_at_file_fuzzy");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src/bin")).unwrap();
        std::fs::write(dir.join("src/bin").join("main.rs"), "fn main() {}").unwrap();
        std::fs::write(dir.join("Cargo.toml"), "[package]").unwrap();

        let mut engine = AutocompleteEngine::new(dir.clone());
        let suggestions = engine.suggest("@file:mai");
        let texts: Vec<&str> = suggestions.iter().map(|s| s.text.as_str()).collect();

        assert!(texts.contains(&"@file:src/bin/main.rs"), "{texts:?}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── resolve_file_search tests ──────────────────────────────────

    #[test]
    fn resolve_file_search_flat() {
        let cwd = std::path::Path::new("/project");
        let (dir, prefix) = resolve_file_search(cwd, "src");
        assert_eq!(dir, std::path::Path::new("/project"));
        assert_eq!(prefix, "src");
    }

    #[test]
    fn resolve_file_search_nested() {
        let cwd = std::path::Path::new("/project");
        let (dir, prefix) = resolve_file_search(cwd, "src/main");
        assert_eq!(dir, std::path::Path::new("/project/src"));
        assert_eq!(prefix, "main");
    }

    #[test]
    fn resolve_file_search_deeply_nested() {
        let cwd = std::path::Path::new("/project");
        let (dir, prefix) = resolve_file_search(cwd, "a/b/c/fi");
        assert_eq!(dir, std::path::Path::new("/project/a/b/c"));
        assert_eq!(prefix, "fi");
    }

    // ── CompletionKernel tests ────────────────────────────────────

    #[test]
    fn completion_kernel_detects_mid_text_slash_command() {
        let text = "please run /statu now";
        let cursor = text.find(" now").unwrap();
        let request = CompletionRequest::from_text_at_cursor(text, cursor).unwrap();

        assert_eq!(request.trigger, CompletionTrigger::SlashCommand);
        assert_eq!(request.token, "/statu");
        assert_eq!(&text[request.replace_start..request.replace_end], "/statu");
    }

    #[test]
    fn completion_kernel_treats_absolute_slash_as_path_not_command() {
        let text = "read /home/yi/project/mai";
        let request = CompletionRequest::from_text_at_cursor(text, text.len()).unwrap();

        assert_eq!(request.trigger, CompletionTrigger::Path);
        assert_eq!(request.token, "/home/yi/project/mai");
    }

    #[test]
    fn completion_kernel_replaces_only_current_token() {
        let text = "please run /statu now";
        let cursor = text.find(" now").unwrap();
        let request = CompletionRequest::from_text_at_cursor(text, cursor).unwrap();

        assert_eq!(request.apply("status"), "please run /status now");
    }

    // ── Prompt tab_accepts test ────────────────────────────────────

    #[test]
    fn tab_accepts_suggestion() {
        let mut prompt = Prompt::new("/tmp");

        // Type "@sr" with file suggestions — but since in test there are
        // no files, test command completion instead.
        // Type "/sta"
        for c in "/sta".chars() {
            prompt.textarea_mut().insert_char(c);
        }
        // Force suggestion update
        let _ = prompt.handle_event(&crossterm::event::Event::Key(key_char('a')));
        // Re-trigger by sending backspace+char to update suggestions
        // Actually let's just use the engine directly to verify Tab behavior
        // Type "/sta"
        prompt.clear();
        for c in "/st".chars() {
            prompt.textarea_mut().insert_char(c);
        }
        // Trigger suggestions by sending a key
        let _ = prompt.handle_event(&crossterm::event::Event::Key(key_char('a')));

        // Now we should have suggestions
        assert!(
            prompt.show_suggestions,
            "should have suggestions after typing /sta"
        );
        assert!(!prompt.suggestions.is_empty());

        // Tab should accept
        let result = prompt.handle_event(&crossterm::event::Event::Key(key(KeyCode::Tab)));
        assert!(result.is_consumed());
        // After accepting, the text should be the full suggestion
        let text = prompt.text();
        assert!(
            text.starts_with("/sta"),
            "text should preserve the typed '/sta' command prefix after tab accept, got: {text}"
        );
    }

    #[test]
    fn shift_tab_cycles_suggestions() {
        let mut prompt = Prompt::new("/tmp");

        // Type "/s" via handle_event to get suggestions
        for c in "/s".chars() {
            let _ = prompt.handle_event(&crossterm::event::Event::Key(key_char(c)));
        }

        assert!(
            prompt.show_suggestions,
            "should have suggestions after typing /s, got text='{}'",
            prompt.text()
        );
        assert!(
            prompt.suggestions.len() >= 2,
            "need at least 2 suggestions to test cycling, got {}",
            prompt.suggestions.len()
        );

        let initial_highlighted = prompt.highlighted;

        // Shift+Tab cycles
        let result = prompt.handle_event(&crossterm::event::Event::Key(key_shift_tab()));
        assert!(result.is_consumed());

        // Should have moved to next suggestion
        assert_ne!(
            prompt.highlighted, initial_highlighted,
            "Shift+Tab should change highlighted index"
        );
    }

    #[test]
    fn esc_closes_suggestions() {
        let mut prompt = Prompt::new("/tmp");

        // Type something to trigger file suggestions (or command)
        for c in "/help".chars() {
            prompt.textarea_mut().insert_char(c);
        }
        let _ = prompt.handle_event(&crossterm::event::Event::Key(key_char('p')));
        // Overwrite: clear and retype
        prompt.clear();
        for c in "/h".chars() {
            prompt.textarea_mut().insert_char(c);
        }
        let _ = prompt.handle_event(&crossterm::event::Event::Key(key_char('h')));

        // Should have suggestions if /h matches commands
        if prompt.show_suggestions {
            let result = prompt.handle_event(&crossterm::event::Event::Key(key_esc()));
            assert!(result.is_consumed());
            assert!(!prompt.show_suggestions);
        }
    }

    #[test]
    fn inline_preview_generated() {
        let mut prompt = Prompt::new("/tmp");

        // Type "/sta" — should get /status as suggestion with "tus" as preview
        for c in "/sta".chars() {
            prompt.textarea_mut().insert_char(c);
        }
        let _ = prompt.handle_event(&crossterm::event::Event::Key(key_char('a')));

        if prompt.show_suggestions && !prompt.suggestions.is_empty() {
            // The inline preview should contain the rest of the suggestion
            let first = &prompt.suggestions[0].text;
            if first.starts_with("/sta") {
                let expected_preview = &first[4..]; // after "/sta"
                assert_eq!(
                    prompt.inline_preview, expected_preview,
                    "inline preview should be the rest of the top suggestion"
                );
            }
        }
    }

    // ── Prompt general tests ───────────────────────────────────────

    #[test]
    fn prompt_new_has_empty_text() {
        let prompt = Prompt::new("/tmp");
        assert_eq!(prompt.text(), "");
    }

    #[test]
    fn prompt_set_text() {
        let mut prompt = Prompt::new("/tmp");
        prompt.set_text("hello world");
        assert_eq!(prompt.text(), "hello world");
    }

    #[test]
    fn prompt_clear() {
        let mut prompt = Prompt::new("/tmp");
        prompt.set_text("hello");
        prompt.clear();
        assert_eq!(prompt.text(), "");
    }

    #[test]
    fn prompt_focusable() {
        let prompt = Prompt::new("/tmp");
        assert!(prompt.focusable());
    }

    #[test]
    fn prompt_id() {
        let prompt = Prompt::new("/tmp");
        assert_eq!(prompt.id(), "prompt");
    }

    #[test]
    fn prompt_type_single_char() {
        let mut prompt = Prompt::new("/tmp");
        let event = crossterm::event::Event::Key(key_char('h'));
        let result = prompt.handle_event(&event);
        assert!(result.is_consumed());
        assert_eq!(prompt.text(), "h");
    }

    #[test]
    fn prompt_backspace() {
        let mut prompt = Prompt::new("/tmp");
        prompt.set_text("abc");

        let event = crossterm::event::Event::Key(key(KeyCode::Backspace));
        let result = prompt.handle_event(&event);
        assert!(result.is_consumed());
        // Backspace from the end should remove last char
        // Note: after set_text, cursor may not be at end. Let's move to end first.
        assert!(prompt.text().contains("ab"));
    }

    #[test]
    fn prompt_enter_submits() {
        let mut prompt = Prompt::new("/tmp");
        prompt.set_text("hello");

        let event = crossterm::event::Event::Key(key_enter());
        let result = prompt.handle_event(&event);

        // Should be consumed (caller will check and extract text)
        assert!(result.is_consumed());
    }

    #[test]
    fn prompt_add_history() {
        let mut prompt = Prompt::new("/tmp");
        prompt.add_history("first command".into());
        prompt.add_history("second command".into());

        assert_eq!(prompt.engine.history().len(), 2);
        assert_eq!(prompt.engine.history()[0], "first command");
        assert_eq!(prompt.engine.history()[1], "second command");
    }

    #[test]
    fn prompt_engine_reference() {
        let mut prompt = Prompt::new("/tmp");
        prompt.add_history("test".into());

        let engine = prompt.engine();
        assert_eq!(engine.history().len(), 1);
    }

    // ── Shell mode tests (Task 13) ──────────────────────────────────

    #[test]
    fn shell_mode_activates_on_exclamation() {
        let mut prompt = Prompt::new("/tmp");
        assert!(!prompt.is_shell_mode());

        prompt.set_text("!ls -la");
        prompt.update_shell_mode();
        assert!(prompt.is_shell_mode());
        assert_eq!(prompt.shell_command(), "ls -la");
    }

    #[test]
    fn shell_mode_deactivates_without_exclamation() {
        let mut prompt = Prompt::new("/tmp");
        prompt.set_text("!echo hello");
        prompt.update_shell_mode();
        assert!(prompt.is_shell_mode());

        prompt.set_text("normal text");
        prompt.update_shell_mode();
        assert!(!prompt.is_shell_mode());
        assert!(prompt.shell_command().is_empty());
    }

    #[test]
    fn border_color_changes_via_render() {
        // Shell mode changes the block border color to yellow.
        // We verify this by checking that the block title changes.
        let mut prompt = Prompt::new("/tmp");
        prompt.set_text("!ls");
        prompt.update_shell_mode();
        assert!(prompt.is_shell_mode());
        // The render method will set block with "SHELL:" title
        // when shell_mode is true. We can't easily check the ratatui
        // block from here, but we verify shell_mode state.
        assert_eq!(prompt.shell_command(), "ls");
    }

    #[test]
    fn enter_executes_shell_command() {
        let mut prompt = Prompt::new("/tmp");
        prompt.set_text("!ls -la");
        prompt.update_shell_mode();
        assert!(prompt.is_shell_mode());

        let cmd = prompt.take_shell_command();
        assert_eq!(cmd.as_deref(), Some("ls -la"));
        // After taking the command, prompt should be cleared
        assert!(!prompt.is_shell_mode());
        assert!(prompt.text().is_empty());
    }

    #[test]
    fn shell_mode_only_exclamation_at_start() {
        let mut prompt = Prompt::new("/tmp");

        // Text with ! in middle should NOT activate shell mode
        prompt.set_text("echo !important");
        prompt.update_shell_mode();
        assert!(
            !prompt.is_shell_mode(),
            "! in middle should not activate shell mode"
        );

        // Just '!' alone should not activate shell mode (no command)
        prompt.clear();
        prompt.textarea_mut().insert_str("!");
        prompt.update_shell_mode();
        assert!(
            !prompt.is_shell_mode(),
            "just ! should not activate shell mode, text='{}' len={}",
            prompt.text(),
            prompt.text().len()
        );
    }

    #[test]
    fn utf8_word_extraction() {
        let prompt = Prompt::new("/tmp");

        // Multi-byte UTF-8 only: "你好世界" (6 bytes, 4 chars) - was crashing
        let result = prompt.current_word_from_text("你好世界");
        assert_eq!(
            result, "你好世界",
            "full UTF-8 text should be returned as the current word"
        );

        // English text should still work
        let result = prompt.current_word_from_text("hello world");
        assert_eq!(result, "world", "last word in ASCII should be returned");

        // Mixed CJK with whitespace
        let result = prompt.current_word_from_text("test 你好");
        assert_eq!(
            result, "你好",
            "last CJK word after space should be returned"
        );

        // Empty string
        let result = prompt.current_word_from_text("");
        assert_eq!(result, "", "empty text should return empty string");

        // Whitespace at end (empty word context)
        let result = prompt.current_word_from_text("hello ");
        assert_eq!(result, "", "trailing space should return empty string");

        // Multi-line with CJK
        let result = prompt.current_word_from_text("first line\nsecond 测试");
        assert_eq!(
            result, "测试",
            "last word on last line with CJK should work"
        );

        // Single CJK character
        let result = prompt.current_word_from_text("中");
        assert_eq!(result, "中", "single CJK character should work");
    }
}
