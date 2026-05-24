// ── File Changes Panel Component ─────────────────────────────────────
// Displays modified files from the session diff with +/- counts,
// collapsible when >8 items, j/k navigation, and Enter→DiffViewer.
// -----------------------------------------------------------------

#![allow(dead_code)]

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::components::base::{Component, EventResult, RenderContext};

/// A single changed file entry in the file changes panel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChangeEntry {
    pub path: String,
    pub added: usize,
    pub removed: usize,
}

/// File Changes Panel: shows modified files with +/- counts, collapsible list.
///
/// # Key bindings
/// | Key | Action |
/// |-----|--------|
/// | `j`/`↓` | Select next file |
/// | `k`/`↑` | Select previous file |
/// | Enter | Open selected file in DiffViewer |
/// | `c` | Toggle collapse/expand |
pub struct FileChangesPanel {
    files: Vec<FileChangeEntry>,
    selected_idx: usize,
    collapsed: bool,
    /// Max items to show before collapsing. Default: 8.
    collapse_limit: usize,
}

impl FileChangesPanel {
    /// Create a new, empty file changes panel.
    #[must_use]
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            selected_idx: 0,
            collapsed: false,
            collapse_limit: 8,
        }
    }

    /// Populate the panel with file changes.
    pub fn load(&mut self, files: Vec<FileChangeEntry>) {
        self.files = files;
        self.selected_idx = 0;
        self.collapsed = self.files.len() > self.collapse_limit;
    }

    /// Return the number of files.
    #[must_use]
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Return true if no files.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Return a reference to the files list.
    #[must_use]
    pub fn files(&self) -> &[FileChangeEntry] {
        &self.files
    }

    /// Return the currently selected file index.
    #[must_use]
    pub fn selected_idx(&self) -> usize {
        self.selected_idx
    }

    /// Return whether the panel is currently collapsed.
    #[must_use]
    pub fn is_collapsed(&self) -> bool {
        self.collapsed
    }

    /// Return the collapse limit.
    #[must_use]
    pub fn collapse_limit(&self) -> usize {
        self.collapse_limit
    }

    /// Set the collapse limit.
    pub fn set_collapse_limit(&mut self, limit: usize) {
        self.collapse_limit = limit;
        self.collapsed = self.files.len() > self.collapse_limit;
    }

    /// Toggle collapse/expand.
    pub fn toggle_collapse(&mut self) {
        self.collapsed = !self.collapsed;
    }

    /// Select the next file (with clamp, no wrap).
    pub fn select_next(&mut self) {
        if !self.files.is_empty() {
            let max_items = if self.collapsed {
                self.collapse_limit.min(self.files.len())
            } else {
                self.files.len()
            };
            self.selected_idx = (self.selected_idx + 1).min(max_items.saturating_sub(1));
        }
    }

    /// Select the previous file (with clamp).
    pub fn select_prev(&mut self) {
        self.selected_idx = self.selected_idx.saturating_sub(1);
    }

    /// Get pending switch index for DiffViewer (consumed by parent).
    /// Resets after being read.
    #[must_use]
    pub fn take_pending_open_file(&mut self) -> Option<usize> {
        if self.files.is_empty() {
            return None;
        }
        Some(self.selected_idx)
    }
}

impl Default for FileChangesPanel {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for FileChangesPanel {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        if area.width < 10 || area.height < 3 {
            return;
        }

        let accent = ctx.theme().accent_color();

        let title = if self.files.is_empty() {
            " Files ".to_string()
        } else {
            format!(" Files ({}) ", self.files.len())
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .title(title.as_str())
            .fg(accent);

        let inner = block.inner(area);
        ctx.frame_mut().render_widget(block, area);

        if self.files.is_empty() {
            let paragraph = Paragraph::new("No file changes.")
                .style(Style::default().fg(Color::DarkGray))
                .wrap(Wrap { trim: false });
            ctx.frame_mut().render_widget(paragraph, inner);
            return;
        }

        // Determine displayed items
        let max_display = if self.collapsed {
            self.collapse_limit
        } else {
            self.files.len()
        };

        let display_files: Vec<&FileChangeEntry> = self.files.iter().take(max_display).collect();

        let mut items: Vec<Line> = Vec::new();

        for (i, file) in display_files.iter().enumerate() {
            let is_selected = i == self.selected_idx;
            let prefix = if is_selected { "▸" } else { " " };
            let label = format!(
                " {} 📄 {} [+{} -{}]",
                prefix,
                file.path,
                file.added,
                file.removed
            );

            let style = if is_selected {
                Style::default()
                    .fg(Color::Black)
                    .bg(accent)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };

            items.push(Line::styled(label, style));
        }

        // Collapse indicator
        if self.collapsed && self.files.len() > self.collapse_limit {
            let hidden = self.files.len() - self.collapse_limit;
            let indicator = if self.selected_idx == self.collapse_limit
                || (self.collapse_limit > 0 && self.selected_idx == self.collapse_limit - 1)
            {
                // Highlight if cursor is on last visible item
                Line::styled(
                    format!("  ▼ {} more files — press 'c' to expand", hidden),
                    Style::default()
                        .fg(Color::Black)
                        .bg(accent)
                        .add_modifier(Modifier::ITALIC),
                )
            } else {
                Line::styled(
                    format!("  ▼ {} more files — press 'c' to expand", hidden),
                    Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC),
                )
            };
            items.push(indicator);
        }

        let text = Text::from(items);
        let paragraph = Paragraph::new(text).wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, inner);
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
        "file_changes_panel"
    }
}

impl FileChangesPanel {
    fn handle_key(&mut self, key: &KeyEvent) -> EventResult {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                self.select_next();
                EventResult::Consumed
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.select_prev();
                EventResult::Consumed
            }
            KeyCode::Char('c') => {
                self.toggle_collapse();
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::RenderContext;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    fn file_entry(path: &str, added: usize, removed: usize) -> FileChangeEntry {
        FileChangeEntry {
            path: path.to_string(),
            added,
            removed,
        }
    }

    fn key_event(code: KeyCode) -> Event {
        Event::Key(KeyEvent::new(code, crossterm::event::KeyModifiers::NONE))
    }

    fn render_panel(panel: &mut FileChangesPanel, width: u16, height: u16) -> Vec<String> {
        let mut terminal = MockTerminal::new(width, height);
        let theme = SkinConfig::default();
        terminal.draw(|f: &mut ratatui::Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            panel.render(&mut ctx, area);
        });
        terminal.buffer_lines()
    }

    // ── Test: file_changes_lists_modified_files ──────────────────────

    #[test]
    fn file_changes_lists_modified_files() {
        let mut panel = FileChangesPanel::new();
        panel.load(vec![
            file_entry("src/main.rs", 12, 3),
            file_entry("src/lib.rs", 5, 1),
            file_entry("Cargo.toml", 2, 2),
        ]);

        assert_eq!(panel.len(), 3);
        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(joined.contains("src/main.rs"), "Should show first file");
        assert!(joined.contains("src/lib.rs"), "Should show second file");
        assert!(joined.contains("Cargo.toml"), "Should show third file");
        assert!(joined.contains("[+12 -3]"), "Should show add/del counts");
        assert!(joined.contains("[+5 -1]"), "Should show add/del for lib");
    }

    // ── Test: file_changes_shows_add_del_counts ─────────────────────

    #[test]
    fn file_changes_shows_add_del_counts() {
        let mut panel = FileChangesPanel::new();
        panel.load(vec![
            file_entry("src/main.rs", 42, 7),
            file_entry("src/utils.rs", 0, 10),
        ]);

        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(joined.contains("[+42 -7]"), "Should show +42 -7");
        assert!(joined.contains("[+0 -10]"), "Should show +0 -10");
    }

    // ── Test: file_changes_collapses_over_limit ──────────────────────

    #[test]
    fn file_changes_collapses_over_limit() {
        let mut panel = FileChangesPanel::new();
        // 10 files, default collapse = 8
        let mut files = Vec::new();
        for i in 0..10 {
            files.push(file_entry(&format!("file_{}.rs", i), 1, 1));
        }
        panel.load(files);
        assert!(panel.is_collapsed(), "Should be collapsed with 10 files");

        let lines = render_panel(&mut panel, 60, 20);
        let joined = lines.join("\n");
        // Should show collapse indicator
        assert!(
            joined.contains("more files"),
            "Should show collapse indicator"
        );
        // Should only show up to 8 files
        assert!(joined.contains("file_0.rs"), "Should show first file");
        assert!(
            !joined.contains("file_9.rs"),
            "Should NOT show the 10th file"
        );
    }

    // ── Navigation tests ────────────────────────────────────────────

    #[test]
    fn file_changes_jk_navigation() {
        let mut panel = FileChangesPanel::new();
        panel.load(vec![
            file_entry("a.rs", 1, 1),
            file_entry("b.rs", 2, 2),
            file_entry("c.rs", 3, 3),
        ]);
        assert_eq!(panel.selected_idx(), 0);

        // j → 1
        let _ = panel.handle_event(&key_event(KeyCode::Char('j')));
        assert_eq!(panel.selected_idx(), 1);

        // j → 2
        let _ = panel.handle_event(&key_event(KeyCode::Char('j')));
        assert_eq!(panel.selected_idx(), 2);

        // j → stays at 2 (clamped)
        let _ = panel.handle_event(&key_event(KeyCode::Char('j')));
        assert_eq!(panel.selected_idx(), 2);

        // k → 1
        let _ = panel.handle_event(&key_event(KeyCode::Char('k')));
        assert_eq!(panel.selected_idx(), 1);
    }

    #[test]
    fn file_changes_toggle_collapse() {
        let mut panel = FileChangesPanel::new();
        let mut files = Vec::new();
        for i in 0..10 {
            files.push(file_entry(&format!("f{}.rs", i), 1, 1));
        }
        panel.load(files);
        assert!(panel.is_collapsed());

        // Toggle expand
        let _ = panel.handle_event(&key_event(KeyCode::Char('c')));
        assert!(!panel.is_collapsed());

        // Toggle collapse back
        let _ = panel.handle_event(&key_event(KeyCode::Char('c')));
        assert!(panel.is_collapsed());
    }

    #[test]
    fn file_changes_empty_panel() {
        let mut panel = FileChangesPanel::new();
        assert!(panel.is_empty());
        assert_eq!(panel.len(), 0);

        let lines = render_panel(&mut panel, 60, 10);
        let joined = lines.join("\n");
        assert!(
            joined.contains("No file changes"),
            "Empty panel should show 'No file changes'"
        );
    }

    #[test]
    fn file_changes_take_pending_open_file() {
        let mut panel = FileChangesPanel::new();
        assert!(panel.take_pending_open_file().is_none());

        panel.load(vec![file_entry("a.rs", 1, 1)]);
        assert_eq!(panel.take_pending_open_file(), Some(0));
    }

    #[test]
    fn file_changes_focusable_and_id() {
        let panel = FileChangesPanel::new();
        assert!(panel.focusable());
        assert_eq!(panel.id(), "file_changes_panel");
    }
}
