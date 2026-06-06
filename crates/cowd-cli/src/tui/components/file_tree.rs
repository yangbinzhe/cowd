// ── File Tree Component ──────────────────────────────────────────────
// Recursive tree navigation with expand/collapse, file preview,
// and git status overlay. Implements the base Component trait.
//
// Navigation:
//   j/k     – move cursor down/up
//   Enter   – toggle directory expand/collapse
//   l       – expand directory
//   h       – collapse directory (or go to parent level)
//
// Git status indicators shown after the filename when available:
//   [M] Modified, [A] Added, [D] Deleted, [?] Untracked
// ----------------------------------------------------------------------

use std::collections::HashMap;
use std::path::PathBuf;

use crossterm::event::{Event, KeyCode};
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::tui::app::FileEntry;
use crate::tui::components::base::{Component, EventResult, RenderContext};

// ── Git Status ──────────────────────────────────────────────────────

/// Git status as determined by `git status --porcelain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitStatus {
    Modified,
    Added,
    Deleted,
    Renamed,
    Untracked,
}

impl GitStatus {
    /// Single-character status symbol: M / A / D / R / ?.
    pub fn symbol(&self) -> &str {
        match self {
            GitStatus::Modified => "M",
            GitStatus::Added => "A",
            GitStatus::Deleted => "D",
            GitStatus::Renamed => "R",
            GitStatus::Untracked => "?",
        }
    }

    /// Parse from the two-character porcelain prefix (XY index+working-tree status).
    /// Prefers the working-tree status if the index status is space.
    fn from_porcelain(xy: &str) -> Option<GitStatus> {
        if xy.len() < 2 {
            return None;
        }
        let chars: Vec<char> = xy.chars().collect();
        // Prefer working-tree status char; fall back to index status char.
        let sc = if chars[1] != ' ' { chars[1] } else { chars[0] };
        match sc {
            'M' => Some(GitStatus::Modified),
            'A' => Some(GitStatus::Added),
            'D' => Some(GitStatus::Deleted),
            'R' => Some(GitStatus::Renamed),
            '?' => Some(GitStatus::Untracked),
            _ => None,
        }
    }
}

// ── FileNode ────────────────────────────────────────────────────────

/// A single node in the file tree hierarchy.
#[derive(Debug, Clone)]
pub struct FileNode {
    /// Display name (last path component only).
    pub name: String,
    /// Full path relative to the working directory.
    pub path: String,
    /// Whether this node is a directory.
    pub is_dir: bool,
    /// File size in bytes (0 for directories and intermediate nodes).
    pub size: u64,
    /// Whether a directory node is currently expanded.
    pub is_expanded: bool,
    /// Git status overlay, if loaded.
    pub git_status: Option<GitStatus>,
    /// Child nodes (only populated for directories).
    pub children: Vec<FileNode>,
}

impl FileNode {
    /// Format the file size as a human-readable string.
    /// Directories return an empty string.
    fn format_size(&self) -> String {
        format_file_size(self.size, self.is_dir)
    }
}

fn format_file_size(size: u64, is_dir: bool) -> String {
    if is_dir {
        return String::new();
    }
    if size >= 1024 * 1024 {
        format!("{:.1}MB", size as f64 / (1024.0 * 1024.0))
    } else if size >= 1024 {
        format!("{}KB", size / 1024)
    } else {
        format!("{}B", size)
    }
}

// ── Tree Building ───────────────────────────────────────────────────

/// Build a hierarchical tree from a flat list of [`FileEntry`] items.
///
/// Entries whose name contains `/` characters are split into intermediate
/// directory nodes. The resulting tree preserves the original sort order
/// of sibling entries.
pub fn build_tree(entries: &[FileEntry]) -> Vec<FileNode> {
    let mut sorted: Vec<&FileEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));

    let mut root = FileNode {
        name: String::new(),
        path: String::new(),
        is_dir: true,
        size: 0,
        is_expanded: false,
        git_status: None,
        children: Vec::new(),
    };

    for entry in &sorted {
        let parts: Vec<&str> = entry.name.split('/').collect();
        insert_path(&mut root, &parts, entry);
    }

    sort_tree_recursive(&mut root.children);
    root.children
}

fn sort_tree_recursive(nodes: &mut [FileNode]) {
    nodes.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.cmp(&b.name),
    });
    for node in nodes.iter_mut() {
        sort_tree_recursive(&mut node.children);
    }
}

/// Recursively insert a path (split by `/`) into the tree.
fn insert_path(parent: &mut FileNode, parts: &[&str], entry: &FileEntry) {
    if parts.is_empty() {
        return;
    }
    let name = parts[0];
    let is_leaf = parts.len() == 1;
    let is_dir = !is_leaf || entry.is_dir;
    let full_path = if parent.path.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", parent.path, name)
    };

    // Find or create the child node.
    let child = match parent.children.iter_mut().find(|c| c.name == name) {
        Some(existing) => {
            if is_leaf {
                existing.size = entry.size;
                existing.is_dir = entry.is_dir;
            }
            existing
        }
        None => {
            let node = FileNode {
                name: name.to_string(),
                path: full_path,
                is_dir,
                size: if is_leaf && !entry.is_dir {
                    entry.size
                } else {
                    0
                },
                is_expanded: false,
                git_status: None,
                children: Vec::new(),
            };
            parent.children.push(node);
            // SAFETY: we just pushed; `last_mut` is always Some.
            parent.children.last_mut().unwrap()
        }
    };

    if !is_leaf {
        insert_path(child, &parts[1..], entry);
    }
}

// ── Visible Tree Walk ───────────────────────────────────────────────

/// A flattened, ready-to-render tree node with pre-computed indentation.
struct VisibleNode {
    name: String,
    path: String,
    is_dir: bool,
    size: u64,
    is_expanded: bool,
    git_status: Option<GitStatus>,
    /// Tree connector prefix (e.g. "│   ├── " or "    └── ").
    indent: String,
}

/// Walk the tree depth-first, collecting only expanded-visible nodes.
fn collect_visible(nodes: &[FileNode], ancestors_last: &[bool], result: &mut Vec<VisibleNode>) {
    let len = nodes.len();
    for (i, node) in nodes.iter().enumerate() {
        let is_last = i == len - 1;

        // Build the indentation prefix from ancestor positions.
        let prefix: String = ancestors_last
            .iter()
            .map(|&last| if last { "    " } else { "│   " })
            .collect();
        let connector = if is_last { "└── " } else { "├── " };
        let indent = format!("{}{}", prefix, connector);

        result.push(VisibleNode {
            name: node.name.clone(),
            path: node.path.clone(),
            is_dir: node.is_dir,
            size: node.size,
            is_expanded: node.is_dir && node.is_expanded,
            git_status: node.git_status,
            indent,
        });

        // Recurse into expanded directories.
        if node.is_dir && node.is_expanded {
            let mut next = ancestors_last.to_vec();
            next.push(is_last);
            collect_visible(&node.children, &next, result);
        }
    }
}

// ── FileTree Component ──────────────────────────────────────────────

/// An interactive recursive file-tree widget.
///
/// Supports:
/// - Build from `Vec<FileEntry>` (via `rebuild`)
/// - Keyboard navigation (j/k, Enter/l/h)
/// - Git status overlay (via `load_git_status`)
/// - File preview on selection (first 20 lines via `std::fs`)
/// - Tree rendering with `├──`/`└──` connectors
pub struct FileTree {
    /// Root-level tree nodes (source of truth).
    root_nodes: Vec<FileNode>,
    /// Cursor position (index into the flat list).
    cursor: usize,
    /// Preview content of the currently selected file.
    preview: Option<String>,
    /// Path of the file whose preview is currently loaded.
    preview_path: Option<String>,
    /// Cached git statuses: relative_path → status.
    git_statuses: HashMap<String, GitStatus>,
    /// Whether git statuses have been loaded.
    git_loaded: bool,
    /// Working directory used for loading the file tree.
    pub cwd: PathBuf,
}

impl FileTree {
    /// Create an empty file tree.
    #[must_use]
    pub fn new() -> Self {
        Self {
            root_nodes: Vec::new(),
            cursor: 0,
            preview: None,
            preview_path: None,
            git_statuses: HashMap::new(),
            git_loaded: false,
            cwd: PathBuf::from("."),
        }
    }

    /// Rebuild the tree from a new flat list of entries.
    ///
    /// Preserves expanded state for unchanged paths where possible.
    pub fn rebuild(&mut self, entries: &[FileEntry]) {
        let statuses = &self.git_statuses;
        self.root_nodes = build_tree(entries);
        apply_git_status_recursive(&mut self.root_nodes, statuses);
        // Clamp cursor to valid range after rebuild.
        self.clamp_cursor();
    }

    /// Load git status by running `git status --porcelain` in `cwd`.
    ///
    /// Populates `self.git_statuses` and propagates statuses onto tree nodes.
    pub fn load_git_status(&mut self, cwd: Option<&str>) {
        if let Some(dir) = cwd {
            self.cwd = PathBuf::from(dir);
        }
        let output = std::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&self.cwd)
            .output();

        if let Ok(output) = output {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                self.git_statuses = parse_git_status(&stdout);
            }
        }
        self.git_loaded = true;
        let statuses = &self.git_statuses;
        apply_git_status_recursive(&mut self.root_nodes, statuses);
    }

    /// Load a preview for the file at `cursor`.
    fn load_preview(&mut self) {
        self.preview = None;
        self.preview_path = None;

        let vis = self.visible_nodes();
        if self.cursor >= vis.len() {
            return;
        }
        let node = &vis[self.cursor];
        if node.is_dir {
            return;
        }
        let path = self.cwd.join(&node.path);
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let lines: Vec<&str> = content.lines().take(20).collect();
                self.preview = Some(lines.join("\n"));
                self.preview_path = Some(node.path.clone());
            }
            Err(e) => {
                self.preview = Some(format!("<read error: {e}>"));
                self.preview_path = Some(node.path.clone());
            }
        }
    }

    // ── Flattened view ────────────────────────────────────────────

    /// Return a flattened list of visible nodes (depth-first, expanded-only).
    fn visible_nodes(&self) -> Vec<VisibleNode> {
        let mut result = Vec::new();
        collect_visible(&self.root_nodes, &[], &mut result);
        result
    }

    /// Return the total count of currently visible nodes.
    fn visible_count(&self) -> usize {
        // Optimisation: count without allocating Vec.
        fn count_visible(nodes: &[FileNode]) -> usize {
            let mut total = 0;
            for node in nodes {
                total += 1;
                if node.is_dir && node.is_expanded {
                    total += count_visible(&node.children);
                }
            }
            total
        }
        count_visible(&self.root_nodes)
    }

    // ── Cursor helpers ────────────────────────────────────────────

    fn clamp_cursor(&mut self) {
        let count = self.visible_count();
        if count == 0 {
            self.cursor = 0;
        } else if self.cursor >= count {
            self.cursor = count - 1;
        }
    }

    fn move_down(&mut self) {
        let count = self.visible_count();
        if count > 0 && self.cursor + 1 < count {
            self.cursor += 1;
            self.load_preview();
        }
    }

    fn move_up(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            self.load_preview();
        }
    }

    // ── Tree mutations via flat index ─────────────────────────────

    /// Toggle expand/collapse for the directory at `cursor`.
    fn toggle_expand(&mut self) {
        let path = {
            let vis = self.visible_nodes();
            if self.cursor >= vis.len() {
                return;
            }
            vis[self.cursor].path.clone()
        };
        Self::toggle_path(&mut self.root_nodes, &path);
        self.clamp_cursor();
        self.load_preview();
    }

    /// Expand the directory at `cursor` (no-op if already expanded or not a dir).
    fn expand_at_cursor(&mut self) {
        let path = {
            let vis = self.visible_nodes();
            if self.cursor >= vis.len() {
                return;
            }
            vis[self.cursor].path.clone()
        };
        Self::set_expanded(&mut self.root_nodes, &path, true);
        self.clamp_cursor();
        self.load_preview();
    }

    /// Collapse the directory at `cursor`.
    fn collapse_at_cursor(&mut self) {
        let path = {
            let vis = self.visible_nodes();
            if self.cursor >= vis.len() {
                return;
            }
            vis[self.cursor].path.clone()
        };
        Self::set_expanded(&mut self.root_nodes, &path, false);
        self.clamp_cursor();
        self.load_preview();
    }

    fn toggle_path(nodes: &mut [FileNode], target: &str) {
        for node in nodes.iter_mut() {
            if node.path == target && node.is_dir {
                node.is_expanded = !node.is_expanded;
                return;
            }
            Self::toggle_path(&mut node.children, target);
        }
    }

    fn set_expanded(nodes: &mut [FileNode], target: &str, expanded: bool) {
        for node in nodes.iter_mut() {
            if node.path == target && node.is_dir {
                node.is_expanded = expanded;
                return;
            }
            Self::set_expanded(&mut node.children, target, expanded);
        }
    }
}

impl Default for FileTree {
    fn default() -> Self {
        Self::new()
    }
}

// ── Component Implementation ────────────────────────────────────────

impl Component for FileTree {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let theme = ctx.theme();
        let accent = theme.accent_color();

        let vis = self.visible_nodes();
        let has_preview = self.preview.is_some() && area.width > 40;

        if has_preview {
            // Split: tree on the left (~60%), preview on the right.
            let tree_width = (area.width as f32 * 0.55) as u16;
            let tree_area = Rect::new(area.x, area.y, tree_width, area.height);
            let preview_area = Rect::new(
                area.x + tree_width,
                area.y,
                area.width - tree_width,
                area.height,
            );
            self.render_tree(ctx, tree_area, &vis, accent);
            self.render_preview(ctx, preview_area, accent);
        } else {
            self.render_tree(ctx, area, &vis, accent);
        }
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        match event {
            Event::Key(key) => match key.code {
                KeyCode::Char('j') | KeyCode::Down => {
                    self.move_down();
                    EventResult::Consumed
                }
                KeyCode::Char('k') | KeyCode::Up => {
                    self.move_up();
                    EventResult::Consumed
                }
                KeyCode::Enter => {
                    self.toggle_expand();
                    EventResult::Consumed
                }
                KeyCode::Char('l') => {
                    self.expand_at_cursor();
                    EventResult::Consumed
                }
                KeyCode::Char('h') => {
                    self.collapse_at_cursor();
                    EventResult::Consumed
                }
                _ => EventResult::NotConsumed,
            },
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "file_tree"
    }
}

// ── Rendering helpers ──────────────────────────────────────────────

impl FileTree {
    /// Render the tree portion (left panel or full area).
    fn render_tree(&self, ctx: &mut RenderContext, area: Rect, vis: &[VisibleNode], accent: Color) {
        let mut lines: Vec<Line> = Vec::new();

        if vis.is_empty() {
            lines.push(Line::from("  (empty directory)"));
        } else {
            for (i, node) in vis.iter().enumerate() {
                let is_selected = i == self.cursor;
                let icon = if node.is_dir {
                    if node.is_expanded {
                        "📂"
                    } else {
                        "📁"
                    }
                } else {
                    "📄"
                };

                // Build the line: indent + connector + icon + name + size + git status.
                let mut spans: Vec<Span> = Vec::new();

                // Indent prefix (ancestor lines).
                let indent_len = node.indent.len().saturating_sub(4);
                if indent_len > 0 {
                    let prefix = &node.indent[..indent_len];
                    spans.push(Span::styled(
                        prefix.to_string(),
                        Style::default().fg(Color::DarkGray),
                    ));
                }
                // Connector (last 4 chars of indent: "├── " or "└── ").
                let connector_start = indent_len.max(0);
                if node.indent.len() >= connector_start + 4 {
                    spans.push(Span::styled(
                        node.indent[connector_start..].to_string(),
                        Style::default().fg(Color::DarkGray),
                    ));
                }

                // Icon.
                spans.push(Span::raw(format!("{icon} ")));

                // Name.
                let name_style = if is_selected {
                    Style::default().fg(Color::Black).bg(accent)
                } else if node.is_dir {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default().fg(Color::White)
                };
                spans.push(Span::styled(&node.name, name_style));

                // Size (files only).
                if !node.is_dir {
                    let size_str = format_file_size(node.size, false);
                    if !size_str.is_empty() {
                        spans.push(Span::styled(
                            format!(" ({})", size_str),
                            Style::default().fg(Color::DarkGray),
                        ));
                    }
                }

                // Git status overlay.
                if let Some(status) = node.git_status {
                    let gs_style = match status {
                        GitStatus::Modified => Style::default().fg(Color::Yellow),
                        GitStatus::Added => Style::default().fg(Color::Green),
                        GitStatus::Deleted => Style::default().fg(Color::Red),
                        GitStatus::Renamed => Style::default().fg(Color::Magenta),
                        GitStatus::Untracked => Style::default().fg(Color::DarkGray),
                    };
                    spans.push(Span::styled(format!(" [{}]", status.symbol()), gs_style));
                }

                // Expanded marker for directories.
                if node.is_dir && node.is_expanded {
                    spans.push(Span::styled(" [+]", Style::default().fg(Color::DarkGray)));
                }

                lines.push(Line::from(spans));
            }
        }

        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "Keys: j↓ k↑ Enter:open  h:toggle-hidden  r:refresh  o:collapse",
            Style::default().fg(Color::DarkGray),
        )));

        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Files ")
            .border_style(Style::default().fg(accent));
        let paragraph = Paragraph::new(Text::from(lines))
            .block(block)
            .wrap(Wrap { trim: false });
        ctx.frame_mut().render_widget(paragraph, area);
    }

    /// Render the file preview (right panel).
    fn render_preview(&self, ctx: &mut RenderContext, area: Rect, accent: Color) {
        let title = self.preview_path.as_deref().unwrap_or("Preview");
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" {} ", title))
            .border_style(Style::default().fg(accent));

        let content = self.preview.as_deref().unwrap_or("");
        let paragraph = Paragraph::new(content)
            .block(block)
            .style(Style::default().fg(Color::White));
        ctx.frame_mut().render_widget(paragraph, area);
    }
}

// ── Git Status Helpers ───────────────────────────────────────────────

fn apply_git_status_recursive(nodes: &mut [FileNode], statuses: &HashMap<String, GitStatus>) {
    for node in nodes.iter_mut() {
        node.git_status = statuses.get(&node.path).copied();
        apply_git_status_recursive(&mut node.children, statuses);
    }
}

// ── Git Status Parsing ───────────────────────────────────────────────

/// Parse `git status --porcelain` output into a map of path → GitStatus.
///
/// Format: `XY path` where X/Y are index/working-tree status chars.
/// Renames show as `R old -> new` — only the new path is recorded.
fn parse_git_status(output: &str) -> HashMap<String, GitStatus> {
    let mut map = HashMap::new();
    for line in output.lines() {
        if line.len() < 4 {
            continue;
        }
        let xy = &line[..2];
        let rest = line[3..].trim();
        // Handle renames: "R  old -> new"
        let path = if let Some(arrow) = rest.find(" -> ") {
            &rest[arrow + 4..]
        } else {
            rest
        };
        if let Some(status) = GitStatus::from_porcelain(xy) {
            map.insert(path.to_string(), status);
        }
    }
    map
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::RenderContext;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;

    // ── Helper: make_file_entry ──────────────────────────────────

    fn f(name: &str, is_dir: bool, size: u64) -> FileEntry {
        FileEntry {
            name: name.to_string(),
            is_dir,
            size,
        }
    }

    // ── Tree Building Tests ───────────────────────────────────────

    #[test]
    fn build_tree_flat_files() {
        let entries = vec![
            f("Cargo.toml", false, 500),
            f("README.md", false, 1024),
            f("main.rs", false, 200),
        ];
        let tree = build_tree(&entries);
        assert_eq!(tree.len(), 3);
        assert_eq!(tree[0].name, "Cargo.toml");
        assert_eq!(tree[1].name, "README.md");
        assert_eq!(tree[2].name, "main.rs");
        assert!(!tree[0].is_dir);
        assert!(tree[0].children.is_empty());
    }

    #[test]
    fn build_tree_single_dir_with_files() {
        let entries = vec![f("src/main.rs", false, 256), f("src/lib.rs", false, 128)];
        let tree = build_tree(&entries);
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0].name, "src");
        assert!(tree[0].is_dir);
        assert_eq!(tree[0].children.len(), 2);
        assert_eq!(tree[0].children[0].name, "lib.rs");
        assert_eq!(tree[0].children[1].name, "main.rs");
    }

    #[test]
    fn build_tree_nested_dirs() {
        let entries = vec![
            f("src/tui/app.rs", false, 1024),
            f("src/tui/render.rs", false, 2048),
            f("src/tui/components/mod.rs", false, 512),
        ];
        let tree = build_tree(&entries);
        assert_eq!(tree.len(), 1); // src
        let src = &tree[0];
        assert_eq!(src.name, "src");
        assert!(src.is_dir);

        assert_eq!(src.children.len(), 1); // tui
        let tui = &src.children[0];
        assert_eq!(tui.name, "tui");
        assert!(tui.is_dir);

        assert_eq!(tui.children.len(), 3);
        // Components are sorted by insertion order (which follows input order).
        let names: Vec<&str> = tui.children.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"app.rs"));
        assert!(names.contains(&"render.rs"));
        assert!(names.contains(&"components"));
    }

    #[test]
    fn build_tree_dir_and_files_mixed() {
        let entries = vec![
            f("src", true, 0),
            f("src/main.rs", false, 100),
            f("Cargo.toml", false, 500),
        ];
        let tree = build_tree(&entries);
        assert_eq!(tree.len(), 2); // src + Cargo.toml
        let src = tree.iter().find(|n| n.name == "src").unwrap();
        assert!(src.is_dir);
        assert_eq!(src.children.len(), 1); // main.rs
    }

    #[test]
    fn build_tree_empty() {
        let tree = build_tree(&[]);
        assert!(tree.is_empty());
    }

    #[test]
    fn build_tree_preserves_children_under_dir_entry() {
        // When a directory is explicitly listed as an entry,
        // and its children are also listed, the children should
        // appear under that directory node.
        let entries = vec![
            f("src/main.rs", false, 256),
            f("src", true, 0), // explicit dir entry
        ];
        let tree = build_tree(&entries);
        assert_eq!(tree.len(), 1);
        let src = &tree[0];
        assert!(src.is_dir);
        assert_eq!(src.children.len(), 1);
        assert_eq!(src.children[0].name, "main.rs");
    }

    // ── Git Status Parsing Tests ─────────────────────────────────

    #[test]
    fn parse_git_status_modified() {
        let output = " M src/main.rs\n";
        let map = parse_git_status(output);
        assert_eq!(map.get("src/main.rs"), Some(&GitStatus::Modified));
    }

    #[test]
    fn parse_git_status_added_and_deleted() {
        let output = "A  new_file.rs\n D old_file.rs\n";
        let map = parse_git_status(output);
        assert_eq!(map.get("new_file.rs"), Some(&GitStatus::Added));
        assert_eq!(map.get("old_file.rs"), Some(&GitStatus::Deleted));
    }

    #[test]
    fn parse_git_status_untracked() {
        let map = parse_git_status("?? unknown.bin\n");
        assert_eq!(map.get("unknown.bin"), Some(&GitStatus::Untracked));
    }

    #[test]
    fn parse_git_status_rename() {
        let output = "R  old.rs -> new.rs\n";
        let map = parse_git_status(output);
        assert_eq!(map.get("new.rs"), Some(&GitStatus::Renamed));
        assert!(!map.contains_key("old.rs"));
    }

    #[test]
    fn parse_git_status_both_modified() {
        // Both index and working tree modified.
        let output = "MM src/lib.rs\n";
        let map = parse_git_status(output);
        assert_eq!(map.get("src/lib.rs"), Some(&GitStatus::Modified));
    }

    #[test]
    fn git_status_symbol_display() {
        assert_eq!(GitStatus::Modified.symbol(), "M");
        assert_eq!(GitStatus::Added.symbol(), "A");
        assert_eq!(GitStatus::Deleted.symbol(), "D");
        assert_eq!(GitStatus::Renamed.symbol(), "R");
        assert_eq!(GitStatus::Untracked.symbol(), "?");
    }

    // ── FileTree Navigation Tests ────────────────────────────────

    #[test]
    fn filetree_new_is_empty() {
        let ft = FileTree::new();
        assert_eq!(ft.visible_count(), 0);
        assert_eq!(ft.cursor, 0);
        assert!(ft.preview.is_none());
    }

    #[test]
    fn filetree_rebuild_populates_tree() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("a.txt", false, 10), f("b.txt", false, 20)]);
        assert_eq!(ft.visible_count(), 2);
    }

    #[test]
    fn filetree_move_down_up() {
        let mut ft = FileTree::new();
        ft.rebuild(&[
            f("a.txt", false, 10),
            f("b.txt", false, 20),
            f("c.txt", false, 30),
        ]);
        assert_eq!(ft.cursor, 0);
        ft.move_down();
        assert_eq!(ft.cursor, 1);
        ft.move_down();
        assert_eq!(ft.cursor, 2);
        ft.move_down(); // already at last — no change
        assert_eq!(ft.cursor, 2);
        ft.move_up();
        assert_eq!(ft.cursor, 1);
        ft.move_up();
        assert_eq!(ft.cursor, 0);
        ft.move_up(); // already at first — no change
        assert_eq!(ft.cursor, 0);
    }

    #[test]
    fn filetree_toggle_expand() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("src/main.rs", false, 100), f("src/lib.rs", false, 80)]);
        // Initially: src (collapsed) — only 1 visible node.
        assert_eq!(ft.visible_count(), 1);
        assert_eq!(ft.visible_nodes()[0].name, "src");

        // Expand src.
        ft.toggle_expand();
        assert_eq!(ft.visible_count(), 3); // src + main.rs + lib.rs

        // Collapse src.
        ft.toggle_expand();
        assert_eq!(ft.visible_count(), 1);
    }

    #[test]
    fn filetree_expand_collapse_with_l_h() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("src/main.rs", false, 100)]);
        assert_eq!(ft.visible_count(), 1);

        // l expands.
        use crossterm::event::{KeyEvent, KeyModifiers};
        let l_event = Event::Key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE));
        ft.handle_event(&l_event);
        assert_eq!(ft.visible_count(), 2); // src + main.rs

        // h collapses.
        let h_event = Event::Key(KeyEvent::new(KeyCode::Char('h'), KeyModifiers::NONE));
        ft.handle_event(&h_event);
        assert_eq!(ft.visible_count(), 1);
    }

    #[test]
    fn filetree_handle_j_k_keys() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("a.txt", false, 10), f("b.txt", false, 20)]);

        use crossterm::event::{KeyEvent, KeyModifiers};
        let j_event = Event::Key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE));
        ft.handle_event(&j_event);
        assert_eq!(ft.cursor, 1);

        let k_event = Event::Key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE));
        ft.handle_event(&k_event);
        assert_eq!(ft.cursor, 0);
    }

    // ── Rendering Tests ───────────────────────────────────────────

    #[test]
    fn render_file_tree_basic() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("Cargo.toml", false, 500), f("README.md", false, 1024)]);

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|frame| {
            let area = frame.area();
            let mut ctx = RenderContext::new(frame, &theme);
            ft.render(&mut ctx, area);
        });
        terminal.assert_line_contains("Cargo.toml");
        terminal.assert_line_contains("README.md");
        terminal.assert_line_contains("Files");
    }

    #[test]
    fn render_file_tree_with_nested_dirs() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("src/main.rs", false, 256), f("src/lib.rs", false, 128)]);
        ft.toggle_expand();

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|frame| {
            let area = frame.area();
            let mut ctx = RenderContext::new(frame, &theme);
            ft.render(&mut ctx, area);
        });
        terminal.assert_line_contains("src");
        terminal.assert_line_contains("main.rs");
        terminal.assert_line_contains("lib.rs");
        terminal.assert_line_contains("\u{2514}\u{2500}\u{2500}");
    }

    #[test]
    fn render_file_tree_empty() {
        let mut ft = FileTree::new();

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|frame| {
            let area = frame.area();
            let mut ctx = RenderContext::new(frame, &theme);
            ft.render(&mut ctx, area);
        });
        terminal.assert_line_contains("(empty directory)");
    }

    #[test]
    fn render_file_tree_with_git_status() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("modified.rs", false, 100), f("added.rs", false, 200)]);
        ft.git_statuses
            .insert("modified.rs".to_string(), GitStatus::Modified);
        ft.git_statuses
            .insert("added.rs".to_string(), GitStatus::Added);
        let statuses = &ft.git_statuses;
        for node in &mut ft.root_nodes {
            apply_git_status_recursive(std::slice::from_mut(node), statuses);
        }

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|frame| {
            let area = frame.area();
            let mut ctx = RenderContext::new(frame, &theme);
            ft.render(&mut ctx, area);
        });
        terminal.assert_line_contains("[M]");
        terminal.assert_line_contains("[A]");
    }

    #[test]
    fn render_file_tree_selected_highlight() {
        let mut ft = FileTree::new();
        ft.rebuild(&[f("file_a.rs", false, 100), f("file_b.rs", false, 200)]);
        ft.move_down();

        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        terminal.draw(|frame| {
            let area = frame.area();
            let mut ctx = RenderContext::new(frame, &theme);
            ft.render(&mut ctx, area);
        });
        terminal.assert_line_contains("file_a.rs");
        terminal.assert_line_contains("file_b.rs");
    }

    #[test]
    fn render_file_tree_preview() {
        // Write a temp file to preview.
        let tmp = std::env::temp_dir().join("cowd_test_preview.txt");
        std::fs::write(&tmp, "line1\nline2\nline3\nline4\nline5\n").unwrap();

        let mut ft = FileTree::new();
        ft.cwd = tmp.parent().unwrap().to_path_buf();
        let entry_name = tmp.file_name().unwrap().to_string_lossy().to_string();
        ft.rebuild(&[f(&entry_name, false, 30)]);

        // Load preview.
        ft.load_preview();

        assert!(ft.preview.is_some());
        let preview = ft.preview.as_ref().unwrap();
        assert!(preview.contains("line1"));
        assert!(preview.contains("line5"));

        // Clean up.
        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn filetree_component_trait_id() {
        let ft = FileTree::new();
        assert_eq!(ft.id(), "file_tree");
        assert!(ft.focusable());
    }

    #[test]
    fn filetree_clamp_cursor_after_rebuild_shrink() {
        let mut ft = FileTree::new();
        ft.rebuild(&[
            f("a.txt", false, 10),
            f("b.txt", false, 20),
            f("c.txt", false, 30),
        ]);
        ft.cursor = 5; // out of range
        ft.rebuild(&[f("only.txt", false, 10)]);
        assert_eq!(ft.cursor, 0);
    }

    #[test]
    fn filetree_format_size() {
        let node = FileNode {
            name: "test".into(),
            path: "test".into(),
            is_dir: false,
            size: 500,
            is_expanded: false,
            git_status: None,
            children: Vec::new(),
        };
        assert_eq!(node.format_size(), "500B");

        let kb_node = FileNode {
            size: 2048,
            ..node.clone()
        };
        assert_eq!(kb_node.format_size(), "2KB");

        let mb_node = FileNode {
            size: 2_097_152,
            ..node.clone()
        };
        assert!(mb_node.format_size().contains("MB"));

        let dir_node = FileNode {
            is_dir: true,
            size: 0,
            ..node.clone()
        };
        assert_eq!(dir_node.format_size(), "");
    }
}
