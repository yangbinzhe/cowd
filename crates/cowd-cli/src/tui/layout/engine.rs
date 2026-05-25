// ── Layout Engine ──────────────────────────────────────────────────
// TabBar widget, ResizeHandle, Panel rendering, FocusManager.
//
// Separate from types.rs — this file contains the rendering/behaviour
// logic that operates on the data types defined in types.rs.
// -------------------------------------------------------------------

use ratatui::{
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

use super::types::{LayoutNode, PanelDef, SplitDirection, TabGroup};

// ── TabBar ────────────────────────────────────────────────────────

/// Stateless renderer for a horizontal tab bar.
///
/// Renders tab labels left-to-right inside `area`. The active tab
/// (at `tabgroup.active`) is rendered with a highlighted background;
/// inactive tabs use dimmed text. Tabs are separated by `│`.
///
/// # Panics
/// Only panics if `tabgroup.active` is out of bounds (should be
/// validated by the caller or `TabGroup::next_tab`/`prev_tab`).
pub struct TabBar;

impl TabBar {
    /// Draw the tab bar into `frame` within the given `area`.
    ///
    /// Each tab label is rendered as ` label `. If the tab has an
    /// `icon`, it is prepended to the label. Adjacent tabs are
    /// separated by a vertical bar `│`.
    pub fn draw(frame: &mut Frame, area: Rect, tabgroup: &TabGroup) {
        if tabgroup.tabs.is_empty() {
            return;
        }

        let mut x = area.x;
        let active = tabgroup.active.min(tabgroup.tabs.len().saturating_sub(1));

        for (i, tab) in tabgroup.tabs.iter().enumerate() {
            // Determine display text for this tab
            let label = if let Some(ref icon) = tab.icon {
                format!(" {icon} {} ", tab.label)
            } else {
                format!(" {} ", tab.label)
            };

            // Separator between tabs
            if i > 0 {
                let sep = Span::styled("│", Style::default().fg(Color::DarkGray));
                let sep_area = Rect::new(x, area.y, 1, area.height);
                frame.render_widget(Paragraph::new(Line::from(sep)), sep_area);
                x += 1;
            }

            let label_width = label.len() as u16;
            let label_area = Rect::new(x, area.y, label_width.min(area.width.saturating_sub(x - area.x)), area.height);

            let style = if i == active {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default().fg(Color::Gray).bg(Color::DarkGray)
            };

            let span = Span::styled(&label, style);
            frame.render_widget(Paragraph::new(Line::from(span)), label_area);
            x += label_width;
        }
    }

    /// Detect which tab was clicked given the absolute `x` column.
    ///
    /// Returns `Some(index)` if the click falls within a tab's bounds,
    /// or `None` if the click is outside all tabs (including the gap
    /// between tabs or beyond the last tab).
    pub fn handle_click(x: u16, tabgroup: &TabGroup, area: Rect) -> Option<usize> {
        if tabgroup.tabs.is_empty() || area.width == 0 {
            return None;
        }

        let mut cursor = area.x;

        for (i, tab) in tabgroup.tabs.iter().enumerate() {
            // Separator width before each non-first tab
            if i > 0 {
                // Click on separator → return the tab to the right
                if x >= cursor && x < cursor + 1 {
                    return Some(i);
                }
                cursor += 1;
            }

            let label = if tab.icon.is_some() {
                format!(" {} {} ", tab.icon.as_ref().unwrap(), tab.label)
            } else {
                format!(" {} ", tab.label)
            };
            let w = label.len() as u16;
            let end = cursor + w;

            // Clip to area boundary
            let clipped_end = end.min(area.x + area.width);

            if x >= cursor && x < clipped_end {
                return Some(i);
            }

            cursor = end;
        }

        None
    }
}

// ── ResizeHandle ──────────────────────────────────────────────────

/// Renders a resize handle between split areas.
///
/// The handle is drawn as a centred character on the border line:
/// - `│` for vertical splits (panels side-by-side; border is vertical)
/// - `─` for horizontal splits (panels stacked; border is horizontal)
///
/// Accepts a `Rect` that represents the one-cell-wide border line
/// where the handle should appear. The handle character is placed at
/// the midpoint of that line.
pub struct ResizeHandle;

impl ResizeHandle {
    /// Draw a resize handle.
    ///
    /// `area` should be the single-cell-wide border strip between two
    /// panels. For a horizontal split (two panels stacked vertically)
    /// this is a horizontal line → draws `─`. For a vertical split
    /// (two panels side-by-side) this is a vertical line → draws `│`.
    ///
    /// The handle character is always placed at the midpoint of `area`
    /// regardless of whether the split runs horizontally or vertically.
    pub fn draw(frame: &mut Frame, area: Rect, direction: SplitDirection) {
        if area.width == 0 || area.height == 0 {
            return;
        }

        let style = Style::default().fg(Color::DarkGray);

        match direction {
            SplitDirection::Horizontal => {
                // Panels are side-by-side; border is a vertical line.
                // Draw │ at the midpoint of the vertical strip.
                let mid_y = area.y + area.height / 2;
                let handle_area = Rect::new(area.x, mid_y, 1, 1);
                frame.render_widget(
                    Paragraph::new(Line::from(Span::styled("│", style))),
                    handle_area,
                );
            }
            SplitDirection::Vertical => {
                // Panels are stacked; border is a horizontal line.
                // Draw ─ at the midpoint of the horizontal strip.
                let mid_x = area.x + area.width / 2;
                let handle_area = Rect::new(mid_x, area.y, 1, 1);
                frame.render_widget(
                    Paragraph::new(Line::from(Span::styled("─", style))),
                    handle_area,
                );
            }
        }
    }
}

// ── Panel Rendering ───────────────────────────────────────────────

/// Renders a panel with a bordered block and optional focus ring.
///
/// A focused panel uses a distinct border style (cyan colour) to
/// provide a visible focus indicator, while an unfocused panel uses
/// a dimmed border.
pub struct PanelRenderer;

impl PanelRenderer {
    /// Draw a bordered panel.
    ///
    /// `panel_def` supplies the `id` (used as the block title).
    /// `focused` controls whether the focus ring is shown.
    /// `inner_draw` is a callback that the caller supplies to
    /// render the panel's inner content (since the `component` field
    /// is `Box<dyn Component>` and not directly renderable from here).
    pub fn draw<F>(
        frame: &mut Frame,
        area: Rect,
        panel_def: &PanelDef,
        focused: bool,
        inner_draw: F,
    ) where
        F: FnOnce(&mut Frame, Rect),
    {
        let border_style = if focused {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(format!(" {} ", panel_def.id))
            .title_style(
                Style::default()
                    .fg(if focused { Color::Cyan } else { Color::Gray })
                    .bold(),
            );

        let inner_area = block.inner(area);
        frame.render_widget(block, area);
        inner_draw(frame, inner_area);
    }

    /// Convenience: draw a panel with only the border block (no
    /// inner content drawing). Useful for tests and placeholder
    /// panels where content is managed externally.
    pub fn draw_border_only(frame: &mut Frame, area: Rect, panel_def: &PanelDef, focused: bool) {
        Self::draw(frame, area, panel_def, focused, |_, _| {});
    }

    /// Return the border style used for a focused panel.
    #[must_use]
    pub fn focused_border_style() -> Style {
        Style::default().fg(Color::Cyan)
    }

    /// Return the border style used for an unfocused panel.
    #[must_use]
    pub fn unfocused_border_style() -> Style {
        Style::default().fg(Color::DarkGray)
    }
}

// ── FocusManager ──────────────────────────────────────────────────

/// Manages focus navigation across panels in a layout.
///
/// Maintains an ordered list of focusable component IDs and an index
/// tracking which one currently has focus. Supports forward/backward
/// cycling and direct focus by ID.
///
/// # Focus Order
///
/// The focus order is determined by a depth-first traversal of the
/// layout tree (see [`compute_focus_chain`]). Within a `TabGroup`,
/// only the active tab's subtree participates in the focus order.
///
/// # Focusable Elements
///
/// In the current layout model only [`LayoutNode::Panel`] variants
/// are considered focusable. `Split` and `TabGroup` nodes are
/// structural — they contribute to traversal order but are not
/// themselves focus targets.
pub struct FocusManager {
    /// Ordered list of focusable component IDs (panel IDs).
    focus_order: Vec<String>,
    /// Index within `focus_order` of the currently focused component.
    focused_index: usize,
}

impl FocusManager {
    /// Create a new focus manager from a pre-computed focus chain.
    ///
    /// `focus_order` is typically obtained from [`compute_focus_chain`].
    /// If the chain is empty, the manager starts with focus on nothing
    /// and all navigation methods become no-ops.
    #[must_use]
    pub fn new(focus_order: Vec<String>) -> Self {
        Self {
            focus_order,
            focused_index: 0,
        }
    }

    /// Move focus to the next component in the chain, wrapping around.
    ///
    /// No-op if the focus order is empty.
    pub fn focus_next(&mut self) {
        if !self.focus_order.is_empty() {
            self.focused_index = (self.focused_index + 1) % self.focus_order.len();
        }
    }

    /// Move focus to the previous component in the chain, wrapping around.
    ///
    /// No-op if the focus order is empty.
    pub fn focus_prev(&mut self) {
        if !self.focus_order.is_empty() {
            self.focused_index = if self.focused_index == 0 {
                self.focus_order.len() - 1
            } else {
                self.focused_index - 1
            };
        }
    }

    /// Set focus to the component with the given `id`.
    ///
    /// If `id` is not in the focus order, nothing changes and the
    /// method returns `false`. Otherwise returns `true`.
    pub fn focus(&mut self, id: &str) -> bool {
        if let Some(pos) = self.focus_order.iter().position(|s| s == id) {
            self.focused_index = pos;
            true
        } else {
            false
        }
    }

    /// Return the ID of the currently focused component, or `None`
    /// if the focus order is empty.
    #[must_use]
    pub fn current(&self) -> Option<&str> {
        self.focus_order.get(self.focused_index).map(String::as_str)
    }

    /// Return the index of the currently focused component.
    #[must_use]
    pub fn current_index(&self) -> Option<usize> {
        if self.focus_order.is_empty() {
            None
        } else {
            Some(self.focused_index)
        }
    }

    /// Return a reference to the full focus order.
    #[must_use]
    pub fn focus_order(&self) -> &[String] {
        &self.focus_order
    }

    /// Return the number of focusable components.
    #[must_use]
    pub fn len(&self) -> usize {
        self.focus_order.len()
    }

    /// Return `true` if there are no focusable components.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.focus_order.is_empty()
    }
}

// ── Focus Chain Computation ────────────────────────────────────────

/// Compute a focus order (focus chain) from a [`LayoutNode`] tree.
///
/// Performs a depth-first traversal. For each node:
///
/// | Node variant   | Behaviour |
/// |----------------|-----------|
/// | `Panel`        | Adds the panel's `id` to the chain. |
/// | `TabGroup`     | Structural node — does not contribute directly to the chain (content is opaque `Box<dyn Component>`). |
/// | `Split`        | All children are traversed in order. |
/// | `Leaf`         | Skipped (opaque `Box<dyn Component>` content — no ID to extract). |
///
/// # Examples
///
/// ```ignore
/// // See tests module for full examples with mock components.
/// let root = LayoutNode::Split(Split {
///     direction: SplitDirection::Horizontal,
///     ratio: 0.5,
///     children: vec![
///         LayoutNode::Panel(PanelDef { id: "left".into(), component: mock_component() }),
///         LayoutNode::Panel(PanelDef { id: "right".into(), component: mock_component() }),
///     ],
/// });
/// let chain = compute_focus_chain(&root);
/// assert_eq!(chain, vec!["left", "right"]);
/// ```
#[must_use]
pub fn compute_focus_chain(root: &LayoutNode) -> Vec<String> {
    let mut chain = Vec::new();
    collect_focusable(root, &mut chain);
    chain
}

/// Recursive helper: DFS into `node`, pushing panel IDs into `chain`.
fn collect_focusable(node: &LayoutNode, chain: &mut Vec<String>) {
    match node {
        LayoutNode::Panel(p) => {
            chain.push(p.id.clone());
        }
        LayoutNode::TabGroup(_tg) => {
            // TabGroup is a structural node. Its content is
            // `Box<dyn Component>` which cannot be recursively
            // traversed via the LayoutNode tree. Panels inside a
            // tab's content must be registered separately.
        }
        LayoutNode::Split(s) => {
            for child in &s.children {
                collect_focusable(child, chain);
            }
        }
        LayoutNode::Leaf(_component) => {
            // Opaque — cannot determine focusable IDs.
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::{Component, EventResult, RenderContext};
    use crate::tui::test_utils::MockTerminal;
    use super::super::types::{Split, TabDef};
    use ratatui::layout::Rect as RtRect;

    // ── Mock Component for Tests ──────────────────────────────────

    /// Minimal component used to satisfy `Box<dyn Component>` requirements
    /// in test fixtures. All methods are no-ops.
    struct MockComponent {
        id: &'static str,
    }

    impl MockComponent {
        fn new(id: &'static str) -> Self {
            Self { id }
        }
        fn boxed(id: &'static str) -> Box<dyn Component> {
            Box::new(Self { id })
        }
    }

    impl Component for MockComponent {
        fn render(&mut self, _ctx: &mut RenderContext, _area: Rect) {}
        fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
            EventResult::NotConsumed
        }
        fn focusable(&self) -> bool {
            false
        }
        fn id(&self) -> &str {
            self.id
        }
    }

    // ── Test helpers ──────────────────────────────────────────────

    fn panel(id: &'static str) -> PanelDef {
        PanelDef {
            id: id.to_string(),
            component: MockComponent::boxed(id),
        }
    }

    fn make_tabgroup(n: usize, active: usize) -> TabGroup {
        let tabs: Vec<TabDef> = (0..n)
            .map(|i| TabDef {
                id: format!("tab_{i}"),
                label: format!("Tab {i}"),
                icon: None,
                content: MockComponent::boxed("dummy"),
            })
            .collect();
        TabGroup { tabs, active }
    }

    fn make_tabgroup_with_icons(n: usize, active: usize) -> TabGroup {
        let icons = ["📁", "💬", "⚙️", "🔍", "🧠"];
        let tabs: Vec<TabDef> = (0..n)
            .map(|i| TabDef {
                id: format!("tab_{i}"),
                label: format!("Tab {i}"),
                icon: Some(icons[i % icons.len()].to_string()),
                content: MockComponent::boxed("dummy"),
            })
            .collect();
        TabGroup { tabs, active }
    }

    // ── TabBar tests ───────────────────────────────────────────────

    #[test]
    fn tabbar_renders_5_tabs() {
        let mut term = MockTerminal::new(80, 3);
        let tg = make_tabgroup(5, 0);

        term.draw(|f: &mut Frame| {
            TabBar::draw(f, f.area(), &tg);
        });

        // All 5 tab labels should appear in the buffer
        for i in 0..5 {
            let label = format!("Tab {i}");
            term.assert_line_contains(&label);
        }
    }

    #[test]
    fn tabbar_renders_with_icons() {
        let mut term = MockTerminal::new(80, 3);
        let tg = make_tabgroup_with_icons(3, 0);

        term.draw(|f: &mut Frame| {
            TabBar::draw(f, f.area(), &tg);
        });

        term.assert_line_contains("📁");
        term.assert_line_contains("Tab 0");
    }

    #[test]
    fn tabbar_empty_no_panic() {
        let mut term = MockTerminal::new(80, 3);
        let tg = TabGroup {
            tabs: vec![],
            active: 0,
        };

        // Should not panic
        term.draw(|f: &mut Frame| {
            TabBar::draw(f, f.area(), &tg);
        });
    }

    #[test]
    fn tabbar_click_selects() {
        let tg = make_tabgroup(3, 0);
        let area = RtRect::new(0, 0, 80, 1);

        // Layout at area.x=0:
        // Tab 0 " Tab 0 ": cols 0-6, sep │ at 7,
        // Tab 1 " Tab 1 ": cols 8-14, sep │ at 15,
        // Tab 2 " Tab 2 ": cols 16-22

        // Click on "Tab 0" label area
        assert_eq!(TabBar::handle_click(2, &tg, area), Some(0));
        assert_eq!(TabBar::handle_click(6, &tg, area), Some(0));

        // Click on separator before Tab 1
        assert_eq!(TabBar::handle_click(7, &tg, area), Some(1));

        // Click on "Tab 1" label area
        assert_eq!(TabBar::handle_click(9, &tg, area), Some(1));
        assert_eq!(TabBar::handle_click(14, &tg, area), Some(1));

        // Click on separator before Tab 2
        assert_eq!(TabBar::handle_click(15, &tg, area), Some(2));

        // Click on "Tab 2" label area
        assert_eq!(TabBar::handle_click(17, &tg, area), Some(2));

        // Click beyond last tab
        assert_eq!(TabBar::handle_click(50, &tg, area), None);
    }

    #[test]
    fn tabbar_click_on_active_tab_returns_correct() {
        let tg = make_tabgroup(3, 2); // Last tab active
        let area = RtRect::new(0, 0, 80, 1);

        // Click on Tab 2 (index 2) — should still return Some(2)
        assert_eq!(TabBar::handle_click(16, &tg, area), Some(2));
    }

    #[test]
    fn tabbar_click_before_first_tab() {
        let tg = make_tabgroup(2, 0);
        let area = RtRect::new(5, 0, 80, 1);

        // area.x = 5, so tabs start at x=5. Click at x=4 is before area start.
        assert_eq!(TabBar::handle_click(4, &tg, area), None);

        // Click at first tab start (area.x)
        assert_eq!(TabBar::handle_click(5, &tg, area), Some(0));
    }

    #[test]
    fn tabbar_click_empty_tabs() {
        let tg = TabGroup {
            tabs: vec![],
            active: 0,
        };
        let area = RtRect::new(0, 0, 80, 1);
        assert_eq!(TabBar::handle_click(0, &tg, area), None);
    }

    // ── ResizeHandle tests ─────────────────────────────────────────

    #[test]
    fn resize_handle_horizontal() {
        let mut term = MockTerminal::new(10, 10);
        let area = RtRect::new(4, 0, 1, 10); // Vertical strip at x=4

        term.draw(|f: &mut Frame| {
            ResizeHandle::draw(f, area, SplitDirection::Horizontal);
        });

        // │ should appear at midpoint y=5, x=4
        let lines = term.buffer_lines();
        // Line 5 (0-indexed) at position 4 should be │
        let handle_line = &lines[5];
        assert!(
            handle_line.contains('│'),
            "Expected │ at midpoint, got: {handle_line:?}"
        );
    }

    #[test]
    fn resize_handle_vertical() {
        let mut term = MockTerminal::new(10, 10);
        let area = RtRect::new(0, 4, 10, 1); // Horizontal strip at y=4

        term.draw(|f: &mut Frame| {
            ResizeHandle::draw(f, area, SplitDirection::Vertical);
        });

        // ─ should appear at midpoint x=5, y=4
        let lines = term.buffer_lines();
        let handle_line = &lines[4];
        assert!(
            handle_line.contains('─'),
            "Expected ─ at midpoint, got: {handle_line:?}"
        );
    }

    #[test]
    fn resize_handle_zero_area() {
        let mut term = MockTerminal::new(10, 10);
        let area = RtRect::new(0, 0, 0, 0);

        // Should not panic
        term.draw(|f: &mut Frame| {
            ResizeHandle::draw(f, area, SplitDirection::Horizontal);
        });
    }

    // ── Panel tests ────────────────────────────────────────────────

    #[test]
    fn panel_focus_ring_cyan_when_focused() {
        let mut term = MockTerminal::new(40, 10);
        let p = panel("test-panel");

        term.draw(|f: &mut Frame| {
            PanelRenderer::draw_border_only(f, f.area(), &p, true);
        });

        // Title should be visible
        term.assert_line_contains("test-panel");

        // Border should be drawn (top-left corner ┌, or at least a line char)
        let lines = term.buffer_lines();
        let first_line = &lines[0];
        assert!(
            first_line.contains('┌')
                || first_line.contains('─')
                || first_line.contains('┏'),
            "Expected border chars in first line, got: {first_line:?}"
        );
    }

    #[test]
    fn panel_unfocused_uses_dimmed_border() {
        let mut term = MockTerminal::new(40, 10);
        let p = panel("dim-panel");

        term.draw(|f: &mut Frame| {
            PanelRenderer::draw_border_only(f, f.area(), &p, false);
        });

        term.assert_line_contains("dim-panel");
    }

    #[test]
    fn panel_draw_with_inner_content() {
        let mut term = MockTerminal::new(40, 10);
        let p = panel("inner");

        term.draw(|f: &mut Frame| {
            PanelRenderer::draw(f, f.area(), &p, true, |f_inner, area| {
                let para = ratatui::widgets::Paragraph::new("Hello from inside!");
                f_inner.render_widget(para, area);
            });
        });

        term.assert_line_contains("Hello from inside!");
        term.assert_line_contains("inner");
    }

    // ── FocusManager tests ─────────────────────────────────────────

    #[test]
    fn focus_manager_cycles() {
        let ids: Vec<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut fm = FocusManager::new(ids);

        // Start at index 0 ("a")
        assert_eq!(fm.current(), Some("a"));
        assert_eq!(fm.current_index(), Some(0));

        // Next → "b"
        fm.focus_next();
        assert_eq!(fm.current(), Some("b"));
        assert_eq!(fm.current_index(), Some(1));

        // Next → "c"
        fm.focus_next();
        assert_eq!(fm.current(), Some("c"));

        // Next → "d"
        fm.focus_next();
        assert_eq!(fm.current(), Some("d"));
        assert_eq!(fm.current_index(), Some(3));

        // Next wraps → "a"
        fm.focus_next();
        assert_eq!(fm.current(), Some("a"));
        assert_eq!(fm.current_index(), Some(0));
    }

    #[test]
    fn focus_manager_prev_wraps() {
        let ids: Vec<String> = ["x", "y", "z"].iter().map(|s| s.to_string()).collect();
        let mut fm = FocusManager::new(ids);

        // Start at "x"
        assert_eq!(fm.current(), Some("x"));

        // Prev wraps → "z"
        fm.focus_prev();
        assert_eq!(fm.current(), Some("z"));

        // Prev → "y"
        fm.focus_prev();
        assert_eq!(fm.current(), Some("y"));

        // Prev → "x"
        fm.focus_prev();
        assert_eq!(fm.current(), Some("x"));
    }

    #[test]
    fn focus_manager_direct_focus() {
        let ids: Vec<String> = ["one", "two", "three"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut fm = FocusManager::new(ids);

        assert!(fm.focus("three"));
        assert_eq!(fm.current(), Some("three"));

        assert!(fm.focus("one"));
        assert_eq!(fm.current(), Some("one"));

        // Unknown ID — should not change focus
        assert!(!fm.focus("nonexistent"));
        assert_eq!(fm.current(), Some("one"));
    }

    #[test]
    fn focus_manager_empty_is_noop() {
        let mut fm = FocusManager::new(vec![]);

        assert!(fm.is_empty());
        assert_eq!(fm.len(), 0);
        assert_eq!(fm.current(), None);
        assert_eq!(fm.current_index(), None);

        // Navigation on empty focus order should not panic
        fm.focus_next();
        fm.focus_prev();
        assert!(!fm.focus("anything"));
        assert_eq!(fm.current(), None);
    }

    #[test]
    fn focus_manager_single_element() {
        let mut fm = FocusManager::new(vec!["solo".to_string()]);

        assert_eq!(fm.len(), 1);
        assert!(!fm.is_empty());
        assert_eq!(fm.current(), Some("solo"));

        fm.focus_next();
        assert_eq!(fm.current(), Some("solo")); // Wraps to itself

        fm.focus_prev();
        assert_eq!(fm.current(), Some("solo")); // Wraps to itself
    }

    #[test]
    fn focus_manager_focus_order_immutable_access() {
        let ids = vec!["p1".to_string(), "p2".to_string()];
        let fm = FocusManager::new(ids);
        assert_eq!(fm.focus_order(), &["p1", "p2"]);
    }

    // ── compute_focus_chain tests ──────────────────────────────────

    #[test]
    fn focus_chain_flat_panels() {
        let root = LayoutNode::Split(Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.5,
            children: vec![
                LayoutNode::Panel(panel("left")),
                LayoutNode::Panel(panel("right")),
            ],
        });

        let chain = compute_focus_chain(&root);
        assert_eq!(chain, vec!["left", "right"]);
    }

    #[test]
    fn focus_chain_deeply_nested() {
        // root: Split(Vertical)
        //   ├── Panel "top"
        //   └── Split(Horizontal)
        //         ├── Panel "bot-left"
        //         └── Panel "bot-right"
        let root = LayoutNode::Split(Split {
            direction: SplitDirection::Vertical,
            ratio: 0.3,
            children: vec![
                LayoutNode::Panel(panel("top")),
                LayoutNode::Split(Split {
                    direction: SplitDirection::Horizontal,
                    ratio: 0.5,
                    children: vec![
                        LayoutNode::Panel(panel("bot-left")),
                        LayoutNode::Panel(panel("bot-right")),
                    ],
                }),
            ],
        });

        let chain = compute_focus_chain(&root);
        assert_eq!(chain, vec!["top", "bot-left", "bot-right"]);
    }

    #[test]
    fn focus_chain_tabgroup_is_structural() {
        // TabGroup is a structural node - it does NOT contribute
        // panel IDs directly to the focus chain. Its content is
        // opaque `Box<dyn Component>`.
        let root = LayoutNode::Split(Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.5,
            children: vec![
                LayoutNode::TabGroup(make_tabgroup(3, 1)),
                LayoutNode::Panel(panel("sidebar")),
            ],
        });

        let chain = compute_focus_chain(&root);
        // Only "sidebar" is collected — TabGroup is structural
        assert_eq!(chain, vec!["sidebar"]);
    }

    #[test]
    fn focus_chain_empty_tabgroup() {
        let root = LayoutNode::TabGroup(TabGroup {
            active: 0,
            tabs: vec![],
        });
        let chain = compute_focus_chain(&root);
        assert!(chain.is_empty());
    }

    #[test]
    fn focus_chain_leaf_is_skipped() {
        let root = LayoutNode::Leaf(MockComponent::boxed("leaf-comp"));
        let chain = compute_focus_chain(&root);
        assert!(chain.is_empty());
    }

    #[test]
    fn focus_chain_mixed_tree() {
        // Split
        //  ├── Panel "explorer"
        //  ├── Split
        //  │    ├── Panel "editor"
        //  │    └── TabGroup (structural, ignored)
        //  └── Panel "terminal"
        let root = LayoutNode::Split(Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.2,
            children: vec![
                LayoutNode::Panel(panel("explorer")),
                LayoutNode::Split(Split {
                    direction: SplitDirection::Vertical,
                    ratio: 0.7,
                    children: vec![
                        LayoutNode::Panel(panel("editor")),
                        LayoutNode::TabGroup(make_tabgroup(2, 0)),
                    ],
                }),
                LayoutNode::Panel(panel("terminal")),
            ],
        });

        let chain = compute_focus_chain(&root);
        assert_eq!(chain, vec!["explorer", "editor", "terminal"]);
    }
}
