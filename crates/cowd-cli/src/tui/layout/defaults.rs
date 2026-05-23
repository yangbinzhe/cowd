// ── Default Layout & State Management ──────────────────────────────
// build_default_layout(), LayoutState (toggle_sidebar, resize_sidebar),
// and unit tests for the default split-view layout.
// --------------------------------------------------------------------

use ratatui::layout::Rect;

use crate::tui::components::{Component, EventResult, RenderContext};
use super::LayoutTree;
use super::types::{LayoutNode, Split, SplitDirection, TabDef, TabGroup};

// ── Placeholder Component ──────────────────────────────────────────

/// Minimal component used as a structural placeholder in layout
/// trees. Production code should replace these with real components
/// before rendering.
struct PlaceholderComponent {
    id: &'static str,
}

impl Component for PlaceholderComponent {
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

// ── Default Layout Builder ─────────────────────────────────────────

/// Build the default split-view layout.
///
/// Returns a [`LayoutTree`] with:
/// ```text
/// Split(Horizontal, 0.7)
///   ├── Leaf("chat_view")           — 70 % of width
///   └── TabGroup (4 tabs)           — 30 % of width
///         ├── Tab 0: "files"        (📁)
///         ├── Tab 1: "sessions"     (💬)
///         ├── Tab 2: "diff"        (📊)
///         └── Tab 3: "gateway"      (🌐)
/// ```
///
/// Components are placeholders — replace with real components before
/// passing the tree to the render engine.
#[must_use]
pub fn build_default_layout() -> LayoutTree {
    let sidebar_tabs = vec![
        TabDef {
            id: "files".to_string(),
            label: "Files".to_string(),
            icon: Some("📁".to_string()),
            content: Box::new(PlaceholderComponent { id: "files" }),
        },
        TabDef {
            id: "sessions".to_string(),
            label: "Sessions".to_string(),
            icon: Some("💬".to_string()),
            content: Box::new(PlaceholderComponent { id: "sessions" }),
        },
        TabDef {
            id: "diff".to_string(),
            label: "Diff".to_string(),
            icon: Some("📊".to_string()),
            content: Box::new(PlaceholderComponent { id: "diff" }),
        },
        TabDef {
            id: "gateway".to_string(),
            label: "Gateway".to_string(),
            icon: Some("🌐".to_string()),
            content: Box::new(PlaceholderComponent { id: "gateway" }),
        },
    ];

    let root = LayoutNode::Split(Split {
        direction: SplitDirection::Horizontal,
        ratio: 0.7,
        children: vec![
            LayoutNode::Leaf(Box::new(PlaceholderComponent { id: "chat_view" })),
            LayoutNode::TabGroup(TabGroup {
                tabs: sidebar_tabs,
                active: 0,
            }),
        ],
    });

    LayoutTree { root }
}

// ── LayoutState ────────────────────────────────────────────────────

/// Tracks layout configuration that can be mutated at runtime:
/// sidebar visibility (Ctrl+B toggle) and split ratio (drag handle).
///
/// # Fields
///
/// * `sidebar_visible` — Whether the sidebar is currently shown.
/// * `saved_ratio` — The ratio to restore when sidebar is toggled back on.
///   Always stays within `[RATIO_MIN, RATIO_MAX]`.
pub struct LayoutState {
    pub sidebar_visible: bool,
    saved_ratio: f32,
}

/// Minimum chat ratio (maximum sidebar size).
pub const RATIO_MIN: f32 = 0.2;

/// Maximum chat ratio (minimum sidebar size).
pub const RATIO_MAX: f32 = 0.9;

/// Default chat ratio (70 % chat / 30 % sidebar).
pub const RATIO_DEFAULT: f32 = 0.7;

impl LayoutState {
    /// Create a new layout state with default values (sidebar visible,
    /// 70/30 split).
    #[must_use]
    pub fn new() -> Self {
        Self {
            sidebar_visible: true,
            saved_ratio: RATIO_DEFAULT,
        }
    }

    // ── Sidebar Toggle ─────────────────────────────────────────────

    /// Toggle sidebar visibility by adjusting the split ratio.
    ///
    /// * **Hiding**: saves the current ratio and sets it to `1.0`
    ///   (chat view fills the entire width).
    /// * **Showing**: restores the previously saved ratio.
    ///
    /// # Panics
    ///
    /// Panics if the root node of `tree` is not a `LayoutNode::Split`.
    /// `build_default_layout()` always produces a `Split` root.
    pub fn toggle_sidebar(&mut self, tree: &mut LayoutTree) {
        match &mut tree.root {
            LayoutNode::Split(ref mut split) => {
                if self.sidebar_visible {
                    // Hide sidebar: save current ratio, set to 1.0
                    self.saved_ratio = split.ratio.clamp(RATIO_MIN, RATIO_MAX);
                    split.ratio = 1.0;
                } else {
                    // Show sidebar: restore saved ratio
                    split.ratio = self.saved_ratio.clamp(RATIO_MIN, RATIO_MAX);
                }
                self.sidebar_visible = !self.sidebar_visible;
            }
            other => panic!(
                "toggle_sidebar expects a Split root node, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    // ── Ratio Resize ───────────────────────────────────────────────

    /// Adjust the split ratio by `delta`.
    ///
    /// * Positive `delta` → increase chat area (shrink sidebar).
    /// * Negative `delta` → decrease chat area (widen sidebar).
    ///
    /// The ratio is clamped to `[`[`RATIO_MIN`]`, `[`RATIO_MAX`]`]`.
    /// If the sidebar is currently hidden, calling this method also
    /// makes it visible again (sets `sidebar_visible = true` and
    /// updates `saved_ratio`).
    ///
    /// # Panics
    ///
    /// Panics if the root node of `tree` is not a `LayoutNode::Split`.
    pub fn resize_sidebar(&mut self, tree: &mut LayoutTree, delta: f32) {
        match &mut tree.root {
            LayoutNode::Split(ref mut split) => {
                let new_ratio = (split.ratio + delta).clamp(RATIO_MIN, RATIO_MAX);
                split.ratio = new_ratio;
                self.saved_ratio = new_ratio;
                self.sidebar_visible = true;
            }
            other => panic!(
                "resize_sidebar expects a Split root node, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    // ── Accessors ──────────────────────────────────────────────────

    /// Current effective split ratio, or `1.0` if the sidebar is hidden.
    #[must_use]
    pub fn current_ratio(&self, tree: &LayoutTree) -> f32 {
        match &tree.root {
            LayoutNode::Split(split) => split.ratio,
            _ => 1.0,
        }
    }

    /// The ratio that will be restored when the sidebar is toggled back on.
    #[must_use]
    pub fn saved_ratio(&self) -> f32 {
        self.saved_ratio
    }
}

impl Default for LayoutState {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::layout::types::SplitDirection;

    // ── Helpers ────────────────────────────────────────────────────

    /// Assert that the root is a `Split` with the expected direction and ratio.
    fn assert_root_split(tree: &LayoutTree, direction: SplitDirection, ratio: f32) {
        match &tree.root {
            LayoutNode::Split(split) => {
                assert_eq!(
                    split.direction, direction,
                    "expected split direction {:?}, got {:?}",
                    direction, split.direction
                );
                let eps = 0.001;
                assert!(
                    (split.ratio - ratio).abs() < eps,
                    "expected ratio {ratio}, got {}",
                    split.ratio
                );
            }
            other => panic!("expected Split root, got {:?}", std::mem::discriminant(other)),
        }
    }

    // ── default_layout_has_chat_and_sidebar ────────────────────────

    #[test]
    fn default_layout_has_chat_and_sidebar() {
        let tree = build_default_layout();

        assert_root_split(&tree, SplitDirection::Horizontal, 0.7);

        match &tree.root {
            LayoutNode::Split(split) => {
                assert_eq!(split.children.len(), 2, "expected 2 children: chat + sidebar");

                // First child: chat view (Leaf)
                assert!(
                    matches!(&split.children[0], LayoutNode::Leaf(_)),
                    "first child should be Leaf (chat_view)"
                );

                // Second child: TabGroup with 4 tabs
                match &split.children[1] {
                    LayoutNode::TabGroup(tg) => {
                        assert_eq!(tg.tabs.len(), 4, "expected 4 sidebar tabs");
                        assert_eq!(tg.active, 0, "first tab should be active by default");

                        // Verify tab IDs and labels
                        let expected: &[(&str, &str)] = &[
                            ("files", "Files"),
                            ("sessions", "Sessions"),
                            ("diff", "Diff"),
                            ("gateway", "Gateway"),
                        ];
                        for (i, (id, label)) in expected.iter().enumerate() {
                            assert_eq!(
                                tg.tabs[i].id, *id,
                                "tab {i}: expected id '{id}', got '{}'",
                                tg.tabs[i].id
                            );
                            assert_eq!(
                                tg.tabs[i].label, *label,
                                "tab {i}: expected label '{label}', got '{}'",
                                tg.tabs[i].label
                            );
                        }
                    }
                    other => panic!(
                        "second child should be TabGroup, got {:?}",
                        std::mem::discriminant(other)
                    ),
                }
            }
            _ => unreachable!(),
        }
    }

    // ── ctrl_b_toggles ─────────────────────────────────────────────

    #[test]
    fn ctrl_b_toggles_sidebar_visibility() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Initially sidebar visible, ratio 0.7
        assert!(state.sidebar_visible);
        assert_eq!(state.current_ratio(&tree), 0.7);

        // Toggle → hide (ratio → 1.0)
        state.toggle_sidebar(&mut tree);
        assert!(!state.sidebar_visible);
        assert_eq!(state.current_ratio(&tree), 1.0);
        assert_eq!(state.saved_ratio(), 0.7);

        // Toggle → show (restore 0.7)
        state.toggle_sidebar(&mut tree);
        assert!(state.sidebar_visible);
        assert_eq!(state.current_ratio(&tree), 0.7);

        // Toggle → hide again
        state.toggle_sidebar(&mut tree);
        assert!(!state.sidebar_visible);
        assert_eq!(state.current_ratio(&tree), 1.0);
    }

    #[test]
    fn ctrl_b_toggles_preserves_custom_ratio() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Change ratio to 0.5 via resize, then toggle
        state.resize_sidebar(&mut tree, -0.2); // 0.7 - 0.2 = 0.5
        assert_eq!(state.current_ratio(&tree), 0.5);
        assert!(state.sidebar_visible);

        // Hide
        state.toggle_sidebar(&mut tree);
        assert_eq!(state.current_ratio(&tree), 1.0);
        assert!(!state.sidebar_visible);
        assert_eq!(state.saved_ratio(), 0.5);

        // Show → restores 0.5 (not 0.7)
        state.toggle_sidebar(&mut tree);
        assert!(state.sidebar_visible);
        assert_eq!(state.current_ratio(&tree), 0.5);

        // Hide again
        state.toggle_sidebar(&mut tree);
        assert!(!state.sidebar_visible);
        assert_eq!(state.saved_ratio(), 0.5);
    }

    // ── resize_moves_ratio ─────────────────────────────────────────

    #[test]
    fn resize_moves_ratio_increase_chat() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Increase chat area (shrink sidebar)
        state.resize_sidebar(&mut tree, 0.1);
        assert_eq!(state.current_ratio(&tree), 0.8);
        assert!(state.sidebar_visible);
        assert_eq!(state.saved_ratio(), 0.8);

        // Increase more
        state.resize_sidebar(&mut tree, 0.05);
        assert_eq!(state.current_ratio(&tree), 0.85);
    }

    #[test]
    fn resize_moves_ratio_decrease_chat() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Decrease chat area (widen sidebar)
        state.resize_sidebar(&mut tree, -0.2);
        assert_eq!(state.current_ratio(&tree), 0.5);
        assert!(state.sidebar_visible);
        assert_eq!(state.saved_ratio(), 0.5);
    }

    #[test]
    fn resize_clamps_to_min() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Try to go below RATIO_MIN (0.2)
        state.resize_sidebar(&mut tree, -1.0);
        assert_eq!(state.current_ratio(&tree), RATIO_MIN);
        assert_eq!(state.saved_ratio(), RATIO_MIN);
        assert!(state.sidebar_visible);
    }

    #[test]
    fn resize_clamps_to_max() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Try to go above RATIO_MAX (0.9)
        state.resize_sidebar(&mut tree, 1.0);
        assert_eq!(state.current_ratio(&tree), RATIO_MAX);
        assert_eq!(state.saved_ratio(), RATIO_MAX);
        assert!(state.sidebar_visible);
    }

    #[test]
    fn resize_from_hidden_state_makes_sidebar_visible() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Hide sidebar
        state.toggle_sidebar(&mut tree);
        assert!(!state.sidebar_visible);
        assert_eq!(state.current_ratio(&tree), 1.0);

        // Resize — should make sidebar visible again
        state.resize_sidebar(&mut tree, -0.1);
        assert!(state.sidebar_visible);
        // 1.0 - 0.1 = 0.9 (within bounds)
        assert_eq!(state.current_ratio(&tree), 0.9);
        assert_eq!(state.saved_ratio(), 0.9);
    }

    #[test]
    fn resize_noop_delta_zero() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        state.resize_sidebar(&mut tree, 0.0);
        assert_eq!(state.current_ratio(&tree), 0.7);
        assert!(state.sidebar_visible);
        assert_eq!(state.saved_ratio(), 0.7);
    }

    // ── LayoutState::new() defaults ────────────────────────────────

    #[test]
    fn layout_state_defaults() {
        let state = LayoutState::new();
        assert!(state.sidebar_visible);
        assert_eq!(state.saved_ratio(), RATIO_DEFAULT);

        let default_state = LayoutState::default();
        assert!(default_state.sidebar_visible);
        assert_eq!(default_state.saved_ratio(), RATIO_DEFAULT);
    }

    // ── current_ratio accessor ─────────────────────────────────────

    #[test]
    fn current_ratio_non_split_root_returns_one() {
        let tree = LayoutTree {
            root: LayoutNode::Leaf(Box::new(PlaceholderComponent { id: "leaf" })),
        };
        let state = LayoutState::new();
        assert_eq!(state.current_ratio(&tree), 1.0);
    }

    // ── Compute areas from default layout ──────────────────────────

    #[test]
    fn default_layout_compute_areas_70_30() {
        let tree = build_default_layout();
        // Simulate full-screen area: 100 cols × 40 rows
        let area = Rect::new(0, 0, 100, 40);

        match &tree.root {
            LayoutNode::Split(split) => {
                let areas = split.compute_areas(area);
                assert_eq!(areas.len(), 2);
                // Chat: 70 cols
                assert_eq!(areas[0].width, 70);
                assert_eq!(areas[0].height, 40);
                // Sidebar: 30 cols
                assert_eq!(areas[1].width, 30);
                assert_eq!(areas[1].height, 40);
            }
            _ => panic!("expected Split root"),
        }
    }

    #[test]
    fn default_layout_compute_areas_after_toggle() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();
        state.toggle_sidebar(&mut tree); // ratio → 1.0

        let area = Rect::new(0, 0, 100, 40);

        match &tree.root {
            LayoutNode::Split(split) => {
                let areas = split.compute_areas(area);
                assert_eq!(areas.len(), 2);
                // Chat: all 100 cols (sidebar hidden area is 0)
                assert_eq!(areas[0].width, 100);
                assert_eq!(areas[1].width, 0);
            }
            _ => panic!("expected Split root"),
        }
    }

    #[test]
    fn default_layout_compute_areas_after_resize() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();
        state.resize_sidebar(&mut tree, 0.1); // ratio → 0.8

        let area = Rect::new(0, 0, 100, 40);

        match &tree.root {
            LayoutNode::Split(split) => {
                let areas = split.compute_areas(area);
                assert_eq!(areas.len(), 2);
                assert_eq!(areas[0].width, 80);
                assert_eq!(areas[1].width, 20);
            }
            _ => panic!("expected Split root"),
        }
    }

    // ── Sidebar TabGroup navigation ────────────────────────────────

    #[test]
    fn sidebar_tabgroup_navigation() {
        let mut tree = build_default_layout();

        // Access the TabGroup in the sidebar (second child of Split)
        match &mut tree.root {
            LayoutNode::Split(split) => {
                match &mut split.children[1] {
                    LayoutNode::TabGroup(ref mut tg) => {
                        assert_eq!(tg.active, 0);
                        assert_eq!(tg.active_tab().unwrap().id, "files");

                        tg.next_tab();
                        assert_eq!(tg.active, 1);
                        assert_eq!(tg.active_tab().unwrap().id, "sessions");

                        tg.next_tab();
                        assert_eq!(tg.active, 2);
                        assert_eq!(tg.active_tab().unwrap().id, "diff");

                        tg.next_tab();
                        assert_eq!(tg.active, 3);
                        assert_eq!(tg.active_tab().unwrap().id, "gateway");

                        // Wrap around
                        tg.next_tab();
                        assert_eq!(tg.active, 0);

                        // Wrap around with prev
                        tg.prev_tab();
                        assert_eq!(tg.active, 3);
                    }
                    _ => panic!("expected TabGroup as second child"),
                }
            }
            _ => panic!("expected Split root"),
        }
    }

    // ── TabGroup icons ─────────────────────────────────────────────

    #[test]
    fn sidebar_tabs_have_icons() {
        let tree = build_default_layout();
        match &tree.root {
            LayoutNode::Split(split) => {
                match &split.children[1] {
                    LayoutNode::TabGroup(tg) => {
                        assert_eq!(tg.tabs[0].icon.as_deref(), Some("📁"));
                        assert_eq!(tg.tabs[1].icon.as_deref(), Some("💬"));
                        assert_eq!(tg.tabs[2].icon.as_deref(), Some("📊"));
                        assert_eq!(tg.tabs[3].icon.as_deref(), Some("🌐"));
                    }
                    _ => panic!("expected TabGroup as second child"),
                }
            }
            _ => panic!("expected Split root"),
        }
    }

    // ── Edge cases ─────────────────────────────────────────────────

    #[test]
    fn multiple_toggle_cycles_are_stable() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        for _ in 0..5 {
            state.toggle_sidebar(&mut tree);
            state.toggle_sidebar(&mut tree);
            assert!(state.sidebar_visible);
            assert_eq!(state.current_ratio(&tree), 0.7);
            assert_eq!(state.saved_ratio(), 0.7);
        }
    }

    #[test]
    fn resize_then_toggle_cycle_preserves_ratio() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        state.resize_sidebar(&mut tree, -0.15); // ~0.55
        let eps = 0.001;
        assert!(
            (state.current_ratio(&tree) - 0.55).abs() < eps,
            "expected ~0.55, got {}",
            state.current_ratio(&tree)
        );

        state.toggle_sidebar(&mut tree); // hide → 1.0
        state.toggle_sidebar(&mut tree); // show → 0.55
        assert!(
            (state.current_ratio(&tree) - 0.55).abs() < eps,
            "expected ~0.55 after toggle cycle, got {}",
            state.current_ratio(&tree)
        );
        assert!(state.sidebar_visible);
    }

    #[test]
    fn toggle_preserves_tab_active_index() {
        let mut tree = build_default_layout();
        let mut state = LayoutState::new();

        // Switch to tab 2 (diff)
        match &mut tree.root {
            LayoutNode::Split(split) => {
                match &mut split.children[1] {
                    LayoutNode::TabGroup(ref mut tg) => {
                        tg.active = 2;
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        state.toggle_sidebar(&mut tree); // hide → ratio 1.0
        state.toggle_sidebar(&mut tree); // show → restore

        match &tree.root {
            LayoutNode::Split(split) => {
                match &split.children[1] {
                    LayoutNode::TabGroup(tg) => {
                        assert_eq!(tg.active, 2, "active tab index should be preserved");
                    }
                    _ => panic!("expected TabGroup"),
                }
            }
            _ => panic!("expected Split root"),
        }
    }
}
