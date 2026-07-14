use std::fmt;

use ratatui::layout::Rect;

use crate::components::Component;

/// Direction of a layout split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitDirection {
    Horizontal,
    Vertical,
}

/// A split divides an area into two or more sub-areas along an axis.
///
/// The first child receives `ratio` of the total area; remaining children
/// split the leftover space equally.
pub struct Split {
    pub direction: SplitDirection,
    /// Proportion of the area given to the first child (clamped to 0.0–1.0).
    pub ratio: f32,
    pub children: Vec<LayoutNode>,
}

impl fmt::Debug for Split {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Split")
            .field("direction", &self.direction)
            .field("ratio", &self.ratio)
            .field("children", &self.children.len())
            .finish()
    }
}

impl Split {
    /// Divide `area` into `self.children.len()` sub-rectangles.
    ///
    /// Returns an empty vec when there are no children.
    pub fn compute_areas(&self, area: Rect) -> Vec<Rect> {
        if self.children.is_empty() {
            return vec![];
        }

        let ratio = self.ratio.clamp(0.0, 1.0);

        match self.direction {
            SplitDirection::Horizontal => {
                let split_w = (area.width as f32 * ratio).round() as u16;
                let split_w = split_w.min(area.width);

                let mut areas = Vec::with_capacity(self.children.len());
                areas.push(Rect {
                    x: area.x,
                    y: area.y,
                    width: split_w,
                    height: area.height,
                });

                let remaining = self.children.len() - 1;
                if remaining == 0 {
                    return areas;
                }

                let rem_w = area.width.saturating_sub(split_w);
                let divisor = crate::components::base::terminal_len(remaining).max(1);
                let base = rem_w / divisor;
                let extra = rem_w % divisor;

                let mut cx = area.x + split_w;
                for i in 0..remaining {
                    let w = base + if i < usize::from(extra) { 1 } else { 0 };
                    areas.push(Rect {
                        x: cx,
                        y: area.y,
                        width: w,
                        height: area.height,
                    });
                    cx += w;
                }

                areas
            }
            SplitDirection::Vertical => {
                let split_h = (area.height as f32 * ratio).round() as u16;
                let split_h = split_h.min(area.height);

                let mut areas = Vec::with_capacity(self.children.len());
                areas.push(Rect {
                    x: area.x,
                    y: area.y,
                    width: area.width,
                    height: split_h,
                });

                let remaining = self.children.len() - 1;
                if remaining == 0 {
                    return areas;
                }

                let rem_h = area.height.saturating_sub(split_h);
                let divisor = crate::components::base::terminal_len(remaining).max(1);
                let base = rem_h / divisor;
                let extra = rem_h % divisor;

                let mut cy = area.y + split_h;
                for i in 0..remaining {
                    let h = base + if i < usize::from(extra) { 1 } else { 0 };
                    areas.push(Rect {
                        x: area.x,
                        y: cy,
                        width: area.width,
                        height: h,
                    });
                    cy += h;
                }

                areas
            }
        }
    }
}

/// A node in the layout tree.
pub enum LayoutNode {
    Split(Split),
    TabGroup(TabGroup),
    Panel(PanelDef),
    Leaf(Box<dyn Component>),
}

impl LayoutNode {
    pub(crate) fn compute_bounds(&mut self, area: Rect) {
        match self {
            LayoutNode::Split(s) => {
                let areas = s.compute_areas(area);
                for (child, child_area) in s.children.iter_mut().zip(areas) {
                    child.compute_bounds(child_area);
                }
            }
            LayoutNode::Panel(p) => {
                p.bounds = Some(area);
            }
            _ => {}
        }
    }

    pub(crate) fn find_area(&self, id: &str) -> Option<Rect> {
        match self {
            LayoutNode::Panel(p) if p.id == id => p.bounds,
            LayoutNode::Split(s) => s.children.iter().find_map(|c| c.find_area(id)),
            _ => None,
        }
    }
}

impl fmt::Debug for LayoutNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Split(s) => f.debug_tuple("Split").field(s).finish(),
            Self::TabGroup(tg) => f.debug_tuple("TabGroup").field(tg).finish(),
            Self::Panel(p) => f.debug_tuple("Panel").field(p).finish(),
            Self::Leaf(_) => f.debug_tuple("Leaf").field(&"<component>").finish(),
        }
    }
}

/// A group of tabs with exactly one active tab at a time.
pub struct TabGroup {
    pub tabs: Vec<TabDef>,
    /// Index of the currently active tab.
    pub active: usize,
}

impl fmt::Debug for TabGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TabGroup")
            .field("active", &self.active)
            .field("tabs", &self.tabs.len())
            .finish()
    }
}

impl TabGroup {
    /// Return a reference to the currently active tab, or `None` if the group is empty.
    #[must_use]
    pub fn active_tab(&self) -> Option<&TabDef> {
        self.tabs.get(self.active)
    }

    /// Return a mutable reference to the currently active tab, or `None` if empty.
    pub fn active_tab_mut(&mut self) -> Option<&mut TabDef> {
        self.tabs.get_mut(self.active)
    }

    /// Advance to the next tab index, wrapping around to 0.
    pub fn next_tab(&mut self) {
        if !self.tabs.is_empty() {
            self.active = (self.active + 1) % self.tabs.len();
        }
    }

    /// Go to the previous tab index, wrapping around to the last tab.
    pub fn prev_tab(&mut self) {
        if !self.tabs.is_empty() {
            self.active = if self.active == 0 {
                self.tabs.len() - 1
            } else {
                self.active - 1
            };
        }
    }
}

/// A single tab within a `TabGroup`.
pub struct TabDef {
    pub id: String,
    pub label: String,
    pub icon: Option<String>,
    pub content: Box<dyn Component>,
}

impl fmt::Debug for TabDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TabDef")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("icon", &self.icon)
            .field("content", &"<component>")
            .finish()
    }
}

/// A standalone panel identified by a string id.
pub struct PanelDef {
    pub id: String,
    pub component: Box<dyn Component>,
    /// Computed screen bounds, set by [`LayoutNode::compute_bounds`].
    pub bounds: Option<Rect>,
}

impl fmt::Debug for PanelDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PanelDef")
            .field("id", &self.id)
            .field("component", &"<component>")
            .finish()
    }
}

/// A sizing constraint, mirroring the common ratatui patterns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Constraint {
    Fixed(u16),
    Fill(f32),
    Min(u16),
    Ratio(u16, u16),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::{EventResult, RenderContext};

    /// Minimal component for layout unit tests.
    #[cfg(test)]
    struct TestComponent {
        id: &'static str,
    }

    #[cfg(test)]
    impl Component for TestComponent {
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

    fn tc(id: &'static str) -> Box<dyn Component> {
        Box::new(TestComponent { id })
    }

    #[test]
    fn split_horizontal_50_50() {
        let split = Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.5,
            children: vec![
                LayoutNode::Panel(PanelDef {
                    id: "left".into(),
                    component: tc("left"),
                    bounds: None,
                }),
                LayoutNode::Panel(PanelDef {
                    id: "right".into(),
                    component: tc("right"),
                    bounds: None,
                }),
            ],
        };
        let area = Rect::new(0, 0, 100, 100);
        let areas = split.compute_areas(area);

        assert_eq!(areas.len(), 2);
        assert_eq!(areas[0], Rect::new(0, 0, 50, 100));
        assert_eq!(areas[1], Rect::new(50, 0, 50, 100));
    }

    #[test]
    fn split_vertical_70_30() {
        let split = Split {
            direction: SplitDirection::Vertical,
            ratio: 0.7,
            children: vec![
                LayoutNode::Panel(PanelDef {
                    id: "top".into(),
                    component: tc("top"),
                    bounds: None,
                }),
                LayoutNode::Panel(PanelDef {
                    id: "bottom".into(),
                    component: tc("bottom"),
                    bounds: None,
                }),
            ],
        };
        let area = Rect::new(0, 0, 100, 100);
        let areas = split.compute_areas(area);

        assert_eq!(areas.len(), 2);
        assert_eq!(areas[0], Rect::new(0, 0, 100, 70));
        assert_eq!(areas[1], Rect::new(0, 70, 100, 30));
    }

    #[test]
    fn split_no_children() {
        let split = Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.5,
            children: vec![],
        };
        let areas = split.compute_areas(Rect::new(0, 0, 100, 100));
        assert!(areas.is_empty());
    }

    #[test]
    fn split_single_child() {
        let split = Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.5,
            children: vec![LayoutNode::Panel(PanelDef {
                id: "only".into(),
                component: tc("only"),
                bounds: None,
            })],
        };
        let areas = split.compute_areas(Rect::new(0, 0, 100, 100));
        assert_eq!(areas.len(), 1);
        assert_eq!(areas[0], Rect::new(0, 0, 50, 100));
    }

    #[test]
    fn split_three_children_even() {
        // Three children horizontally: first gets 50%, rest split 50%
        let split = Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.5,
            children: vec![
                LayoutNode::Panel(PanelDef {
                    id: "a".into(),
                    component: tc("a"),
                    bounds: None,
                }),
                LayoutNode::Panel(PanelDef {
                    id: "b".into(),
                    component: tc("b"),
                    bounds: None,
                }),
                LayoutNode::Panel(PanelDef {
                    id: "c".into(),
                    component: tc("c"),
                    bounds: None,
                }),
            ],
        };
        let areas = split.compute_areas(Rect::new(0, 0, 100, 100));

        assert_eq!(areas.len(), 3);
        assert_eq!(areas[0], Rect::new(0, 0, 50, 100)); // 50% for first
        assert_eq!(areas[1], Rect::new(50, 0, 25, 100)); // 25%
        assert_eq!(areas[2], Rect::new(75, 0, 25, 100)); // 25%
    }

    #[test]
    fn tabgroup_next_normal() {
        let mut tg = TabGroup {
            active: 0,
            tabs: vec![
                TabDef {
                    id: "a".into(),
                    label: "A".into(),
                    icon: None,
                    content: tc("ta"),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: tc("tb"),
                },
            ],
        };
        tg.next_tab();
        assert_eq!(tg.active, 1);
    }

    #[test]
    fn tabgroup_next_wraps() {
        let mut tg = TabGroup {
            active: 2,
            tabs: vec![
                TabDef {
                    id: "a".into(),
                    label: "A".into(),
                    icon: None,
                    content: tc("ta"),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: tc("tb"),
                },
                TabDef {
                    id: "c".into(),
                    label: "C".into(),
                    icon: None,
                    content: tc("tc"),
                },
            ],
        };
        tg.next_tab();
        assert_eq!(tg.active, 0);
    }

    #[test]
    fn tabgroup_prev_wraps() {
        let mut tg = TabGroup {
            active: 0,
            tabs: vec![
                TabDef {
                    id: "a".into(),
                    label: "A".into(),
                    icon: None,
                    content: tc("ta"),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: tc("tb"),
                },
                TabDef {
                    id: "c".into(),
                    label: "C".into(),
                    icon: None,
                    content: tc("tc"),
                },
            ],
        };
        tg.prev_tab();
        assert_eq!(tg.active, 2);
    }

    #[test]
    fn tabgroup_prev_normal() {
        let mut tg = TabGroup {
            active: 1,
            tabs: vec![
                TabDef {
                    id: "a".into(),
                    label: "A".into(),
                    icon: None,
                    content: tc("ta"),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: tc("tb"),
                },
            ],
        };
        tg.prev_tab();
        assert_eq!(tg.active, 0);
    }

    #[test]
    fn tabgroup_empty_noop() {
        let mut tg = TabGroup {
            active: 0,
            tabs: vec![],
        };
        tg.next_tab();
        assert_eq!(tg.active, 0);
        tg.prev_tab();
        assert_eq!(tg.active, 0);
    }

    #[test]
    fn layout_node_type_check() {
        let nodes: Vec<LayoutNode> = vec![
            LayoutNode::Split(Split {
                direction: SplitDirection::Horizontal,
                ratio: 0.5,
                children: vec![],
            }),
            LayoutNode::TabGroup(TabGroup {
                active: 0,
                tabs: vec![],
            }),
            LayoutNode::Panel(PanelDef {
                id: "p".into(),
                component: tc("p"),
                bounds: None,
            }),
        ];

        assert!(matches!(nodes[0], LayoutNode::Split(_)));
        assert!(matches!(nodes[1], LayoutNode::TabGroup(_)));
        assert!(matches!(nodes[2], LayoutNode::Panel(_)));
    }

    #[test]
    fn constraint_equality() {
        assert_eq!(Constraint::Fixed(10), Constraint::Fixed(10));
        assert_eq!(Constraint::Fill(1.0), Constraint::Fill(1.0));
        assert_eq!(Constraint::Min(5), Constraint::Min(5));
        assert_eq!(Constraint::Ratio(1, 2), Constraint::Ratio(1, 2));
        assert_ne!(Constraint::Fixed(10), Constraint::Fill(1.0));
    }
}
