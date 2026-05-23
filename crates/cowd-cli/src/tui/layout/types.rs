use ratatui::layout::Rect;

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
#[derive(Debug)]
pub struct Split {
    pub direction: SplitDirection,
    /// Proportion of the area given to the first child (clamped to 0.0–1.0).
    pub ratio: f32,
    pub children: Vec<LayoutNode>,
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
                let base = rem_w / remaining as u16;
                let extra = rem_w % remaining as u16;

                let mut cx = area.x + split_w;
                for i in 0..remaining {
                    let w = base + if (i as u16) < extra { 1 } else { 0 };
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
                let base = rem_h / remaining as u16;
                let extra = rem_h % remaining as u16;

                let mut cy = area.y + split_h;
                for i in 0..remaining {
                    let h = base + if (i as u16) < extra { 1 } else { 0 };
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
#[derive(Debug)]
pub enum LayoutNode {
    Split(Split),
    TabGroup(TabGroup),
    Panel(PanelDef),
    /// Placeholder until the Component trait is available from a sibling task.
    // TODO: Replace with Box<dyn Component> after Component trait compiles
    Leaf(Box<dyn std::any::Any>),
}

/// A group of tabs with exactly one active tab at a time.
#[derive(Debug)]
pub struct TabGroup {
    pub tabs: Vec<TabDef>,
    /// Index of the currently active tab.
    pub active: usize,
}

impl TabGroup {
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
#[derive(Debug)]
pub struct TabDef {
    pub id: String,
    pub label: String,
    pub icon: Option<String>,
    // TODO: Replace with Box<dyn Component> after Component trait compiles
    pub content: Box<dyn std::any::Any>,
}

/// A standalone panel identified by a string id.
#[derive(Debug)]
pub struct PanelDef {
    pub id: String,
    // TODO: Replace with Box<dyn Component> after Component trait compiles
    pub component: Box<dyn std::any::Any>,
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

    #[test]
    fn split_horizontal_50_50() {
        let split = Split {
            direction: SplitDirection::Horizontal,
            ratio: 0.5,
            children: vec![
                LayoutNode::Panel(PanelDef {
                    id: "left".into(),
                    component: Box::new(()),
                }),
                LayoutNode::Panel(PanelDef {
                    id: "right".into(),
                    component: Box::new(()),
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
                    component: Box::new(()),
                }),
                LayoutNode::Panel(PanelDef {
                    id: "bottom".into(),
                    component: Box::new(()),
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
                component: Box::new(()),
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
                    component: Box::new(()),
                }),
                LayoutNode::Panel(PanelDef {
                    id: "b".into(),
                    component: Box::new(()),
                }),
                LayoutNode::Panel(PanelDef {
                    id: "c".into(),
                    component: Box::new(()),
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
                    content: Box::new(()),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: Box::new(()),
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
                    content: Box::new(()),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: Box::new(()),
                },
                TabDef {
                    id: "c".into(),
                    label: "C".into(),
                    icon: None,
                    content: Box::new(()),
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
                    content: Box::new(()),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: Box::new(()),
                },
                TabDef {
                    id: "c".into(),
                    label: "C".into(),
                    icon: None,
                    content: Box::new(()),
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
                    content: Box::new(()),
                },
                TabDef {
                    id: "b".into(),
                    label: "B".into(),
                    icon: None,
                    content: Box::new(()),
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
                component: Box::new(()),
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
