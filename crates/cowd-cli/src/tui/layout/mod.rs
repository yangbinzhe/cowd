pub mod types;
pub mod engine;
pub mod defaults;

pub use types::LayoutNode;
pub use defaults::{build_default_layout, LayoutPreset, LayoutState};

use ratatui::layout::Rect;

/// Root of the component layout tree.
#[derive(Debug)]
pub struct LayoutTree {
    pub root: LayoutNode,
}

impl LayoutTree {
    pub fn resize(&mut self, area: Rect) {
        self.root.compute_bounds(area);
    }

    pub fn area_of(&self, panel_id: &str) -> Option<Rect> {
        self.root.find_area(panel_id)
    }
}
