pub mod types;
pub mod engine;
pub mod defaults;

pub use types::{LayoutNode, PanelDef, Split, SplitDirection, TabDef, TabGroup};
pub use defaults::{build_default_layout, LayoutState, RATIO_DEFAULT, RATIO_MAX, RATIO_MIN};

/// Root of the component layout tree.
#[derive(Debug)]
pub struct LayoutTree {
    pub root: LayoutNode,
}
