pub mod types;

pub use types::{LayoutNode, PanelDef, Split, SplitDirection, TabDef, TabGroup};

/// Root of the component layout tree.
pub struct LayoutTree {
    pub root: LayoutNode,
}
pub mod engine;
