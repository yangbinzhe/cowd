pub mod types;
pub mod engine;
pub mod defaults;

pub use types::LayoutNode;
pub use defaults::{build_default_layout, LayoutState};

/// Root of the component layout tree.
#[derive(Debug)]
pub struct LayoutTree {
    pub root: LayoutNode,
}
