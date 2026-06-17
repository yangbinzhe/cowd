// Thin compatibility re-export. Runtime storage core moved to crates/storage in
// 0.9.295; remove this module once all downstream imports use `storage`
// directly.
pub use cowd_storage::*;
