//! Memory layer abstraction.
//!
//! Each concrete layer (L0–L4) implements the `MemoryLayer` trait and is
//! responsible for managing entries within its scope: insertion, eviction,
//! promotion/demotion between layers, and preparing context fragments.

use async_trait::async_trait;

use crate::{
    error::MemoryError,
    types::{MemoryEntry, MemoryId, MemoryLayer, PreparedContext, TokenBudget},
};

pub mod deep;
pub mod essential;
pub mod identity;
pub mod project;
pub mod shared;

/// Result alias for layer operations.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// Behaviour contract for a single memory layer.
#[async_trait]
pub trait LayerManager: Send + Sync {
    /// Which logical layer this manager handles.
    fn layer(&self) -> MemoryLayer;

    /// Add or refresh an entry in this layer.
    async fn insert(&self, entry: MemoryEntry) -> Result<MemoryId>;

    /// Remove an entry from this layer.
    async fn remove(&self, id: &MemoryId) -> Result<()>;

    /// Prepare a context fragment from this layer within the given budget.
    async fn prepare_context(&self, budget: &TokenBudget) -> Result<PreparedContext>;

    /// Run any periodic maintenance (eviction, staleness updates, …).
    async fn tick(&self) -> Result<()>;
}
