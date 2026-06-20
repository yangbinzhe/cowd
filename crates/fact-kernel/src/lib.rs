//! Unified fact semantics for Cowd memory, matrix, and growth.
//!
//! This crate contains contracts only.  It must not depend on Memory stores,
//! Matrix repositories, Gateway services, Runtime executors, or external SDKs.

pub mod bridge;
pub mod core;
pub mod growth;
pub mod health;
pub mod hypothesis;
pub mod indexer;
pub mod matrix;
pub mod memory;
pub mod service;
pub mod store;

pub use bridge::{decide_candidate_promotion, BridgeDecision};
pub use core::{
    Confidence, EvidenceId, EvidencePacket, FactId, FactRecord, FactSource, Provenance, SourceKind,
};
pub use indexer::FactSearchHit;
pub use service::{FactKernelService, PromotionReceipt};
pub use store::{FactStore, InMemoryFactStore};
