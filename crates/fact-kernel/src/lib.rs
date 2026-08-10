#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
//! Unified fact semantics for Cowd memory, matrix, and growth.
//!
//! This crate contains contracts only.  It must not depend on Memory stores,
//! Matrix repositories, Gateway services, Runtime executors, or external SDKs.

pub mod bridge;
pub mod candidate;
pub mod core;
pub mod extraction;
pub mod growth;
pub mod health;
pub mod hypothesis;
pub mod indexer;
pub mod ledger;
pub mod matrix;
pub mod memory;
pub mod review;
pub mod service;
pub mod store;

pub use bridge::{decide_candidate_promotion, BridgeDecision};
pub use candidate::{
    ExtractionMethod, FactCandidate, FactCandidateId, FactCandidateRelation,
    FactCandidateRelationKind, FactScope, FactStatus,
};
pub use core::{
    Confidence, EvidencePacket, FactEvidenceId, FactId, FactRecord, FactSource, Provenance,
    SourceKind,
};
pub use extraction::{
    FactExtractionBatch, FactExtractionBatchId, FactExtractionTokenUsage, FactExtractionTrigger,
};
pub use indexer::FactSearchHit;
pub use ledger::{
    FactGrowthBatch, FactLedger, FactLedgerError, FactLedgerResult, FactLedgerSnapshot,
    FactRecallQuery, GrowthPromotionRecord, UnavailableFactLedger,
};
pub use review::{FactConflict, FactReviewDecision, FactReviewDecisionKind, FactReviewReceipt};
pub use service::{FactKernelService, PromotionReceipt};
pub use store::{FactStore, InMemoryFactStore};
