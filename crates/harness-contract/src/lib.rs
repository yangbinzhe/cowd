//! Cowd AI harness semantic kernel.
//!
//! This crate is the single public home for pure AI harness contracts.  It
//! intentionally avoids stores, SDK clients, gateway APIs, runtime executors,
//! and external adapters.

// Test assertions intentionally use unwrap/expect; normal library builds remain strict.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

pub mod agent;
pub mod behavior;
pub mod context;
pub mod core;
pub mod evaluation;
pub mod execution;
pub mod execution_graph;
pub mod goal;
pub mod growth;
pub mod harness;
pub mod knowledge;
pub mod live;
pub mod managed_agent;
pub mod mission;
pub mod outcome;
pub mod policy;
pub mod projection;
pub mod reality;
pub mod security;
pub mod skill;
pub mod strategy;
pub mod task;
pub mod team;
pub mod tool;
pub mod turn;
pub mod verification;

pub use core::{
    AiKernelError, AiKernelResult, ExecutionModifier, ExecutionPattern, ExecutionPolicyGate,
    KernelCapability, KernelRef, MeasureProvenance, TaskComplexity, TaskRisk,
};
