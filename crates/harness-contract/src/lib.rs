//! Cowd AI harness semantic kernel.
//!
//! This crate is the single public home for pure AI harness contracts.  It
//! intentionally avoids stores, SDK clients, gateway APIs, runtime executors,
//! and external adapters.

pub mod agent;
pub mod behavior;
pub mod context;
pub mod core;
pub mod execution_graph;
pub mod growth;
pub mod harness;
pub mod knowledge;
pub mod policy;
pub mod reality;
pub mod skill;
pub mod strategy;
pub mod task;
pub mod team;
pub mod tool;
pub mod turn;
pub mod verification;

pub use core::{
    AiKernelError, AiKernelResult, ExecutionModifier, ExecutionPattern, ExecutionPolicyGate,
    KernelCapability, KernelRef, TaskComplexity, TaskRisk,
};
