//! Cowd AI harness semantic kernel.
//!
//! This crate is the single public home for pure AI harness contracts.  It
//! intentionally avoids stores, SDK clients, gateway APIs, runtime executors,
//! and external adapters.

pub mod agent {
    pub use ai_agent_spec::*;
}

pub mod behavior {
    pub use ai_behavior_policy::*;
}

pub mod context {
    pub use ai_context::*;
}

pub mod core {
    pub use ai_core::*;
}

pub mod growth {
    pub use ai_growth::*;
}

pub mod harness {
    pub use ai_harness::*;
}

pub mod policy;

pub mod strategy {
    pub use ai_strategy::*;
}

pub mod task;

pub mod tool {
    pub use ai_tool_transaction::*;
}

pub mod turn;

pub mod verification {
    pub use ai_verification::*;
}

pub mod workgraph {
    pub use ai_workgraph::*;
}

pub use core::{
    AiKernelError, AiKernelResult, ExecutionMode, KernelCapability, KernelRef, StrategyDecorator,
    TaskComplexity, TaskRisk,
};
