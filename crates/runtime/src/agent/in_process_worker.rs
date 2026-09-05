use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommandRequest, AgentInput, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
};
use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};
use sha2::{Digest, Sha256};

use crate::{
    ContextProfile, PermissionMode, PermissionPolicy, RuntimeExecutionHost, RuntimeServices,
    RuntimeToolExecutionRequest, RuntimeToolExecutionStatus, Session, SharedPrompter,
    StandardRuntimeHost, StandardRuntimeHostConfig, ToolError, ToolExecutor,
};

use crate::agent_model_selector::AgentModelSelection;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
use crate::agent_runtime::AgentRuntimeBackend;
use crate::execution_core::graph::{
    ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
};

#[path = "in_process/stages.rs"]
mod stages;
use stages::*;
#[path = "in_process/terminal.rs"]
mod terminal;
use terminal::*;
#[path = "in_process/model_loop.rs"]
mod model_loop;
#[path = "in_process/tool_turn.rs"]
mod tool_turn;
use tool_turn::*;
#[path = "in_process/path_scope.rs"]
mod path_scope;
#[path = "in_process/tool_scope.rs"]
mod tool_scope;
use path_scope::*;
#[path = "in_process/prompt.rs"]
mod prompt;
use prompt::*;
#[path = "in_process/evidence_collector.rs"]
mod evidence_collector;
use evidence_collector::*;
pub(crate) use evidence_collector::{
    structured_agent_output, structured_agent_output_for_fields,
    structured_contract_field_materialized,
};

/// Executes a delegated task through the same RuntimeServices/Runner/provider
/// path as a primary turn. It never calls `ConversationRuntime` directly.
pub struct InProcessAgentWorker {
    services: Weak<RuntimeServices>,
    active_runs: Mutex<BTreeMap<String, ActiveInProcessRun>>,
    pending_cancellations: Mutex<BTreeSet<String>>,
    completed_runs: Mutex<VecDeque<String>>,
}
#[cfg(test)]
#[path = "in_process/tests.rs"]
mod tests;

const COMPLETED_RUN_TOMBSTONE_LIMIT: usize = 1_024;

#[derive(Clone)]
struct ActiveInProcessRun {
    cancellation: crate::CancellationToken,
    session_id: String,
    input_stream: crate::SessionInputStream,
}

struct ActiveRunCleanup<'a> {
    worker: &'a InProcessAgentWorker,
    run_id: String,
}

impl Drop for ActiveRunCleanup<'_> {
    fn drop(&mut self) {
        // The tombstone is visible before the active handle disappears; the
        // completion flag/notification are published only after all maps are
        // clean. This also runs when the execute future is aborted.
        self.worker.record_completed_run(&self.run_id);
        self.worker
            .active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
        self.worker
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
    }
}

impl InProcessAgentWorker {
    #[must_use]
    pub fn new(services: Weak<RuntimeServices>) -> Self {
        Self {
            services,
            active_runs: Mutex::new(BTreeMap::new()),
            pending_cancellations: Mutex::new(BTreeSet::new()),
            completed_runs: Mutex::new(VecDeque::new()),
        }
    }

    fn record_completed_run(&self, run_id: &str) {
        let mut completed = self
            .completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !completed.iter().any(|candidate| candidate == run_id) {
            completed.push_back(run_id.to_string());
        }
        while completed.len() > COMPLETED_RUN_TOMBSTONE_LIMIT {
            completed.pop_front();
        }
    }

    fn run_completed(&self, run_id: &str) -> bool {
        self.completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|candidate| candidate == run_id)
    }
}
