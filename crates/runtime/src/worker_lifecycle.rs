use std::sync::Arc;
use chrono::Utc;
use tokio::sync::{broadcast, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Initializing,
    Ready,
    Running,
    AwaitingApproval,
    Blocked,
    Completed,
    Failed,
    ShuttingDown,
    Terminated,
}

impl WorkerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Initializing => "initializing",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::AwaitingApproval => "awaiting_approval",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::ShuttingDown => "shutting_down",
            Self::Terminated => "terminated",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Terminated)
    }
}

#[derive(Debug, Clone)]
pub struct StateTransition {
    pub from: WorkerState,
    pub to: WorkerState,
    pub timestamp: chrono::DateTime<Utc>,
    pub elapsed_ms: u64,
}

pub struct WorkerLifecycle {
    state: RwLock<WorkerState>,
    transitions: RwLock<Vec<StateTransition>>,
    started_at: std::time::Instant,
    tx: broadcast::Sender<WorkerState>,
}

impl WorkerLifecycle {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(64);
        Self {
            state: RwLock::new(WorkerState::Initializing),
            transitions: RwLock::new(Vec::new()),
            started_at: std::time::Instant::now(),
            tx,
        }
    }

    pub async fn state(&self) -> WorkerState {
        *self.state.read().await
    }

    pub async fn transition(&self, new_state: WorkerState) {
        let old = *self.state.read().await;
        let now = Utc::now();
        self.transitions.write().await.push(StateTransition {
            from: old,
            to: new_state,
            timestamp: now,
            elapsed_ms: self.started_at.elapsed().as_millis() as u64,
        });
        *self.state.write().await = new_state;
        let _ = self.tx.send(new_state);
        tracing::info!(from=?old, to=?new_state, uptime_ms=self.uptime_ms(), "worker state transition");
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WorkerState> {
        self.tx.subscribe()
    }

    pub fn uptime_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    pub async fn is_terminal(&self) -> bool {
        self.state().await.is_terminal()
    }
}
