//! Supervised projection from execution evidence to evolution signals.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness_contract::outcome::{ExecutionOutcome, OutcomeTerminalClass};
use harness_contract::reality::EvidenceRef;
use sha2::{Digest, Sha256};

use super::{
    EvolutionDiscoveryService, EvolutionSignal, EvolutionSignalSeverity, EvolutionSignalSource,
    EvolutionSignalType,
};
use crate::execution_core::outcome_service::OUTCOME_EVENT_KIND;
use crate::{
    CancellationToken, CommittedEventBatch, DurableRuntimeEvent, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope, RuntimeEventStore, RuntimeTransactionEventInput,
};

const PROJECTOR_STREAM: &str = "evolution-signal-projector";
const PROJECTOR_BATCH: usize = 128;
const MAX_SOURCE_RETRIES: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EvolutionProjectorHealth {
    pub source_cursor: u64,
    pub latest_commit_cursor: u64,
    pub lag_commits: u64,
    pub dead_letter_count: usize,
    pub worker_running: bool,
}

pub(crate) struct EvolutionSignalProjector {
    event_store: Arc<RuntimeEventStore>,
    discovery: Arc<EvolutionDiscoveryService>,
    cancellation: CancellationToken,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl EvolutionSignalProjector {
    #[must_use]
    pub(crate) fn new(
        event_store: Arc<RuntimeEventStore>,
        discovery: Arc<EvolutionDiscoveryService>,
    ) -> Self {
        Self {
            event_store,
            discovery,
            cancellation: CancellationToken::new(),
            worker: Mutex::new(None),
        }
    }

    pub(crate) fn start(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        let projector = Arc::clone(self);
        *worker = Some(handle.spawn(async move {
            let mut commits = projector.event_store.subscribe_commits();
            loop {
                if let Err(error) = projector.run_once(PROJECTOR_BATCH) {
                    tracing::warn!(%error, "evolution signal projector pass failed");
                }
                tokio::select! {
                    _ = projector.cancellation.cancelled() => break,
                    changed = commits.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
            }
        }));
    }

    pub(crate) async fn shutdown(&self) {
        self.cancellation.cancel();
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
    }

    pub(crate) fn health(&self) -> Result<EvolutionProjectorHealth, String> {
        let source_cursor = self.cursor()?;
        let (latest_commit_cursor, lag_commits) = self.source_lag(source_cursor)?;
        let dead_letter_count = self
            .event_store
            .replay_scope_kind(
                RuntimeEventScope::Evolution,
                "evolution.signal.projector.failed.v1",
            )?
            .into_iter()
            .filter(|event| event.stream_id == PROJECTOR_STREAM)
            .count();
        let worker_running = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|worker| !worker.is_finished());
        Ok(EvolutionProjectorHealth {
            source_cursor,
            latest_commit_cursor,
            lag_commits,
            dead_letter_count,
            worker_running,
        })
    }

    /// Measure lag only against commits that contain Runtime source events.
    ///
    /// The projector records its output and checkpoint in the same event
    /// store. Counting those Evolution-only commits would make a healthy idle
    /// projector report permanent lag after every successful pass.
    fn source_lag(&self, source_cursor: u64) -> Result<(u64, u64), String> {
        const HEALTH_SCAN_BATCH: usize = 256;

        let upper_bound = *self.event_store.subscribe_commits().borrow();
        let mut scan_cursor = source_cursor;
        let mut latest_source_cursor = source_cursor;
        let mut lag_commits = 0_u64;
        while scan_cursor < upper_bound {
            let batches = self
                .event_store
                .events_after_cursor(scan_cursor, HEALTH_SCAN_BATCH)
                .map_err(|error| error.to_string())?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                if batch.commit_cursor > upper_bound {
                    break;
                }
                scan_cursor = batch.commit_cursor;
                if !is_projector_output_only(&batch) {
                    latest_source_cursor = batch.commit_cursor;
                    lag_commits = lag_commits.saturating_add(1);
                }
            }
        }
        Ok((latest_source_cursor, lag_commits))
    }

    /// Consume a bounded number of non-Evolution source commits.
    ///
    /// Evolution output and checkpoint commits do not consume the source
    /// budget, so a small batch cannot starve execution events behind the
    /// projector's own writes.
    pub(crate) fn run_once(&self, max_commits: usize) -> Result<usize, String> {
        let cursor = self.cursor()?;
        let max_commits = max_commits.max(1);
        let mut scan_cursor = cursor;
        let mut last_source_cursor = cursor;
        let mut processed = 0;
        loop {
            let batches = self
                .event_store
                .events_after_cursor(scan_cursor, max_commits - processed)
                .map_err(|error| error.to_string())?;
            if batches.is_empty() {
                break;
            }
            for batch in batches {
                scan_cursor = batch.commit_cursor;
                if is_projector_output_only(&batch) {
                    continue;
                }
                let has_agent_evaluation = batch
                    .events
                    .iter()
                    .any(|event| event.kind == "agent.run_evaluated");
                for event in batch
                    .events
                    .iter()
                    .filter(|event| !(has_agent_evaluation && event.kind == "agent.terminal"))
                {
                    self.process_source(event, batch.commit_cursor)?;
                }
                last_source_cursor = batch.commit_cursor;
                processed += 1;
                if processed == max_commits {
                    break;
                }
            }
            if processed == max_commits {
                break;
            }
        }
        if last_source_cursor > cursor {
            self.checkpoint(last_source_cursor)?;
        }
        Ok(processed)
    }

    fn process_source(
        &self,
        event: &DurableRuntimeEvent,
        source_cursor: u64,
    ) -> Result<(), String> {
        let signal = if event.kind == OUTCOME_EVENT_KIND {
            outcome_signal(event)?
        } else if event.kind == "agent.run_evaluated" {
            self.repeated_agent_failure_signal(event)?
        } else {
            signal_from_source_event(event)
        };
        let Some(signal) = signal else {
            return Ok(());
        };
        let mut last_error = None;
        for _ in 0..MAX_SOURCE_RETRIES {
            match self
                .discovery
                .record_signal(signal.clone())
                .and_then(|recorded| {
                    self.discovery
                        .create_lifecycle(vec![recorded.signal_id])
                        .map(|_| ())
                }) {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        self.dead_letter(
            event,
            source_cursor,
            last_error
                .as_deref()
                .unwrap_or("unknown projection failure"),
        )
    }

    /// Aggregate immutable Agent evaluation events before emitting a signal.
    ///
    /// This preserves the useful repeated-failure detector that previously
    /// lived in Agent Runtime while keeping all discovery writes behind the
    /// single bounded projector. One failed run cannot create a noisy
    /// proposal, and the deterministic key collapses replay into one chain.
    fn repeated_agent_failure_signal(
        &self,
        source_event: &DurableRuntimeEvent,
    ) -> Result<Option<EvolutionSignal>, String> {
        let trigger = source_event
            .payload
            .get("evaluation")
            .cloned()
            .ok_or_else(|| "agent evaluation source is missing its typed payload".to_string())
            .and_then(|value| {
                serde_json::from_value::<crate::AgentRunEvaluation>(value)
                    .map_err(|error| error.to_string())
            })?;
        if trigger.is_success() {
            return Ok(None);
        }
        let definition_id =
            harness_contract::agent::AgentDefinitionId::try_from(trigger.definition_id.as_str())
                .map_err(|error| error.to_string())?;
        if definition_id.scope() == harness_contract::agent::DefinitionScope::Builtin {
            return Ok(None);
        }

        let mut relevant = self
            .event_store
            .replay_scope_kind(RuntimeEventScope::Evolution, "agent.run_evaluated")?
            .into_iter()
            .filter_map(|event| {
                event.payload.get("evaluation").cloned().and_then(|value| {
                    serde_json::from_value::<crate::AgentRunEvaluation>(value).ok()
                })
            })
            .filter(|evaluation| {
                evaluation.definition_id == trigger.definition_id
                    && evaluation.definition_revision == trigger.definition_revision
                    && evaluation.environment_fingerprint == trigger.environment_fingerprint
            })
            .collect::<Vec<_>>();
        relevant.sort_by_key(|evaluation| evaluation.created_at_ms);
        if relevant.len() > 128 {
            relevant.drain(..relevant.len() - 128);
        }
        let failures = relevant
            .iter()
            .filter(|evaluation| !evaluation.is_success())
            .collect::<Vec<_>>();
        if relevant.len() < 3 || failures.len() < 2 {
            return Ok(None);
        }
        let success_count = relevant.len().saturating_sub(failures.len());
        if success_count.saturating_mul(1_000) / relevant.len() >= 700 {
            return Ok(None);
        }

        let mut failure_patterns = failures
            .iter()
            .filter_map(|evaluation| evaluation.failure.as_deref())
            .map(|failure| failure.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|failure| !failure.is_empty())
            .collect::<Vec<_>>();
        failure_patterns.sort();
        failure_patterns.dedup();
        failure_patterns.truncate(8);
        let evidence_refs = failures
            .iter()
            .rev()
            .take(10)
            .map(|evaluation| {
                EvidenceRef::observed("agent_run_evaluation", evaluation.evaluation_id.clone())
                    .with_source(format!(
                        "{}@{}",
                        evaluation.definition_id, evaluation.definition_revision
                    ))
            })
            .collect::<Vec<_>>();
        let signal_seed = format!(
            "{}:{}:{}",
            definition_id.as_str(),
            trigger.definition_revision,
            trigger.environment_fingerprint
        );
        let signal_digest = format!("{:x}", Sha256::digest(signal_seed.as_bytes()));
        Ok(Some(EvolutionSignal {
            signal_id: format!("evo-signal-agent-{}", &signal_digest[..20]),
            signal_type: EvolutionSignalType::AgentFailurePattern,
            source: EvolutionSignalSource {
                owner: format!("agent-definition:{}", definition_id.as_str()),
                session_id: ref_id(source_event, "session"),
                agent_id: Some(trigger.agent_instance_id.clone()),
                team_id: trigger.team_id.clone(),
                run_id: Some(trigger.run_id.clone()),
            },
            evidence_refs,
            severity: EvolutionSignalSeverity::Warning,
            summary: format!(
                "Agent definition {}@{} repeatedly failed in environment {}: {}",
                definition_id.as_str(),
                trigger.definition_revision,
                trigger.environment_fingerprint,
                if failure_patterns.is_empty() {
                    "no structured failure reason".to_string()
                } else {
                    failure_patterns.join("; ")
                }
            ),
            suggested_action:
                "Create an isolated definition revision and compare it against the bound baseline."
                    .to_string(),
            immediate_task_can_continue: true,
            created_at_ms: u128::from(source_event.created_at_ms),
        }))
    }

    fn cursor(&self) -> Result<u64, String> {
        Ok(self
            .event_store
            .latest_for_stream_kind(PROJECTOR_STREAM, "evolution.signal.projector.checkpoint.v1")?
            .and_then(|event| {
                event
                    .payload
                    .get("source_cursor")
                    .and_then(serde_json::Value::as_u64)
            })
            .unwrap_or_default())
    }

    fn checkpoint(&self, source_cursor: u64) -> Result<(), String> {
        let key = format!("source-cursor:{source_cursor}");
        if self
            .event_store
            .event_by_idempotency_key(PROJECTOR_STREAM, &key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        self.event_store
            .append_batch_if_revision(
                PROJECTOR_STREAM,
                self.event_store
                    .stream_revision(PROJECTOR_STREAM)
                    .map_err(|error| error.to_string())?,
                format!("evolution-projector:{source_cursor}"),
                vec![projector_event(
                    "evolution.signal.projector.checkpoint.v1",
                    Some("completed"),
                    Vec::new(),
                    serde_json::json!({"source_cursor": source_cursor}),
                    key,
                )],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn dead_letter(
        &self,
        event: &DurableRuntimeEvent,
        source_cursor: u64,
        error: &str,
    ) -> Result<(), String> {
        let key = format!("dead-letter:{}", event.event_id);
        if self
            .event_store
            .event_by_idempotency_key(PROJECTOR_STREAM, &key)
            .map_err(|store_error| store_error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        self.event_store
            .append_batch_if_revision(
                PROJECTOR_STREAM,
                self.event_store
                    .stream_revision(PROJECTOR_STREAM)
                    .map_err(|store_error| store_error.to_string())?,
                format!("evolution-projector-dead-letter:{}", event.event_id),
                vec![projector_event(
                    "evolution.signal.projector.failed.v1",
                    Some("dead_letter"),
                    vec![RuntimeEventRef {
                        kind: "source_event".to_string(),
                        id: event.event_id.clone(),
                    }],
                    serde_json::json!({
                        "source_cursor": source_cursor,
                        "source_event_id": event.event_id,
                        "source_kind": event.kind,
                        "attempts": MAX_SOURCE_RETRIES,
                        "error": error,
                    }),
                    key,
                )],
            )
            .map_err(|store_error| store_error.to_string())?;
        Ok(())
    }
}

fn outcome_signal(event: &DurableRuntimeEvent) -> Result<Option<EvolutionSignal>, String> {
    let outcome = serde_json::from_value::<ExecutionOutcome>(event.payload.clone())
        .map_err(|error| format!("canonical outcome payload is invalid: {error}"))?;
    let classification = match &outcome.terminal {
        OutcomeTerminalClass::Failed(reason)
        | OutcomeTerminalClass::Blocked(reason)
        | OutcomeTerminalClass::PartialFailure(reason) => Some((
            EvolutionSignalType::RecoveryGap,
            EvolutionSignalSeverity::Critical,
            format!(
                "{} execution {}: {}",
                outcome.terminal.class_name(),
                outcome.identity.execution_id,
                reason
            ),
            "Create an isolated recovery candidate and falsify it against the pinned baseline.",
            false,
        )),
        OutcomeTerminalClass::Cancelled(reason) => Some((
            EvolutionSignalType::SlowProgress,
            EvolutionSignalSeverity::Info,
            format!(
                "execution {} was cancelled: {}",
                outcome.identity.execution_id, reason
            ),
            "Inspect the attributable cancellation evidence before proposing any policy change.",
            true,
        )),
        OutcomeTerminalClass::Succeeded(_) if outcome.usage.duplicate_tool_calls > 0 => Some((
            EvolutionSignalType::LowNoveltyToolLoop,
            EvolutionSignalSeverity::Warning,
            format!(
                "execution {} completed with {} duplicate tool calls",
                outcome.identity.execution_id, outcome.usage.duplicate_tool_calls
            ),
            "Compare a dependency-aware batch or Tool DAG candidate against the pinned baseline.",
            true,
        )),
        _ => None,
    };
    let Some((signal_type, severity, summary, suggested_action, continue_task)) = classification
    else {
        return Ok(None);
    };
    let mut evidence_refs = outcome.evidence_refs.clone();
    evidence_refs.push(
        EvidenceRef::observed("execution_outcome", event.event_id.clone())
            .with_source(outcome.identity.execution_id.clone()),
    );
    let signal_digest = format!("{:x}", Sha256::digest(event.event_id.as_bytes()));
    Ok(Some(EvolutionSignal {
        signal_id: format!("evo-signal-outcome-{}", &signal_digest[..24]),
        signal_type,
        source: EvolutionSignalSource {
            owner: "runtime.outcome".to_string(),
            session_id: Some(outcome.identity.session_id),
            agent_id: outcome.identity.agent_id,
            team_id: outcome.identity.team_id,
            run_id: Some(outcome.identity.execution_id),
        },
        evidence_refs,
        severity,
        summary,
        suggested_action: suggested_action.to_string(),
        immediate_task_can_continue: continue_task,
        created_at_ms: u128::from(event.created_at_ms),
    }))
}

fn signal_from_source_event(event: &DurableRuntimeEvent) -> Option<EvolutionSignal> {
    let (signal_type, severity, summary, suggested_action, continue_task) =
        classify_source_event(event)?;
    let source = EvolutionSignalSource {
        owner: event
            .actor
            .clone()
            .unwrap_or_else(|| scope_owner(event.scope).to_string()),
        session_id: ref_id(event, "session"),
        agent_id: ref_id(event, "agent_run").or_else(|| ref_id(event, "agent")),
        team_id: ref_id(event, "team_run").or_else(|| ref_id(event, "team")),
        run_id: ref_id(event, "run"),
    };
    let digest = format!("{:x}", Sha256::digest(event.event_id.as_bytes()));
    Some(EvolutionSignal {
        signal_id: format!("evo-signal-source-{}", &digest[..24]),
        signal_type,
        source,
        evidence_refs: vec![
            EvidenceRef::observed("runtime_event", event.event_id.clone())
                .with_source(event.kind.clone()),
        ],
        severity,
        summary,
        suggested_action,
        immediate_task_can_continue: continue_task,
        created_at_ms: u128::from(event.created_at_ms),
    })
}

fn classify_source_event(
    event: &DurableRuntimeEvent,
) -> Option<(
    EvolutionSignalType,
    EvolutionSignalSeverity,
    String,
    String,
    bool,
)> {
    match event.kind.as_str() {
        "goal.intervention" => {
            let intervention = event.payload.get("intervention");
            let reason = intervention
                .and_then(|value| value.get("reason"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Runtime changed execution strategy after a progress review");
            let low_novelty = reason.to_ascii_lowercase().contains("novel")
                || reason.to_ascii_lowercase().contains("repeat");
            Some((
                if low_novelty {
                    EvolutionSignalType::LowNoveltyToolLoop
                } else {
                    EvolutionSignalType::SlowProgress
                },
                EvolutionSignalSeverity::Warning,
                reason.to_string(),
                "Compare the triggering observations and execution strategy in a bounded paired scenario."
                    .to_string(),
                true,
            ))
        }
        "goal.observation" => {
            let observation = event.payload.get("observation")?;
            let failed = observation
                .get("result_class")
                .and_then(serde_json::Value::as_str)
                == Some("failed")
                || observation
                    .get("failure_class")
                    .is_some_and(|value| !value.is_null());
            let tool_calls = observation
                .pointer("/cost_delta/tool_calls")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            let information_gain = observation
                .pointer("/information_gain/new_evidence")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
                + observation
                    .pointer("/information_gain/resolved_unknowns")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default();
            if failed {
                Some((
                    EvolutionSignalType::AgentFailurePattern,
                    EvolutionSignalSeverity::Warning,
                    "A typed Goal observation recorded an execution failure.".to_string(),
                    "Cluster attributable failures and run a frozen recovery scenario.".to_string(),
                    true,
                ))
            } else if tool_calls > 0 && information_gain == 0 {
                Some((
                    EvolutionSignalType::LowNoveltyToolLoop,
                    EvolutionSignalSeverity::Warning,
                    "Tool work consumed budget without verified information gain.".to_string(),
                    "Compare batched evidence acquisition or a different execution strategy."
                        .to_string(),
                    true,
                ))
            } else {
                None
            }
        }
        "goal.completed" if event.status.as_deref() != Some("satisfied") => Some((
            EvolutionSignalType::RecoveryGap,
            EvolutionSignalSeverity::Warning,
            format!(
                "A Goal completed without satisfaction ({})",
                event.status.as_deref().unwrap_or("unknown")
            ),
            "Preserve the terminal evidence and evaluate a bounded recovery strategy.".to_string(),
            false,
        )),
        _ => None,
    }
}

fn ref_id(event: &DurableRuntimeEvent, kind: &str) -> Option<String> {
    event
        .refs
        .iter()
        .find(|reference| reference.kind == kind)
        .map(|reference| reference.id.clone())
}

const fn scope_owner(scope: RuntimeEventScope) -> &'static str {
    match scope {
        RuntimeEventScope::Agent => "runtime.agent",
        RuntimeEventScope::Team => "runtime.team",
        RuntimeEventScope::Task => "runtime.task",
        RuntimeEventScope::Goal => "runtime.goal",
        RuntimeEventScope::Session => "runtime.session",
        RuntimeEventScope::Skill => "runtime.skill",
        RuntimeEventScope::Tool => "runtime.tool",
        RuntimeEventScope::Knowledge => "runtime.knowledge",
        _ => "runtime",
    }
}

fn is_projector_output_only(batch: &CommittedEventBatch) -> bool {
    !batch.events.is_empty()
        && batch.events.iter().all(|event| {
            is_projector_checkpoint(event)
                || (event.scope == RuntimeEventScope::Evolution
                    && (event.stream_id.starts_with("evolution:")
                        || event.stream_id == PROJECTOR_STREAM))
        })
}

fn is_projector_checkpoint(event: &DurableRuntimeEvent) -> bool {
    event.kind.ends_with(".projector.checkpoint.v1")
}

fn projector_event(
    kind: &str,
    status: Option<&str>,
    refs: Vec<RuntimeEventRef>,
    payload: serde_json::Value,
    idempotency_key: String,
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: PROJECTOR_STREAM.to_string(),
            scope: RuntimeEventScope::Evolution,
            kind: kind.to_string(),
            status: status.map(str::to_string),
            actor: Some("runtime.evolution_signal_projector".to_string()),
            refs,
            payload,
        },
        idempotency_key: Some(idempotency_key),
        schema_version: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{
        outcome::{
            OutcomeIdentity, OutcomeObservation, OutcomeQuality, OutcomeTiming, OutcomeUsage,
            RuntimeIdentity, StrategyIdentity, OUTCOME_SCHEMA_REVISION,
        },
        reality::EvidenceCompleteness,
        strategy::ExecutionCandidateKind,
    };

    fn failed_agent_evaluation(index: u64) -> crate::AgentRunEvaluation {
        crate::AgentRunEvaluation {
            evaluation_id: format!("evaluation-{index}"),
            run_id: format!("run-{index}"),
            agent_instance_id: format!("agent-{index}"),
            definition_id: "workspace/cowd/learner".to_string(),
            definition_revision: 1,
            binding_digest: "binding-digest".to_string(),
            release_assignment_id: None,
            release_generation: None,
            release_channel: Some(harness_contract::agent::ReleaseChannel::Stable),
            task_id: format!("task-{index}"),
            task_domain: "coding".to_string(),
            complexity: "medium".to_string(),
            role_slot_id: "worker".to_string(),
            model: "test-model".to_string(),
            provider: "test-provider".to_string(),
            granted_capabilities: vec!["read".to_string()],
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            memory_reality_fingerprint: "memory-fingerprint".to_string(),
            team_id: None,
            environment_fingerprint: "environment-fingerprint".to_string(),
            terminal_status: harness_contract::agent::AgentTerminalStatus::Failed,
            acceptance: vec!["task_success".to_string()],
            outcome: String::new(),
            failure: Some("repeated evidence validation failure".to_string()),
            input_tokens: 10,
            output_tokens: 2,
            tool_calls: 1,
            evidence_refs: vec![format!("evidence-{index}")],
            created_at_ms: index,
        }
    }

    fn failed_outcome() -> ExecutionOutcome {
        ExecutionOutcome {
            identity: OutcomeIdentity {
                execution_id: "execution-outcome-failed".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                terminal_generation: 1,
                paired_sample_id: None,
                task_id: None,
                mission_id: None,
                agent_id: None,
                team_id: None,
                execution_graph_ref: None,
            },
            runtime: RuntimeIdentity {
                workspace_key: "workspace".to_string(),
                runtime_revision: "test".to_string(),
                config_revision: "config".to_string(),
            },
            provider: None,
            strategy: StrategyIdentity {
                decision_id: "decision".to_string(),
                policy_revision: "policy".to_string(),
                decision_source: "test".to_string(),
                selected_candidate: ExecutionCandidateKind::Direct,
                selected_pattern: "direct".to_string(),
            },
            timing: OutcomeTiming {
                started_at_ms: 1,
                completed_at_ms: 2,
                duration_ms: 1,
            },
            usage: OutcomeUsage::default(),
            terminal: OutcomeTerminalClass::Failed("provider failed".to_string()),
            quality: OutcomeQuality::Unknown,
            observation: OutcomeObservation {
                source: "test".to_string(),
                observed_at_ms: 2,
                freshness_ms: 0,
            },
            evidence_refs: Vec::new(),
            evidence_completeness: EvidenceCompleteness::None,
            schema_revision: OUTCOME_SCHEMA_REVISION,
        }
    }

    #[test]
    fn projector_replays_source_once_and_crosses_its_own_checkpoint() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        events
            .append(RuntimeEventInput {
                stream_id: "goal:goal-1".to_string(),
                scope: RuntimeEventScope::Goal,
                kind: "goal.intervention".to_string(),
                status: Some("replan".to_string()),
                actor: Some("runtime.intervention_policy".to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "session".to_string(),
                    id: "session-1".to_string(),
                }],
                payload: serde_json::json!({
                    "intervention": {
                        "reason": "repeated low novelty reads",
                    }
                }),
            })
            .expect("source event");
        assert_eq!(projector.run_once(1).expect("first pass"), 1);
        assert_eq!(discovery.list_signals().expect("signals").len(), 1);
        assert_eq!(discovery.list_diagnoses().expect("diagnoses").len(), 1);
        assert_eq!(discovery.list_missions().expect("missions").len(), 1);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 1);
        let health = projector.health().expect("health after checkpoint");
        assert_eq!(health.source_cursor, health.latest_commit_cursor);
        assert_eq!(health.lag_commits, 0);

        events
            .append(RuntimeEventInput {
                stream_id: "goal:goal-2".to_string(),
                scope: RuntimeEventScope::Goal,
                kind: "goal.observation".to_string(),
                status: Some("failed".to_string()),
                actor: Some("runtime.goal".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "observation": {
                        "result_class": "failed",
                        "failure_class": "provider_unavailable",
                        "cost_delta": {"tool_calls": 0},
                        "information_gain": {
                            "new_evidence": 0,
                            "resolved_unknowns": 0
                        }
                    }
                }),
            })
            .expect("second source event");
        assert_eq!(projector.run_once(1).expect("second pass"), 1);
        assert_eq!(discovery.list_signals().expect("signals").len(), 2);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 2);

        for index in 1..=3 {
            events
                .append(RuntimeEventInput {
                    stream_id: format!("agent-evaluation:run-{index}"),
                    scope: RuntimeEventScope::Evolution,
                    kind: "agent.run_evaluated".to_string(),
                    status: Some("failed".to_string()),
                    actor: Some("runtime.agent_evaluation".to_string()),
                    refs: vec![RuntimeEventRef {
                        kind: "run".to_string(),
                        id: format!("run-{index}"),
                    }],
                    payload: serde_json::json!({
                        "evaluation": failed_agent_evaluation(index),
                    }),
                })
                .expect("evaluation source event");
        }
        assert_eq!(projector.run_once(3).expect("evaluation pass"), 3);
        assert_eq!(discovery.list_signals().expect("signals").len(), 3);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 3);

        let restarted = EvolutionSignalProjector::new(events, Arc::clone(&discovery));
        assert_eq!(restarted.run_once(64).expect("restart pass"), 0);
        assert_eq!(restarted.health().expect("restart health").lag_commits, 0);
        assert_eq!(discovery.list_signals().expect("signals").len(), 3);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 3);
    }

    #[test]
    fn projector_does_not_echo_another_projector_checkpoint() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), discovery);
        events
            .append(RuntimeEventInput {
                stream_id: "knowledge-candidate-projector".to_string(),
                scope: RuntimeEventScope::Recovery,
                kind: "knowledge.candidate.projector.checkpoint.v1".to_string(),
                status: Some("completed".to_string()),
                actor: Some("runtime.knowledge_candidate_projector".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({"source_cursor": 1}),
            })
            .expect("foreign projector checkpoint");

        assert_eq!(projector.run_once(64).expect("projector pass"), 0);
        assert!(events
            .list_stream(PROJECTOR_STREAM)
            .expect("projector stream")
            .is_empty());
    }

    #[test]
    fn canonical_outcome_enters_governed_evolution_without_direct_promotion() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(Arc::clone(&events)));
        let projector = EvolutionSignalProjector::new(Arc::clone(&events), Arc::clone(&discovery));
        crate::execution_core::OutcomeService::new(Arc::clone(&events))
            .record_terminal(&failed_outcome())
            .expect("outcome");

        assert_eq!(projector.run_once(64).expect("project"), 1);
        assert_eq!(discovery.list_signals().expect("signals").len(), 1);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 1);
        assert!(events
            .list_scope(RuntimeEventScope::Evolution, 1_000)
            .expect("events")
            .iter()
            .all(|event| !event.kind.contains("stable")));
    }
}
