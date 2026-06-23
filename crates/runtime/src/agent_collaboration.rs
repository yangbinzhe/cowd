//! Collaborative agent orchestration: team assembly, task decomposition,
//! wave-based parallel dispatch, result synthesis with fact-checking, and
//! L4 team memory finalization.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::agent::{SubAgentConfig, SubAgentExecutor, SubAgentResult};
use crate::agent_workgraph::AgentWorkGraph;
use crate::collaboration_template::{CollaborationDecision, CollaborationTemplateMatcher};
use crate::context_runtime::ContextItem;
use crate::team_discovery::TeamDiscoveryProtocol;
use crate::wave::{TaskId, WaveConfig, WaveOrchestrator, WaveTask};
use ai_kernel::strategy::{decide_strategy, StrategyInput};

use memory::agent_directory::{AgentDirectory, AgentInfo};
use memory::fact_checker::{FactCheckResult, FactChecker};
use memory::project_scope::MemoryScope;
use memory::temporal_graph::Triple;
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::{
    MaintenanceCandidate, MaintenanceCandidateKind, MaintenanceCandidateStatus, MemoryKernel,
    MemoryTurnContext,
};

// ── SubTask ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubTask {
    pub id: String,
    pub description: String,
    pub required_skills: Vec<String>,
    pub depends_on: Vec<String>,
}

// ── CollaborationTask ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollaborationTask {
    pub description: String,
    pub required_skills: Vec<String>,
    pub subtasks: Vec<SubTask>,
    pub review_criteria: Option<String>,
    pub collaboration_decision: Option<CollaborationDecision>,
}

#[derive(Debug, Clone)]
pub struct CollaborationContextResult {
    pub synthesis: String,
    pub context_items: Vec<ContextItem>,
    pub collaboration_task: CollaborationTask,
    pub review_packet: CollaborationReviewPacket,
    pub work_graph: AgentWorkGraph,
}

// ── Shared Board / Synthesis Scoring ───────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryPulseKind {
    Remember,
    Refresh,
    Promote,
    Retire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPulseCandidate {
    pub kind: MemoryPulseKind,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedBoardEntry {
    pub agent_id: String,
    pub completed_normally: bool,
    pub output_preview: String,
    pub decisions: Vec<String>,
    pub evidence: Vec<String>,
    pub conflicts: Vec<String>,
    pub memory_pulses: Vec<MemoryPulseCandidate>,
    pub next_actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollaborationScorecard {
    pub completion_rate: f32,
    pub synthesis_lift: f32,
    pub complementarity_score: f32,
    pub active_memory_score: f32,
    pub conflict_count: usize,
    pub memory_pulse_count: usize,
    pub surfaced_conflicts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationValueVerdict {
    pub positive_lift: bool,
    pub continue_multi_agent: bool,
    pub value_score: u16,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskTrace {
    pub task_id: String,
    pub parent_run_id: Option<String>,
    pub agent_run_id: Option<String>,
    pub role: String,
    pub objective: String,
    pub status: String,
    pub context_envelope_id: Option<String>,
    pub result_summary: String,
    pub evidence_refs: Vec<String>,
    pub collaboration_board_id: String,
    pub confidence: f32,
    pub conflicts: Vec<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollaborationReviewPacket {
    pub board_id: String,
    pub parent_run_id: Option<String>,
    pub scorecard: CollaborationScorecard,
    pub agent_tasks: Vec<AgentTaskTrace>,
    pub maintenance_candidates: Vec<MaintenanceCandidate>,
}

impl CollaborationScorecard {
    pub fn shows_multi_agent_lift(&self) -> bool {
        self.synthesis_lift > 1.0 && self.complementarity_score > 0.0
    }

    pub fn needs_memory_pulse(&self) -> bool {
        self.memory_pulse_count > 0 || self.conflict_count > 0
    }

    #[must_use]
    pub fn value_verdict(&self) -> CollaborationValueVerdict {
        let completion = (self.completion_rate.clamp(0.0, 1.0) * 35.0).round() as u16;
        let lift = (((self.synthesis_lift - 1.0).max(0.0).min(1.0)) * 35.0).round() as u16;
        let complement = (self.complementarity_score.clamp(0.0, 1.0) * 20.0).round() as u16;
        let memory = (self.active_memory_score.clamp(0.0, 1.0) * 10.0).round() as u16;
        let conflict_penalty = (self.conflict_count as u16).saturating_mul(8).min(24);
        let value_score = completion
            .saturating_add(lift)
            .saturating_add(complement)
            .saturating_add(memory)
            .saturating_sub(conflict_penalty)
            .min(100);

        let positive_lift = self.shows_multi_agent_lift()
            && self.completion_rate >= 0.66
            && value_score >= 50
            && self.conflict_count <= 2;
        let continue_multi_agent = positive_lift || (value_score >= 65 && self.conflict_count <= 1);
        let mut reasons = Vec::new();
        if self.completion_rate < 0.66 {
            reasons.push("low_completion_rate".to_string());
        }
        if self.synthesis_lift <= 1.0 {
            reasons.push("no_synthesis_lift".to_string());
        }
        if self.complementarity_score <= 0.0 {
            reasons.push("no_complementarity".to_string());
        }
        if self.conflict_count > 2 {
            reasons.push("excessive_conflict".to_string());
        }
        if reasons.is_empty() {
            reasons.push("positive_multi_agent_lift".to_string());
        }

        CollaborationValueVerdict {
            positive_lift,
            continue_multi_agent,
            value_score,
            reasons,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollaborationBoard {
    pub board_id: String,
    pub entries: Vec<SharedBoardEntry>,
    pub scorecard: CollaborationScorecard,
}

impl CollaborationBoard {
    pub fn memory_maintenance_candidates(&self) -> Vec<MaintenanceCandidate> {
        let mut candidates = Vec::new();
        for conflict in &self.scorecard.surfaced_conflicts {
            candidates.push(collaboration_maintenance_candidate(
                MaintenanceCandidateKind::Conflict,
                format!(
                    "Review multi-agent conflict: {}",
                    truncate_for_candidate(conflict)
                ),
                conflict.clone(),
                self.scorecard_candidate_confidence(),
                &self.board_id,
            ));
        }

        for entry in &self.entries {
            for pulse in &entry.memory_pulses {
                let (kind, summary) = match pulse.kind {
                    MemoryPulseKind::Remember => (
                        MaintenanceCandidateKind::RelationshipRefresh,
                        "Review agent-discovered knowledge",
                    ),
                    MemoryPulseKind::Refresh => (
                        MaintenanceCandidateKind::RelationshipRefresh,
                        "Refresh agent-recalled knowledge",
                    ),
                    MemoryPulseKind::Promote => (
                        MaintenanceCandidateKind::AuthorityPromotion,
                        "Consider promoting agent-verified knowledge",
                    ),
                    MemoryPulseKind::Retire => (
                        MaintenanceCandidateKind::Stale,
                        "Move stale agent-mentioned knowledge out of foreground",
                    ),
                };
                candidates.push(collaboration_maintenance_candidate(
                    kind,
                    format!("{}: {}", summary, truncate_for_candidate(&pulse.content)),
                    format!(
                        "agent={} pulse={:?}; {}",
                        entry.agent_id, pulse.kind, pulse.content
                    ),
                    self.scorecard_candidate_confidence(),
                    &self.board_id,
                ));
            }
        }

        candidates
    }

    pub fn review_packet(
        &self,
        parent_run_id: Option<String>,
        agent_tasks: Vec<AgentTaskTrace>,
    ) -> CollaborationReviewPacket {
        CollaborationReviewPacket {
            board_id: self.board_id.clone(),
            parent_run_id,
            scorecard: self.scorecard.clone(),
            agent_tasks,
            maintenance_candidates: self.memory_maintenance_candidates(),
        }
    }

    fn scorecard_candidate_confidence(&self) -> f32 {
        let base = 0.55 + (self.scorecard.active_memory_score * 0.25);
        let lift = (self.scorecard.synthesis_lift - 1.0).max(0.0).min(1.0) * 0.15;
        let completion = self.scorecard.completion_rate * 0.05;
        (base + lift + completion).clamp(0.1, 0.95)
    }
}

// ── AgentTeam ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AgentTeam {
    pub leader: AgentInfo,
    pub workers: Vec<AgentInfo>,
}

// ── CollaborationOrchestrator ──────────────────────────────────────────────────

pub struct CollaborationOrchestrator<E: SubAgentExecutor> {
    executor: Arc<E>,
    parent_memory: Option<Arc<memory::CognitiveContextManager>>,
    discovery: TeamDiscoveryProtocol,
}

impl<E: SubAgentExecutor + 'static> CollaborationOrchestrator<E> {
    pub fn new(executor: Arc<E>) -> Self {
        Self {
            executor,
            parent_memory: None,
            discovery: TeamDiscoveryProtocol::new(),
        }
    }

    pub fn with_parent_memory(mut self, memory: Arc<memory::CognitiveContextManager>) -> Self {
        self.parent_memory = Some(memory);
        self
    }

    /// Inject a `TeamDiscoveryProtocol` for reputation-aware team assembly.
    pub fn with_discovery(mut self, discovery: TeamDiscoveryProtocol) -> Self {
        self.discovery = discovery;
        self
    }

    // ── decompose_task ─────────────────────────────────────────────────────

    /// Heuristic decomposition: identify phases by delimiter keywords and
    /// assign required skills via keyword matching.
    pub fn decompose_task(&self, task: &str) -> Vec<SubTask> {
        let phases = split_phases(task);
        phases
            .into_iter()
            .enumerate()
            .map(|(i, desc)| {
                let skills = infer_skills(&desc);
                SubTask {
                    id: format!("subtask-{}", i + 1),
                    description: desc,
                    required_skills: skills,
                    depends_on: Vec::new(),
                }
            })
            .collect()
    }

    /// Decompose with ordered dependencies (each phase depends on the
    /// previous one).
    pub fn decompose_sequential(&self, task: &str) -> Vec<SubTask> {
        let mut subtasks = self.decompose_task(task);
        for i in 1..subtasks.len() {
            let prev_id = subtasks[i - 1].id.clone();
            subtasks[i].depends_on.push(prev_id);
        }
        subtasks
    }

    // ── assemble_team ──────────────────────────────────────────────────────

    /// Query the `TeamDiscoveryProtocol` for agents matching required skills,
    /// ranked by skill-overlap * reputation composite.
    ///
    /// Falls back to the basic `AgentDirectory::discover()` if the discovery
    /// protocol produces no results but raw candidates exist.
    ///
    /// The highest-ranked agent is elected leader; the rest become workers.
    pub fn assemble_team(&self, task: &CollaborationTask) -> Option<AgentTeam> {
        // Try reputation-aware discovery first.
        if let Some(discovered) = self
            .discovery
            .auto_assemble(&task.description, &task.required_skills)
        {
            return Some(AgentTeam {
                leader: discovered.leader,
                workers: discovered.workers,
            });
        }

        // Fallback: simple AgentDirectory discovery.
        let candidates = AgentDirectory::global().discover(&task.required_skills);
        if candidates.is_empty() {
            return None;
        }
        let mut scored: Vec<_> = candidates
            .into_iter()
            .map(|a| {
                let score = a
                    .capabilities
                    .iter()
                    .filter(|c| task.required_skills.contains(c))
                    .count();
                (score, a)
            })
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0));

        let mut iter = scored.into_iter();
        let leader = iter.next().map(|(_, a)| a).expect("non-empty");
        let workers: Vec<AgentInfo> = iter.map(|(_, a)| a).collect();

        Some(AgentTeam { leader, workers })
    }

    // ── dispatch_subtasks ──────────────────────────────────────────────────

    /// Dispatch subtasks in dependency-resolved waves.
    /// Tasks within each wave run sequentially.
    pub async fn dispatch_subtasks(
        &self,
        _team: &AgentTeam,
        subtasks: &[SubTask],
    ) -> Vec<SubAgentResult> {
        if subtasks.is_empty() {
            return Vec::new();
        }

        let mut orchestrator = WaveOrchestrator::new().with_config(
            WaveConfig::default()
                .with_max_parallel(4)
                .with_continue_on_failure(true),
        );

        for st in subtasks {
            let mut wave_task = WaveTask::new(&st.id, &st.description);
            wave_task = wave_task.with_description(&st.description);
            for dep in &st.depends_on {
                wave_task = wave_task.with_dependency(TaskId::new(dep.as_str()));
            }
            wave_task = wave_task.with_payload(serde_json::json!({
                "description": st.description,
                "skills": st.required_skills,
            }));
            orchestrator.add_task(wave_task);
        }

        // Build the dependency-resolved wave plan.
        if let Err(e) = orchestrator.build_waves() {
            tracing::warn!("wave build failed: {e} — falling back to sequential");
            return self.dispatch_sequential(subtasks).await;
        }

        let mut all_results: Vec<SubAgentResult> = Vec::new();
        let wave_count = orchestrator.wave_count();

        for wave_num in 1..=wave_count as u32 {
            let tasks = match orchestrator.get_wave_tasks(wave_num) {
                Some(t) => t,
                None => continue,
            };

            for task in tasks {
                let desc = task.description.clone().unwrap_or_default();
                let config = SubAgentConfig {
                    task_description: task.name.clone(),
                    ..SubAgentConfig::default()
                };
                match self.executor.execute(config, &desc).await {
                    Ok(result) => all_results.push(result),
                    Err(e) => all_results.push(SubAgentResult {
                        output: format!("dispatch error: {e}"),
                        completed_normally: false,
                        ..SubAgentResult::default()
                    }),
                }
            }
        }

        all_results
    }

    /// Sequential fallback when wave building fails.
    async fn dispatch_sequential(&self, subtasks: &[SubTask]) -> Vec<SubAgentResult> {
        let mut results = Vec::new();
        for st in subtasks {
            let config = SubAgentConfig {
                task_description: st.description.clone(),
                ..SubAgentConfig::default()
            };
            match self.executor.execute(config, &st.description).await {
                Ok(r) => results.push(r),
                Err(e) => results.push(SubAgentResult {
                    output: format!("error: {e}"),
                    completed_normally: false,
                    ..SubAgentResult::default()
                }),
            }
        }
        results
    }

    // ── synthesize ─────────────────────────────────────────────────────────

    /// Build a lightweight shared board from sub-agent returns.
    ///
    /// The board keeps the experiment cheap: it only inspects structured text
    /// prefixes already returned by agents, then computes deterministic scores
    /// that indicate collaboration lift, surfaced conflicts, and live-memory
    /// pulse candidates.
    pub fn build_shared_board(&self, results: &[SubAgentResult]) -> CollaborationBoard {
        build_collaboration_board(results)
    }

    /// Merge sub-agent results into a coherent synthesis, detecting and
    /// flagging conflicts via `FactChecker`.
    pub fn synthesize(&self, results: &[SubAgentResult]) -> String {
        let mut output = String::from("## Collaboration Synthesis\n\n");
        let board = self.build_shared_board(results);

        let mut checker = FactChecker::new();
        let mut conflicts: Vec<FactCheckResult> = Vec::new();

        // Register each result as a pseudo-triple for conflict detection.
        for (i, r) in results.iter().enumerate() {
            let triple = Triple {
                id: format!("synth-{i}"),
                subject: "subtask".to_string(),
                predicate: format!("result-{i}"),
                object: truncate_str(&r.output, 200),
                valid_from: None,
                valid_until: None,
                confidence: if r.completed_normally { 0.85 } else { 0.3 },
                source_memory_id: None,
                source_file: None,
                source_agent: Some(format!("agent-{i}")),
            };
            checker.register_triple(triple.clone());

            let check = checker.check_triple(&triple);
            if !check.is_consistent {
                conflicts.push(check);
            }
        }

        // Auto-correct and build summary.
        let report = checker.auto_correct();

        output.push_str(&format!(
            "**Sub-agents completed**: {} / {}\n",
            results.iter().filter(|r| r.completed_normally).count(),
            results.len()
        ));
        output.push_str(&format!("**Corrections**: {}\n", report.corrected));
        output.push_str(&format!("**Flagged for review**: {}\n", report.flagged));
        output.push_str(&format!("**Pruned**: {}\n\n", report.pruned));

        output.push_str("### Shared Board Scorecard\n\n");
        output.push_str(&format!(
            "**Completion rate**: {:.0}%\n",
            board.scorecard.completion_rate * 100.0
        ));
        output.push_str(&format!(
            "**Synthesis lift**: {:.2}x\n",
            board.scorecard.synthesis_lift
        ));
        output.push_str(&format!(
            "**Complementarity**: {:.2}\n",
            board.scorecard.complementarity_score
        ));
        output.push_str(&format!(
            "**Memory pulses**: {}\n",
            board.scorecard.memory_pulse_count
        ));
        output.push_str(&format!(
            "**Surfaced conflicts**: {}\n\n",
            board.scorecard.conflict_count + conflicts.len()
        ));

        if !board.scorecard.surfaced_conflicts.is_empty() {
            output.push_str("### Surfaced Conflicts\n\n");
            for conflict in &board.scorecard.surfaced_conflicts {
                output.push_str(&format!("- {}\n", truncate_str(conflict, 240)));
            }
            output.push('\n');
        }

        let memory_pulses: Vec<_> = board
            .entries
            .iter()
            .flat_map(|entry| {
                entry
                    .memory_pulses
                    .iter()
                    .map(move |pulse| (&entry.agent_id, pulse))
            })
            .collect();
        if !memory_pulses.is_empty() {
            output.push_str("### Memory Pulse Candidates\n\n");
            for (agent_id, pulse) in memory_pulses {
                output.push_str(&format!(
                    "- **{}** [{:?}]: {}\n",
                    agent_id,
                    pulse.kind,
                    truncate_str(&pulse.content, 240)
                ));
            }
            output.push('\n');
        }

        if !conflicts.is_empty() {
            output.push_str("### Conflicts Detected\n\n");
            for c in &conflicts {
                output.push_str(&format!(
                    "- **{}**: {} (confidence: {:.2})\n",
                    c.triple_id,
                    c.contradiction.as_deref().unwrap_or("unknown"),
                    c.confidence
                ));
            }
            output.push('\n');
        }

        output.push_str("### Agent Results\n\n");
        for (i, r) in results.iter().enumerate() {
            let status = if r.completed_normally { "OK" } else { "FAILED" };
            output.push_str(&format!(
                "**Agent {}** [{status}]: {}\n",
                i + 1,
                truncate_str(&r.output, 500)
            ));
            output.push('\n');
        }

        output
    }

    pub fn context_items_from_results(
        &self,
        team: &AgentTeam,
        results: &[SubAgentResult],
    ) -> Vec<ContextItem> {
        results
            .iter()
            .enumerate()
            .map(|(idx, result)| {
                let child_agent_id = team
                    .workers
                    .get(idx)
                    .map(|agent| agent.agent_id.clone())
                    .unwrap_or_else(|| format!("agent-{}", idx + 1));
                result.to_context_item("collaboration-orchestrator", child_agent_id)
            })
            .collect()
    }

    // ── finalize ───────────────────────────────────────────────────────────

    /// Persist the collaboration synthesis to L4 team-shared memory so it is
    /// discoverable by future agents via `team_query`.
    pub async fn finalize(&self, synthesis: &str, task_desc: &str) {
        let Some(ref mem) = self.parent_memory else {
            tracing::debug!("no parent memory configured — skipping L4 finalize");
            return;
        };
        let memory_ctx =
            MemoryTurnContext::new("collaboration-orchestrator", "collaboration-orchestrator");
        let kernel = MemoryKernel::new(Arc::clone(mem));

        let entry = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L4,
            category: MemoryCategory::Shared,
            priority: Priority::High,
            source: MemorySource::Import,
            title: format!("collaboration: {}", truncate_str(task_desc, 100)),
            content: synthesis.to_string(),
            embedding: None,
            tags: vec![
                "collaboration".to_string(),
                "team-shared".to_string(),
                "synthesis".to_string(),
            ],
            relations: vec![],
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: None,
            visibility: AgentVisibility::Shared,
        };

        if let Err(e) = kernel.remember(&memory_ctx, entry).await {
            tracing::warn!("failed to persist collaboration synthesis to L4: {e}");
        } else {
            tracing::info!("collaboration synthesis persisted to L4 shared memory");
        }
    }

    // ── run ────────────────────────────────────────────────────────────────

    /// End-to-end collaboration loop: decompose, assemble, dispatch,
    /// synthesize, finalize.
    pub async fn run(&self, task: &str, required_skills: &[String]) -> Option<String> {
        self.run_with_context(task, required_skills)
            .await
            .map(|result| result.synthesis)
    }

    pub async fn run_with_context(
        &self,
        task: &str,
        required_skills: &[String],
    ) -> Option<CollaborationContextResult> {
        // 1. Decompose
        let subtasks = self.decompose_sequential(task);
        let strategy = decide_strategy(&StrategyInput::from_prompt(task));
        let collaboration_decision =
            CollaborationTemplateMatcher::default().decide(task, &strategy);

        // 2. Assemble team
        let collab_task = CollaborationTask {
            description: task.to_string(),
            required_skills: required_skills.to_vec(),
            subtasks: subtasks.clone(),
            review_criteria: None,
            collaboration_decision: Some(collaboration_decision),
        };
        let team = self.assemble_team(&collab_task)?;

        // 3. Dispatch
        let results = self.dispatch_subtasks(&team, &subtasks).await;

        // 4. Synthesize
        let synthesis = self.synthesize(&results);
        let context_items = self.context_items_from_results(&team, &results);
        let board = self.build_shared_board(&results);
        let agent_tasks = agent_task_traces_from_results(&subtasks, &results, &board.board_id);
        let review_packet = board.review_packet(None, agent_tasks);
        let work_graph = AgentWorkGraph::from_collaboration_task("runtime-session", &collab_task)
            .with_review_packet(&review_packet);

        // 5. Finalize
        self.finalize(&synthesis, task).await;

        Some(CollaborationContextResult {
            synthesis,
            context_items,
            collaboration_task: collab_task,
            review_packet,
            work_graph,
        })
    }
}

impl<E: SubAgentExecutor + Default + 'static> Default for CollaborationOrchestrator<E> {
    fn default() -> Self {
        Self::new(Arc::new(E::default()))
    }
}

/// Factory: create a type-erased `Arc<dyn CollaborationOps>`.
///
/// Produces a boxed orchestrator that can be passed to
/// `ConversationRuntime::with_collaboration()` without propagating the
/// `E` type parameter.
pub fn new_boxed<E>(executor: Arc<E>) -> Arc<dyn CollaborationOps>
where
    E: SubAgentExecutor + 'static,
{
    Arc::new(CollaborationOrchestrator::<E>::new(executor))
}

// ── helpers ────────────────────────────────────────────────────────────────────

/// Split a task description into phases using delimiter keywords.
fn split_phases(task: &str) -> Vec<String> {
    let delimiters = [
        "Step 1", "Step 2", "Step 3", "Step 4", "Step 5", "Phase 1", "Phase 2", "Phase 3",
        "Phase 4", "First", "Next", "Then", "Finally",
    ];

    let mut phases: Vec<String> = Vec::new();
    // Try delimiter-based splitting first.
    let mut last = 0;
    let mut found = false;
    let task_lower = task.to_lowercase();
    for delim in &delimiters {
        let delim_lower = delim.to_lowercase();
        if let Some(pos) = task_lower[last..].find(&delim_lower) {
            let real_pos = last + pos;
            if real_pos > last {
                let segment = task[last..real_pos].trim().to_string();
                if !segment.is_empty() {
                    phases.push(segment);
                }
            }
            last = real_pos;
            found = true;
        }
    }
    if last < task.len() {
        let remainder = task[last..].trim().to_string();
        if !remainder.is_empty() {
            phases.push(remainder);
        }
    }

    if found && phases.len() > 1 {
        // Collapse very short segments (only when delimiter-based splitting succeeded).
        collapse_phases(&mut phases, 20);
    } else {
        // Fallback: split by sentences.
        phases = task
            .split(|c| c == '.' || c == '\n')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }

    phases
}

/// Merge short phases (< min_len chars) into adjacent ones.
fn collapse_phases(phases: &mut Vec<String>, min_len: usize) {
    if phases.len() <= 1 {
        return;
    }
    let mut merged: Vec<String> = Vec::new();
    let mut acc = String::new();
    for p in phases.drain(..) {
        if p.len() < min_len {
            if acc.is_empty() {
                acc = p;
            } else {
                acc.push_str("; ");
                acc.push_str(&p);
            }
        } else {
            if !acc.is_empty() {
                merged.push(std::mem::take(&mut acc));
            }
            merged.push(p);
        }
    }
    if !acc.is_empty() {
        merged.push(acc);
    }
    *phases = merged;
}

/// Infer required skills from a task description via keyword matching.
fn infer_skills(desc: &str) -> Vec<String> {
    let desc_lower = desc.to_lowercase();
    let mut skills: Vec<String> = Vec::new();

    let keyword_map: &[(&str, &[&str])] = &[
        ("rust", &["rust", "cargo", "borrow checker", "lifetime"]),
        (
            "testing",
            &["test", "assert", "mock", "coverage", "fixture"],
        ),
        (
            "refactoring",
            &["refactor", "extract", "rename", "restructure", "clean"],
        ),
        (
            "review",
            &["review", "audit", "inspect", "examine", "check"],
        ),
        (
            "documentation",
            &["document", "doc", "readme", "explain", "describe"],
        ),
        (
            "planning",
            &["plan", "design", "architect", "spec", "outline"],
        ),
        (
            "execution",
            &["execute", "run", "build", "compile", "deploy"],
        ),
        ("debugging", &["debug", "fix", "bug", "error", "crash"]),
        (
            "security",
            &["security", "vuln", "exploit", "injection", "xss"],
        ),
        (
            "performance",
            &["perf", "benchmark", "optimize", "slow", "latency"],
        ),
    ];

    for (skill, keywords) in keyword_map {
        for kw in *keywords {
            if desc_lower.contains(kw) {
                skills.push(skill.to_string());
                break;
            }
        }
    }

    if skills.is_empty() {
        skills.push("general".to_string());
    }
    skills
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len).collect();
        format!("{truncated}...")
    }
}

fn build_collaboration_board(results: &[SubAgentResult]) -> CollaborationBoard {
    let entries: Vec<SharedBoardEntry> = results
        .iter()
        .enumerate()
        .map(|(idx, result)| board_entry_from_result(idx, result))
        .collect();
    let scorecard = score_collaboration_board(&entries);

    CollaborationBoard {
        board_id: format!("board-{}", Uuid::new_v4()),
        entries,
        scorecard,
    }
}

fn board_entry_from_result(idx: usize, result: &SubAgentResult) -> SharedBoardEntry {
    let agent_id = format!("agent-{}", idx + 1);
    let mut conflicts = extract_prefixed_lines(
        &result.output,
        &["conflict:", "risk:", "blocked:", "disagreement:"],
    );

    if !result.completed_normally {
        conflicts.push(format!(
            "agent did not complete normally: {}",
            truncate_str(&result.output, 240)
        ));
    }

    SharedBoardEntry {
        agent_id,
        completed_normally: result.completed_normally,
        output_preview: truncate_str(&normalize_whitespace(&result.output), 320),
        decisions: extract_prefixed_lines(
            &result.output,
            &["decision:", "decided:", "conclusion:"],
        ),
        evidence: extract_prefixed_lines(
            &result.output,
            &["evidence:", "source:", "verified:", "test:"],
        ),
        conflicts,
        memory_pulses: extract_memory_pulses(&result.output),
        next_actions: extract_prefixed_lines(&result.output, &["next:", "todo:", "action:"]),
    }
}

fn score_collaboration_board(entries: &[SharedBoardEntry]) -> CollaborationScorecard {
    if entries.is_empty() {
        return CollaborationScorecard {
            completion_rate: 0.0,
            synthesis_lift: 0.0,
            complementarity_score: 0.0,
            active_memory_score: 0.0,
            conflict_count: 0,
            memory_pulse_count: 0,
            surfaced_conflicts: Vec::new(),
        };
    }

    let completed = entries
        .iter()
        .filter(|entry| entry.completed_normally)
        .count();
    let completion_rate = completed as f32 / entries.len() as f32;

    let mut all_signals = BTreeSet::new();
    let mut strongest_single_agent = 0usize;
    let mut memory_pulse_count = 0usize;
    let mut surfaced_conflicts = Vec::new();
    let mut agents_with_memory_pulse = 0usize;

    for entry in entries {
        let signals = entry_signal_set(entry);
        strongest_single_agent = strongest_single_agent.max(signals.len());
        all_signals.extend(signals);

        let pulse_count = entry.memory_pulses.len();
        memory_pulse_count += pulse_count;
        if pulse_count > 0 {
            agents_with_memory_pulse += 1;
        }

        surfaced_conflicts.extend(
            entry
                .conflicts
                .iter()
                .map(|conflict| format!("{}: {}", entry.agent_id, conflict)),
        );
    }

    let synthesis_lift = if strongest_single_agent == 0 {
        0.0
    } else {
        all_signals.len() as f32 / strongest_single_agent as f32
    };
    let complementarity_score = if all_signals.is_empty() {
        0.0
    } else {
        (all_signals.len().saturating_sub(strongest_single_agent)) as f32 / all_signals.len() as f32
    };
    let active_memory_score = agents_with_memory_pulse as f32 / entries.len() as f32;

    CollaborationScorecard {
        completion_rate,
        synthesis_lift,
        complementarity_score,
        active_memory_score,
        conflict_count: surfaced_conflicts.len(),
        memory_pulse_count,
        surfaced_conflicts,
    }
}

fn entry_signal_set(entry: &SharedBoardEntry) -> BTreeSet<String> {
    let mut signals = BTreeSet::new();
    add_signals(&mut signals, "decision", &entry.decisions);
    add_signals(&mut signals, "evidence", &entry.evidence);
    add_signals(&mut signals, "next", &entry.next_actions);

    for pulse in &entry.memory_pulses {
        let key = format!(
            "memory:{:?}:{}",
            pulse.kind,
            normalize_for_scoring(&pulse.content)
        );
        signals.insert(key);
    }

    signals
}

fn add_signals(signals: &mut BTreeSet<String>, kind: &str, values: &[String]) {
    for value in values {
        let normalized = normalize_for_scoring(value);
        if !normalized.is_empty() {
            signals.insert(format!("{kind}:{normalized}"));
        }
    }
}

fn extract_prefixed_lines(text: &str, prefixes: &[&str]) -> Vec<String> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let lower = trimmed.to_ascii_lowercase();
            prefixes.iter().find_map(|prefix| {
                lower
                    .strip_prefix(prefix)
                    .map(|_| trimmed[prefix.len()..].trim().to_string())
            })
        })
        .filter(|line| !line.is_empty())
        .collect()
}

fn extract_memory_pulses(text: &str) -> Vec<MemoryPulseCandidate> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let lower = trimmed.to_ascii_lowercase();
            let (kind, prefix) = [
                (MemoryPulseKind::Remember, "memory:"),
                (MemoryPulseKind::Remember, "remember:"),
                (MemoryPulseKind::Refresh, "refresh:"),
                (MemoryPulseKind::Refresh, "stale:"),
                (MemoryPulseKind::Promote, "promote:"),
                (MemoryPulseKind::Retire, "forget:"),
                (MemoryPulseKind::Retire, "retire:"),
            ]
            .into_iter()
            .find(|(_, prefix)| lower.starts_with(prefix))?;

            let content = trimmed[prefix.len()..].trim();
            if content.is_empty() {
                None
            } else {
                Some(MemoryPulseCandidate {
                    kind,
                    content: content.to_string(),
                })
            }
        })
        .collect()
}

fn collaboration_maintenance_candidate(
    kind: MaintenanceCandidateKind,
    summary: String,
    reason: String,
    confidence: f32,
    board_id: &str,
) -> MaintenanceCandidate {
    let now = Utc::now();
    MaintenanceCandidate {
        id: Uuid::new_v4().to_string(),
        kind,
        status: MaintenanceCandidateStatus::Open,
        entry_ids: Vec::new(),
        summary,
        reason: format!("multi-agent collaboration pulse: {reason}"),
        confidence: confidence.clamp(0.0, 1.0),
        source: Some("collaboration_board".to_string()),
        source_ref: Some(board_id.to_string()),
        created_at: now,
        updated_at: now,
    }
}

fn truncate_for_candidate(text: &str) -> String {
    const LIMIT: usize = 96;
    let normalized = normalize_whitespace(text);
    if normalized.chars().count() <= LIMIT {
        return normalized;
    }
    let mut truncated = normalized.chars().take(LIMIT).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn normalize_for_scoring(text: &str) -> String {
    normalize_whitespace(text)
        .to_ascii_lowercase()
        .trim_matches(|c: char| c.is_ascii_punctuation())
        .to_string()
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn agent_task_traces_from_results(
    subtasks: &[SubTask],
    results: &[SubAgentResult],
    board_id: &str,
) -> Vec<AgentTaskTrace> {
    let now = Utc::now().timestamp_millis().max(0) as u64;
    results
        .iter()
        .enumerate()
        .map(|(idx, result)| {
            let subtask = subtasks.get(idx);
            let task_id = subtask
                .map(|task| task.id.clone())
                .unwrap_or_else(|| format!("agent-task-{}", idx + 1));
            AgentTaskTrace {
                task_id,
                parent_run_id: None,
                agent_run_id: None,
                role: subtask
                    .and_then(|task| task.required_skills.first().cloned())
                    .unwrap_or_else(|| "agent".to_string()),
                objective: subtask
                    .map(|task| task.description.clone())
                    .unwrap_or_else(|| "agent collaboration task".to_string()),
                status: if result.completed_normally {
                    "completed".to_string()
                } else {
                    "failed".to_string()
                },
                context_envelope_id: None,
                result_summary: truncate_for_candidate(&result.output),
                evidence_refs: extract_prefixed_lines(&result.output, &["Evidence:", "evidence:"]),
                collaboration_board_id: board_id.to_string(),
                confidence: if result.completed_normally {
                    0.75
                } else {
                    0.25
                },
                conflicts: extract_prefixed_lines(&result.output, &["Conflict:", "conflict:"]),
                created_at_ms: now,
                updated_at_ms: now,
            }
        })
        .collect()
}

// ── CollaborationOps ────────────────────────────────────────────────────────────

use futures::Future;
use std::pin::Pin;

/// Type-erased handle for CollaborationOrchestrator.
/// Enables storage without generic parameter propagation.
pub trait CollaborationOps: Send + Sync {
    fn run_boxed<'a>(
        &'a self,
        task: &'a str,
        skills: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Option<String>> + 'a>>;
    fn run_with_context_boxed<'a>(
        &'a self,
        task: &'a str,
        skills: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Option<CollaborationContextResult>> + 'a>>;
    fn decompose_task(&self, task: &str) -> Vec<SubTask>;
    fn assemble_team(&self, task: &CollaborationTask) -> Option<AgentTeam>;
}

impl<E: SubAgentExecutor + 'static> CollaborationOps for CollaborationOrchestrator<E> {
    fn run_boxed<'a>(
        &'a self,
        task: &'a str,
        skills: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Option<String>> + 'a>> {
        Box::pin(self.run(task, skills))
    }
    fn run_with_context_boxed<'a>(
        &'a self,
        task: &'a str,
        skills: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Option<CollaborationContextResult>> + 'a>> {
        Box::pin(self.run_with_context(task, skills))
    }
    fn decompose_task(&self, task: &str) -> Vec<SubTask> {
        self.decompose_task(task)
    }
    fn assemble_team(&self, task: &CollaborationTask) -> Option<AgentTeam> {
        self.assemble_team(task)
    }
}

// ── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use memory::agent_directory::AgentStatus;

    // Minimal executor that always succeeds.
    #[derive(Default)]
    pub(crate) struct DummyExecutor;

    impl SubAgentExecutor for DummyExecutor {
        fn execute(
            &self,
            _config: SubAgentConfig,
            task: &str,
        ) -> impl std::future::Future<Output = Result<SubAgentResult, crate::agent::SubAgentError>>
        {
            let task = task.to_string();
            async move {
                Ok(SubAgentResult {
                    output: format!("dummy output for: {task}"),
                    completed_normally: true,
                    ..SubAgentResult::default()
                })
            }
        }
    }

    #[test]
    fn decompose_splits_by_delimiters() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let input = "Step 1: analyze code. Step 2: refactor module. Step 3: write tests.";
        let subtasks = orch.decompose_task(input);
        assert!(
            subtasks.len() >= 2,
            "expected >= 2 subtasks, got {}",
            subtasks.len()
        );
        for st in &subtasks {
            assert!(!st.description.is_empty());
            assert!(!st.required_skills.is_empty());
        }
    }

    #[test]
    fn decompose_falls_back_to_sentences() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let input = "Analyze the code. Refactor the module. Write tests.";
        let subtasks = orch.decompose_task(input);
        assert!(subtasks.len() >= 2);
    }

    #[test]
    fn decompose_sequential_chains_dependencies() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let subtasks = orch.decompose_sequential("Step 1: plan. Step 2: execute.");
        assert!(subtasks.len() >= 1);
        for (i, st) in subtasks.iter().enumerate() {
            if i > 0 {
                let expected_dep = format!("subtask-{i}");
                assert!(
                    st.depends_on.contains(&expected_dep),
                    "subtask {i} should depend on {expected_dep}"
                );
            }
        }
    }

    #[test]
    fn assemble_team_returns_none_for_unknown_skills() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let task = CollaborationTask {
            description: "do something obscure".to_string(),
            required_skills: vec!["quantum-xeno-linguistics".to_string()],
            subtasks: vec![],
            review_criteria: None,
            collaboration_decision: None,
        };
        assert!(orch.assemble_team(&task).is_none());
    }

    #[tokio::test]
    async fn dispatch_subtasks_empty_input() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let team = AgentTeam {
            leader: dummy_agent_info("lead", vec![]),
            workers: vec![],
        };
        let results = orch.dispatch_subtasks(&team, &[]).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn dispatch_subtasks_single_subtask() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let team = AgentTeam {
            leader: dummy_agent_info("lead", vec![]),
            workers: vec![],
        };
        let subtasks = vec![SubTask {
            id: "s1".to_string(),
            description: "analyze".to_string(),
            required_skills: vec!["planning".to_string()],
            depends_on: vec![],
        }];
        let results = orch.dispatch_subtasks(&team, &subtasks).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].completed_normally);
    }

    #[test]
    fn context_items_from_results_returns_agent_peer_items() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let team = AgentTeam {
            leader: dummy_agent_info("lead", vec![]),
            workers: vec![dummy_agent_info("reviewer", vec!["testing".to_string()])],
        };
        let results = vec![SubAgentResult {
            output: "Decision: add regression test".to_string(),
            ..SubAgentResult::default()
        }];
        let context_items = orch.context_items_from_results(&team, &results);

        assert_eq!(context_items.len(), 1);
        assert_eq!(
            context_items[0].source,
            crate::context_runtime::ContextSourceKind::AgentPeer
        );
        assert_eq!(
            context_items[0].authority,
            crate::context_runtime::ContextAuthority::Agent
        );
        assert!(context_items[0].content.contains("reviewer"));
    }

    #[test]
    fn synthesize_handles_empty_results() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let output = orch.synthesize(&[]);
        assert!(output.contains("Sub-agents completed"));
    }

    #[test]
    fn synthesize_detects_conflicts() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let results = vec![
            SubAgentResult {
                output: "alpha".to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
            SubAgentResult {
                output: "beta".to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
        ];
        let output = orch.synthesize(&results);
        assert!(output.contains("Sub-agents completed"));
        assert!(output.contains("Agent Results"));
    }

    #[test]
    fn shared_board_extracts_conflicts_and_memory_pulses() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let results = vec![
            SubAgentResult {
                output: "\
Decision: keep runtime scoring pure
Evidence: unit test covers board extraction
Memory: runtime shared board produced a useful signal
Next: wire scorecard into synthesis"
                    .to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
            SubAgentResult {
                output: "\
Decision: surface conflicts before final synthesis
Conflict: scoring disagrees with missing evidence
Refresh: collaboration scoring should be revisited after live agent runs"
                    .to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
        ];

        let board = orch.build_shared_board(&results);

        assert!(board.board_id.starts_with("board-"));
        assert_eq!(board.entries.len(), 2);
        assert_eq!(board.entries[0].decisions.len(), 1);
        assert_eq!(board.entries[0].memory_pulses.len(), 1);
        assert_eq!(
            board.entries[0].memory_pulses[0].kind,
            MemoryPulseKind::Remember
        );
        assert_eq!(board.scorecard.conflict_count, 1);
        assert_eq!(board.scorecard.memory_pulse_count, 2);
        assert!(board.scorecard.needs_memory_pulse());
    }

    #[test]
    fn collaboration_board_exports_reviewable_memory_maintenance_candidates() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let results = vec![
            SubAgentResult {
                output: "\
Conflict: implementation confidence differs from review evidence
Promote: context stable head policy is verified by runtime tests
Retire: stale local jsonl resume assumptions"
                    .to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
            SubAgentResult {
                output: "Refresh: memory pulse candidates should feed the review queue".to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
        ];

        let board = orch.build_shared_board(&results);
        let candidates = board.memory_maintenance_candidates();

        assert_eq!(candidates.len(), 4);
        assert!(candidates
            .iter()
            .any(|candidate| candidate.kind == MaintenanceCandidateKind::Conflict));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.kind == MaintenanceCandidateKind::AuthorityPromotion));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.kind == MaintenanceCandidateKind::Stale));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.kind == MaintenanceCandidateKind::RelationshipRefresh));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.status == MaintenanceCandidateStatus::Open));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.entry_ids.is_empty()));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.source.as_deref() == Some("collaboration_board")));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.source_ref.as_deref() == Some(board.board_id.as_str())));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.reason.contains("multi-agent collaboration pulse")));
    }

    #[test]
    fn collaboration_review_packet_binds_agent_tasks_and_memory_candidates() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let results = vec![SubAgentResult {
            output: "\
Decision: use bounded context policy
Evidence: runtime probe pressure stayed low
Conflict: agent evidence needs review
Promote: stable head policy verified by tests"
                .to_string(),
            completed_normally: true,
            ..SubAgentResult::default()
        }];

        let board = orch.build_shared_board(&results);
        let task = AgentTaskTrace {
            task_id: "task-1".to_string(),
            parent_run_id: Some("run-parent".to_string()),
            agent_run_id: Some("run-agent-1".to_string()),
            role: "reviewer".to_string(),
            objective: "review context policy".to_string(),
            status: "completed".to_string(),
            context_envelope_id: Some("ctx-1".to_string()),
            result_summary: "bounded policy reviewed".to_string(),
            evidence_refs: vec!["test:runtime_probe".to_string()],
            collaboration_board_id: board.board_id.clone(),
            confidence: 0.82,
            conflicts: board.scorecard.surfaced_conflicts.clone(),
            created_at_ms: 10,
            updated_at_ms: 20,
        };

        let packet = board.review_packet(Some("run-parent".to_string()), vec![task]);

        assert_eq!(packet.board_id, board.board_id);
        assert_eq!(packet.parent_run_id.as_deref(), Some("run-parent"));
        assert_eq!(packet.agent_tasks.len(), 1);
        assert_eq!(
            packet.agent_tasks[0].collaboration_board_id,
            packet.board_id
        );
        assert!(!packet.maintenance_candidates.is_empty());
        assert!(packet.maintenance_candidates.iter().all(|candidate| {
            candidate.source_ref.as_deref() == Some(packet.board_id.as_str())
        }));
    }

    #[test]
    fn scorecard_value_verdict_detects_positive_lift() {
        let scorecard = CollaborationScorecard {
            completion_rate: 1.0,
            synthesis_lift: 1.25,
            complementarity_score: 0.7,
            active_memory_score: 0.5,
            conflict_count: 0,
            memory_pulse_count: 1,
            surfaced_conflicts: Vec::new(),
        };

        let verdict = scorecard.value_verdict();
        assert!(verdict.positive_lift);
        assert!(verdict.continue_multi_agent);
        assert!(verdict.value_score >= 50);
        assert_eq!(verdict.reasons, vec!["positive_multi_agent_lift"]);
    }

    #[test]
    fn scorecard_value_verdict_detects_non_lift_and_conflict() {
        let scorecard = CollaborationScorecard {
            completion_rate: 0.5,
            synthesis_lift: 1.0,
            complementarity_score: 0.0,
            active_memory_score: 0.0,
            conflict_count: 4,
            memory_pulse_count: 0,
            surfaced_conflicts: vec!["agents disagree".to_string()],
        };

        let verdict = scorecard.value_verdict();
        assert!(!verdict.positive_lift);
        assert!(!verdict.continue_multi_agent);
        assert!(verdict.reasons.contains(&"low_completion_rate".to_string()));
        assert!(verdict.reasons.contains(&"no_synthesis_lift".to_string()));
        assert!(verdict.reasons.contains(&"excessive_conflict".to_string()));
    }

    #[test]
    fn scorecard_detects_multi_agent_lift() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let results = vec![
            SubAgentResult {
                output: "\
Decision: add deterministic scorecard
Evidence: board unit test"
                    .to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
            SubAgentResult {
                output: "\
Decision: expose memory pulse candidates
Evidence: synthesis includes live memory section"
                    .to_string(),
                completed_normally: true,
                ..SubAgentResult::default()
            },
        ];

        let board = orch.build_shared_board(&results);

        assert_eq!(board.scorecard.completion_rate, 1.0);
        assert!(
            board.scorecard.synthesis_lift > 1.0,
            "expected complementary agent outputs to score above single-agent baseline"
        );
        assert!(board.scorecard.complementarity_score > 0.0);
        assert!(board.scorecard.shows_multi_agent_lift());
    }

    #[test]
    fn synthesize_includes_shared_board_scorecard() {
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));
        let results = vec![SubAgentResult {
            output: "\
Decision: keep the spike lightweight
Memory: lightweight spike has a live-memory pulse candidate"
                .to_string(),
            completed_normally: true,
            ..SubAgentResult::default()
        }];

        let output = orch.synthesize(&results);

        assert!(output.contains("Shared Board Scorecard"));
        assert!(output.contains("Synthesis lift"));
        assert!(output.contains("Memory Pulse Candidates"));
    }

    #[test]
    fn infer_skills_detects_rust() {
        let skills = infer_skills("refactor the Rust module and write tests");
        assert!(skills.contains(&"rust".to_string()));
        assert!(skills.contains(&"refactoring".to_string()));
        assert!(skills.contains(&"testing".to_string()));
    }

    #[test]
    fn infer_skills_falls_back_to_general() {
        let skills = infer_skills("do something completely unrelated");
        assert_eq!(skills, vec!["general".to_string()]);
    }

    #[tokio::test]
    async fn run_with_context_attaches_collaboration_template_decision() {
        AgentDirectory::global().register(dummy_agent_info(
            "template-contract-worker",
            vec!["rust".to_string(), "testing".to_string()],
        ));
        let orch = CollaborationOrchestrator::<DummyExecutor>::new(Arc::new(DummyExecutor));

        let result = orch
            .run_with_context(
                "implement a runtime refactor then compile and test",
                &["rust".to_string(), "testing".to_string()],
            )
            .await
            .expect("collaboration result");
        let decision = result
            .collaboration_task
            .collaboration_decision
            .expect("collaboration decision");

        assert_eq!(
            decision.template_id,
            crate::collaboration_template::CollaborationTemplateId::ImplementationReviewFix
        );
        assert!(decision.plan.review_contract.contains("mandatory"));
        assert!(decision.plan.budget_policy.max_parallel_agents >= 2);
    }

    fn dummy_agent_info(id: &str, capabilities: Vec<String>) -> AgentInfo {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        AgentInfo {
            agent_id: id.to_string(),
            role: "Executor".to_string(),
            capabilities,
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: None,
        }
    }
}
