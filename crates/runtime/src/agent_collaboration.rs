//! Collaborative agent orchestration: team assembly, task decomposition,
//! wave-based parallel dispatch, result synthesis with fact-checking, and
//! L4 team memory finalization.

use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::agent::{SubAgentConfig, SubAgentExecutor, SubAgentResult};
use crate::context_runtime::ContextItem;
use crate::team_discovery::TeamDiscoveryProtocol;
use crate::wave::{TaskId, WaveConfig, WaveOrchestrator, WaveTask};

use memory::agent_directory::{AgentDirectory, AgentInfo};
use memory::fact_checker::{FactCheckResult, FactChecker};
use memory::project_scope::MemoryScope;
use memory::temporal_graph::Triple;
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::{MemoryKernel, MemoryTurnContext};

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
}

#[derive(Debug, Clone)]
pub struct CollaborationContextResult {
    pub synthesis: String,
    pub context_items: Vec<ContextItem>,
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

    /// Merge sub-agent results into a coherent synthesis, detecting and
    /// flagging conflicts via `FactChecker`.
    pub fn synthesize(&self, results: &[SubAgentResult]) -> String {
        let mut output = String::from("## Collaboration Synthesis\n\n");

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

        // 2. Assemble team
        let collab_task = CollaborationTask {
            description: task.to_string(),
            required_skills: required_skills.to_vec(),
            subtasks: subtasks.clone(),
            review_criteria: None,
        };
        let team = self.assemble_team(&collab_task)?;

        // 3. Dispatch
        let results = self.dispatch_subtasks(&team, &subtasks).await;

        // 4. Synthesize
        let synthesis = self.synthesize(&results);
        let context_items = self.context_items_from_results(&team, &results);

        // 5. Finalize
        self.finalize(&synthesis, task).await;

        Some(CollaborationContextResult {
            synthesis,
            context_items,
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
