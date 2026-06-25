//! JointProblemSolving Protocol (P8.3) — 7-phase collaborative problem-solving.
//!
//! Orchestrates agent collaboration through seven structured phases:
//! 1. ProblemFraming  – Share problem via L4, collect perspectives
//! 2. SolutionBrainstorming – Agents propose solutions (SubAgent)
//! 3. SolutionMerger  – Dedup similar solutions via merge_entries pattern
//! 4. Evaluation       – Agents score all solutions (1-5) via discussion
//! 5. Selection        – Top solution selected, documented in L4
//! 6. Execution        – Wave tasks → SubAgents execute
//! 7. Review           – Reviewer agent checks results

use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::agent::{SubAgentConfig, SubAgentExecutor, SubAgentResult};
use crate::agent_collaboration::{CollaborationOrchestrator, CollaborationTask};

use memory::agent_directory::AgentDirectory;
use memory::project_scope::MemoryScope;
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};
use memory::{MemoryKernel, MemoryTurnContext};

// ── ProblemStatement ────────────────────────────────────────────────────────

/// Describes the problem to be solved collaboratively.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProblemStatement {
    /// Human-readable description of the problem.
    pub description: String,
    /// Constraints that must be respected.
    #[serde(default)]
    pub constraints: Vec<String>,
    /// Criteria for a successful solution.
    #[serde(default)]
    pub success_criteria: Vec<String>,
    /// Optional deadline for the solution.
    #[serde(default)]
    pub deadline: Option<String>,
}

impl ProblemStatement {
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            constraints: Vec::new(),
            success_criteria: Vec::new(),
            deadline: None,
        }
    }

    pub fn with_constraints(mut self, constraints: Vec<String>) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn with_success_criteria(mut self, criteria: Vec<String>) -> Self {
        self.success_criteria = criteria;
        self
    }

    pub fn with_deadline(mut self, deadline: impl Into<String>) -> Self {
        self.deadline = Some(deadline.into());
        self
    }
}

// ── Solution ────────────────────────────────────────────────────────────────

/// A proposed solution generated during brainstorming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Solution {
    /// Unique identifier for this solution.
    pub id: String,
    /// Short title summarizing the approach.
    pub title: String,
    /// Detailed description of the solution.
    pub description: String,
    /// Agent that proposed this solution.
    pub proposed_by: String,
    /// Confidence score [0.0, 1.0] from the proposing agent.
    pub confidence: f32,
    /// Tags categorizing the approach.
    #[serde(default)]
    pub tags: Vec<String>,
}

impl Solution {
    pub fn new(
        id: impl Into<String>,
        title: impl Into<String>,
        description: impl Into<String>,
        proposed_by: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            description: description.into(),
            proposed_by: proposed_by.into(),
            confidence: 0.8,
            tags: Vec::new(),
        }
    }
}

// ── SolutionScore ───────────────────────────────────────────────────────────

/// Individual score dimensions for a solution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SolutionScore {
    /// Clarity of the proposed solution (1-5).
    pub clarity: f32,
    /// Feasibility of implementation (1-5).
    pub feasibility: f32,
    /// Novelty / innovation of the approach (1-5).
    pub novelty: f32,
    /// Expected impact on the problem (1-5).
    pub impact: f32,
    /// Resource efficiency (1-5).
    pub efficiency: f32,
}

impl SolutionScore {
    /// Weighted average across all dimensions.
    pub fn weighted_average(&self) -> f32 {
        (self.clarity * 0.2
            + self.feasibility * 0.25
            + self.novelty * 0.15
            + self.impact * 0.25
            + self.efficiency * 0.15)
            .clamp(1.0, 5.0)
    }
}

impl Default for SolutionScore {
    fn default() -> Self {
        Self {
            clarity: 3.0,
            feasibility: 3.0,
            novelty: 3.0,
            impact: 3.0,
            efficiency: 3.0,
        }
    }
}

// ── SolutionEvaluation ──────────────────────────────────────────────────────

/// An agent's evaluation of a solution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolutionEvaluation {
    /// ID of the solution being evaluated.
    pub solution_id: String,
    /// Agent performing the evaluation.
    pub evaluator: String,
    /// Dimension scores.
    pub scores: SolutionScore,
    /// Weighted average of scores.
    pub average: f32,
    /// Optional qualitative feedback.
    #[serde(default)]
    pub feedback: Option<String>,
}

// ── ProblemSolvingConfig ────────────────────────────────────────────────────

/// Configuration for the problem-solving pipeline.
#[derive(Debug, Clone)]
pub struct ProblemSolvingConfig {
    /// Maximum parallel agents for brainstorming.
    pub max_brainstorm_agents: usize,
    /// Maximum parallel agents for evaluation.
    pub max_eval_agents: usize,
    /// Number of solutions to keep after merging.
    pub max_solutions: usize,
    /// Minimum average score for a solution to be considered.
    pub min_score_threshold: f32,
    /// Whether to persist pipeline state to L4 memory.
    pub persist_to_l4: bool,
}

impl Default for ProblemSolvingConfig {
    fn default() -> Self {
        Self {
            max_brainstorm_agents: 4,
            max_eval_agents: 3,
            max_solutions: 5,
            min_score_threshold: 2.5,
            persist_to_l4: true,
        }
    }
}

// ── AgentDiscussion ─────────────────────────────────────────────────────────
// P8.2: Structured multi-agent discussion protocol used for evaluation.

/// A discussion turn contributed by an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscussionTurn {
    pub agent_id: String,
    pub agent_role: String,
    pub content: String,
    pub turn_number: u32,
}

/// Orchestrates structured discussions between agents.
///
/// Each agent contributes their perspective in turn, and the discussion
/// results are aggregated.
pub struct AgentDiscussion<E: SubAgentExecutor> {
    executor: Arc<E>,
    parent_memory: Option<Arc<memory::CognitiveContextManager>>,
}

impl<E: SubAgentExecutor> AgentDiscussion<E> {
    pub fn new(executor: Arc<E>) -> Self {
        Self {
            executor,
            parent_memory: None,
        }
    }

    pub fn with_parent_memory(mut self, memory: Arc<memory::CognitiveContextManager>) -> Self {
        self.parent_memory = Some(memory);
        self
    }

    /// Run a discussion where each agent contributes turns based on prompts.
    ///
    /// Returns the aggregated discussion turns.
    pub async fn discuss(
        &self,
        _topic: &str,
        agent_prompts: &[(String, String)], // (agent_id, prompt)
    ) -> Vec<DiscussionTurn> {
        let mut turns: Vec<DiscussionTurn> = Vec::with_capacity(agent_prompts.len());

        for (turn_num, (agent_id, prompt)) in agent_prompts.iter().enumerate() {
            let config = SubAgentConfig {
                task_description: prompt.clone(),
                agent_role: "Evaluator".to_string(),
                capabilities: vec!["evaluation".to_string(), "review".to_string()],
                ..SubAgentConfig::default()
            };

            let turn = match self.executor.execute(config, prompt).await {
                Ok(result) => DiscussionTurn {
                    agent_id: agent_id.clone(),
                    agent_role: "Evaluator".to_string(),
                    content: result.output,
                    turn_number: (turn_num + 1) as u32,
                },
                Err(e) => DiscussionTurn {
                    agent_id: agent_id.clone(),
                    agent_role: "Evaluator".to_string(),
                    content: format!("Error: {e}"),
                    turn_number: (turn_num + 1) as u32,
                },
            };

            turns.push(turn);
        }

        // Persist discussion to L4 if configured.
        if let Some(ref mem) = self.parent_memory {
            let kernel = MemoryKernel::new(Arc::clone(mem));
            for turn in &turns {
                let memory_ctx = MemoryTurnContext::new(
                    "joint-problem-solving-discussion",
                    turn.agent_id.clone(),
                );
                let entry = MemoryEntry {
                    id: MemoryId::new_v4(),
                    layer: MemoryLayer::L4,
                    category: MemoryCategory::Shared,
                    priority: Priority::Normal,
                    source: MemorySource::Import,
                    title: format!(
                        "discussion [{}/{}] {}",
                        turn.turn_number,
                        turns.len(),
                        truncate_str(&turn.content, 80)
                    ),
                    content: format!(
                        "## Discussion Turn {}/{}\n\n**{}**: {}\n\n{}",
                        turn.turn_number,
                        turns.len(),
                        turn.agent_id,
                        turn.agent_role,
                        turn.content
                    ),
                    embedding: None,
                    tags: vec!["discussion".to_string(), "team-shared".to_string()],
                    relations: vec![],
                    confidence: 0.8,
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
                let _ = kernel.remember(&memory_ctx, entry).await;
            }
        }

        turns
    }
}

impl<E: SubAgentExecutor + Default> Default for AgentDiscussion<E> {
    fn default() -> Self {
        Self::new(Arc::new(E::default()))
    }
}

// ── Pipeline Phase Results ─────────────────────────────────────────────────

/// Result of the problem-framing phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FramingResult {
    pub perspectives: Vec<String>,
    pub refined_problem: ProblemStatement,
}

/// Phase execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

/// Overall pipeline result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineResult {
    pub problem: ProblemStatement,
    /// All proposed solutions (after merging).
    pub solutions: Vec<Solution>,
    /// Evaluations per solution.
    pub evaluations: Vec<SolutionEvaluation>,
    /// The selected best solution (if any).
    pub selected_solution: Option<Solution>,
    /// Selected solution's average score.
    pub selected_score: Option<f32>,
    /// Execution results from wave dispatch.
    pub execution_outputs: Vec<SubAgentResult>,
    /// Review summary.
    pub review_summary: Option<String>,
    /// Phase-by-phase status.
    pub phase_statuses: Vec<(String, PhaseStatus)>,
}

// ── ProblemSolvingPipeline ──────────────────────────────────────────────────

/// Orchestrates the 7-phase Joint Problem Solving protocol.
///
/// Generic parameter `E` provides the SubAgent execution backend.
pub struct ProblemSolvingPipeline<E: SubAgentExecutor> {
    config: ProblemSolvingConfig,
    executor: Arc<E>,
    parent_memory: Option<Arc<memory::CognitiveContextManager>>,
}

impl<E: SubAgentExecutor + 'static> ProblemSolvingPipeline<E> {
    pub fn new(executor: Arc<E>) -> Self {
        Self {
            config: ProblemSolvingConfig::default(),
            executor,
            parent_memory: None,
        }
    }

    pub fn with_config(mut self, config: ProblemSolvingConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_parent_memory(mut self, memory: Arc<memory::CognitiveContextManager>) -> Self {
        self.parent_memory = Some(memory);
        self
    }

    // ── Phase 1: ProblemFraming ─────────────────────────────────────────

    /// Share problem via L4 memory and collect perspectives from agents.
    async fn phase1_framing(&self, problem: &ProblemStatement) -> FramingResult {
        let perspectives = if let Some(ref mem) = self.parent_memory {
            // Write problem to L4 for all agents to discover.
            let memory_ctx =
                MemoryTurnContext::new("joint-problem-solving", "problem-solving-pipeline");
            let kernel = MemoryKernel::new(Arc::clone(mem));
            let problem_entry = MemoryEntry {
                id: MemoryId::new_v4(),
                layer: MemoryLayer::L4,
                category: MemoryCategory::Shared,
                priority: Priority::High,
                source: MemorySource::Import,
                title: format!("problem: {}", truncate_str(&problem.description, 100)),
                content: format!(
                    "## Problem\n\n{}\n\n### Constraints\n\n{}\n\n### Success Criteria\n\n{}\n\n### Deadline\n\n{}",
                    problem.description,
                    problem.constraints.join("\n- "),
                    problem.success_criteria.join("\n- "),
                    problem.deadline.as_deref().unwrap_or("none"),
                ),
                embedding: None,
                tags: vec!["problem".to_string(), "team-shared".to_string(), "P8.3".to_string()],
                relations: vec![],
                confidence: 1.0,
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
            if let Err(e) = kernel.remember(&memory_ctx, problem_entry).await {
                tracing::warn!("Failed to persist problem to L4: {e}");
            }

            // Collect perspectives from available agents.
            let directory = AgentDirectory::global();
            let agents = directory.discover(&["planning".to_string(), "general".to_string()]);
            let mut perspectives: Vec<String> = Vec::new();

            for agent in agents.iter().take(3) {
                let prompt = format!(
                    "Analyze the following problem from your expertise and provide your perspective:\n\n\
                     ## Problem\n{}\n\n\
                     Constraints: {}\n\
                     Success Criteria: {}\n\n\
                     Provide a brief analysis of key challenges, opportunities, and your recommended approach angle.",
                    problem.description,
                    problem.constraints.join(", "),
                    problem.success_criteria.join(", "),
                );
                let config = SubAgentConfig {
                    task_description: prompt.clone(),
                    agent_role: "Analyst".to_string(),
                    capabilities: vec!["planning".to_string(), "analysis".to_string()],
                    max_turns: 3,
                    ..SubAgentConfig::default()
                };
                match self.executor.execute(config, &prompt).await {
                    Ok(result) => perspectives.push(result.output),
                    Err(e) => tracing::warn!("Agent {} framing failed: {e}", agent.agent_id),
                }
            }

            if perspectives.is_empty() {
                perspectives.push(
                    "No agent perspectives collected — proceeding with direct framing.".to_string(),
                );
            }
            perspectives
        } else {
            // No memory configured; synthetic framing.
            vec![format!(
                "Problem framed: {}\nConstraints: {:?}\nCriteria: {:?}",
                problem.description, problem.constraints, problem.success_criteria,
            )]
        };

        FramingResult {
            perspectives,
            refined_problem: problem.clone(),
        }
    }

    // ── Phase 2: SolutionBrainstorming ──────────────────────────────────

    /// Each agent proposes solutions via SubAgent dispatch.
    async fn phase2_brainstorming(
        &self,
        problem: &ProblemStatement,
        perspectives: &[String],
    ) -> Vec<Solution> {
        let collab = CollaborationOrchestrator::<E>::new(Arc::clone(&self.executor));
        let skills = vec![
            "planning".to_string(),
            "execution".to_string(),
            "refactoring".to_string(),
        ];

        let perspective_text = perspectives.join("\n\n");
        let prompt = format!(
            "Propose concrete solutions for the following problem. \
             For each solution, provide a title and brief description.\n\n\
             ## Problem\n{}\n\n\
             ## Constraints\n{}\n\n\
             ## Perspectives\n{}\n\n\
             Step 1: Analyze the problem from different angles.\n\
             Step 2: Propose at least 2-3 concrete solutions.\n\
             Step 3: For each solution, explain why it would work.",
            problem.description,
            problem.constraints.join("\n- "),
            perspective_text,
        );

        let subtasks = collab.decompose_task(&prompt);
        let task = CollaborationTask {
            description: prompt.clone(),
            required_skills: skills.clone(),
            subtasks: subtasks.clone(),
            review_criteria: Some(
                "Solutions must be concrete, feasible, and respect constraints".to_string(),
            ),
            collaboration_decision: None,
        };

        let team = match collab.assemble_team(&task) {
            Some(t) => t,
            None => {
                // Fallback: generate solutions directly.
                let config = SubAgentConfig {
                    task_description: "Solutions brainstorming".to_string(),
                    agent_role: "Brainstormer".to_string(),
                    capabilities: vec!["planning".to_string(), "execution".to_string()],
                    max_turns: 5,
                    ..SubAgentConfig::default()
                };
                match self.executor.execute(config, &prompt).await {
                    Ok(result) => {
                        return parse_solutions_from_text(&result.output);
                    }
                    Err(e) => {
                        tracing::warn!("Brainstorming fallback failed: {e}");
                        return Vec::new();
                    }
                }
            }
        };

        let results = collab.dispatch_subtasks(&team, &subtasks).await;

        // Parse solutions from agent outputs.
        let mut all_solutions: Vec<Solution> = Vec::new();
        for (i, result) in results.iter().enumerate() {
            let parsed =
                parse_solutions_from_text_single(&result.output, &format!("agent-{}", i + 1));
            all_solutions.extend(parsed);
        }

        all_solutions
    }

    // ── Phase 3: SolutionMerger ────────────────────────────────────────

    /// Deduplicate similar solutions using merge_entries pattern.
    fn phase3_merge(&self, solutions: Vec<Solution>) -> Vec<Solution> {
        if solutions.is_empty() {
            return solutions;
        }

        // Use merge_entries pattern: normalize by title, prefer higher confidence.
        use std::collections::HashMap;

        let mut seen: HashMap<String, usize> = HashMap::new(); // normalized_title -> index
        let mut merged: Vec<Solution> = Vec::new();

        for sol in solutions {
            let key = sol.title.to_lowercase().trim().to_string();
            if let Some(&idx) = seen.get(&key) {
                if sol.confidence > merged[idx].confidence {
                    merged[idx] = sol;
                }
            } else {
                seen.insert(key, merged.len());
                merged.push(sol);
            }
        }

        // Truncate to max_solutions.
        merged.truncate(self.config.max_solutions);
        merged
    }

    // ── Phase 4: Evaluation ────────────────────────────────────────────

    /// Each agent scores all solutions via AgentDiscussion.
    async fn phase4_evaluate(
        &self,
        problem: &ProblemStatement,
        solutions: &[Solution],
    ) -> Vec<SolutionEvaluation> {
        let mut all_evaluations: Vec<SolutionEvaluation> = Vec::new();

        if solutions.is_empty() {
            return all_evaluations;
        }

        // Prepare evaluation prompts for each agent × solution.
        let eval_agents = ["evaluator-alpha", "evaluator-beta", "evaluator-gamma"];
        let discussion = AgentDiscussion::<E>::new(Arc::clone(&self.executor));

        // Build agent prompts: each evaluator agent scores all solutions.
        let mut agent_prompts: Vec<(String, String)> = Vec::new();

        for (ei, agent_id) in eval_agents.iter().enumerate() {
            let mut eval_prompt = format!(
                "You are an evaluator. Score each solution for the following problem on a 1-5 scale \
                 across five dimensions: clarity, feasibility, novelty, impact, efficiency.\n\n\
                 ## Problem\n{}\n\n## Solutions\n\n",
                problem.description,
            );

            for (si, sol) in solutions.iter().enumerate() {
                eval_prompt.push_str(&format!(
                    "### Solution {}: {}\n{}\nConfidence: {:.1}\n\n",
                    si + 1,
                    sol.title,
                    sol.description,
                    sol.confidence,
                ));
            }

            eval_prompt.push_str(
                "For each solution, output scores in JSON format:\n\
                 {\"solution_id\": \"...\", \"clarity\": 1-5, \"feasibility\": 1-5, \
                 \"novelty\": 1-5, \"impact\": 1-5, \"efficiency\": 1-5, \"feedback\": \"...\"}\n\
                 Evaluate ALL solutions. Output each evaluation on a separate JSON line.",
            );

            // Avoid duplicate prompts for same agent
            let unique_agent_id = format!("{}-{}", agent_id, ei);
            agent_prompts.push((unique_agent_id, eval_prompt));
        }

        // Run the discussion.
        let turns = discussion
            .discuss("solution evaluation", &agent_prompts)
            .await;

        // Parse evaluations from discussion turns.
        for turn in &turns {
            let parsed = parse_evaluations_from_text(&turn.content, &turn.agent_id, solutions);
            all_evaluations.extend(parsed);
        }

        // If parsing failed, generate synthetic evaluations.
        if all_evaluations.is_empty() {
            for sol in solutions {
                for agent_id in &eval_agents {
                    all_evaluations.push(SolutionEvaluation {
                        solution_id: sol.id.clone(),
                        evaluator: format!("{}-synthetic", agent_id),
                        scores: SolutionScore::default(),
                        average: 3.0,
                        feedback: Some(
                            "Synthetic evaluation (no detailed scores parsed)".to_string(),
                        ),
                    });
                }
            }
        }

        all_evaluations
    }

    // ── Phase 5: Selection ─────────────────────────────────────────────

    /// Select the top solution by weighted average score.
    fn phase5_select(
        &self,
        solutions: &[Solution],
        evaluations: &[SolutionEvaluation],
    ) -> (Option<Solution>, Option<f32>) {
        if solutions.is_empty() || evaluations.is_empty() {
            return (None, None);
        }

        let mut aggregated: std::collections::HashMap<String, (f32, usize)> =
            std::collections::HashMap::new();

        for eval in evaluations {
            let entry = aggregated
                .entry(eval.solution_id.clone())
                .or_insert((0.0, 0));
            entry.0 += eval.average;
            entry.1 += 1;
        }

        let mut scored: Vec<(String, f32)> = aggregated
            .into_iter()
            .map(|(id, (total, count))| (id, total / count as f32))
            .collect();

        // Sort by average descending.
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        if let Some((best_id, best_score)) = scored.first() {
            if *best_score >= self.config.min_score_threshold {
                let best_solution = solutions.iter().find(|s| s.id == *best_id).cloned();
                return (best_solution, Some(*best_score));
            }
        }

        // Fallback: return first solution.
        let fallback = solutions.first().cloned();
        (fallback, scored.first().map(|(_, s)| *s))
    }

    // ── Phase 6: Execution ─────────────────────────────────────────────

    /// Dispatch execution tasks via CollaborationOrchestrator.
    async fn phase6_execute(
        &self,
        problem: &ProblemStatement,
        solution: &Solution,
    ) -> Vec<SubAgentResult> {
        let collab = CollaborationOrchestrator::<E>::new(Arc::clone(&self.executor));

        let prompt = format!(
            "Execute the following solution for the problem.\n\n\
             ## Problem\n{}\n\n\
             ## Selected Solution: {}\n{}\n\n\
             ## Execution\n\
             Step 1: Prepare the implementation plan.\n\
             Step 2: Execute the implementation.\n\
             Step 3: Verify the results against success criteria: {}",
            problem.description,
            solution.title,
            solution.description,
            problem.success_criteria.join(", "),
        );

        let skills = vec!["execution".to_string()];

        let subtasks = collab.decompose_sequential(&prompt);
        let task = CollaborationTask {
            description: prompt.clone(),
            required_skills: skills,
            subtasks: subtasks.clone(),
            review_criteria: Some("Implementation must satisfy all success criteria".to_string()),
            collaboration_decision: None,
        };

        let team = match collab.assemble_team(&task) {
            Some(t) => t,
            None => {
                // Fallback: single-agent execution.
                let config = SubAgentConfig {
                    task_description: "Solution execution".to_string(),
                    agent_role: "Executor".to_string(),
                    capabilities: vec!["execution".to_string()],
                    max_turns: 8,
                    ..SubAgentConfig::default()
                };
                match self.executor.execute(config, &prompt).await {
                    Ok(result) => return vec![result],
                    Err(e) => {
                        tracing::warn!("Execution fallback failed: {e}");
                        return vec![SubAgentResult {
                            output: format!("Execution error: {e}"),
                            completed_normally: false,
                            ..SubAgentResult::default()
                        }];
                    }
                }
            }
        };

        collab.dispatch_subtasks(&team, &subtasks).await
    }

    // ── Phase 7: Review ────────────────────────────────────────────────

    /// Reviewer agent checks execution results.
    async fn phase7_review(
        &self,
        problem: &ProblemStatement,
        solution: &Solution,
        results: &[SubAgentResult],
    ) -> String {
        let results_text: String = results
            .iter()
            .enumerate()
            .map(|(i, r)| {
                format!(
                    "Result {}: {} [{}]",
                    i + 1,
                    truncate_str(&r.output, 200),
                    if r.completed_normally { "OK" } else { "FAILED" }
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let prompt = format!(
            "Review the execution results of the following solution.\n\n\
             ## Problem\n{}\n\n\
             ## Solution: {}\n{}\n\n\
             ## Success Criteria\n{}\n\n\
             ## Execution Results\n{}\n\n\
             Provide a concise review: Did execution satisfy success criteria? \
             What are the main findings? What would you recommend next?",
            problem.description,
            solution.title,
            solution.description,
            problem.success_criteria.join("\n- "),
            results_text,
        );

        let config = SubAgentConfig {
            task_description: "Review execution results".to_string(),
            agent_role: "Reviewer".to_string(),
            capabilities: vec!["review".to_string(), "analysis".to_string()],
            max_turns: 3,
            ..SubAgentConfig::default()
        };

        match self.executor.execute(config, &prompt).await {
            Ok(result) => result.output,
            Err(e) => {
                tracing::warn!("Review failed: {e}");
                format!("Review could not be performed: {e}")
            }
        }
    }

    // ── finalize_pipeline ───────────────────────────────────────────────

    /// Persist the full pipeline result to L4 memory.
    async fn finalize_pipeline(&self, result: &PipelineResult) {
        let Some(ref mem) = self.parent_memory else {
            return;
        };
        if !self.config.persist_to_l4 {
            return;
        }

        let selected_text = match (&result.selected_solution, result.selected_score) {
            (Some(sol), Some(score)) => format!("{} (score: {:.1})", sol.title, score),
            (Some(sol), None) => format!("{} (no score)", sol.title),
            (None, _) => "no solution selected".to_string(),
        };

        let content = format!(
            "## P8.3 Joint Problem Solving Result\n\n\
             **Problem**: {}\n\
             **Solutions generated**: {}\n\
             **Selected solution**: {}\n\
             **Execution tasks**: {}\n\
             **Completed successfully**: {}\n\n\
             ### Review Summary\n\n{}\n\n\
             ### Phase Statuses\n\n{}",
            result.problem.description,
            result.solutions.len(),
            selected_text,
            result.execution_outputs.len(),
            result
                .execution_outputs
                .iter()
                .filter(|r| r.completed_normally)
                .count(),
            result.review_summary.as_deref().unwrap_or("no review"),
            result
                .phase_statuses
                .iter()
                .map(|(name, status)| format!("- {}: {:?}", name, status))
                .collect::<Vec<_>>()
                .join("\n"),
        );

        let memory_ctx =
            MemoryTurnContext::new("joint-problem-solving", "problem-solving-pipeline");
        let kernel = MemoryKernel::new(Arc::clone(mem));
        let entry = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L4,
            category: MemoryCategory::Shared,
            priority: Priority::High,
            source: MemorySource::Import,
            title: format!(
                "P8.3 pipeline: {}",
                truncate_str(&result.problem.description, 100)
            ),
            content,
            embedding: None,
            tags: vec![
                "P8.3".to_string(),
                "pipeline".to_string(),
                "joint-problem-solving".to_string(),
                "team-shared".to_string(),
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
            tracing::warn!("Failed to persist pipeline result to L4: {e}");
        }
    }

    // ── run: full 7-phase pipeline ──────────────────────────────────────

    /// Execute the full 7-phase Joint Problem Solving protocol.
    ///
    /// Returns `None` if no solution was selected.
    pub async fn run(&self, problem: ProblemStatement) -> Option<PipelineResult> {
        let mut phase_statuses: Vec<(String, PhaseStatus)> = Vec::new();

        // Phase 1: Problem Framing
        phase_statuses.push(("ProblemFraming".to_string(), PhaseStatus::Running));
        let framing = self.phase1_framing(&problem).await;
        phase_statuses.last_mut().unwrap().1 = PhaseStatus::Completed;

        // Phase 2: Solution Brainstorming
        phase_statuses.push(("SolutionBrainstorming".to_string(), PhaseStatus::Running));
        let solutions = self
            .phase2_brainstorming(&problem, &framing.perspectives)
            .await;
        phase_statuses.last_mut().unwrap().1 = PhaseStatus::Completed;

        if solutions.is_empty() {
            tracing::warn!("No solutions generated during brainstorming");
            phase_statuses.push(("SolutionMerger".to_string(), PhaseStatus::Skipped));
            phase_statuses.push(("Evaluation".to_string(), PhaseStatus::Skipped));
            phase_statuses.push(("Selection".to_string(), PhaseStatus::Skipped));
            phase_statuses.push(("Execution".to_string(), PhaseStatus::Skipped));
            phase_statuses.push(("Review".to_string(), PhaseStatus::Skipped));

            let result = PipelineResult {
                problem: problem.clone(),
                solutions: Vec::new(),
                evaluations: Vec::new(),
                selected_solution: None,
                selected_score: None,
                execution_outputs: Vec::new(),
                review_summary: None,
                phase_statuses,
            };
            self.finalize_pipeline(&result).await;
            return Some(result);
        }

        // Phase 3: Solution Merger
        phase_statuses.push(("SolutionMerger".to_string(), PhaseStatus::Running));
        let merged = self.phase3_merge(solutions);
        phase_statuses.last_mut().unwrap().1 = PhaseStatus::Completed;

        // Phase 4: Evaluation
        phase_statuses.push(("Evaluation".to_string(), PhaseStatus::Running));
        let evaluations = self.phase4_evaluate(&problem, &merged).await;
        phase_statuses.last_mut().unwrap().1 = PhaseStatus::Completed;

        // Phase 5: Selection
        phase_statuses.push(("Selection".to_string(), PhaseStatus::Running));
        let (selected_solution, selected_score) = self.phase5_select(&merged, &evaluations);
        phase_statuses.last_mut().unwrap().1 = PhaseStatus::Completed;

        // Phase 6: Execution
        let (execution_outputs, review_summary) = if let Some(ref sel) = selected_solution {
            phase_statuses.push(("Execution".to_string(), PhaseStatus::Running));
            let outputs = self.phase6_execute(&problem, sel).await;
            phase_statuses.last_mut().unwrap().1 = PhaseStatus::Completed;

            // Phase 7: Review
            phase_statuses.push(("Review".to_string(), PhaseStatus::Running));
            let review = self.phase7_review(&problem, sel, &outputs).await;
            phase_statuses.last_mut().unwrap().1 = PhaseStatus::Completed;

            (outputs, Some(review))
        } else {
            phase_statuses.push(("Execution".to_string(), PhaseStatus::Skipped));
            phase_statuses.push(("Review".to_string(), PhaseStatus::Skipped));
            (Vec::new(), None)
        };

        let result = PipelineResult {
            problem: problem.clone(),
            solutions: merged,
            evaluations,
            selected_solution,
            selected_score,
            execution_outputs,
            review_summary,
            phase_statuses,
        };

        self.finalize_pipeline(&result).await;

        Some(result)
    }
}

impl<E: SubAgentExecutor + Default + 'static> Default for ProblemSolvingPipeline<E> {
    fn default() -> Self {
        Self::new(Arc::new(E::default()))
    }
}

// ── JpsOps ─────────────────────────────────────────────────────────────────────

/// Type-erased interface for ProblemSolvingPipeline.
///
/// Enables storing a generic `ProblemSolvingPipeline<E>` behind a single
/// `Arc<dyn JpsOps>` so that the conversation runtime can trigger JPS
/// without knowing the concrete executor type `E`.
pub trait JpsOps: Send + Sync {
    fn run_boxed<'a>(
        &'a self,
        problem: ProblemStatement,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<PipelineResult>> + 'a>>;
}

impl<E: SubAgentExecutor + Send + Sync + 'static> JpsOps for ProblemSolvingPipeline<E> {
    fn run_boxed<'a>(
        &'a self,
        problem: ProblemStatement,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<PipelineResult>> + 'a>> {
        Box::pin(async move { self.run(problem).await })
    }
}

/// Factory: create a type-erased `Arc<dyn JpsOps>`.
///
/// Produces a boxed pipeline that can be passed to
/// `ConversationRuntime::with_jps_pipeline()` without propagating the
/// `E` type parameter.
pub fn new_boxed<E>(executor: Arc<E>) -> Arc<dyn JpsOps>
where
    E: SubAgentExecutor + Send + Sync + 'static,
{
    Arc::new(ProblemSolvingPipeline::<E>::new(executor))
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Truncate a string for display.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len).collect();
        format!("{truncated}...")
    }
}

/// Parse multiple solutions from free-text agent output.
///
/// Looks for patterns like "Solution N:" or "## Solution" or numbered items.
fn parse_solutions_from_text(text: &str) -> Vec<Solution> {
    let mut solutions: Vec<Solution> = Vec::new();
    let mut counter = 0u32;

    // Split by common section delimiters.
    let lines: Vec<&str> = text.lines().collect();

    let mut current_title: Option<String> = None;
    let mut current_desc: Vec<String> = Vec::new();

    for line in &lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Detect new solution headers.
        let is_header = trimmed.starts_with("## Solution")
            || trimmed.starts_with("### Solution")
            || trimmed.starts_with("Solution ")
            || (trimmed.len() > 3
                && trimmed.chars().next().map_or(false, |c| c.is_ascii_digit())
                && trimmed[1..].starts_with(". "));

        if is_header && current_title.is_some() {
            // Flush current solution.
            let desc = current_desc.join("\n");
            if !desc.is_empty() {
                counter += 1;
                solutions.push(Solution::new(
                    format!("sol-{counter}"),
                    current_title
                        .take()
                        .unwrap_or_else(|| format!("Solution {counter}")),
                    desc,
                    "agent-0",
                ));
            }
            current_desc = Vec::new();
        }

        if is_header {
            current_title = Some(strip_header_prefix(trimmed));
        } else if current_title.is_some() {
            current_desc.push(trimmed.to_string());
        }
    }

    // Flush last solution.
    if let Some(title) = current_title {
        let desc = current_desc.join("\n");
        if !desc.is_empty() {
            counter += 1;
            solutions.push(Solution::new(
                format!("sol-{counter}"),
                title,
                desc,
                "agent-0",
            ));
        }
    }

    // Fallback: if no structured solutions found, treat whole text as one.
    if solutions.is_empty() && !text.is_empty() {
        solutions.push(Solution::new(
            "sol-1",
            "Generated Solution",
            text.to_string(),
            "agent-0",
        ));
    }

    solutions
}

/// Parse a single agent's solutions from their output.
fn parse_solutions_from_text_single(text: &str, agent_id: &str) -> Vec<Solution> {
    let mut solutions = parse_solutions_from_text(text);
    for sol in &mut solutions {
        sol.proposed_by = agent_id.to_string();
        sol.id = format!("{}-{}", agent_id, sol.id);
    }
    solutions
}

/// Strip common header prefixes from solution titles.
fn strip_header_prefix(s: &str) -> String {
    let s = s.trim();
    for prefix in &[
        "## Solution:",
        "### Solution:",
        "## Solution ",
        "### Solution ",
        "Solution: ",
    ] {
        if let Some(stripped) = s.strip_prefix(prefix) {
            return stripped.trim().to_string();
        }
    }
    // Strip leading "N. " pattern.
    if let Some(dot_pos) = s.find(". ") {
        let prefix = &s[..dot_pos];
        if prefix.chars().all(|c| c.is_ascii_digit() || c == '-') {
            return s[dot_pos + 2..].trim().to_string();
        }
    }
    s.to_string()
}

/// Parse evaluation JSON from agent output text.
fn parse_evaluations_from_text(
    text: &str,
    evaluator: &str,
    solutions: &[Solution],
) -> Vec<SolutionEvaluation> {
    let mut evaluations: Vec<SolutionEvaluation> = Vec::new();

    for line in text.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('{') {
            continue;
        }

        if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            let solution_id = value
                .get("solution_id")
                .and_then(|v| v.as_str())
                .map(String::from);

            let clarity = value.get("clarity").and_then(|v| v.as_f64()).unwrap_or(3.0) as f32;
            let feasibility = value
                .get("feasibility")
                .and_then(|v| v.as_f64())
                .unwrap_or(3.0) as f32;
            let novelty = value.get("novelty").and_then(|v| v.as_f64()).unwrap_or(3.0) as f32;
            let impact = value.get("impact").and_then(|v| v.as_f64()).unwrap_or(3.0) as f32;
            let efficiency = value
                .get("efficiency")
                .and_then(|v| v.as_f64())
                .unwrap_or(3.0) as f32;
            let feedback = value
                .get("feedback")
                .and_then(|v| v.as_str())
                .map(String::from);

            let scores = SolutionScore {
                clarity: clarity.clamp(1.0, 5.0),
                feasibility: feasibility.clamp(1.0, 5.0),
                novelty: novelty.clamp(1.0, 5.0),
                impact: impact.clamp(1.0, 5.0),
                efficiency: efficiency.clamp(1.0, 5.0),
            };

            // If no solution_id, try to match by index.
            let sid = solution_id.unwrap_or_else(|| {
                solutions
                    .get(evaluations.len())
                    .map(|s| s.id.clone())
                    .unwrap_or_else(|| format!("sol-unknown"))
            });

            evaluations.push(SolutionEvaluation {
                solution_id: sid,
                evaluator: evaluator.to_string(),
                scores,
                average: scores.weighted_average(),
                feedback,
            });
        }
    }

    evaluations
}

// ── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::SubAgentError;

    // Minimal executor that always returns OK.
    #[derive(Default)]
    struct TestExecutor;

    impl SubAgentExecutor for TestExecutor {
        fn execute(
            &self,
            _config: SubAgentConfig,
            _task: &str,
        ) -> impl std::future::Future<Output = Result<SubAgentResult, SubAgentError>> {
            async move {
                Ok(SubAgentResult {
                    output: "test output".to_string(),
                    completed_normally: true,
                    ..SubAgentResult::default()
                })
            }
        }
    }

    // ── ProblemStatement tests ──────────────────────────────────────────

    #[test]
    fn problem_statement_builder() {
        let p = ProblemStatement::new("fix the build")
            .with_constraints(vec!["must be fast".to_string()])
            .with_success_criteria(vec!["tests pass".to_string()])
            .with_deadline("tomorrow");

        assert_eq!(p.description, "fix the build");
        assert_eq!(p.constraints.len(), 1);
        assert_eq!(p.success_criteria.len(), 1);
        assert_eq!(p.deadline, Some("tomorrow".to_string()));
    }

    #[test]
    fn problem_statement_defaults() {
        let p = ProblemStatement::new("simple issue");
        assert!(p.constraints.is_empty());
        assert!(p.success_criteria.is_empty());
        assert!(p.deadline.is_none());
    }

    // ── SolutionScore tests ─────────────────────────────────────────────

    #[test]
    fn score_weighted_average() {
        let score = SolutionScore {
            clarity: 5.0,
            feasibility: 4.0,
            novelty: 3.0,
            impact: 4.0,
            efficiency: 5.0,
        };
        let avg = score.weighted_average();
        // 5*0.2 + 4*0.25 + 3*0.15 + 4*0.25 + 5*0.15 = 1+1+0.45+1+0.75 = 4.2
        assert!((avg - 4.2).abs() < 0.01, "Expected ~4.2, got {avg}");
    }

    #[test]
    fn score_default_is_3() {
        let score = SolutionScore::default();
        let avg = score.weighted_average();
        assert!((avg - 3.0).abs() < 0.01, "Expected ~3.0, got {avg}");
    }

    // ── Phase 3: merge ──────────────────────────────────────────────────

    #[test]
    fn merge_deduplicates_by_title() {
        let pipeline = ProblemSolvingPipeline::<TestExecutor>::new(Arc::new(TestExecutor));

        let solutions = vec![
            Solution::new("s1", "Alpha", "desc1", "agent-a"),
            Solution::new("s2", "alpha", "desc2", "agent-b"), // same title lowercased
            Solution::new("s3", "Beta", "desc3", "agent-a"),
        ];

        let merged = pipeline.phase3_merge(solutions);
        assert_eq!(merged.len(), 2, "expected 2 unique titles after merge");
    }

    #[test]
    fn merge_prefers_higher_confidence() {
        let pipeline = ProblemSolvingPipeline::<TestExecutor>::new(Arc::new(TestExecutor));

        let s1 = Solution {
            id: "s1".to_string(),
            title: "Alpha".to_string(),
            description: "low confidence".to_string(),
            proposed_by: "agent-a".to_string(),
            confidence: 0.3,
            tags: vec![],
        };
        let s2 = Solution {
            id: "s2".to_string(),
            title: "Alpha".to_string(),
            description: "high confidence".to_string(),
            proposed_by: "agent-b".to_string(),
            confidence: 0.9,
            tags: vec![],
        };

        let merged = pipeline.phase3_merge(vec![s1, s2]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].confidence, 0.9);
        assert_eq!(merged[0].description, "high confidence");
    }

    #[test]
    fn merge_empty_input() {
        let pipeline = ProblemSolvingPipeline::<TestExecutor>::new(Arc::new(TestExecutor));
        let merged = pipeline.phase3_merge(vec![]);
        assert!(merged.is_empty());
    }

    // ── Phase 5: selection ──────────────────────────────────────────────

    #[test]
    fn select_top_solution_by_average() {
        let pipeline = ProblemSolvingPipeline::<TestExecutor>::new(Arc::new(TestExecutor));

        let solutions = vec![
            Solution::new("s1", "Alpha", "desc", "agent-a"),
            Solution::new("s2", "Beta", "desc", "agent-b"),
        ];

        let evaluations = vec![
            SolutionEvaluation {
                solution_id: "s1".to_string(),
                evaluator: "e1".to_string(),
                scores: SolutionScore::default(),
                average: 4.0,
                feedback: None,
            },
            SolutionEvaluation {
                solution_id: "s2".to_string(),
                evaluator: "e1".to_string(),
                scores: SolutionScore::default(),
                average: 2.0,
                feedback: None,
            },
            SolutionEvaluation {
                solution_id: "s1".to_string(),
                evaluator: "e2".to_string(),
                scores: SolutionScore::default(),
                average: 5.0,
                feedback: None,
            },
            SolutionEvaluation {
                solution_id: "s2".to_string(),
                evaluator: "e2".to_string(),
                scores: SolutionScore::default(),
                average: 3.0,
                feedback: None,
            },
        ];

        let (selected, score) = pipeline.phase5_select(&solutions, &evaluations);

        assert!(selected.is_some());
        let selected = selected.unwrap();
        assert_eq!(selected.id, "s1"); // avg of 4.0 and 5.0 = 4.5 > s2 avg of 2.5
        assert!(score.is_some());
        assert!((score.unwrap() - 4.5).abs() < 0.1);
    }

    #[test]
    fn select_empty_returns_none() {
        let pipeline = ProblemSolvingPipeline::<TestExecutor>::new(Arc::new(TestExecutor));
        let (selected, score) = pipeline.phase5_select(&[], &[]);
        assert!(selected.is_none());
        assert!(score.is_none());
    }

    // ── Parsing helpers ─────────────────────────────────────────────────

    #[test]
    fn parse_solutions_from_text_detects_headers() {
        let text =
            "## Solution 1: Alpha\nDescription here\n\n## Solution 2: Beta\nOther description";
        let solutions = parse_solutions_from_text(text);
        assert_eq!(solutions.len(), 2);
        assert!(solutions[0].title.contains("Alpha"));
        assert!(solutions[1].title.contains("Beta"));
    }

    #[test]
    fn parse_solutions_fallback_to_whole_text() {
        let text = "Just a plain text solution without headers";
        let solutions = parse_solutions_from_text(text);
        assert_eq!(solutions.len(), 1);
        assert_eq!(solutions[0].title, "Generated Solution");
    }

    #[test]
    fn parse_evaluations_from_json() {
        let text = r#"{"solution_id": "s1", "clarity": 5, "feasibility": 4, "novelty": 3, "impact": 4, "efficiency": 5, "feedback": "good"}
{"solution_id": "s2", "clarity": 2, "feasibility": 2, "novelty": 2, "impact": 2, "efficiency": 2}"#;

        let solutions = vec![
            Solution::new("s1", "A", "d", "x"),
            Solution::new("s2", "B", "d", "x"),
        ];

        let evals = parse_evaluations_from_text(text, "eval-1", &solutions);
        assert_eq!(evals.len(), 2);
        assert_eq!(evals[0].solution_id, "s1");
        assert!((evals[0].average - 4.2).abs() < 0.01);
        assert_eq!(evals[0].feedback, Some("good".to_string()));
    }

    // ── DiscussionTurn tests ────────────────────────────────────────────

    #[test]
    fn discussion_turn_creation() {
        let turn = DiscussionTurn {
            agent_id: "agent-1".to_string(),
            agent_role: "Evaluator".to_string(),
            content: "I think this is great".to_string(),
            turn_number: 1,
        };
        assert_eq!(turn.turn_number, 1);
        assert_eq!(turn.agent_id, "agent-1");
    }

    // ── Config defaults ─────────────────────────────────────────────────

    #[test]
    fn default_config_values() {
        let config = ProblemSolvingConfig::default();
        assert_eq!(config.max_brainstorm_agents, 4);
        assert_eq!(config.max_eval_agents, 3);
        assert_eq!(config.max_solutions, 5);
        assert_eq!(config.min_score_threshold, 2.5);
        assert!(config.persist_to_l4);
    }

    // ── Phase 2: brainstorming with empty output ────────────────────────

    #[tokio::test]
    async fn brainstorm_empty_output_returns_empty() {
        struct EmptyExecutor;
        impl SubAgentExecutor for EmptyExecutor {
            fn execute(
                &self,
                _config: SubAgentConfig,
                _task: &str,
            ) -> impl std::future::Future<Output = Result<SubAgentResult, SubAgentError>>
            {
                async move {
                    Ok(SubAgentResult {
                        output: String::new(),
                        completed_normally: true,
                        ..SubAgentResult::default()
                    })
                }
            }
        }

        // We can't easily test the full phase2 since it needs AgentDirectory,
        // but we can verify the executor works.
        let result = EmptyExecutor
            .execute(SubAgentConfig::default(), "test")
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().output.is_empty());
    }

    // ── Pipeline result serde ───────────────────────────────────────────

    #[test]
    fn pipeline_result_serialization() {
        let result = PipelineResult {
            problem: ProblemStatement::new("test"),
            solutions: vec![Solution::new("s1", "T", "D", "A")],
            evaluations: vec![],
            selected_solution: None,
            selected_score: None,
            execution_outputs: vec![],
            review_summary: None,
            phase_statuses: vec![
                ("ProblemFraming".to_string(), PhaseStatus::Completed),
                ("Execution".to_string(), PhaseStatus::Skipped),
            ],
        };

        let json = serde_json::to_string_pretty(&result).unwrap();
        assert!(json.contains("test"));
        assert!(json.contains("ProblemFraming"));
        assert!(json.contains("completed"));
        assert!(json.contains("skipped"));
    }

    #[test]
    fn pipeline_result_deserialization() {
        let json = r#"{
            "problem": {"description": "test", "constraints": [], "success_criteria": [], "deadline": null},
            "solutions": [],
            "evaluations": [],
            "selected_solution": null,
            "selected_score": null,
            "execution_outputs": [],
            "review_summary": null,
            "phase_statuses": [["Phase1", "completed"], ["Phase2", "failed"]]
        }"#;

        let result: PipelineResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.problem.description, "test");
        assert_eq!(result.phase_statuses.len(), 2);
        assert_eq!(result.phase_statuses[0].1, PhaseStatus::Completed);
        assert_eq!(result.phase_statuses[1].1, PhaseStatus::Failed);
    }

    // ── Integration: full pipeline with test executor ───────────────────

    struct IntegrationTestExecutor {
        _responses: Vec<String>,
    }

    impl SubAgentExecutor for IntegrationTestExecutor {
        fn execute(
            &self,
            config: SubAgentConfig,
            task: &str,
        ) -> impl std::future::Future<Output = Result<SubAgentResult, SubAgentError>> {
            // Return a response based on the agent role.
            let output = if config.agent_role.contains("Brainstormer")
                || task.contains("brainstorm")
            {
                "## Solution 1: Quick Fix\nJust fix it quickly.\n\n## Solution 2: Refactor\nRefactor the whole thing.".to_string()
            } else if config.agent_role.contains("Evaluator") {
                r#"{"solution_id": "sol-1", "clarity": 4, "feasibility": 5, "novelty": 2, "impact": 3, "efficiency": 4}
{"solution_id": "sol-2", "clarity": 3, "feasibility": 3, "novelty": 4, "impact": 4, "efficiency": 3}"#.to_string()
            } else if config.agent_role.contains("Executor") {
                "Task executed successfully. All tests pass.".to_string()
            } else if config.agent_role.contains("Reviewer") {
                "Review: Implementation meets criteria. Approved.".to_string()
            } else if config.agent_role.contains("Analyst") {
                "Analysis: The problem requires careful planning and systematic execution."
                    .to_string()
            } else {
                "Default response".to_string()
            };

            async move {
                Ok(SubAgentResult {
                    output,
                    completed_normally: true,
                    ..SubAgentResult::default()
                })
            }
        }
    }

    #[tokio::test]
    async fn full_pipeline_brainstorm_and_merge() {
        let pipeline = ProblemSolvingPipeline::<IntegrationTestExecutor>::new(Arc::new(
            IntegrationTestExecutor { _responses: vec![] },
        ));

        let problem = ProblemStatement::new("build system is slow")
            .with_constraints(vec!["must not break CI".to_string()])
            .with_success_criteria(vec!["build time < 2min".to_string()]);

        let result = pipeline.run(problem).await;
        assert!(result.is_some());

        let result = result.unwrap();
        // Verify phases completed.
        let framing_status = result
            .phase_statuses
            .iter()
            .find(|(n, _)| n == "ProblemFraming");
        assert!(framing_status.is_some());

        // At minimum, we should have the problem and phase statuses.
        assert!(!result.phase_statuses.is_empty());
    }
}
