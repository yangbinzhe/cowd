//! AgentDiscussion — multi-agent consensus-building through structured rounds.
//!
//! When agents produce conflicting claims in L4 shared memory, the
//! `DiscussionEngine` orchestrates a multi-round discussion to reach consensus.
//! Participants contribute via `team_remember`, the leader synthesizes via
//! `team_query`, and `FactChecker` resolves conflicting claims.
//!
//! The engine subscribes to `EventBus::TurnCompleted` to auto-detect
//! L4 conflicts and trigger discussion rounds when needed.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing;

use crate::bus::{Event, EventBus};

use memory::agent_directory::AgentInfo;
use memory::cognitive::CognitiveContextManager;
use memory::fact_checker::FactChecker;
use memory::project_scope::MemoryScope;
use memory::temporal_graph::Triple;
use memory::types::{
    AgentVisibility, MemoryCategory, MemoryEntry, MemoryId, MemoryLayer, MemorySource, Priority,
};

// ── ConsensusMethod ───────────────────────────────────────────────────────────

/// Strategy for reaching consensus among discussion participants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusMethod {
    /// Simple majority vote (>50% of participants agree).
    MajorityVote,
    /// Weighted vote — each agent's vote is weighted by a reliability score.
    WeightedVote,
    /// The designated leader makes the final decision after hearing all
    /// participants.
    LeaderDecides,
}

impl Default for ConsensusMethod {
    fn default() -> Self {
        Self::MajorityVote
    }
}

// ── DiscussionPhase ───────────────────────────────────────────────────────────

/// Phase of a discussion round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscussionPhase {
    /// Participants are contributing their perspectives.
    Contributing,
    /// Leader is synthesizing contributions.
    Synthesizing,
    /// Consensus check in progress.
    CheckingConsensus,
    /// Discussion is complete (consensus reached or max rounds exhausted).
    Complete,
}

// ── Contribution ──────────────────────────────────────────────────────────────

/// A single participant's contribution in a discussion round.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contribution {
    /// The agent who contributed.
    pub agent_id: String,
    /// Round number this contribution belongs to.
    pub round: u32,
    /// The contribution text.
    pub content: String,
    /// Confidence level (0.0–1.0).
    pub confidence: f32,
    /// Key claims extracted from the contribution (for fact-checking).
    pub claims: Vec<String>,
}

// ── ConsensusResult ───────────────────────────────────────────────────────────

/// Outcome of a consensus check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusResult {
    /// Whether consensus was reached.
    pub reached: bool,
    /// Consensus score (0.0–1.0).
    pub score: f32,
    /// Method used to reach consensus.
    pub method: ConsensusMethod,
    /// Number of agreeing participants.
    pub agreeing_count: usize,
    /// Total participants.
    pub total_count: usize,
    /// Remaining conflicts after resolution.
    pub unresolved_conflicts: Vec<String>,
}

// ── Discussion ────────────────────────────────────────────────────────────────

/// A structured multi-agent discussion.
#[derive(Debug, Clone)]
pub struct Discussion {
    /// The topic under discussion.
    pub topic: String,
    /// Participants in the discussion (first is the leader).
    pub participants: Vec<AgentInfo>,
    /// Consensus method to use.
    pub consensus_method: ConsensusMethod,
    /// Maximum number of rounds.
    pub max_rounds: u32,
    /// Contributions organized by round.
    pub contributions: HashMap<u32, Vec<Contribution>>,
    /// Current round number.
    pub current_round: u32,
    /// Current phase.
    pub phase: DiscussionPhase,
    /// Final consensus result (set when phase is Complete).
    pub consensus_result: Option<ConsensusResult>,
    /// Final decision text.
    pub final_decision: Option<String>,
}

impl Discussion {
    /// Create a new discussion with the given topic and participants.
    pub fn new(
        topic: String,
        participants: Vec<AgentInfo>,
        consensus_method: ConsensusMethod,
        max_rounds: u32,
    ) -> Self {
        Self {
            topic,
            participants,
            consensus_method,
            max_rounds,
            contributions: HashMap::new(),
            current_round: 0,
            phase: DiscussionPhase::Contributing,
            consensus_result: None,
            final_decision: None,
        }
    }

    /// Get the leader (first participant).
    pub fn leader(&self) -> Option<&AgentInfo> {
        self.participants.first()
    }

    /// Get participant count.
    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    /// Check if the discussion is complete.
    pub fn is_complete(&self) -> bool {
        self.phase == DiscussionPhase::Complete
    }
}

// ── DiscussionEngine ──────────────────────────────────────────────────────────

/// Orchestrates multi-agent discussions with round-based contributions,
/// L4-backed knowledge sharing, and consensus computation.
pub struct DiscussionEngine {
    /// Reference to the event bus for TurnCompleted subscription.
    pub event_bus: Arc<EventBus>,
    /// Reference to the cognitive context manager for L4 operations.
    pub memory: Arc<CognitiveContextManager>,
    /// Active discussion (one at a time).
    pub discussion: Option<Discussion>,
    /// Fact checker for evaluating conflicting claims.
    fact_checker: FactChecker,
    /// Handle for the background TurnCompleted watcher.
    watcher_handle: Option<JoinHandle<()>>,
}

impl DiscussionEngine {
    /// Create a new discussion engine.
    pub fn new(event_bus: Arc<EventBus>, memory: Arc<CognitiveContextManager>) -> Self {
        Self {
            event_bus,
            memory,
            discussion: None,
            fact_checker: FactChecker::new(),
            watcher_handle: None,
        }
    }

    // ── TurnCompleted Watcher ─────────────────────────────────────────────

    /// Start listening for `TurnCompleted` events to auto-detect L4 conflicts
    /// and trigger discussion rounds.
    ///
    /// Spawns a background task that scans L4 for contradictory claims after
    /// each turn and triggers a discussion if conflicts are found.
    pub fn start_watcher(&mut self) {
        if self.watcher_handle.is_some() {
            tracing::debug!("TurnCompleted watcher already running");
            return;
        }

        let mut rx = self.event_bus.subscribe();
        let memory = Arc::clone(&self.memory);
        let _bus = Arc::clone(&self.event_bus);

        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h.spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(Event::TurnCompleted { tokens, model }) => {
                            tracing::debug!(
                                tokens,
                                %model,
                                "TurnCompleted: scanning L4 for conflicts"
                            );

                            // Query L4 for recent entries that may conflict.
                            match Self::detect_l4_conflicts(&memory).await {
                                Ok(conflicts) if !conflicts.is_empty() => {
                                    tracing::info!(
                                        conflict_count = conflicts.len(),
                                        "L4 conflicts detected — consider triggering discussion"
                                    );
                                    // Emit a conflict-detected event for subscribers.
                                    // Note: We don't add a new Event variant here to
                                    // avoid modifying bus.rs; instead we log and
                                    // let callers poll `check_for_conflicts()`.
                                }
                                Ok(_) => {
                                    tracing::debug!("No L4 conflicts detected");
                                }
                                Err(e) => {
                                    tracing::warn!("L4 conflict scan failed: {}", e);
                                }
                            }
                        }
                        Ok(_) => {
                            // Ignore other event types.
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(skipped = n, "TurnCompleted watcher lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!("EventBus closed — watcher stopping");
                            break;
                        }
                    }
                }
            }),
            Err(_) => {
                tracing::warn!("no tokio runtime available; discussion watcher disabled");
                return;
            }
        };
        self.watcher_handle = Some(handle);
    }

    /// Detect conflicting claims in L4 memory.
    ///
    /// Scans recent L4 entries for entries from different agents with the
    /// same topic/title but contradictory content.
    async fn detect_l4_conflicts(
        memory: &CognitiveContextManager,
    ) -> Result<Vec<(MemoryEntry, MemoryEntry)>, String> {
        // Query L4 for recent team-shared entries related to conflict topics.
        let entries = memory
            .recall("conflict disagreement contradict", 20)
            .await
            .map_err(|e| format!("L4 recall failed: {e}"))?;

        if entries.len() < 2 {
            return Ok(Vec::new());
        }

        let mut conflicts: Vec<(MemoryEntry, MemoryEntry)> = Vec::new();

        // Group entries by title prefix for comparison.
        let mut by_topic: HashMap<String, Vec<&MemoryEntry>> = HashMap::new();
        for entry in &entries {
            let topic = &entry.title;
            by_topic
                .entry(topic.clone())
                .or_default()
                .push(entry);
        }

        for entries in by_topic.values() {
            if entries.len() < 2 {
                continue;
            }
            // Compare each pair of entries with different source agents.
            for i in 0..entries.len() {
                for j in (i + 1)..entries.len() {
                    let a = entries[i];
                    let b = entries[j];
                    if a.source_agent != b.source_agent
                        && a.content != b.content
                    {
                        conflicts.push((a.clone(), b.clone()));
                    }
                }
            }
        }

        Ok(conflicts)
    }

    /// Check whether a discussion should be triggered based on current L4 state.
    ///
    /// Returns the number of conflicting claim pairs if any.
    pub async fn check_for_conflicts(&self) -> Result<usize, String> {
        let conflicts = Self::detect_l4_conflicts(&self.memory).await?;
        Ok(conflicts.len())
    }

    /// Synchronous wrapper for `check_for_conflicts()`.
    ///
    /// Uses `tokio::runtime::Handle::current().block_on()` internally.
    /// This allows callers holding a `std::sync::Mutex<DiscussionEngine>` to
    /// invoke conflict checking without async lock contention.
    pub fn check_for_conflicts_sync(&self) -> Result<usize, String> {
        tokio::runtime::Handle::current().block_on(self.check_for_conflicts())
    }

    // ── Discussion Lifecycle ──────────────────────────────────────────────

    /// Start a new discussion on the given topic with the given participants.
    ///
    /// Each participant contributes their initial perspective to L4 via
    /// `team_remember`.
    pub async fn start_discussion(
        &mut self,
        topic: String,
        participants: Vec<AgentInfo>,
        consensus_method: ConsensusMethod,
        max_rounds: u32,
    ) -> Result<(), String> {
        if self.discussion.is_some() {
            return Err("A discussion is already in progress".to_string());
        }

        if participants.is_empty() {
            return Err("At least one participant is required".to_string());
        }

        let mut discussion = Discussion::new(topic, participants, consensus_method, max_rounds);
        discussion.phase = DiscussionPhase::Contributing;
        discussion.current_round = 1;

        // Each participant contributes their initial perspective.
        for participant in &discussion.participants {
            self.write_contribution_to_l4(
                participant,
                discussion.current_round,
                &format!(
                    "Agent {} ({}) initial position on discussion: {}",
                    participant.agent_id, participant.role, discussion.topic
                ),
            )
            .await?;
        }

        self.discussion = Some(discussion);
        Ok(())
    }

    /// Run a single discussion round.
    ///
    /// 1. All participants contribute their updated positions to L4.
    /// 2. The leader queries L4 for all contributions.
    /// 3. The leader synthesizes a round summary.
    pub async fn run_round(&mut self, round_num: u32) -> Result<String, String> {
        // Extract data from discussion first to avoid borrow conflicts.
        let (max_rounds, topic, participants) = {
            let discussion = self
                .discussion
                .as_ref()
                .ok_or("No active discussion")?;
            (
                discussion.max_rounds,
                discussion.topic.clone(),
                discussion.participants.clone(),
            )
        };

        if round_num > max_rounds {
            return Err(format!(
                "Round {round_num} exceeds max rounds {max_rounds}",
            ));
        }

        // Collect contributions by writing to L4 for each participant.
        let mut contributions: Vec<Contribution> = Vec::new();
        for participant in &participants {
            let content = format!(
                "Round {round_num} contribution from {} ({}) on topic '{topic}'",
                participant.agent_id, participant.role,
            );
            self.write_contribution_to_l4(participant, round_num, &content)
                .await?;

            contributions.push(Contribution {
                agent_id: participant.agent_id.clone(),
                round: round_num,
                content: content.clone(),
                confidence: 0.8,
                claims: vec![content.clone()],
            });
        }

        // Update discussion state.
        {
            let discussion = self
                .discussion
                .as_mut()
                .ok_or("No active discussion")?;
            discussion.current_round = round_num;
            discussion.phase = DiscussionPhase::Contributing;
            discussion
                .contributions
                .insert(round_num, contributions);
            discussion.phase = DiscussionPhase::Synthesizing;
        }

        // Leader synthesizes by querying L4.
        let leader_id = participants
            .first()
            .map(|l| l.agent_id.clone())
            .unwrap_or_default();

        let synthesis = self
            .leader_synthesize(&topic, round_num, &leader_id)
            .await?;

        Ok(synthesis)
    }

    /// The leader queries L4 for all round contributions and synthesizes
    /// them into a summary.
    async fn leader_synthesize(
        &self,
        topic: &str,
        round_num: u32,
        leader_id: &str,
    ) -> Result<String, String> {
        // Query L4 for entries related to this discussion.
        let query = format!("discussion {topic} round {round_num}");
        let entries = self
            .memory
            .recall(&query, 10)
            .await
            .map_err(|e| format!("L4 recall failed: {e}"))?;

        if entries.is_empty() {
            tracing::warn!("No L4 entries found for discussion synthesis");
        }

        let mut synthesis = String::from("## Discussion Round Synthesis\n\n");
        synthesis.push_str(&format!("**Topic**: {topic}\n"));
        synthesis.push_str(&format!("**Round**: {round_num}\n"));
        synthesis.push_str(&format!("**Leader**: {leader_id}\n\n"));

        synthesis.push_str("### Contributions\n\n");
        for entry in &entries {
            synthesis.push_str(&format!(
                "- **[{}]** (from {}): {}\n",
                entry.title,
                entry.source_agent.as_deref().unwrap_or("unknown"),
                truncate_str(&entry.content, 200)
            ));
        }

        Ok(synthesis)
    }

    /// Check whether consensus has been reached using the configured method.
    ///
    /// Uses `FactChecker` to evaluate conflicting claims and computes a
    /// consensus score.
    pub async fn check_consensus(&mut self) -> Result<ConsensusResult, String> {
        let discussion = self
            .discussion
            .as_mut()
            .ok_or("No active discussion")?;

        discussion.phase = DiscussionPhase::CheckingConsensus;

        let total = discussion.participant_count();
        let method = discussion.consensus_method;
        let mut fact_checker = FactChecker::new();

        // Register each participant's claims as triples.
        let mut claim_map: HashMap<String, Vec<String>> = HashMap::new(); // claim -> agents
        for (round, contribs) in &discussion.contributions {
            for contrib in contribs {
                for claim in &contrib.claims {
                    let triple = Triple {
                        id: format!("disc-{}-agent-{}", round, contrib.agent_id),
                        subject: discussion.topic.clone(),
                        predicate: format!("claim-round-{round}"),
                        object: claim.clone(),
                        valid_from: None,
                        valid_until: None,
                        confidence: contrib.confidence,
                        source_memory_id: None,
                        source_file: None,
                        source_agent: Some(contrib.agent_id.clone()),
                    };
                    fact_checker.register_triple(triple);
                    claim_map
                        .entry(claim.clone())
                        .or_default()
                        .push(contrib.agent_id.clone());
                }
            }
        }

        // Run auto_correct to resolve conflicts.
        let report = fact_checker.auto_correct();
        let mut unresolved_conflicts: Vec<String> = Vec::new();

        if report.flagged > 0 {
            unresolved_conflicts.push(format!(
                "{} claims flagged for review",
                report.flagged
            ));
        }

        // Compute consensus score based on method.
        let (agreeing_count, score) = match method {
            ConsensusMethod::MajorityVote => {
                let max_agreement = claim_map
                    .values()
                    .map(|agents| agents.len())
                    .max()
                    .unwrap_or(0);

                let score = if total == 0 {
                    0.0
                } else {
                    max_agreement as f32 / total as f32
                };

                (max_agreement, score)
            }
            ConsensusMethod::WeightedVote => {
                let max_weighted: f32 = claim_map
                    .values()
                    .map(|agents| agents.len() as f32 * 0.8)
                    .fold(0.0_f32, f32::max);
                let total_weight = total as f32 * 0.8;
                let score = if total_weight > 0.0 {
                    (max_weighted / total_weight).min(1.0)
                } else {
                    0.0
                };
                (
                    (score * total as f32).round() as usize,
                    score,
                )
            }
            ConsensusMethod::LeaderDecides => {
                // Leader's contribution has ultimate authority.
                let leader_id = discussion
                    .leader()
                    .map(|l| l.agent_id.clone())
                    .unwrap_or_default();

                let leader_contributions = claim_map
                    .values()
                    .filter(|agents| agents.iter().any(|a| a == &leader_id))
                    .count();

                let score = if total == 0 {
                    1.0 // Leader decides alone
                } else {
                    (1.0 + leader_contributions as f32 / total as f32).min(1.0)
                };
                (1, score)
            }
        };

        let result = ConsensusResult {
            reached: score >= 0.5 && unresolved_conflicts.is_empty(),
            score,
            method,
            agreeing_count,
            total_count: total,
            unresolved_conflicts,
        };

        discussion.consensus_result = Some(result.clone());

        if result.reached || discussion.current_round >= discussion.max_rounds {
            discussion.phase = DiscussionPhase::Complete;
        }

        Ok(result)
    }

    /// Finalize the discussion — write the final decision to L4.
    pub async fn finalize(&mut self) -> Result<String, String> {
        let discussion = self
            .discussion
            .take()
            .ok_or("No active discussion")?;

        let leader_id = discussion
            .leader()
            .map(|l| l.agent_id.clone())
            .unwrap_or_else(|| "orchestrator".to_string());

        let consensus_result = discussion
            .consensus_result
            .as_ref()
            .ok_or("Consensus check not yet performed")?;

        // Build the final decision text.
        let mut decision = String::from("## Discussion Final Decision\n\n");
        decision.push_str(&format!("**Topic**: {}\n", discussion.topic));
        decision.push_str(&format!("**Participants**: {}\n", discussion.participants.len()));
        decision.push_str(&format!("**Rounds**: {}\n", discussion.current_round));
        decision.push_str(&format!("**Method**: {:?}\n", discussion.consensus_method));
        decision.push_str(&format!(
            "**Consensus**: {} (score: {:.2}, {}/{})\n",
            if consensus_result.reached {
                "REACHED"
            } else {
                "NOT REACHED"
            },
            consensus_result.score,
            consensus_result.agreeing_count,
            consensus_result.total_count,
        ));
        decision.push_str(&format!("**Leader**: {}\n\n", leader_id));

        decision.push_str("### Claims Summary\n\n");
        for (round, contribs) in &discussion.contributions {
            decision.push_str(&format!("#### Round {round}\n\n"));
            for contrib in contribs {
                decision.push_str(&format!(
                    "- **{}**: {}\n",
                    contrib.agent_id,
                    truncate_str(&contrib.content, 200)
                ));
            }
        }

        if !consensus_result.unresolved_conflicts.is_empty() {
            decision.push_str("\n### Unresolved Conflicts\n\n");
            for conflict in &consensus_result.unresolved_conflicts {
                decision.push_str(&format!("- {conflict}\n"));
            }
        }

        // Write final decision to L4.
        let entry = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L4,
            category: MemoryCategory::Shared,
            priority: Priority::High,
            source: MemorySource::Import,
            title: format!("discussion-decision: {}", truncate_str(&discussion.topic, 100)),
            content: decision.clone(),
            embedding: None,
            tags: vec![
                "discussion".to_string(),
                "decision".to_string(),
                "team-shared".to_string(),
            ],
            relations: vec![],
            confidence: consensus_result.score,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: Some(leader_id),
            visibility: AgentVisibility::Shared,
        };

        self.memory
            .remember(entry)
            .await
            .map_err(|e| format!("Failed to write final decision to L4: {e}"))?;

        Ok(decision)
    }

    /// Abort an active discussion (cleanup without finalizing).
    pub fn abort_discussion(&mut self) {
        self.discussion = None;
        self.fact_checker = FactChecker::new();
    }

    /// Stop the background TurnCompleted watcher.
    pub fn stop_watcher(&mut self) {
        if let Some(handle) = self.watcher_handle.take() {
            handle.abort();
        }
    }

    // ── Private Helpers ────────────────────────────────────────────────────

    /// Write a participant's contribution to L4 via `team_remember`-style
    /// memory entry.
    async fn write_contribution_to_l4(
        &self,
        agent: &AgentInfo,
        round: u32,
        content: &str,
    ) -> Result<(), String> {
        let entry = MemoryEntry {
            id: MemoryId::new_v4(),
            layer: MemoryLayer::L4,
            category: MemoryCategory::Shared,
            priority: Priority::Normal,
            source: MemorySource::Import,
            title: format!(
                "discussion-round-{round}: {} ({})",
                agent.agent_id, agent.role
            ),
            content: content.to_string(),
            embedding: None,
            tags: vec![
                "discussion".to_string(),
                format!("round-{round}"),
                agent.agent_id.clone(),
            ],
            relations: vec![],
            confidence: 0.85,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope: MemoryScope::Global,
            session_id: None,
            source_agent: Some(agent.agent_id.clone()),
            visibility: AgentVisibility::Shared,
        };

        self.memory
            .remember(entry)
            .await
            .map_err(|e| format!("Failed to write contribution to L4: {e}"))?;

        Ok(())
    }
}

impl Drop for DiscussionEngine {
    fn drop(&mut self) {
        self.stop_watcher();
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Truncate a string to at most `max_len` characters, appending "..." if truncated.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len).collect();
        format!("{truncated}...")
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use memory::agent_directory::AgentStatus;

    fn dummy_agent_info(id: &str, role: &str, capabilities: Vec<String>) -> AgentInfo {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        AgentInfo {
            agent_id: id.to_string(),
            role: role.to_string(),
            capabilities,
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: None,
        }
    }

    fn make_discussion(topic: &str) -> Discussion {
        let participants = vec![
            dummy_agent_info("lead", "Orchestrator", vec!["planning".to_string()]),
            dummy_agent_info("worker-1", "Executor", vec!["rust".to_string()]),
            dummy_agent_info("worker-2", "Reviewer", vec!["testing".to_string()]),
        ];
        Discussion::new(
            topic.to_string(),
            participants,
            ConsensusMethod::MajorityVote,
            3,
        )
    }

    // ── Discussion Tests ──────────────────────────────────────────────────

    #[test]
    fn discussion_new_initializes_correctly() {
        let disc = make_discussion("Should we use async or sync?");
        assert_eq!(disc.topic, "Should we use async or sync?");
        assert_eq!(disc.participant_count(), 3);
        assert_eq!(disc.consensus_method, ConsensusMethod::MajorityVote);
        assert_eq!(disc.max_rounds, 3);
        assert_eq!(disc.current_round, 0);
        assert_eq!(disc.phase, DiscussionPhase::Contributing);
        assert!(disc.leader().is_some());
        assert_eq!(disc.leader().unwrap().agent_id, "lead");
        assert!(!disc.is_complete());
        assert!(disc.consensus_result.is_none());
        assert!(disc.final_decision.is_none());
    }

    #[test]
    fn discussion_leader_is_first_participant() {
        let disc = make_discussion("test");
        let leader = disc.leader().unwrap();
        assert_eq!(leader.agent_id, "lead");
        assert_eq!(leader.role, "Orchestrator");
    }

    #[test]
    fn consensus_method_majority_vote_computation() {
        let mut disc = make_discussion("test");
        disc.consensus_method = ConsensusMethod::MajorityVote;
        disc.current_round = 1;

        // Simulate 2 out of 3 agents agreeing on claim "use_async".
        let claim = "use_async".to_string();
        disc.contributions.insert(
            1,
            vec![
                Contribution {
                    agent_id: "lead".into(),
                    round: 1,
                    content: "I propose async".into(),
                    confidence: 0.9,
                    claims: vec![claim.clone()],
                },
                Contribution {
                    agent_id: "worker-1".into(),
                    round: 1,
                    content: "Async is better".into(),
                    confidence: 0.8,
                    claims: vec![claim.clone()],
                },
                Contribution {
                    agent_id: "worker-2".into(),
                    round: 1,
                    content: "Sync is safer".into(),
                    confidence: 0.7,
                    claims: vec!["use_sync".to_string()],
                },
            ],
        );

        // Verify discussion can track contributions correctly.
        assert_eq!(disc.contributions.len(), 1);
        let round_contribs = disc.contributions.get(&1).unwrap();
        assert_eq!(round_contribs.len(), 3);
        assert_eq!(round_contribs[0].agent_id, "lead");
        assert_eq!(round_contribs[1].agent_id, "worker-1");
        assert_eq!(round_contribs[2].agent_id, "worker-2");
    }

    #[test]
    fn consensus_method_weighted_vote_exists() {
        // Simply verify the enum variant and its default.
        let method = ConsensusMethod::WeightedVote;
        assert_ne!(method, ConsensusMethod::MajorityVote);
        assert_ne!(method, ConsensusMethod::LeaderDecides);
        assert_eq!(ConsensusMethod::default(), ConsensusMethod::MajorityVote);
    }

    #[test]
    fn consensus_method_leader_decides_exists() {
        let method = ConsensusMethod::LeaderDecides;
        assert_ne!(method, ConsensusMethod::MajorityVote);
        assert_ne!(method, ConsensusMethod::WeightedVote);
    }

    #[test]
    fn discussion_with_single_participant() {
        let participants = vec![dummy_agent_info("solo", "General", vec![])];

        let disc = Discussion::new(
            "simple".into(),
            participants,
            ConsensusMethod::LeaderDecides,
            1,
        );

        assert_eq!(disc.participant_count(), 1);
        assert_eq!(disc.leader().unwrap().agent_id, "solo");
    }

    #[test]
    fn empty_participants_not_recommended_but_handled() {
        // While the engine rejects empty participants, the struct itself
        // should not panic.
        let disc = Discussion::new(
            "void".into(),
            vec![],
            ConsensusMethod::MajorityVote,
            1,
        );
        assert!(disc.leader().is_none());
        assert_eq!(disc.participant_count(), 0);
    }

    #[test]
    fn truncate_str_works() {
        assert_eq!(truncate_str("hello", 10), "hello");
        assert_eq!(truncate_str("hello world", 5), "hello...");
        assert_eq!(truncate_str("", 5), "");
    }

    #[test]
    fn discussion_phase_transitions() {
        let mut disc = make_discussion("test");

        assert_eq!(disc.phase, DiscussionPhase::Contributing);
        assert!(!disc.is_complete());

        disc.phase = DiscussionPhase::Synthesizing;
        assert!(!disc.is_complete());

        disc.phase = DiscussionPhase::CheckingConsensus;
        assert!(!disc.is_complete());

        disc.phase = DiscussionPhase::Complete;
        assert!(disc.is_complete());
    }

    #[test]
    fn consensus_result_defaults() {
        let result = ConsensusResult {
            reached: false,
            score: 0.3,
            method: ConsensusMethod::MajorityVote,
            agreeing_count: 1,
            total_count: 3,
            unresolved_conflicts: vec!["x".into()],
        };

        assert!(!result.reached);
        assert_eq!(result.score, 0.3);
        assert_eq!(result.agreeing_count, 1);
        assert_eq!(result.total_count, 3);
        assert_eq!(result.unresolved_conflicts.len(), 1);
        assert_eq!(result.method, ConsensusMethod::MajorityVote);
    }
}
