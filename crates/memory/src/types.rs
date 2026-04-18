//! Core types for the unified memory framework.
//!
//! All primitive data structures used across the memory system are defined here,
//! including memory entries, metadata, relations, handoff data, seeds, and
//! compression-related types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// --- Primitive type aliases ---

pub type MemoryId = Uuid;
pub type SeedId = Uuid;

// --- Enumerations ---

/// The memory layer a given entry belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryLayer {
    /// L0 – identity / global facts that never change.
    L0,
    /// L1 – essential working memory (high-churn, short-lived).
    L1,
    /// L2 – project-specific conventions and decisions.
    L2,
    /// L3 – deep, long-term knowledge accumulation.
    L3,
    /// L4 – shared / team-scoped memory.
    L4,
}

/// Semantic category of a memory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryCategory {
    UserPreference,
    ProjectConvention,
    Decision,
    Reference,
    Shared,
    CompressedSummary,
}

/// Priority of a memory entry, used during budget allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    Critical,
    High,
    Normal,
    Low,
}

/// How the memory entry was originally created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemorySource {
    UserExplicit,
    AutoExtracted,
    Compression,
    Import,
}

// --- Memory entry ---

/// A single unit of persistent memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub id: MemoryId,
    pub layer: MemoryLayer,
    pub category: MemoryCategory,
    pub priority: Priority,
    pub source: MemorySource,
    /// Short human-readable title.
    pub title: String,
    /// Full markdown-formatted content.
    pub content: String,
    /// Optional embedding vector for semantic search.
    pub embedding: Option<Vec<f32>>,
    /// Tags for faceted filtering.
    pub tags: Vec<String>,
    /// IDs of related entries.
    pub relations: Vec<Relation>,
    /// Confidence score in [0.0, 1.0].
    pub confidence: f32,
    /// Access frequency counter.
    pub access_count: u64,
    /// Staleness score; higher = more likely to be pruned.
    pub staleness: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_accessed_at: Option<DateTime<Utc>>,
    /// Project or workspace scope; `None` means global.
    pub scope: Option<String>,
    /// Session ID that created this entry.
    pub session_id: Option<String>,
}

// --- Memory metadata (frontmatter) ---

/// Lightweight metadata summary used for listing / indexing without loading full content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMeta {
    pub id: MemoryId,
    pub layer: MemoryLayer,
    pub category: MemoryCategory,
    pub priority: Priority,
    pub title: String,
    pub tags: Vec<String>,
    pub confidence: f32,
    pub access_count: u64,
    pub staleness: f32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub scope: Option<String>,
}

// --- Relations ---

/// A directed relationship between two memory entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub target_id: MemoryId,
    pub kind: RelationKind,
    pub strength: f32,
    /// Optional timestamp for temporal knowledge graphs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temporal: Option<TemporalMarker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RelationKind {
    /// This entry depends on the target.
    DependsOn,
    /// This entry supersedes the target.
    Supersedes,
    /// This entry is a summary of the target.
    Summarizes,
    /// Generic association.
    Related,
    /// Temporal: this happened before the target (causal ordering).
    Before,
    /// Temporal: this happened after the target.
    After,
    /// Temporal: concurrent with target.
    Concurrent,
    /// Project-level: causes this outcome.
    Causes,
    /// Project-level: this is the result of target.
    ResultsFrom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalMarker {
    /// When this relation was established.
    pub established_at: chrono::DateTime<chrono::Utc>,
    /// Optional time range for the relationship.
    pub valid_from: Option<chrono::DateTime<chrono::Utc>>,
    pub valid_until: Option<chrono::DateTime<chrono::Utc>>,
    /// Sequence order for events with same timestamp.
    pub sequence: u32,
}

// --- Context monitoring ---

/// Alert level reported by the context monitor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AlertLevel {
    Normal,
    Warning,
    Critical,
}

/// Action recommended by the context monitor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContextAction {
    /// Continue normally.
    Continue,
    /// Avoid starting expensive / complex work.
    AvoidComplexWork,
    /// Persist state and pause until next session.
    SaveStateAndPause { handoff: HandoffData },
}

/// Snapshot of current context window usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextMonitor {
    pub used_tokens: u64,
    pub total_tokens: u64,
    pub alert_level: AlertLevel,
    pub recommended_action: ContextAction,
    pub sampled_at: DateTime<Utc>,
}

// --- Cross-session handoff ---

/// Data package handed off from one session to the next.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffData {
    pub session_id: String,
    pub timestamp: DateTime<Utc>,
    pub work_items: Vec<WorkItem>,
    pub decisions: Vec<Decision>,
    pub blockers: Vec<Blocker>,
    pub task_states: Vec<TaskState>,
    pub summary: String,
}

/// A discrete unit of work carried across sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: String,
    pub title: String,
    pub description: String,
    pub status: WorkItemStatus,
    pub priority: Priority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemStatus {
    Pending,
    InProgress,
    Blocked,
    Done,
}

/// A recorded decision with rationale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub id: String,
    pub summary: String,
    pub rationale: String,
    pub status: DecisionStatus,
    pub made_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecisionStatus {
    Implemented,
    Superseded,
    Deferred,
}

/// A blocker preventing forward progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blocker {
    pub id: String,
    pub description: String,
    pub resolution_hint: Option<String>,
}

/// Serialisable state of a long-running task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskState {
    pub task_id: String,
    pub progress_percent: u8,
    pub last_checkpoint: String,
    pub context: serde_json::Value,
}

// --- Seed system ---

/// A named "seed" that injects pre-written context at the right moment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Seed {
    pub id: SeedId,
    pub name: String,
    pub content: String,
    pub trigger: SeedTrigger,
    pub priority: Priority,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

/// Condition under which a seed is activated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SeedTrigger {
    /// Activate when entering a named phase.
    Phase(String),
    /// Activate when any of the keywords are mentioned.
    Keyword(Vec<String>),
    /// Activate at or after a specific datetime.
    Time(DateTime<Utc>),
    /// Always active; manually managed.
    Manual,
}

/// A thread of related decisions tracked over time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionThread {
    pub id: String,
    pub topic: String,
    pub entries: Vec<DecisionEntry>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A single entry within a `DecisionThread`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionEntry {
    pub id: String,
    pub summary: String,
    pub rationale: String,
    pub status: DecisionStatus,
    pub alternatives: Vec<String>,
    pub made_at: DateTime<Utc>,
}

// --- Conversation message types ---

/// Role of a conversation participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

impl std::fmt::Display for MessageRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageRole::System => write!(f, "system"),
            MessageRole::User => write!(f, "user"),
            MessageRole::Assistant => write!(f, "assistant"),
            MessageRole::Tool => write!(f, "tool"),
        }
    }
}

/// A single turn in a conversation context window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique turn index (0-based); used for age calculation.
    pub turn_index: usize,
    /// Who sent this message.
    pub role: MessageRole,
    /// Text content.
    pub content: String,
    /// Optional tool call identifier (for tool result messages).
    pub tool_use_id: Option<String>,
    /// Name of the tool that produced this result, if any.
    pub tool_name: Option<String>,
    /// Whether this message is pinned and must not be compressed away.
    pub pinned: bool,
}

impl Message {
    /// Create a simple user message.
    #[must_use]
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            turn_index: 0,
            role: MessageRole::User,
            content: content.into(),
            tool_use_id: None,
            tool_name: None,
            pinned: false,
        }
    }

    /// Create a simple assistant message.
    #[must_use]
    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            turn_index: 0,
            role: MessageRole::Assistant,
            content: content.into(),
            tool_use_id: None,
            tool_name: None,
            pinned: false,
        }
    }

    /// Create a tool result message.
    #[must_use]
    pub fn tool_result(
        tool_use_id: impl Into<String>,
        tool_name: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            turn_index: 0,
            role: MessageRole::Tool,
            content: content.into(),
            tool_use_id: Some(tool_use_id.into()),
            tool_name: Some(tool_name.into()),
            pinned: false,
        }
    }

    /// Approximate token count for this message (chars / 4).
    #[must_use]
    pub fn token_estimate(&self) -> u32 {
        (self.content.len() as u32).div_ceil(4)
    }

    /// Returns `true` if this is a tool result message.
    #[must_use]
    pub fn is_tool_result(&self) -> bool {
        self.role == MessageRole::Tool
    }
}

// --- Compression result types ---

/// Statistics returned after a compression stage completes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompactionResult {
    /// Approximate tokens in the message list before compression.
    pub tokens_before: u32,
    /// Approximate tokens in the message list after compression.
    pub tokens_after: u32,
    /// Number of memory entries written to persistent storage.
    pub memories_extracted: u32,
    /// Approximate token count of any generated summary.
    pub summary_tokens: u32,
}

impl CompactionResult {
    /// Compute the token reduction ratio (0.0 = no reduction, 1.0 = all gone).
    #[must_use]
    pub fn reduction_ratio(&self) -> f32 {
        if self.tokens_before == 0 {
            return 0.0;
        }
        1.0 - (self.tokens_after as f32 / self.tokens_before as f32)
    }
}

// --- Compression / budget types ---

/// Token budget allocation for context preparation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBudget {
    pub total: u64,
    pub reserved_system: u64,
    pub reserved_response: u64,
    pub allocated_memory: u64,
    pub allocated_conversation: u64,
    pub available: u64,
}

impl TokenBudget {
    /// Compute the actually available tokens given all reservations.
    #[must_use]
    pub fn compute_available(&self) -> u64 {
        self.total
            .saturating_sub(self.reserved_system)
            .saturating_sub(self.reserved_response)
            .saturating_sub(self.allocated_memory)
            .saturating_sub(self.allocated_conversation)
    }
}

/// The assembled context ready for injection into the model prompt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedContext {
    pub entries: Vec<MemoryEntry>,
    pub total_tokens: u64,
    pub budget: TokenBudget,
    pub depth_scale: f32,
    pub prepared_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// FTS5 Full-text search types
// ---------------------------------------------------------------------------

/// Search mode for FTS5 queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchMode {
    /// Standard FTS5 MATCH query.
    Match,
    /// Boolean FTS5 query with AND/OR/NOT operators.
    Boolean,
    /// Prefix search for autocomplete-style queries.
    Prefix,
}

/// Request for full-text memory search, matching Hermes-Agent sessions pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMemoriesRequest {
    /// The search query string.
    pub query: String,
    /// Optional category filter.
    pub category: Option<MemoryCategory>,
    /// Optional layer filter.
    pub layer: Option<MemoryLayer>,
    /// Maximum number of results to return.
    pub limit: usize,
    /// Search mode (default: Match).
    pub mode: SearchMode,
    /// Include highlighted snippets in results.
    pub with_snippets: bool,
    /// Include matched keywords in results.
    pub with_keywords: bool,
}

impl Default for SearchMemoriesRequest {
    fn default() -> Self {
        Self {
            query: String::new(),
            category: None,
            layer: None,
            limit: 10,
            mode: SearchMode::Match,
            with_snippets: true,
            with_keywords: true,
        }
    }
}

/// A highlighted snippet from FTS5 search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSnippet {
    /// The highlighted text with match markers.
    pub text: String,
    /// Match positions in the original text.
    pub positions: Vec<u32>,
}

/// A keyword extracted from FTS5 search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedKeyword {
    /// The keyword that matched.
    pub keyword: String,
    /// Number of occurrences.
    pub count: u32,
}

/// Result of a full-text memory search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMemoriesResult {
    /// Matching memory entries.
    pub entries: Vec<MemoryEntry>,
    /// Highlighted snippets for each entry (if requested).
    pub snippets: Vec<Option<SearchSnippet>>,
    /// Keywords that triggered the match (if requested).
    pub keywords: Vec<MatchedKeyword>,
    /// Total number of matches found (may exceed limit).
    pub total_matches: usize,
    /// Search query that was executed.
    pub query: String,
    /// Categories found in results (for explore mode).
    pub categories_found: Vec<MemoryCategory>,
    /// Search mode used: "semantic", "local", or "keyword".
    #[serde(default = "default_search_mode")]
    pub search_mode: String,
}

fn default_search_mode() -> String {
    "keyword".to_string()
}
