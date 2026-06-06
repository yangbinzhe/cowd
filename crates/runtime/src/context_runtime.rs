//! Intelligent runtime context envelope.
//!
//! This module introduces a typed context boundary without changing provider
//! behavior yet. The first invariant is prompt-cache friendliness: stable
//! system instructions stay ahead of runtime and dynamic packets.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::prompt_cache::stable_hash_bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextMode {
    MainTurn,
    SoloGoal,
    YoloGoal,
    SubAgent,
    Collaboration,
    Review,
    Resume,
    Cron,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextProfile {
    MainTurn,
    SoloGoal,
    YoloGoal,
    SubAgent,
    Collaboration,
    Review,
    Resume,
    Cron,
}

impl From<ContextMode> for ContextProfile {
    fn from(mode: ContextMode) -> Self {
        match mode {
            ContextMode::MainTurn => Self::MainTurn,
            ContextMode::SoloGoal => Self::SoloGoal,
            ContextMode::YoloGoal => Self::YoloGoal,
            ContextMode::SubAgent => Self::SubAgent,
            ContextMode::Collaboration => Self::Collaboration,
            ContextMode::Review => Self::Review,
            ContextMode::Resume => Self::Resume,
            ContextMode::Cron => Self::Cron,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIdentity {
    pub session_id: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: String,
    pub parent_agent_id: Option<String>,
    pub team_id: Option<String>,
    pub mode: ContextMode,
}

impl ContextIdentity {
    pub fn main(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            project_id: None,
            task_id: None,
            agent_id: "primary".to_string(),
            parent_agent_id: None,
            team_id: None,
            mode: ContextMode::MainTurn,
        }
    }

    pub fn sub_agent(
        session_id: impl Into<String>,
        agent_id: impl Into<String>,
        parent_agent_id: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            project_id: None,
            task_id: None,
            agent_id: agent_id.into(),
            parent_agent_id: Some(parent_agent_id.into()),
            team_id: None,
            mode: ContextMode::SubAgent,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextSourceKind {
    StableHead,
    RuntimeHeader,
    Conversation,
    Memory,
    Task,
    ToolTrace,
    Workspace,
    AgentPeer,
    Handoff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextAuthority {
    System,
    User,
    Project,
    Session,
    Agent,
    Tool,
    Derived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextVisibility {
    Private,
    Shared,
    Team,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextRole {
    Instruction,
    Identity,
    Orientation,
    Evidence,
    Warning,
    TaskState,
    RecentTurn,
    ToolSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    pub id: String,
    pub source: ContextSourceKind,
    pub authority: ContextAuthority,
    pub visibility: ContextVisibility,
    pub role: ContextRole,
    pub content: String,
    pub token_estimate: u64,
    pub score: f32,
    pub evidence: Vec<String>,
}

impl ContextItem {
    pub fn new(
        id: impl Into<String>,
        source: ContextSourceKind,
        role: ContextRole,
        content: impl Into<String>,
    ) -> Self {
        let content = content.into();
        Self {
            id: id.into(),
            source,
            authority: ContextAuthority::Derived,
            visibility: ContextVisibility::Private,
            role,
            token_estimate: estimate_tokens(&content),
            content,
            score: 1.0,
            evidence: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextOmission {
    pub source: ContextSourceKind,
    pub reason: String,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextLease {
    pub source: ContextSourceKind,
    pub min_tokens: u64,
    pub target_tokens: u64,
    pub max_tokens: u64,
    pub priority: u8,
    pub degradation: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentContextLease {
    pub parent_session_id: String,
    pub parent_agent_id: String,
    pub child_agent_id: String,
    pub task_contract: String,
    pub allowed_sources: Vec<ContextSourceKind>,
    pub max_tokens: u64,
    pub required_return: Vec<AgentReturnRequirement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentReturnRequirement {
    ResultSummary,
    Evidence,
    Decisions,
    Conflicts,
    MemoryCandidates,
    NextActions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentReturnPacket {
    pub parent_session_id: String,
    pub child_agent_id: String,
    pub result_summary: String,
    pub evidence: Vec<String>,
    pub decisions: Vec<String>,
    pub conflicts: Vec<String>,
    pub memory_candidates: Vec<String>,
    pub next_actions: Vec<String>,
    pub failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolTracePacket {
    pub tool_name: String,
    pub invocation_id: String,
    pub status: ToolTraceStatus,
    pub summary: String,
    pub changed_files: Vec<String>,
    pub evidence_ids: Vec<String>,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolTraceStatus {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePacket {
    pub root: String,
    pub touched_files: Vec<String>,
    pub hot_symbols: Vec<String>,
    pub project_notes: Vec<String>,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeContextPacket {
    pub session_id: String,
    pub handoff_summary: Option<String>,
    pub active_task: Option<String>,
    pub recent_decisions: Vec<String>,
    pub blockers: Vec<String>,
    pub source: ResumeContextSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumeContextSource {
    SessionDb,
    Handoff,
    TaskRegistry,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudgetReport {
    pub total_tokens: u64,
    pub used_tokens: u64,
    pub leases: Vec<ContextLease>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextDiagnostics {
    pub stable_head_hash: String,
    pub runtime_header_hash: String,
    pub dynamic_tail_hash: String,
    pub degraded_sources: Vec<ContextSourceKind>,
    pub pressure_bp: u16,
    #[serde(default)]
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssembledContext {
    pub stable_head: Vec<String>,
    pub runtime_header: Vec<String>,
    pub dynamic_tail: Vec<String>,
}

impl AssembledContext {
    pub fn system_prompt(&self) -> Vec<String> {
        let mut prompt = Vec::with_capacity(
            self.stable_head.len() + self.runtime_header.len() + self.dynamic_tail.len(),
        );
        prompt.extend(self.stable_head.clone());
        prompt.extend(self.runtime_header.clone());
        prompt.extend(self.dynamic_tail.clone());
        prompt
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEnvelope {
    pub id: String,
    pub identity: ContextIdentity,
    pub profile: ContextProfile,
    pub intent: String,
    pub selected: Vec<ContextItem>,
    pub omitted: Vec<ContextOmission>,
    pub budget: ContextBudgetReport,
    pub diagnostics: ContextDiagnostics,
    pub assembled: AssembledContext,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ContextEnvelopeRequest {
    pub identity: ContextIdentity,
    pub profile: ContextProfile,
    pub intent: String,
    pub stable_head: Vec<String>,
    pub runtime_header: Vec<String>,
    pub dynamic_items: Vec<ContextItem>,
    pub omitted: Vec<ContextOmission>,
    pub total_budget_tokens: u64,
}

pub struct ContextRuntimeKernel;

impl ContextRuntimeKernel {
    pub fn build_envelope(request: ContextEnvelopeRequest) -> ContextEnvelope {
        let dynamic_tail = request
            .dynamic_items
            .iter()
            .map(format_context_item)
            .collect::<Vec<_>>();
        let used_tokens = request
            .dynamic_items
            .iter()
            .map(|item| item.token_estimate)
            .sum::<u64>()
            + request
                .stable_head
                .iter()
                .chain(request.runtime_header.iter())
                .map(|text| estimate_tokens(text))
                .sum::<u64>();
        let pressure_bp = if request.total_budget_tokens == 0 {
            0
        } else {
            ((used_tokens.saturating_mul(10_000)) / request.total_budget_tokens).min(10_000) as u16
        };
        let assembled = AssembledContext {
            stable_head: request.stable_head,
            runtime_header: request.runtime_header,
            dynamic_tail,
        };
        let diagnostics = ContextDiagnostics {
            stable_head_hash: hash_segments(&assembled.stable_head),
            runtime_header_hash: hash_segments(&assembled.runtime_header),
            dynamic_tail_hash: hash_segments(&assembled.dynamic_tail),
            degraded_sources: Vec::new(),
            pressure_bp,
            recommendations: context_recommendations(
                pressure_bp,
                request.dynamic_items.len(),
                request.omitted.len(),
            ),
        };
        let id = envelope_id(&request.identity, &request.intent, &diagnostics);

        ContextEnvelope {
            id,
            identity: request.identity,
            profile: request.profile,
            intent: request.intent,
            selected: request.dynamic_items,
            omitted: request.omitted,
            budget: ContextBudgetReport {
                total_tokens: request.total_budget_tokens,
                used_tokens,
                leases: Vec::new(),
            },
            diagnostics,
            assembled,
            created_at: Utc::now(),
        }
    }

    pub fn apply_leases(
        mut items: Vec<ContextItem>,
        leases: &[ContextLease],
    ) -> (Vec<ContextItem>, Vec<ContextOmission>) {
        items.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });

        let mut used_by_source = std::collections::BTreeMap::<String, u64>::new();
        let mut selected = Vec::new();
        let mut omitted = Vec::new();

        for item in items {
            let lease = leases.iter().find(|lease| lease.source == item.source);
            let max_tokens = lease.map(|lease| lease.max_tokens).unwrap_or(u64::MAX);
            let key = format!("{:?}", item.source);
            let used = used_by_source.get(&key).copied().unwrap_or(0);
            if used.saturating_add(item.token_estimate) > max_tokens {
                omitted.push(ContextOmission {
                    source: item.source,
                    reason: "context lease exhausted".to_string(),
                    token_estimate: item.token_estimate,
                });
                continue;
            }
            used_by_source.insert(key, used.saturating_add(item.token_estimate));
            selected.push(item);
        }

        (selected, omitted)
    }

    pub fn child_identity_from_lease(lease: &AgentContextLease) -> ContextIdentity {
        ContextIdentity::sub_agent(
            lease.parent_session_id.clone(),
            lease.child_agent_id.clone(),
            lease.parent_agent_id.clone(),
        )
    }

    pub fn agent_return_item(packet: &AgentReturnPacket) -> ContextItem {
        let mut content = format!(
            "Agent {} returned: {}",
            packet.child_agent_id, packet.result_summary
        );
        if !packet.decisions.is_empty() {
            content.push_str("\nDecisions:\n");
            for decision in &packet.decisions {
                content.push_str("- ");
                content.push_str(decision);
                content.push('\n');
            }
        }
        if !packet.conflicts.is_empty() {
            content.push_str("\nConflicts:\n");
            for conflict in &packet.conflicts {
                content.push_str("- ");
                content.push_str(conflict);
                content.push('\n');
            }
        }
        let mut item = ContextItem::new(
            format!("agent-return:{}", packet.child_agent_id),
            ContextSourceKind::AgentPeer,
            if packet.failed {
                ContextRole::Warning
            } else {
                ContextRole::Evidence
            },
            content,
        );
        item.authority = ContextAuthority::Agent;
        item.visibility = ContextVisibility::Shared;
        item.evidence = packet.evidence.clone();
        item
    }

    pub fn tool_trace_item(packet: &ToolTracePacket) -> ContextItem {
        let mut item = ContextItem::new(
            format!("tool-trace:{}", packet.invocation_id),
            ContextSourceKind::ToolTrace,
            ContextRole::ToolSummary,
            format!(
                "{} {:?}: {}",
                packet.tool_name, packet.status, packet.summary
            ),
        );
        item.authority = ContextAuthority::Tool;
        item.evidence = packet.evidence_ids.clone();
        item.token_estimate = packet.token_estimate;
        item
    }

    pub fn resume_item(packet: &ResumeContextPacket) -> ContextItem {
        let mut parts = Vec::new();
        if let Some(summary) = &packet.handoff_summary {
            parts.push(format!("Handoff: {summary}"));
        }
        if let Some(task) = &packet.active_task {
            parts.push(format!("Active task: {task}"));
        }
        if !packet.recent_decisions.is_empty() {
            parts.push(format!("Decisions: {}", packet.recent_decisions.join("; ")));
        }
        if !packet.blockers.is_empty() {
            parts.push(format!("Blockers: {}", packet.blockers.join("; ")));
        }
        let source = match packet.source {
            ResumeContextSource::SessionDb => ContextSourceKind::Conversation,
            ResumeContextSource::Handoff | ResumeContextSource::Mixed => ContextSourceKind::Handoff,
            ResumeContextSource::TaskRegistry => ContextSourceKind::Task,
        };
        let mut item = ContextItem::new(
            format!("resume:{}", packet.session_id),
            source,
            ContextRole::TaskState,
            parts.join("\n"),
        );
        item.authority = ContextAuthority::Session;
        item
    }
}

fn format_context_item(item: &ContextItem) -> String {
    format!(
        "<context_item source=\"{:?}\" role=\"{:?}\" score=\"{:.2}\">\n{}\n</context_item>",
        item.source, item.role, item.score, item.content
    )
}

fn estimate_tokens(text: &str) -> u64 {
    (text.len() as u64).div_ceil(4).max(1)
}

fn context_recommendations(
    pressure_bp: u16,
    selected_count: usize,
    omitted_count: usize,
) -> Vec<String> {
    let mut recommendations = Vec::new();
    if pressure_bp >= 9_000 {
        recommendations.push(
            "Start a handoff or session boundary before adding more large context".to_string(),
        );
        recommendations
            .push("Prefer summarized tool traces and memory packets over raw evidence".to_string());
    } else if pressure_bp >= 7_000 {
        recommendations
            .push("Review omitted context and compact low-value recent turns".to_string());
    }
    if omitted_count > 0 {
        recommendations.push(format!(
            "{omitted_count} context items were omitted; inspect omissions before relying on recall completeness"
        ));
    }
    if selected_count == 0 {
        recommendations.push(
            "No dynamic context selected; verify memory/session/task sources are available"
                .to_string(),
        );
    }
    recommendations
}

fn hash_segments(segments: &[String]) -> String {
    let mut bytes = Vec::new();
    for segment in segments {
        bytes.extend_from_slice(segment.as_bytes());
        bytes.push(0);
    }
    format!("{:016x}", stable_hash_bytes(&bytes))
}

fn envelope_id(
    identity: &ContextIdentity,
    intent: &str,
    diagnostics: &ContextDiagnostics,
) -> String {
    let raw = format!(
        "{}:{}:{:?}:{}:{}:{}",
        identity.session_id,
        identity.agent_id,
        identity.mode,
        intent,
        diagnostics.runtime_header_hash,
        diagnostics.dynamic_tail_hash
    );
    format!("{:016x}", stable_hash_bytes(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_dynamic(content: &str) -> ContextEnvelopeRequest {
        let identity = ContextIdentity::main("session-1");
        ContextEnvelopeRequest {
            profile: ContextProfile::from(identity.mode),
            identity,
            intent: "ship context runtime".to_string(),
            stable_head: vec!["system: stable instructions".to_string()],
            runtime_header: vec!["runtime: main session".to_string()],
            dynamic_items: vec![ContextItem::new(
                "memory-1",
                ContextSourceKind::Memory,
                ContextRole::Orientation,
                content,
            )],
            omitted: Vec::new(),
            total_budget_tokens: 10_000,
        }
    }

    #[test]
    fn envelope_preserves_prompt_segment_order() {
        let envelope = ContextRuntimeKernel::build_envelope(request_with_dynamic("dynamic memory"));
        let prompt = envelope.assembled.system_prompt();

        assert_eq!(prompt[0], "system: stable instructions");
        assert_eq!(prompt[1], "runtime: main session");
        assert!(prompt[2].contains("dynamic memory"));
    }

    #[test]
    fn stable_head_hash_survives_dynamic_tail_changes() {
        let a = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory alpha"));
        let b = ContextRuntimeKernel::build_envelope(request_with_dynamic("memory beta"));

        assert_eq!(
            a.diagnostics.stable_head_hash,
            b.diagnostics.stable_head_hash
        );
        assert_eq!(
            a.diagnostics.runtime_header_hash,
            b.diagnostics.runtime_header_hash
        );
        assert_ne!(
            a.diagnostics.dynamic_tail_hash,
            b.diagnostics.dynamic_tail_hash
        );
    }

    #[test]
    fn sub_agent_identity_tracks_parent() {
        let identity = ContextIdentity::sub_agent("session-1", "reviewer", "primary");

        assert_eq!(identity.mode, ContextMode::SubAgent);
        assert_eq!(identity.agent_id, "reviewer");
        assert_eq!(identity.parent_agent_id.as_deref(), Some("primary"));
    }

    #[test]
    fn envelope_serializes_for_ui_diagnostics() {
        let envelope = ContextRuntimeKernel::build_envelope(request_with_dynamic("serialize me"));
        let json = serde_json::to_string(&envelope).expect("envelope should serialize");

        assert!(json.contains("stable_head_hash"));
        assert!(json.contains("dynamic_tail_hash"));
        assert!(json.contains("recommendations"));
        assert!(json.contains("serialize me"));
    }

    #[test]
    fn envelope_reports_pressure_recommendations() {
        let mut request = request_with_dynamic(&"x".repeat(3_600));
        request.total_budget_tokens = 1_000;
        request.omitted.push(ContextOmission {
            source: ContextSourceKind::Memory,
            reason: "lease exhausted".to_string(),
            token_estimate: 128,
        });

        let envelope = ContextRuntimeKernel::build_envelope(request);

        assert!(envelope.diagnostics.pressure_bp >= 9_000);
        assert!(
            envelope
                .diagnostics
                .recommendations
                .iter()
                .any(|item| item.contains("handoff"))
        );
        assert!(
            envelope
                .diagnostics
                .recommendations
                .iter()
                .any(|item| item.contains("omitted"))
        );
    }

    #[test]
    fn lease_budget_omits_items_deterministically() {
        let mut high = ContextItem::new(
            "b-high",
            ContextSourceKind::Memory,
            ContextRole::Orientation,
            "keep",
        );
        high.score = 0.9;
        high.token_estimate = 4;
        let mut low = ContextItem::new(
            "a-low",
            ContextSourceKind::Memory,
            ContextRole::Orientation,
            "omit",
        );
        low.score = 0.1;
        low.token_estimate = 4;
        let lease = ContextLease {
            source: ContextSourceKind::Memory,
            min_tokens: 0,
            target_tokens: 4,
            max_tokens: 4,
            priority: 8,
            degradation: vec!["omit lower score".to_string()],
        };

        let (selected, omitted) = ContextRuntimeKernel::apply_leases(vec![low, high], &[lease]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "b-high");
        assert_eq!(omitted.len(), 1);
        assert_eq!(omitted[0].reason, "context lease exhausted");
    }

    #[test]
    fn agent_context_lease_creates_child_identity_and_return_item() {
        let lease = AgentContextLease {
            parent_session_id: "session-1".to_string(),
            parent_agent_id: "primary".to_string(),
            child_agent_id: "reviewer".to_string(),
            task_contract: "review diff".to_string(),
            allowed_sources: vec![ContextSourceKind::Memory, ContextSourceKind::ToolTrace],
            max_tokens: 2_000,
            required_return: vec![AgentReturnRequirement::ResultSummary],
        };
        let identity = ContextRuntimeKernel::child_identity_from_lease(&lease);
        assert_eq!(identity.mode, ContextMode::SubAgent);
        assert_eq!(identity.parent_agent_id.as_deref(), Some("primary"));

        let packet = AgentReturnPacket {
            parent_session_id: "session-1".to_string(),
            child_agent_id: "reviewer".to_string(),
            result_summary: "diff is safe".to_string(),
            evidence: vec!["test:passed".to_string()],
            decisions: vec!["ship".to_string()],
            conflicts: Vec::new(),
            memory_candidates: Vec::new(),
            next_actions: Vec::new(),
            failed: false,
        };
        let item = ContextRuntimeKernel::agent_return_item(&packet);
        assert_eq!(item.source, ContextSourceKind::AgentPeer);
        assert_eq!(item.authority, ContextAuthority::Agent);
        assert!(item.content.contains("diff is safe"));
    }

    #[test]
    fn tool_trace_and_resume_packets_become_context_items() {
        let trace = ToolTracePacket {
            tool_name: "bash".to_string(),
            invocation_id: "tool-1".to_string(),
            status: ToolTraceStatus::Failed,
            summary: "cargo test failed in parser".to_string(),
            changed_files: vec!["src/parser.rs".to_string()],
            evidence_ids: vec!["event-9".to_string()],
            token_estimate: 12,
        };
        let trace_item = ContextRuntimeKernel::tool_trace_item(&trace);
        assert_eq!(trace_item.source, ContextSourceKind::ToolTrace);
        assert_eq!(trace_item.token_estimate, 12);
        assert!(trace_item.content.contains("parser"));

        let resume = ResumeContextPacket {
            session_id: "session-1".to_string(),
            handoff_summary: Some("continue context runtime".to_string()),
            active_task: Some("phase 6".to_string()),
            recent_decisions: vec!["db-first".to_string()],
            blockers: Vec::new(),
            source: ResumeContextSource::Mixed,
        };
        let resume_item = ContextRuntimeKernel::resume_item(&resume);
        assert_eq!(resume_item.source, ContextSourceKind::Handoff);
        assert!(resume_item.content.contains("phase 6"));

        let mut task_resume = resume.clone();
        task_resume.source = ResumeContextSource::TaskRegistry;
        let task_item = ContextRuntimeKernel::resume_item(&task_resume);
        assert_eq!(task_item.source, ContextSourceKind::Task);
        assert!(task_item.content.contains("phase 6"));
    }
}
