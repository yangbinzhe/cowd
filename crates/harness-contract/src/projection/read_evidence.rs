//! Authenticated content delivery, distinct from acquisition and full-file coverage.
use serde::{Deserialize, Serialize};

pub const READ_EVIDENCE_SCHEMA_VERSION: u32 = 1;
pub const READ_EVIDENCE_MAX_LIMIT: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidenceQuery {
    #[serde(default)]
    pub after_sequence: u64,
    pub through: Option<u64>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}
fn default_limit() -> usize {
    READ_EVIDENCE_MAX_LIMIT
}
impl Default for ReadEvidenceQuery {
    fn default() -> Self {
        Self {
            after_sequence: 0,
            through: None,
            limit: default_limit(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidenceSource {
    pub stream_id: String,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadEvidenceIncompleteReason {
    Budget,
    SourceChanged,
    UnsupportedIdentity,
    AuthenticationFailed,
    ZeroProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidencePage {
    pub schema_version: u32,
    pub execution_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub workspace_id: String,
    pub authorization_revision: u64,
    pub redaction_revision: String,
    pub graph_revision: u64,
    /// Global durable commit cursor, never a stream sequence.
    pub through: u64,
    pub next_after_sequence: u64,
    pub exhausted: bool,
    pub incomplete: bool,
    pub reason: Option<ReadEvidenceIncompleteReason>,
    pub source_incarnation: String,
    pub source_mutation_epoch: u64,
    pub sources: Vec<ReadEvidenceSource>,
    pub reads: Vec<VerifiedReadEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidenceEventRef {
    pub stream_id: String,
    pub sequence: u64,
    pub event_id: String,
    pub commit_cursor: u64,
    pub payload_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadEvidenceRecipient {
    Agent {
        identity: crate::execution::ExecutionIdentity,
        graph_id: String,
        node_id: String,
        run_id: String,
        attempt: u32,
    },
    Root {
        binding: crate::projection::RuntimeActivityBinding,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadEvidenceGrade {
    ProviderModelObserved,
    AgentChannelSend,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReadEvidenceExtent {
    FileLines {
        start_line: u64,
        line_count: u64,
        total_lines: u64,
        ends_with_newline: bool,
        body_sha256: String,
        body_bytes: u64,
        source_omitted: bool,
    },
    ArtifactChunks {
        total_chunks: u64,
        chunks: Vec<ReadEvidenceChunk>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidenceChunk {
    pub index: u64,
    pub body_sha256: String,
    pub body_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedReadEvidence {
    pub path: crate::context::WorkspacePathIdentity,
    pub scope: crate::policy::PermissionScope,
    pub recipient: ReadEvidenceRecipient,
    pub step: crate::step::StepIdentity,
    pub call_id: String,
    pub tool_name: String,
    pub invocation_id: String,
    pub delivery: ReadEvidenceEventRef,
    pub acquisition: ReadEvidenceEventRef,
    pub index: ReadEvidenceEventRef,
    pub source_sha256: String,
    pub source_bytes: u64,
    pub delivered_extent: Option<ReadEvidenceExtent>,
    pub grade: ReadEvidenceGrade,
    pub body_delivered: bool,
}
