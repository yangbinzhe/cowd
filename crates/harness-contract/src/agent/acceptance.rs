//! Generic Agent output acceptance contracts.
//!
//! These checks are compiled by Runtime and evaluated against durable Agent
//! evidence. They do not prescribe a Team topology or require the model to
//! emit a framework-owned execution structure.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputAcceptanceRequirement {
    pub criterion: String,
    pub check: OutputAcceptanceCheck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputAcceptanceCheck {
    StructuredField {
        field: StructuredOutputField,
    },
    StructuredArtifact {
        name: String,
    },
    ScopedEvidence {
        scopes: Vec<String>,
    },
    WorkspaceChange {
        field: StructuredOutputField,
        scopes: Vec<String>,
    },
    SourceVerification {
        scopes: Vec<String>,
    },
    UpstreamReview,
    UpstreamEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuredOutputField {
    Summary,
    Findings,
    Plan,
    Implementation,
    SourceVerification,
    Review,
    Risks,
    Unresolved,
    KeyDecisions,
    UnresolvedOrRisks,
    Proposal,
    Critique,
    Mitigation,
    Checkpoint,
}

impl StructuredOutputField {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::Findings => "findings",
            Self::Plan => "plan",
            Self::Implementation => "implementation",
            Self::SourceVerification => "source_verification",
            Self::Review => "review",
            Self::Risks => "risks",
            Self::Unresolved => "unresolved",
            Self::KeyDecisions => "key_decisions",
            Self::UnresolvedOrRisks => "unresolved_or_risks",
            Self::Proposal => "proposal",
            Self::Critique => "critique",
            Self::Mitigation => "mitigation",
            Self::Checkpoint => "checkpoint",
        }
    }
}
