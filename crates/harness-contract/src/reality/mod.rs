use serde::{Deserialize, Serialize};

/// Boundary assigned to a fact, memory, or recall candidate before it can be
/// used as authoritative reality context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealityBoundary {
    Observed,
    Inferred,
    Simulated,
    Hypothetical,
    Conflict,
    Unknown,
}

impl RealityBoundary {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Inferred => "inferred",
            Self::Simulated => "simulated",
            Self::Hypothetical => "hypothetical",
            Self::Conflict => "conflict",
            Self::Unknown => "unknown",
        }
    }

    pub const fn can_be_authoritative(self) -> bool {
        matches!(self, Self::Observed | Self::Inferred)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ClaimSupportState {
    #[default]
    Unknown,
    Supported,
    Contradicted,
    Both,
}

impl ClaimSupportState {
    #[must_use]
    pub const fn combine(self, incoming: Self) -> Self {
        use ClaimSupportState::{Both, Contradicted, Supported, Unknown};
        match (self, incoming) {
            (Both, _) | (_, Both) | (Supported, Contradicted) | (Contradicted, Supported) => Both,
            (Supported, _) | (_, Supported) => Supported,
            (Contradicted, _) | (_, Contradicted) => Contradicted,
            (Unknown, Unknown) => Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCompleteness {
    #[default]
    None,
    Partial,
    Sufficient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSourceAuthority {
    Unverified,
    UserAsserted,
    RuntimeObserved,
    IndependentlyVerified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ConfidenceEstimate {
    Heuristic {
        basis: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_bp: Option<u16>,
    },
    Calibrated {
        value_bp: u16,
        calibration_ref: String,
    },
}

impl ConfidenceEstimate {
    pub fn calibrated(value_bp: u16, calibration_ref: impl Into<String>) -> Result<Self, String> {
        let calibration_ref = calibration_ref.into();
        if calibration_ref.trim().is_empty() {
            return Err("calibrated confidence requires a calibration reference".to_string());
        }
        Ok(Self::Calibrated {
            value_bp: value_bp.min(10_000),
            calibration_ref,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbabilityEstimate {
    pub value_bp: u16,
    pub calibration_ref: String,
}

impl ProbabilityEstimate {
    pub fn new(value_bp: u16, calibration_ref: impl Into<String>) -> Result<Self, String> {
        let calibration_ref = calibration_ref.into();
        if calibration_ref.trim().is_empty() {
            return Err("probability requires a calibration reference".to_string());
        }
        Ok(Self {
            value_bp: value_bp.min(10_000),
            calibration_ref,
        })
    }
}

/// Wiring-level status for Reality Core capabilities. This is intentionally
/// stricter than a generic health state: a feature can be configured and still
/// be reported as not wired into the active runtime path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealityCapabilityStatus {
    Disabled,
    ConfiguredButUnwired,
    Degraded,
    EnabledAndWired,
}

impl RealityCapabilityStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::ConfiguredButUnwired => "configured_but_unwired",
            Self::Degraded => "degraded",
            Self::EnabledAndWired => "enabled_and_wired",
        }
    }

    pub const fn is_fully_wired(self) -> bool {
        matches!(self, Self::EnabledAndWired)
    }
}

/// Logical source categories participating in context recall.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallSourceKind {
    Memory,
    Knowledge,
    Matrix,
    Fact,
    ToolTrace,
    SessionCheckpoint,
    AgentPeer,
    Workspace,
    Runtime,
}

impl RecallSourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Knowledge => "knowledge",
            Self::Matrix => "matrix",
            Self::Fact => "fact",
            Self::ToolTrace => "tool_trace",
            Self::SessionCheckpoint => "session_checkpoint",
            Self::AgentPeer => "agent_peer",
            Self::Workspace => "workspace",
            Self::Runtime => "runtime",
        }
    }
}

/// Stable evidence pointer shared by runtime, gateway, memory, matrix, and UI
/// projections.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EvidenceRef {
    pub ref_type: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub boundary: RealityBoundary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence_bp: Option<u16>,
}

impl EvidenceRef {
    /// Construct an evidence reference with an explicitly unknown boundary.
    ///
    /// Callers that possess checked evidence must use one of the named
    /// constructors below. This fail-closed constructor remains only for
    /// deserialisation migrations and never confers authority.
    pub fn new(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::unknown(ref_type, id)
    }

    fn with_explicit_boundary(
        ref_type: impl Into<String>,
        id: impl Into<String>,
        boundary: RealityBoundary,
    ) -> Self {
        Self {
            ref_type: ref_type.into(),
            id: id.into(),
            source: None,
            boundary,
            confidence_bp: None,
        }
    }

    pub fn observed(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::with_explicit_boundary(ref_type, id, RealityBoundary::Observed)
    }

    pub fn inferred(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::with_explicit_boundary(ref_type, id, RealityBoundary::Inferred)
    }

    pub fn simulated(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::with_explicit_boundary(ref_type, id, RealityBoundary::Simulated)
    }

    pub fn hypothetical(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::with_explicit_boundary(ref_type, id, RealityBoundary::Hypothetical)
    }

    pub fn conflict(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::with_explicit_boundary(ref_type, id, RealityBoundary::Conflict)
    }

    pub fn unknown(ref_type: impl Into<String>, id: impl Into<String>) -> Self {
        Self::with_explicit_boundary(ref_type, id, RealityBoundary::Unknown)
    }

    #[must_use]
    pub fn with_boundary(mut self, boundary: RealityBoundary) -> Self {
        self.boundary = boundary;
        self
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_confidence_bp(mut self, confidence_bp: u16) -> Self {
        self.confidence_bp = Some(confidence_bp.min(10_000));
        self
    }

    #[must_use]
    pub fn durable(id: impl Into<String>) -> Self {
        Self::observed("durable_evidence", id).with_source("runtime.artifact")
    }

    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Explains why a recall candidate was selected, omitted, or downgraded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecallSelectionReason {
    pub source: RecallSourceKind,
    pub score: f32,
    #[serde(default)]
    pub matched_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub omitted_reason: Option<String>,
}

impl RecallSelectionReason {
    pub fn selected(source: RecallSourceKind, score: f32, matched_by: Vec<String>) -> Self {
        Self {
            source,
            score,
            matched_by,
            omitted_reason: None,
        }
    }

    pub fn omitted(
        source: RecallSourceKind,
        score: f32,
        matched_by: Vec<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            source,
            score,
            matched_by,
            omitted_reason: Some(reason.into()),
        }
    }
}

/// Gateway-facing capability probe that makes partial wiring visible instead
/// of silently presenting a feature as ready.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealityCapabilityProbe {
    pub id: String,
    pub status: RealityCapabilityStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
}

impl RealityCapabilityProbe {
    pub fn new(id: impl Into<String>, status: RealityCapabilityStatus) -> Self {
        Self {
            id: id.into(),
            status,
            reason: None,
            evidence: Vec::new(),
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_evidence(mut self, evidence: EvidenceRef) -> Self {
        self.evidence.push(evidence);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_capability_status_as_snake_case() {
        let value = serde_json::to_value(RealityCapabilityStatus::ConfiguredButUnwired).unwrap();
        assert_eq!(value, serde_json::json!("configured_but_unwired"));
    }

    #[test]
    fn separates_authoritative_and_non_authoritative_boundaries() {
        assert!(RealityBoundary::Observed.can_be_authoritative());
        assert!(RealityBoundary::Inferred.can_be_authoritative());
        assert!(!RealityBoundary::Simulated.can_be_authoritative());
        assert!(!RealityBoundary::Hypothetical.can_be_authoritative());
        assert!(!RealityBoundary::Conflict.can_be_authoritative());
        assert!(!RealityBoundary::Unknown.can_be_authoritative());
    }

    #[test]
    fn support_reducer_is_order_independent() {
        assert_eq!(
            ClaimSupportState::Supported.combine(ClaimSupportState::Contradicted),
            ClaimSupportState::Both
        );
        assert_eq!(
            ClaimSupportState::Contradicted.combine(ClaimSupportState::Supported),
            ClaimSupportState::Both
        );
    }

    #[test]
    fn default_evidence_reference_is_not_observed() {
        assert_eq!(
            EvidenceRef::new("migration", "unknown").boundary,
            RealityBoundary::Unknown
        );
    }

    #[test]
    fn all_reality_boundaries_round_trip_without_collapsing() {
        for boundary in [
            RealityBoundary::Observed,
            RealityBoundary::Inferred,
            RealityBoundary::Simulated,
            RealityBoundary::Hypothetical,
            RealityBoundary::Conflict,
            RealityBoundary::Unknown,
        ] {
            let wire = serde_json::to_value(boundary).unwrap();
            assert_eq!(
                serde_json::from_value::<RealityBoundary>(wire).unwrap(),
                boundary
            );
        }
    }

    #[test]
    fn calibrated_probability_requires_provenance() {
        assert!(ProbabilityEstimate::new(5_000, "").is_err());
        assert!(ConfidenceEstimate::calibrated(5_000, "").is_err());
        assert!(ProbabilityEstimate::new(5_000, "calibration:test").is_ok());
    }
}
