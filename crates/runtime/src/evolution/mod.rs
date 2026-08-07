pub mod candidate_kind;
pub mod capability_goal;
pub mod case;
pub mod diagnosis;
pub(crate) mod discovery;
pub mod governance;
pub mod lifecycle;
pub mod mission;
pub mod planner;
pub(crate) mod projector;
pub mod signal;
pub mod triage;

pub use candidate_kind::{
    candidate_kind_from_proposal, candidate_kinds_from_root_cause, EvolutionCandidateKind,
};
pub use capability_goal::EvolutionCapabilityGoal;
pub use case::{
    EvolutionCase, EvolutionCaseCatalogPage, EvolutionCaseIndex, EvolutionCaseKey,
    EvolutionCasePage, EvolutionCaseState, EvolutionCaseSummary, EVOLUTION_CASE_CATALOG_PAGE_SIZE,
};
pub use diagnosis::{
    EvolutionDiagnosis, EvolutionDiagnosisEngine, EvolutionHypothesis, EvolutionRootCauseKind,
};
pub(crate) use discovery::EvolutionDiscoveryService;
pub(crate) use governance::EvolutionCandidateRegistration;
pub use governance::{
    CanaryObservationReport, CanaryRolloutPolicy, EvaluationDirection,
    EvaluationPolicyChangeIntent, EvaluationPolicyChangeReview, EvolutionCandidateIntent,
    EvolutionCandidateLifecycle, EvolutionCandidateSubject, EvolutionComparisonDimension,
    EvolutionComparisonReportV2, EvolutionEvalRunner, EvolutionEvaluationReadiness,
    EvolutionGovernanceCandidate, EvolutionGovernanceError, EvolutionGovernanceService,
    EvolutionReleaseAssignment, ReleaseChangeAction, ReleaseChangeRequest, ReleaseChangeReview,
    ReleaseChangeReviewClass, ReleaseChangeReviewDecision, ReleaseChangeReviewStatus,
};
pub use lifecycle::{EvolutionLifecycleDraft, EvolutionLifecycleService};
pub use mission::{EvolutionMission, EvolutionMissionStatus};
pub use planner::{
    EvolutionPlanDraft, EvolutionProposal, EvolutionProposalKind, EvolutionProposalRisk,
    EvolutionSkillDraft,
};
pub use projector::EvolutionProjectorHealth;
pub(crate) use projector::EvolutionSignalProjector;
pub use signal::{
    EvolutionSignal, EvolutionSignalInput, EvolutionSignalScope, EvolutionSignalSeverity,
    EvolutionSignalSource, EvolutionSignalType,
};
pub use triage::{EvolutionTriageCluster, EvolutionTriageService};
