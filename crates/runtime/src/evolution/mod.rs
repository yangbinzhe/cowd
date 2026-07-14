pub mod candidate_kind;
pub mod capability_goal;
pub mod diagnosis;
pub mod governance;
pub mod lifecycle;
pub mod mission;
pub mod planner;
pub mod signal;
pub mod signal_bridge;
pub mod triage;

pub use candidate_kind::{
    candidate_kind_from_proposal, candidate_kinds_from_root_cause, EvolutionCandidateKind,
};
pub use capability_goal::EvolutionCapabilityGoal;
pub use diagnosis::{
    EvolutionDiagnosis, EvolutionDiagnosisEngine, EvolutionDiagnosisStore, EvolutionRootCauseKind,
};
pub(crate) use governance::EvolutionCandidateRegistration;
pub use governance::{
    CanaryObservationReport, CanaryRolloutPolicy, EvaluationDirection,
    EvaluationPolicyChangeIntent, EvaluationPolicyChangeReview, EvolutionCandidateIntent,
    EvolutionCandidateLifecycle, EvolutionCandidateSubject, EvolutionComparisonDimension,
    EvolutionComparisonReportV2, EvolutionEvalRunner, EvolutionGovernanceCandidate,
    EvolutionGovernanceError, EvolutionGovernanceService, EvolutionReleaseAssignment,
    ReleaseChangeAction, ReleaseChangeRequest, ReleaseChangeReview, ReleaseChangeReviewClass,
    ReleaseChangeReviewDecision, ReleaseChangeReviewStatus,
};
pub use lifecycle::{EvolutionLifecycleDraft, EvolutionLifecycleService};
pub use mission::{EvolutionMission, EvolutionMissionStatus, EvolutionMissionStore};
pub use planner::{
    EvolutionPlanDraft, EvolutionProposal, EvolutionProposalKind, EvolutionProposalRisk,
    EvolutionProposalStore, EvolutionSkillDraft,
};
pub use signal::{
    EvolutionSignal, EvolutionSignalInput, EvolutionSignalSeverity, EvolutionSignalSource,
    EvolutionSignalStore, EvolutionSignalType,
};
pub use signal_bridge::{signal_from_intervention, EvolutionSignalCollector};
pub use triage::{EvolutionTriageCluster, EvolutionTriageService};
