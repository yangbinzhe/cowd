pub mod adoption;
pub mod applied_registry;
pub mod artifact_builder;
pub mod candidate;
pub mod candidate_generator;
pub mod candidate_kind;
pub mod capability_goal;
pub mod diagnosis;
pub mod evaluation_request;
pub mod isolated_runner;
pub mod lifecycle;
pub mod memory_activation;
pub mod memory_bridge;
pub mod memory_scope;
pub mod mission;
pub mod planner;
pub mod promotion;
pub mod rollback;
pub mod runner_policy;
pub mod runner_result;
pub mod sandbox;
pub mod signal;
pub mod signal_bridge;
pub mod triage;
pub mod versioning;
pub mod worktree_runner;

pub use adoption::{EvolutionAdoptionManager, EvolutionAdoptionReceipt};
pub use applied_registry::{EvolutionAppliedCapabilityRecord, EvolutionAppliedCapabilityRegistry};
pub use artifact_builder::{EvolutionArtifactBuilder, EvolutionGeneratedArtifact};
pub use candidate::{
    EvolutionCandidate, EvolutionCandidatePlan, EvolutionCandidateStatus, EvolutionCandidateStore,
};
pub use candidate_generator::EvolutionCandidateGenerator;
pub use candidate_kind::{
    candidate_kind_from_proposal, candidate_kinds_from_root_cause, EvolutionCandidateKind,
};
pub use capability_goal::EvolutionCapabilityGoal;
pub use diagnosis::{
    EvolutionDiagnosis, EvolutionDiagnosisEngine, EvolutionDiagnosisStore, EvolutionRootCauseKind,
};
pub use evaluation_request::{
    EvolutionComparisonReport, EvolutionEvaluationRequest, EvolutionMetric,
};
pub use isolated_runner::IsolatedRunner;
pub use lifecycle::{EvolutionLifecycleDraft, EvolutionLifecycleService};
pub use memory_activation::evolution_memory_context_items;
pub use memory_bridge::{EvolutionMemoryBridge, EvolutionMemoryRecord};
pub use memory_scope::EvolutionMemoryScope;
pub use mission::{EvolutionMission, EvolutionMissionStatus, EvolutionMissionStore};
pub use planner::{
    EvolutionPlanDraft, EvolutionProposal, EvolutionProposalKind, EvolutionProposalRisk,
    EvolutionProposalStore, EvolutionSkillDraft,
};
pub use promotion::{
    EvolutionPromotionAdapter, EvolutionPromotionManager, EvolutionPromotionReceipt,
};
pub use rollback::{EvolutionRollbackManager, EvolutionRollbackReceipt};
pub use runner_policy::EvolutionRunnerPolicy;
pub use runner_result::EvolutionRunnerResult;
pub use sandbox::{
    EvolutionSandboxEval, EvolutionSandboxOrchestrator, EvolutionSandboxRecommendation,
    EvolutionSandboxStore, EvolutionVerificationResult,
};
pub use signal::{
    EvolutionSignal, EvolutionSignalInput, EvolutionSignalSeverity, EvolutionSignalSource,
    EvolutionSignalStore, EvolutionSignalType,
};
pub use signal_bridge::{signal_from_self_regulation_decision, EvolutionSignalCollector};
pub use triage::{EvolutionTriageCluster, EvolutionTriageService};
pub use versioning::EvolutionVersionRecord;
pub use worktree_runner::WorktreeRunner;
