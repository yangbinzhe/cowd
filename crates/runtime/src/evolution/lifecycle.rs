use super::{
    candidate_kind::candidate_kinds_from_root_cause, diagnosis::EvolutionDiagnosisEngine,
    mission::EvolutionMission, planner::EvolutionProposal, signal::EvolutionSignal,
    triage::EvolutionTriageService, EvolutionCapabilityGoal,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvolutionLifecycleDraft {
    pub mission: EvolutionMission,
    pub proposal: EvolutionProposal,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionLifecycleService;

impl EvolutionLifecycleService {
    #[must_use]
    pub fn open_from_signals(signals: &[EvolutionSignal]) -> Vec<EvolutionLifecycleDraft> {
        EvolutionTriageService::cluster(signals)
            .into_iter()
            .filter(|cluster| !cluster.signals.is_empty())
            .map(|cluster| {
                let diagnosis = EvolutionDiagnosisEngine::diagnose(&cluster.signals);
                let goals = candidate_kinds_from_root_cause(&diagnosis.root_cause_kind)
                    .into_iter()
                    .map(EvolutionCapabilityGoal::for_kind)
                    .collect::<Vec<_>>();
                let mut mission = EvolutionMission::new(
                    diagnosis.affected_owner.clone(),
                    diagnosis.affected_files_or_modules.clone(),
                    diagnosis.source_signal_ids.clone(),
                    diagnosis.diagnosis_id.clone(),
                    goals,
                );
                let mut proposal = EvolutionProposal::from_diagnosis(&diagnosis, &cluster.signals);
                proposal.mission_id = Some(mission.mission_id.clone());
                proposal.goal_ids = mission.goal_ids.clone();
                mission.attach_proposal(proposal.proposal_id.clone());
                EvolutionLifecycleDraft { mission, proposal }
            })
            .collect()
    }
}
