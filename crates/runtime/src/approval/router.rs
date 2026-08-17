//! Global approval router.
//!
//! Every approval submission site resolves its decision through this one
//! matrix instead of hand-writing TrustAll / steward / deterministic branches.
//! Inputs are the five autonomy levels, the approval domain, risk, whether
//! execution blocks, and whether the user explicitly asked for confirmation.

use harness_contract::core::TaskRisk;
use harness_contract::policy::{ApprovalDomain, ApprovalProfile, AutonomyProfileId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    /// Auto-approve with an audit trail (actor: policy).
    AutoApprove,
    /// Approve through the bounded Steward policy when the effect is eligible.
    StewardApprove,
    /// Queue for a human decision.
    Human,
    /// Fail closed.
    Deny,
    /// Non-blocking sub-step: continue without promotion.
    ContinueAlternative,
}

pub struct ApprovalRouter;

impl ApprovalRouter {
    /// Map the legacy four-value approval profile back to the five-level
    /// autonomy ladder so the router can distinguish Stewarded from
    /// Autonomous instead of collapsing them.
    #[must_use]
    pub fn profile_for_approval_profile(profile: ApprovalProfile) -> AutonomyProfileId {
        match profile {
            ApprovalProfile::Supervised => AutonomyProfileId::Cautious,
            ApprovalProfile::Balanced => AutonomyProfileId::Supervised,
            ApprovalProfile::Autonomous => AutonomyProfileId::Autonomous,
            ApprovalProfile::TrustAll => AutonomyProfileId::Yolo,
        }
    }

    #[must_use]
    pub fn resolve(
        profile: AutonomyProfileId,
        domain: ApprovalDomain,
        risk: TaskRisk,
        blocks_execution: bool,
        explicit_ask: bool,
    ) -> ApprovalDecision {
        // Background sub-steps (knowledge/evolution/skill) never pin execution.
        // Low-trust levels queue them with a ContinueAlternative TTL; higher
        // levels auto-approve so a sub-step never asks a human by accident.
        let non_blocking_background = !blocks_execution
            && matches!(
                domain,
                ApprovalDomain::Knowledge
                    | ApprovalDomain::Evolution
                    | ApprovalDomain::Skill
            );
        match profile {
            AutonomyProfileId::Cautious | AutonomyProfileId::Supervised => {
                if risk == TaskRisk::Low && !explicit_ask {
                    ApprovalDecision::AutoApprove
                } else if non_blocking_background {
                    ApprovalDecision::ContinueAlternative
                } else {
                    ApprovalDecision::Human
                }
            }
            AutonomyProfileId::Stewarded => match risk {
                TaskRisk::Low => ApprovalDecision::AutoApprove,
                TaskRisk::Medium if !explicit_ask => ApprovalDecision::StewardApprove,
                _ if non_blocking_background => ApprovalDecision::ContinueAlternative,
                _ => ApprovalDecision::Human,
            },
            AutonomyProfileId::Autonomous | AutonomyProfileId::Yolo => {
                ApprovalDecision::AutoApprove
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(
        profile: AutonomyProfileId,
        domain: ApprovalDomain,
        risk: TaskRisk,
        blocks: bool,
    ) -> ApprovalDecision {
        ApprovalRouter::resolve(profile, domain, risk, blocks, false)
    }

    #[test]
    fn yolo_auto_approves_every_domain_and_risk() {
        for domain in [
            ApprovalDomain::Execution,
            ApprovalDomain::System,
            ApprovalDomain::Knowledge,
            ApprovalDomain::Evolution,
            ApprovalDomain::Skill,
        ] {
            for risk in [TaskRisk::Low, TaskRisk::Medium, TaskRisk::High, TaskRisk::Critical] {
                assert_eq!(
                    resolve(AutonomyProfileId::Yolo, domain, risk, true),
                    ApprovalDecision::AutoApprove,
                    "yolo must auto-approve {domain:?} {risk:?}"
                );
            }
        }
    }

    #[test]
    fn autonomous_auto_approves_with_audit() {
        assert_eq!(
            resolve(AutonomyProfileId::Autonomous, ApprovalDomain::Execution, TaskRisk::Critical, true),
            ApprovalDecision::AutoApprove
        );
        assert_eq!(
            resolve(
                AutonomyProfileId::Autonomous,
                ApprovalDomain::Knowledge,
                TaskRisk::Medium,
                false
            ),
            ApprovalDecision::AutoApprove
        );
    }

    #[test]
    fn stewarded_uses_steward_for_medium_work() {
        assert_eq!(
            resolve(AutonomyProfileId::Stewarded, ApprovalDomain::Execution, TaskRisk::Medium, true),
            ApprovalDecision::StewardApprove
        );
        assert_eq!(
            resolve(AutonomyProfileId::Stewarded, ApprovalDomain::Execution, TaskRisk::High, true),
            ApprovalDecision::Human
        );
    }

    #[test]
    fn supervised_keeps_human_for_medium_and_up() {
        assert_eq!(
            resolve(AutonomyProfileId::Supervised, ApprovalDomain::Execution, TaskRisk::Low, true),
            ApprovalDecision::AutoApprove
        );
        assert_eq!(
            resolve(AutonomyProfileId::Supervised, ApprovalDomain::Execution, TaskRisk::Medium, true),
            ApprovalDecision::Human
        );
    }

    #[test]
    fn non_blocking_background_never_forces_human_at_low_trust() {
        assert_eq!(
            resolve(
                AutonomyProfileId::Cautious,
                ApprovalDomain::Knowledge,
                TaskRisk::High,
                false
            ),
            ApprovalDecision::ContinueAlternative
        );
        assert_eq!(
            resolve(
                AutonomyProfileId::Supervised,
                ApprovalDomain::Evolution,
                TaskRisk::High,
                false
            ),
            ApprovalDecision::ContinueAlternative
        );
    }
}
