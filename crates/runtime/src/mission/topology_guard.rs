//! PLAN M1 wiring: a mission topology ADVISORY, not a replacement for the model.
//!
//! # What the code survey found (and why this is an advisory, not an executor)
//!
//! * `authorization.rs` admits `AgentAction::TeamCreate` for
//!   `AgentActorKind::Root | Supervisor` as well as for a delegated
//!   `Agent`/`TeamLead`. So the root model IS able to create the first team - the
//!   path exists.
//! * But nothing in the runtime runs BEFORE the root turn to analyse the
//!   objective and choose a topology. `mission::mission_control` is a read-only
//!   projection reducer; the agentic modules only APPLY actions the model already
//!   decided to send (`program::apply`, `roster::apply_team_create`).
//! * There is therefore no phase in which the runtime could create teams on the
//!   model's behalf, and building one would be a new orchestration phase with its
//!   own failure modes - not a wiring job.
//!
//! # So this module does the honest thing
//!
//! It turns the objective plus observable signals into an ADVISORY the mission
//! context can carry:
//!
//! * the recommended topology and its reason, written so the model can disagree
//!   with it (the reason is an argument, not an instruction);
//! * a hard ceiling when the USER specified a topology - the one case where the
//!   framework should not be second-guessed.
//!
//! The model keeps every bit of agency it had; it simply stops having to guess
//! what the runtime saw.

use super::topology_planner::{
    plan_topology, TopologyDecision, TopologyIntent, TopologySignals,
};

/// A mission-topology advisory plus the ceiling derived from the user's words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyAdvisory {
    /// What the runtime's analysis suggests. Advisory unless `user_specified`.
    pub decision: TopologyDecision,
    /// Whether the user named a topology, in which case `team_ceiling` is binding.
    pub user_specified: bool,
    /// Hard ceiling on teams the mission may create. `None` means "no runtime
    /// opinion"; the model decides.
    pub team_ceiling: Option<usize>,
}

impl TopologyAdvisory {
    /// Build the advisory for one objective.
    #[must_use]
    pub fn for_objective(objective: &str, signals: TopologySignals) -> Self {
        let intent = super::topology_planner::parse_topology_intent(objective);
        let decision = plan_topology(objective, intent, signals);
        let user_specified = !matches!(intent, TopologyIntent::Autonomous);
        let team_ceiling = if user_specified {
            Some(decision.team_count())
        } else {
            None
        };
        Self {
            decision,
            user_specified,
            team_ceiling,
        }
    }

    /// Whether `teams` more may be created.
    ///
    /// Only ever restricts what the USER explicitly asked for. An autonomous
    /// recommendation never blocks the model: it can always decide otherwise and
    /// the mission records that it did.
    #[must_use]
    pub fn allows_more_teams(&self, existing: usize) -> bool {
        match self.team_ceiling {
            None => true,
            Some(ceiling) => existing < ceiling,
        }
    }

    /// The advisory text for the mission context.
    ///
    /// Written as an argument the model may reject, because that is the only
    /// honest form: the runtime does not own this decision.
    #[must_use]
    pub fn context_note(&self) -> String {
        if self.user_specified {
            return format!(
                "# Mission topology\nThe user specified the topology for this mission: {}\n\
                 This is a requirement, not a suggestion - the runtime will refuse to admit \
                 more teams than this once the ceiling is reached.",
                self.decision.summarize()
            );
        }
        format!(
            "# Mission topology (advisory)\nRuntime's analysis of this objective: {}\n\
             This is an argument, not an instruction. If your own reading of the work says \
             otherwise, do it your way - the runtime records what you decided and does not \
             override you.",
            self.decision.summarize()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signals(domains: usize, items: usize, verify: bool, work: usize) -> TopologySignals {
        TopologySignals {
            independent_domains: domains,
            independent_work_items: items,
            needs_cross_verification: verify,
            estimated_work_units: work,
        }
    }

    /// The advisory is advisory: it states a reason and does not block the model.
    #[test]
    fn an_autonomous_recommendation_is_advice_that_does_not_block() {
        let advisory = TopologyAdvisory::for_objective(
            "审查 runtime/memory/gateway 三个 crate 的职责边界",
            signals(3, 9, true, 30),
        );
        assert!(!advisory.user_specified);
        assert_eq!(advisory.team_ceiling, None);
        assert!(advisory.allows_more_teams(50), "an advisory never blocks");
        let note = advisory.context_note();
        assert!(note.contains("advisory"), "{note}");
        assert!(
            note.contains("not an instruction") && note.contains("does not override you"),
            "the model must be told it may disagree: {note}"
        );
    }

    /// The one case where the framework does not second-guess the operator.
    #[test]
    fn an_explicit_user_topology_becomes_a_binding_ceiling() {
        let advisory = TopologyAdvisory::for_objective(
            "请用 2 个团队完成这次调研",
            signals(5, 30, true, 90),
        );
        assert!(advisory.user_specified);
        assert_eq!(advisory.team_ceiling, Some(2));
        assert!(advisory.allows_more_teams(1));
        assert!(!advisory.allows_more_teams(2), "the user's ceiling is binding");
        let note = advisory.context_note();
        assert!(note.contains("requirement, not a suggestion"), "{note}");
    }

    /// A single-agent request sets the ceiling to zero, which is what the user
    /// asked for and must not be overridden by an "efficiency" argument.
    #[test]
    fn an_explicit_single_agent_request_caps_teams_at_zero() {
        let advisory = TopologyAdvisory::for_objective(
            "请单独完成这次审查，不要启动团队",
            signals(4, 20, true, 60),
        );
        assert!(advisory.user_specified);
        assert_eq!(advisory.team_ceiling, Some(0));
        assert!(!advisory.allows_more_teams(0), "no team may be created at all");
    }

    /// The advisory must survive a task with no analysable domain at all.
    #[test]
    fn a_bare_objective_still_produces_a_note() {
        let advisory = TopologyAdvisory::for_objective("把这个文件读一下", TopologySignals::default());
        assert_eq!(advisory.team_ceiling, None);
        assert!(advisory.allows_more_teams(0));
        let note = advisory.context_note();
        assert!(note.contains("single agent") || note.contains("one agent"));
    }
}
