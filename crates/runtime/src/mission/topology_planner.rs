//! PLAN M1: the runtime decides a collaboration topology, the model is not told.
//!
//! # Why this exists
//!
//! Team topology used to be dictated in the prompt, in both directions:
//!
//! ```text
//! "必须先连续调用 team_create 创建满至少 3 个 Team"
//! "不要自行指定 Team、Agent、角色、模板或编排拓扑"
//! ```
//!
//! One forced teams into existence, the other forbade them. Both were the same
//! mistake - a fixed recipe written by us instead of a decision made by whoever
//! can actually see the work - and both suppressed the agency that makes a
//! multi-agent runtime worth having.
//!
//! The prompts used to have to choose because **the runtime had no entry point
//! that produced a topology.** Tool-level autonomy already existed
//! (`TaskIntent` -> `context_fanout` prefetch); team-level autonomy did not.
//!
//! # What this module is
//!
//! A pure planner: observable signals plus the user's words in, a topology and
//! its reasoning out. It never calls the model and never inspects model output
//! as if it were truth. The three rules, in priority order:
//!
//! 1. **The user wins.** If the user named a topology, produce exactly that. A
//!    framework that second-guesses an explicit instruction has replaced the
//!    operator's judgement with its own.
//! 2. **Do not build teams for form.** One responsibility domain, work that
//!    fits one agent: one agent. A team created for ceremony costs budget and
//!    coordination and buys nothing.
//! 3. **Split when the work is actually splittable**, and let independent
//!    branches run concurrently rather than walking one simple path at a time.
//!
//! Every decision carries its reason. The model may disagree - the reason is
//! written for the model to argue with, not to obey.

/// What the user said about topology, if anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopologyIntent {
    /// The user said nothing about topology: the runtime decides.
    #[default]
    Autonomous,
    /// The user named a topology. `teams`/`agents` are `None` where the user
    /// left them unspecified.
    UserSpecified {
        /// How many teams the user asked for, if stated.
        teams: Option<usize>,
        /// How many agents per team, if stated.
        agents: Option<usize>,
    },
    /// The user explicitly asked for a single agent.
    UserSingleAgent,
}

/// Observable signals about the work. None of these come from model prose; a
/// number the model asserts about itself is not a signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TopologySignals {
    /// Distinct responsibility domains that can be worked without coordinating.
    pub independent_domains: usize,
    /// Work items that do not depend on each other.
    pub independent_work_items: usize,
    /// Whether a claim has to be checked by a second, independent viewpoint.
    pub needs_cross_verification: bool,
    /// Rough count of bounded work units (file reads, checks, analyses).
    pub estimated_work_units: usize,
}

/// The topology the runtime proposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyDecision {
    /// One agent does the whole thing.
    SingleAgent {
        /// Why, so the model can argue with it.
        reason: String,
    },
    /// Several teams, because the work is genuinely splittable.
    Teams {
        /// How many teams.
        teams: usize,
        /// A hint, not a rule: how many agents per team fits the work.
        agents_per_team: usize,
        /// Why.
        reason: String,
    },
}

impl TopologyDecision {
    /// One line for a log or a report.
    #[must_use]
    pub fn summarize(&self) -> String {
        match self {
            Self::SingleAgent { reason } => format!("single agent: {reason}"),
            Self::Teams {
                teams,
                agents_per_team,
                reason,
            } => format!("{teams} teams x {agents_per_team} agents (hint): {reason}"),
        }
    }

    /// How many teams this decision implies.
    #[must_use]
    pub const fn team_count(&self) -> usize {
        match self {
            Self::SingleAgent { .. } => 0,
            Self::Teams { teams, .. } => *teams,
        }
    }
}

/// Upper bound on teams a single objective may be split into.
///
/// Beyond this the coordination cost dominates: every extra team is another
/// dependency edge, another review, another chance for a descendant to stall.
pub const MAX_TEAMS_PER_OBJECTIVE: usize = 8;

/// Work units below which splitting is ceremony rather than parallelism.
const SINGLE_AGENT_WORK_UNIT_CEILING: usize = 3;

/// Decide a topology.
///
/// Pure: same inputs, same decision, same reason. No I/O, no clock, no model.
#[must_use]
pub fn plan_topology(
    objective: &str,
    intent: TopologyIntent,
    signals: TopologySignals,
) -> TopologyDecision {
    // Rule 1: the user wins, always, and without commentary about whether their
    // choice looks efficient.
    match intent {
        TopologyIntent::UserSingleAgent => {
            return TopologyDecision::SingleAgent {
                reason: "the user asked for a single agent".to_string(),
            };
        }
        TopologyIntent::UserSpecified { teams, agents } => {
            let teams = teams.unwrap_or(1).clamp(1, MAX_TEAMS_PER_OBJECTIVE);
            let agents = agents.unwrap_or(1).max(1);
            return TopologyDecision::Teams {
                teams,
                agents_per_team: agents,
                reason: format!(
                    "the user asked for {teams} team(s) with {} agent(s) each",
                    agents
                ),
            };
        }
        TopologyIntent::Autonomous => {}
    }

    // From here the runtime decides, and it must justify itself.
    let domains = signals.independent_domains.max(1);
    let work = signals.estimated_work_units;

    // Rule 2: no ceremony. A single domain with little work is one agent, even
    // if the objective sounds large - "research X" with one domain is still one
    // agent's work.
    if domains <= 1 && work <= SINGLE_AGENT_WORK_UNIT_CEILING {
        return TopologyDecision::SingleAgent {
            reason: format!(
                "one responsibility domain and {work} work unit(s) fit one agent; a team would add coordination without adding coverage"
            ),
        };
    }

    if signals.needs_cross_verification && domains < 2 {
        return TopologyDecision::Teams {
            teams: 2,
            agents_per_team: 1,
            reason: "the objective requires an independent second viewpoint, which one agent cannot provide about itself".to_string(),
        };
    }

    if domains < 2 {
        return TopologyDecision::SingleAgent {
            reason: format!(
                "one responsibility domain; {work} work unit(s) still fit one agent because nothing can run concurrently"
            ),
        };
    }

    // Rule 3: split along the real seam - the independent domains - and cap it,
    // because a team per domain past the cap costs more than it parallelises.
    let mut teams = domains.min(MAX_TEAMS_PER_OBJECTIVE);
    if signals.independent_work_items < teams {
        teams = signals.independent_work_items.max(2).min(teams);
    }
    // Cross-verification needs a reviewer outside each branch, so a team of one
    // cannot review its own work.
    let agents_per_team = if signals.needs_cross_verification { 2 } else { 1 };

    TopologyDecision::Teams {
        teams,
        agents_per_team,
        reason: format!(
            "{domains} independent responsibility domain(s) and {} independent work item(s) can run concurrently; splitting along the domain seam{}",
            signals.independent_work_items,
            if signals.needs_cross_verification {
                " with a second agent per team so every claim gets an independent check"
            } else {
                ""
            }
        ),
    }
}

/// Read a topology request out of free-form user text.
///
/// Deliberately conservative: it only recognises an EXPLICIT request. A user
/// saying "use a few teams" has specified teams; a user saying "this is complex"
/// has not, and must not be read as one.
#[must_use]
pub fn parse_topology_intent(objective: &str) -> TopologyIntent {
    // Deliberately a DELEGATION, not a parser. `strategy` already owns this
    // question and has done since before this module existed:
    //
    // * `TaskUnderstanding::forbids_team` is produced by
    //   `explicitly_forbids_collaboration`, whose whole purpose is the case this
    //   module's first draft missed - "a request may mention teams solely to
    //   prohibit them";
    // * `required_team_count` is documented as "the orchestration authority":
    //   the explicit number of Team entities the user requested. It is non-zero
    //   exactly when the user gave an explicit Team constraint, and the framework
    //   deliberately freezes that same number as the exact count (see
    //   `singular_explicit_team_freezes_one_exact_team`), so an uncounted
    //   "start a team" is a count of one rather than an unknown.
    //
    // A second parser here would be a second source of truth, and the two would
    // eventually disagree - which is precisely what happened when this module was
    // first written.
    let understanding = harness_contract::strategy::understand(
        &harness_contract::strategy::StrategyInput::from_prompt(objective),
    );
    if understanding.forbids_team {
        return TopologyIntent::UserSingleAgent;
    }
    if understanding.required_team_count > 0 {
        return TopologyIntent::UserSpecified {
            teams: Some(usize::from(understanding.required_team_count)),
            // The framework does not parse a per-Team Agent count, and inventing
            // one here would be a second, weaker authority for the same fact.
            agents: None,
        };
    }
    TopologyIntent::Autonomous
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

    /// Rule 1: the user's explicit instruction is executed as given, with no
    /// second-guessing about whether it looks efficient.
    #[test]
    fn an_explicit_user_topology_is_executed_exactly() {
        let decision = plan_topology(
            "请用 3 个团队做这次调研",
            parse_topology_intent("请用 3 个团队做这次调研"),
            signals(5, 20, true, 40),
        );
        assert_eq!(decision.team_count(), 3);
        assert!(decision.summarize().contains("user asked"));
    }

    #[test]
    fn an_explicit_single_agent_request_is_honoured_even_for_large_work() {
        let objective = "请单独完成这次审查，不要启动团队";
        let decision = plan_topology(objective, parse_topology_intent(objective), signals(6, 30, true, 90));
        assert_eq!(decision.team_count(), 0, "the user asked for one agent");
        assert!(decision.summarize().contains("single agent"));
    }

    /// Rule 2: no ceremony.
    #[test]
    fn a_single_domain_with_little_work_is_one_agent() {
        let decision = plan_topology(
            "调研一下这个字段的含义",
            TopologyIntent::Autonomous,
            signals(1, 1, false, 2),
        );
        assert_eq!(decision.team_count(), 0);
        assert!(decision.summarize().contains("one responsibility domain"));
    }

    #[test]
    fn a_large_sounding_single_domain_task_is_still_one_agent() {
        // "research X" with one domain is still one agent's work, however big it
        // sounds; teams would add coordination without adding coverage.
        let decision = plan_topology(
            "全面调研一遍这个模块",
            TopologyIntent::Autonomous,
            signals(1, 1, false, 12),
        );
        assert_eq!(decision.team_count(), 0);
    }

    /// Rule 3: split when the work is actually splittable, along the real seam.
    #[test]
    fn independent_domains_become_teams_along_the_domain_seam() {
        let decision = plan_topology(
            "审查 runtime/memory/gateway 三个 crate",
            TopologyIntent::Autonomous,
            signals(3, 9, true, 30),
        );
        assert_eq!(decision.team_count(), 3);
        assert!(decision.summarize().contains("3 teams"));
    }

    #[test]
    fn cross_verification_alone_forces_a_second_viewpoint() {
        let decision = plan_topology(
            "核对这个结论",
            TopologyIntent::Autonomous,
            signals(1, 2, true, 6),
        );
        assert_eq!(decision.team_count(), 2);
        assert!(decision.summarize().contains("independent second viewpoint"));
    }

    /// The cap is real: more teams than that costs more than it parallelises.
    #[test]
    fn the_team_count_is_capped() {
        let decision = plan_topology(
            "审查整个仓库",
            TopologyIntent::Autonomous,
            signals(40, 200, true, 500),
        );
        assert_eq!(
            decision.team_count(),
            MAX_TEAMS_PER_OBJECTIVE,
            "past the cap the coordination cost dominates"
        );
    }

    #[test]
    fn every_decision_carries_a_reason_the_model_can_argue_with() {
        for intent in [
            TopologyIntent::Autonomous,
            TopologyIntent::UserSingleAgent,
            TopologyIntent::UserSpecified { teams: Some(2), agents: None },
        ] {
            for verify in [false, true] {
                let decision = plan_topology("x", intent, signals(3, 6, verify, 20));
                let summary = decision.summarize();
                assert!(
                    summary.len() > 20 && (summary.contains(':')),
                    "a decision must explain itself: {summary}"
                );
            }
        }
    }

    /// Conservative by design: "this is complex" is not a topology request.
    #[test]
    fn an_unstated_topology_stays_autonomous() {
        for objective in [
            "这个事情比较复杂",
            "帮我调研一下",
            "审查 runtime 和 memory",
            "deep dive please",
        ] {
            assert_eq!(
                parse_topology_intent(objective),
                TopologyIntent::Autonomous,
                "`{objective}` must not be read as a topology instruction"
            );
        }
    }

    #[test]
    /// The counts come from the framework authority, not from a parser here.
    ///
    /// `agents` is always `None`: `strategy` does not parse a per-Team Agent
    /// count, and a second weaker authority for the same fact is what this module
    /// was rewritten to remove.
    fn explicit_counts_delegate_to_the_framework_authority() {
        for (prompt, teams) in [
            ("请用 3 个团队", 3usize),
            ("请用三个团队", 3),
            ("use 4 teams for this", 4),
        ] {
            assert_eq!(
                parse_topology_intent(prompt),
                TopologyIntent::UserSpecified {
                    teams: Some(teams),
                    agents: None
                },
                "`{prompt}` must delegate to strategy::required_team_count"
            );
        }
        // The framework's deliberate reading: an uncounted "start a team" is a
        // count of one, not an unknown. Delegating keeps that behaviour instead
        // of inventing a different one.
        assert_eq!(
            parse_topology_intent("请启动协作团队审查架构"),
            TopologyIntent::UserSpecified {
                teams: Some(1),
                agents: None
            }
        );
        // Prohibiting Teams is read from the framework's own negation detection.
        assert_eq!(
            parse_topology_intent("请单独完成，不要启动团队"),
            TopologyIntent::UserSingleAgent
        );
        assert_eq!(
            parse_topology_intent("把这个模块读一下"),
            TopologyIntent::Autonomous
        );
    }
}
