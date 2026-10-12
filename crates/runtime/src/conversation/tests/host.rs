include!("tool.rs");

#[test]
fn cold_start_failure_replan_covers_the_root_turn() {
    use super::{cold_start_tool_failure_replan_available, COLD_START_TOOL_FAILURE_REPLAN_BUDGET};
    // A cold start is not a spin. The root turn - the actor that has to drive a
    // multi-Team collaboration - must get the same single bounded recovery a
    // delegated leaf gets, or two early mistakes end the whole mission.
    assert!(cold_start_tool_failure_replan_available(false, 0));
    // Once any successful tool receipt exists the streak is no longer a cold
    // start, so the fail-closed path owns it again.
    assert!(!cold_start_tool_failure_replan_available(true, 0));
    // The budget is a hard bound: a consumed lease never reopens.
    assert!(!cold_start_tool_failure_replan_available(
        false,
        COLD_START_TOOL_FAILURE_REPLAN_BUDGET
    ));
    assert!(!cold_start_tool_failure_replan_available(
        false,
        COLD_START_TOOL_FAILURE_REPLAN_BUDGET + 1
    ));
}

include!("attestation.rs");
include!("agentic_content_bridge.rs");
include!("agentic_protocol_state.rs");
// Retired one-shot collaboration admission tests intentionally are not
// included. Agent-first coverage lives with `agentic::{action_service,
// execution}` and exercises incremental actions and durable projections.

include!("continuation.rs");

/// one19's counterfactual at the projection level.
///
/// The run carried `program.unresolved` empty (Goal-level, no gap) together with
/// a Task holding two unclassified issue descriptions. The closure view read the
/// Goal field, so it rendered `satisfied: [... classify_issues]` and
/// `facts.unresolved: 0` in the same object that reported
/// `completion_gap: issue_requires_classification` and two entries in
/// `unclassified_issue_refs`. The model read "already done" and wrote its final
/// answer. One source must answer both questions, and the rung must name the
/// action that closes the gap it advertises.
#[test]
fn the_closure_view_never_calls_a_gap_both_open_and_closed() {
    use super::agentic_closure_view;
    use crate::{
        AgenticProgramProjection, AgenticTaskProjection, AgenticTaskStatus,
        AgenticTeamProjection,
    };

    let mut program = AgenticProgramProjection::empty("program", "objective");
    program.required_team_count = 1;
    program.teams.insert(
        "team:1".to_string(),
        AgenticTeamProjection {
            team_id: "team:1".to_string(),
            name: "Team".into(),
            mission: "m".into(),
            objective: None,
            topic_ref: "topic:team:1".into(),
            created_by: "root".into(),
            member_ids: vec!["agent:a".into(), "agent:b".into()],
            task_ids: vec!["task:0".into(), "task:1".into()],
            lifecycle: Default::default(),
        },
    );
    for (index, title) in ["runtime lib.rs", "tools lib.rs"].iter().enumerate() {
        program.tasks.insert(
            format!("task:{index}"),
            AgenticTaskProjection {
                task_id: format!("task:{index}"),
                team_id: "team:1".to_string(),
                title: (*title).to_string(),
                objective: "o".to_string(),
                acceptance: "a".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Accepted,
                claimant: Some("agent:a".into()),
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                active_attempts: Default::default(),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: vec![format!("unresolved finding in {title}")],
                review_reason: None,
                reviewed_by: Some("agent:b".into()),
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
    }
    // Goal-level gaps are empty. This is the exact one19 divergence.
    program.unresolved = Vec::new();

    let gap = crate::agentic::issues::completion_gap(&program)
        .expect("two unclassified Task issues produce a gap");
    assert!(gap.starts_with("issue_requires_classification:"));

    let view = agentic_closure_view(&program, Some(&gap));
    let satisfied = view["satisfied"].as_array().expect("satisfied array");
    let satisfied_steps = satisfied
        .iter()
        .filter_map(|item| item.as_str())
        .collect::<Vec<_>>();

    // The gap is open, so the rung it belongs to cannot be satisfied.
    assert!(
        !satisfied_steps.contains(&"classify_issues"),
        "classify_issues must not read as satisfied while a Task issue is unclassified: {view}"
    );
    assert_eq!(
        view["facts"]["unclassified_issues"].as_u64(),
        Some(2),
        "the unclassified count must come from the Task record: {view}"
    );
    let remaining = view["remaining"].as_array().expect("remaining array");
    let issue_rung = remaining
        .iter()
        .find(|row| row["step"] == "classify_issues")
        .unwrap_or_else(|| panic!("classify_issues rung must be advertised: {view}"));
    // The model must be handed the exact references to classify, not told to go
    // looking for them.
    assert_eq!(
        issue_rung["issue_refs"].as_array().map(Vec::len),
        Some(2),
        "the rung must name the issue refs it wants classified: {view}"
    );
    // And the rung must name the action that closes THIS gap, which is
    // `message_publish` - not `objective_review`, which cannot classify an issue.
    let gap_rung = remaining
        .iter()
        .find(|row| row.get("outstanding_gap").is_some())
        .unwrap_or_else(|| panic!("the gap must appear in remaining: {view}"));
    assert_eq!(gap_rung["step"], "classify_issues");
    assert_eq!(gap_rung["closing_tool"], "message_publish");
    assert!(gap_rung["hint"]
        .as_str()
        .is_some_and(|hint| hint.contains("message_publish") && hint.contains("issue_dispositions")));
}

/// A repeated restatement is not a new conclusion. Normalizing whitespace and
/// case is what makes a restating model recognizable; a model that genuinely
/// revises its answer produces a different digest and is never counted.
#[test]
fn the_terminal_answer_digest_sees_a_restatement_and_misses_a_revision() {
    use super::host_presentation::terminal_answer_digest;
    let base = "  The Team finished.\n\n  Three facts were verified.  ";
    let restated = "the team finished. three facts were verified.";
    let revised = "The Team finished, but one fact could not be verified.";
    assert_eq!(terminal_answer_digest(base), terminal_answer_digest(restated));
    assert_ne!(terminal_answer_digest(base), terminal_answer_digest(revised));
}

/// The control-plane event used to carry two literals: `program_admitted: false`
/// and `required_actions: [team_create, agent_invite, task_publish]`, on every
/// event for the whole run. one19 emitted 19 of them, the last ones written after
/// one Team, six Agents and three accepted Tasks existed. Deriving both from the
/// Program is what makes the event a statement about what is owed.
#[test]
fn the_control_plane_requirement_is_derived_from_the_program_not_written_as_literals() {
    use super::host_backend::root_control_plane_requirement;
    use crate::{AgenticProgramProjection, AgenticTeamProjection};

    // No Program at all: the original three actions are the truth.
    let (admitted, actions, steps) = root_control_plane_requirement(None, 1);
    assert!(!admitted);
    assert_eq!(
        actions,
        vec!["team_create", "agent_invite", "task_publish"],
        "with no Program the admission actions are genuinely outstanding"
    );
    assert!(steps.is_empty());

    // The one19 end state: one Team, Agents and Tasks all present. Nothing is
    // owed at admission, so the event must stop claiming it is.
    let mut program = AgenticProgramProjection::empty("program", "objective");
    program.required_team_count = 1;
    program.teams.insert(
        "team:1".to_string(),
        AgenticTeamProjection {
            team_id: "team:1".to_string(),
            name: "Team".into(),
            mission: "m".into(),
            objective: None,
            topic_ref: "topic:team:1".into(),
            created_by: "root".into(),
            member_ids: vec!["agent:a".into()],
            task_ids: vec!["task:0".into()],
            lifecycle: Default::default(),
        },
    );
    program.agents.insert(
        "agent:a".to_string(),
        crate::AgentMemberProjection {
            agent_id: "agent:a".into(),
            display_name: "Reader A".into(),
            role: "Reader".into(),
            mission: "read".into(),
            required_capabilities: Vec::new(),
            invited_by: "root".into(),
            membership_ids: vec!["membership:1".into()],
            definition_ref: None,
            model_profile_ref: None,
            expertise_hints: Vec::new(),
            execution_requirements: Vec::new(),
        },
    );
    program.tasks.insert(
        "task:0".to_string(),
        crate::AgenticTaskProjection {
            task_id: "task:0".into(),
            team_id: "team:1".into(),
            title: "t".into(),
            objective: "o".into(),
            acceptance: "a".into(),
            acceptance_checks: Vec::new(),
            criterion_source: Default::default(),
            required_capabilities: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
            depends_on: Vec::new(),
            status: crate::AgenticTaskStatus::Accepted,
            claimant: Some("agent:a".into()),
            claim_generation: 0,
            claim_execution_id: None,
            claimed_at_ms: None,
            lease_expires_at_ms: None,
            active_attempts: Default::default(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            review_reason: None,
            reviewed_by: Some("agent:b".into()),
            failed_attempts: 0,
            review_generation: 0,
            failed_review_attempts: 0,
            last_failure: None,
            replacement_task_refs: Vec::new(),
            supersede_evidence_refs: Vec::new(),
            superseded_reason: None,
            superseded_by: None,
            cancel_requested_by: None,
            cancel_reason_ref: None,
            cancel_evidence_refs: Vec::new(),
            pending_retirement: None,
        },
    );
    let (admitted, actions, _steps) = root_control_plane_requirement(Some(&program), 1);
    assert!(admitted, "a Program with a Team is admitted");
    assert!(
        actions.is_empty(),
        "no admission action is outstanding once Team, Agents and Tasks exist: {actions:?}"
    );
}
