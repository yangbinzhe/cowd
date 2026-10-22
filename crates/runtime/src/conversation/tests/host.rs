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
include!("agency_economy.rs");
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
                claim_renewal_streak: 0,
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
            claim_renewal_streak: 0,
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

/// Prompt isolation and capability are two different axes, and only one of them
/// may be decided by "this is the terminal request".
///
/// `clean_terminal_synthesis` exists to keep a long exploratory transcript out of
/// the reduction request - a real cost optimization, worth 1.5M cache-read tokens
/// on a single one19 run. It was also silently withdrawing every tool, on the
/// theory that "a verified collaboration terminal leaves only presentation". That
/// theory is false whenever an obligation is still open: run one19 spent six
/// requests with an empty `tools` array while `message_publish` was the only thing
/// that could close its Objective. The tools a terminal request exposes must be a
/// function of what is owed, never of which prompt shape was chosen.
#[test]
fn a_terminal_request_keeps_the_tools_it_still_owes_and_drops_them_only_when_nothing_is_owed() {
    use super::host_backend::mandatory_action_tool_ids;

    // Nothing owed: a tool-less terminal is correct, and this is the only case
    // in which it is.
    let idle = mandatory_action_tool_ids(false, &[]);
    assert!(
        idle.is_empty(),
        "with no outstanding obligation the terminal request may drop every tool: {idle:?}"
    );

    // A completion gap is owed: the closing tool must survive the terminal
    // request, together with the reads the closing action needs.
    let owed = mandatory_action_tool_ids(true, &["classify_issues".to_string()]);
    for required in ["message_publish"] {
        assert!(
            owed.contains(required),
            "a terminal request that still owes an issue classification must expose {required}: {owed:?}"
        );
    }

    // An Objective-review gap owes a different tool. The capability follows the
    // obligation, not the rung name the model happens to be on.
    let review = mandatory_action_tool_ids(true, &["objective_review".to_string()]);
    assert!(review.contains("objective_review"));
    assert!(!review.contains("state_inspect"));
    assert!(
        !review.contains("message_publish"),
        "an Objective-review gap must not be handed the issue-classification tool: {review:?}"
    );

    // The read tools are always present, because every closing action above
    // legitimately needs its references read first.
    let integration = mandatory_action_tool_ids(true, &["publish_integration_task".to_string()]);
    assert!(integration.contains("task_publish"));
    assert!(!integration.contains("evidence_retrieve"));
}

/// The cumulative repeat counter must not count a wait, and must not count a call
/// that failed. Both exclusions are what keep the diagnosis worth reading: a model
/// that is told its legitimate `wait_for_workers` poll is a loop learns to ignore
/// the diagnosis, which is how an earlier monitor lost its operator.
#[test]
fn the_repeat_counter_skips_waits_and_failed_calls() {
    use super::host_backend::record_repeated_calls;
    use crate::conversation::ModelToolCall;

    fn call(id: &str, name: &str, input: &str) -> ModelToolCall {
        ModelToolCall {
            binding: None,
            id: id.to_string(),
            name: name.to_string(),
            input: input.to_string(),
            depends_on: Vec::new(),
        }
    }

    let turn = &crate::repetition_guard::RepetitionScope::turn("host-guard-test-session", "v22-repeat-filter");
    crate::repetition_guard::clear_turn(turn);
    let bound = crate::repetition_guard::repeated_call_bound();

    // A wait, issued far more times than the bound, is never counted.
    let wait = call("w", "state_inspect", r#"{"wait_for_workers":true}"#);
    for _ in 0..(bound + 3) {
        let succeeded = std::collections::BTreeSet::from(["w".to_string()]);
        assert!(
            record_repeated_calls(turn, std::slice::from_ref(&wait), &succeeded).is_empty(),
            "a wait_for_workers poll is not a repeat"
        );
    }

    // A call Runtime refused before dispatch still counts. The model decided to
    // ask for it, and asking again is the decision that repeats - run one26
    // re-issued the same `agent_invite` sixteen times, every one refused before
    // dispatch, and a counter keyed on successful dispatch never moved.
    let refused = call("f", "read_file", r#"{"path":"a.rs"}"#);
    for _ in 1..bound {
        assert!(
            record_repeated_calls(turn, std::slice::from_ref(&refused), &std::collections::BTreeSet::new())
                .is_empty(),
            "below the bound the counter stays silent"
        );
    }
    assert_eq!(
        record_repeated_calls(turn, std::slice::from_ref(&refused), &std::collections::BTreeSet::new()),
        vec![("read_file".to_string(), bound)],
        "a call refused before dispatch must still reach the bound"
    );

    // The same call succeeding does count, which is the whole point.
    let working = call("s", "read_file", r#"{"path":"b.rs"}"#);
    let succeeded = std::collections::BTreeSet::from(["s".to_string()]);
    for _ in 1..bound {
        assert!(record_repeated_calls(turn, std::slice::from_ref(&working), &succeeded).is_empty());
    }
    assert_eq!(
        record_repeated_calls(turn, std::slice::from_ref(&working), &succeeded),
        vec![("read_file".to_string(), bound)]
    );
    crate::repetition_guard::clear_turn(turn);
}

/// Re-dispatch opens a new agent graph but keeps the turn, and the count has to
/// survive it. Run one20's 31 identical `state_inspect` queries were spread over
/// five agent graphs; a counter keyed on the graph would have seen at most
/// `bound` of them, which is exactly the blind spot this closes.
#[test]
fn the_repeat_count_survives_a_re_dispatch_that_opens_a_new_graph() {
    use super::host_backend::record_repeated_calls;
    use crate::conversation::ModelToolCall;

    fn call(id: &str, input: &str) -> ModelToolCall {
        ModelToolCall {
            binding: None,
            id: id.to_string(),
            name: "state_inspect".to_string(),
            input: input.to_string(),
            depends_on: Vec::new(),
        }
    }

    let turn = &crate::repetition_guard::RepetitionScope::turn("host-guard-test-session", "v22-repeat-across-graphs");
    crate::repetition_guard::clear_turn(turn);
    let bound = crate::repetition_guard::repeated_call_bound();
    let query = r#"{"query":"task:77fb8340a3eb5091224bb31d"}"#;

    // Attempt one runs below the bound.
    for index in 0..(bound - 1) {
        let attempt = call(&format!("a{index}"), query);
        let succeeded = std::collections::BTreeSet::from([format!("a{index}")]);
        assert!(record_repeated_calls(turn, &[attempt], &succeeded).is_empty());
    }
    // Re-dispatch: a fresh graph, a fresh call id, the same question. The count
    // must carry over rather than restart.
    let redispatched = call("b0", query);
    let succeeded = std::collections::BTreeSet::from(["b0".to_string()]);
    assert_eq!(
        record_repeated_calls(turn, &[redispatched], &succeeded),
        vec![("state_inspect".to_string(), bound)],
        "a re-dispatched identical call must be recognised as the same call"
    );
    crate::repetition_guard::clear_turn(turn);
}

/// The signal V23-1 now reads. A Program with Teams but no Tasks owes
/// `publish_task`, and that fact has to be visible in `outstanding_closure_steps`
/// - the two flags the continuation paths set are both false at that moment,
/// which is exactly why a tool-less synthesis was handed to a model that still
/// had to publish its Tasks.
#[test]
fn a_program_with_teams_but_no_tasks_still_owes_publish_task() {
    use crate::{AgenticProgramProjection, AgenticTeamProjection};

    let mut program = AgenticProgramProjection::empty("program", "objective");
    program.required_team_count = 3;
    for index in 0..3 {
        program.teams.insert(
            format!("team:{index}"),
            AgenticTeamProjection {
                team_id: format!("team:{index}"),
                name: format!("Team {index}"),
                mission: "m".into(),
                objective: None,
                topic_ref: format!("topic:team:{index}"),
                created_by: "root".into(),
                member_ids: vec![format!("agent:{index}")],
                task_ids: Vec::new(),
                lifecycle: Default::default(),
            },
        );
    }
    let steps = super::outstanding_closure_steps(&program);
    assert!(
        steps.iter().any(|step| step == "publish_task"),
        "three Teams and no Tasks must still owe publish_task: {steps:?}"
    );
    // The raw view also names the exit request, because with no completion request
    // on file that is the only remaining step. It is the model's own decision
    // rather than an obligation, so the terminal-request site filters it out
    // before deciding whether tools are owed - otherwise every unfinished Program
    // would look like it owed the exit action and never get a tool-less terminal.
    assert!(
        steps.iter().any(|step| step == "objective_complete_request"),
        "the view names the exit request while none exists: {steps:?}"
    );
    let owed = steps
        .iter()
        .filter(|step| step.as_str() != "objective_complete_request")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(owed, vec!["publish_task".to_string()], "{owed:?}");
}

#[test]
fn an_unfinished_root_program_keeps_the_action_channel_until_a_durable_terminal() {
    use super::host_backend::root_program_requires_action_channel;
    use super::TurnExecutionRole;
    use crate::AgenticProgramStatus;

    assert!(!root_program_requires_action_channel(
        TurnExecutionRole::RootTurn,
        None
    ));
    for status in [
        AgenticProgramStatus::Open,
        AgenticProgramStatus::Waiting,
        AgenticProgramStatus::CompletionRequested,
        AgenticProgramStatus::Draining,
    ] {
        assert!(root_program_requires_action_channel(
            TurnExecutionRole::RootTurn,
            Some(status)
        ));
        assert!(!root_program_requires_action_channel(
            TurnExecutionRole::DelegatedLeaf,
            Some(status)
        ));
    }
    for status in [
        AgenticProgramStatus::Verified,
        AgenticProgramStatus::Partial,
        AgenticProgramStatus::Blocked,
        AgenticProgramStatus::Failed,
        AgenticProgramStatus::Cancelled,
    ] {
        assert!(!root_program_requires_action_channel(
            TurnExecutionRole::RootTurn,
            Some(status)
        ));
    }
}

/// Multiple accepted deliveries alone do not create an extra business Task.
/// The original Goal's explicit review gap still prevents completion.
#[test]
fn multiple_results_do_not_create_synthesis_debt_or_discharge_goal_review() {
    use super::agentic_closure_view;
    use crate::{AgenticProgramProjection, AgenticTaskProjection, AgenticTaskStatus};

    let mut program = AgenticProgramProjection::empty("program", "objective");
    for index in 0..3 {
        let artifact_ref = format!("artifact:{index}");
        program.artifacts.insert(
            artifact_ref.clone(),
            crate::AgenticArtifactProjection {
                artifact_ref: artifact_ref.clone(),
                kind: "report".into(),
                title: format!("report {index}"),
                content_ref: format!("artifact://art_{index}"),
                committed_by: format!("agent:{index}"),
                relates_to: Vec::new(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        program.tasks.insert(
            format!("task:{index}"),
            AgenticTaskProjection {
                task_id: format!("task:{index}"),
                team_id: "team:1".into(),
                title: format!("task {index}"),
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
                status: AgenticTaskStatus::Accepted,
                claimant: Some(format!("agent:{index}")),
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                claim_renewal_streak: 0,
                active_attempts: Default::default(),
                artifact_refs: vec![artifact_ref],
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: Some("agent:reviewer".into()),
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
    let view = agentic_closure_view(&program, None);
    assert!(!view["remaining"]
        .as_array()
        .unwrap()
        .iter()
        .any(|step| step["step"] == "publish_integration_task"));
    assert_eq!(view["exit_ready"], false);

    // Even a plausible integrated Artifact does not replace the original review.
    program.artifacts.insert(
        "artifact:synthesis".to_string(),
        crate::AgenticArtifactProjection {
            artifact_ref: "artifact:synthesis".to_string(),
            kind: "report".into(),
            title: "synthesis".into(),
            content_ref: "artifact://art_synthesis".into(),
            committed_by: "agent:0".into(),
            relates_to: vec!["task:0".to_string(), "task:1".to_string()],
            claim_execution_id: None,
            claim_generation: None,
        },
    );
    program
        .tasks
        .get_mut("task:0")
        .expect("task 0")
        .artifact_refs
        .push("artifact:synthesis".to_string());
    let view = agentic_closure_view(
        &program,
        Some("original_intent_review_required:user_intent"),
    );
    assert!(view["remaining"]
        .as_array()
        .unwrap()
        .iter()
        .any(|step| step["step"] == "objective_review"));
    assert!(!view["remaining"]
        .as_array()
        .unwrap()
        .iter()
        .any(|step| step["step"] == "objective_complete_request"));
    assert_eq!(view["exit_ready"], false);
}

#[test]
fn same_state_owed_context_separates_mutations_reads_and_unknown_wire() {
    use super::host_backend::owed_action_context;
    let names = ["artifact_commit", "read_ref"].into_iter().map(str::to_string).collect();
    let state = serde_json::json!({"remaining":[{"step":"register_delivery"}],
        "program_source":{"revision":4},"goal":{"spec_revision":2}});
    let context = owed_action_context(&state, Some(&names));
    assert_eq!(context["remaining"], state["remaining"]);
    assert_eq!(context["program_source"], state["program_source"]);
    assert_eq!(context["candidate_mutations"], serde_json::json!(["artifact_commit"]));
    assert_eq!(context["prerequisite_reads"], serde_json::json!(["read_ref"]));
    assert_eq!(context["final_wire_inventory"], "unknown_until_provider_compilation");
    let missing = owed_action_context(&state, None);
    assert_eq!(missing["candidate_mutations"], serde_json::json!([]));
    assert_eq!(missing["tool_inventory_status"], "unknown");
}

#[test]
fn same_state_optional_review_route_is_not_an_immediate_root_mutation() {
    use super::host_backend::owed_action_context;
    let names = ["objective_review", "team_create", "task_publish", "read_ref"]
        .into_iter().map(str::to_string).collect();
    let state = serde_json::json!({"remaining":[{
        "step":"objective_review","closing_tool":"objective_review",
        "actionability":"requires_explicit_independent_actor",
        "optional_route":{"execution":"optional_explicit_authorized_calls_only"}
    }]});
    let context = owed_action_context(&state, Some(&names));
    assert_eq!(context["candidate_mutations"], serde_json::json!([]));
    assert_eq!(context["advisory_routes"][0]["optional_route"],
        state["remaining"][0]["optional_route"]);
    assert_eq!(context["remaining"], state["remaining"]);
    assert_eq!(context["final_wire_inventory"], "unknown_until_provider_compilation");
    let unknown = owed_action_context(&state, None);
    assert_eq!(unknown["candidate_mutations"], serde_json::json!([]));
    assert_eq!(unknown["tool_inventory_status"], "unknown");
}

#[test]
fn same_state_publication_discovery_is_advisory_not_forced_business_debt() {
    use super::host_backend::owed_action_context;
    let names = ["artifact_commit", "artifact_publish", "tool_search"]
        .into_iter().map(str::to_string).collect();
    let state = serde_json::json!({"remaining":[{
        "step":"register_delivery","closing_tool":"artifact_commit",
        "publication_route":{"discovery_tool":"tool_search",
            "file_publication_tool":"artifact_publish","registration_tool":"artifact_commit",
            "body_alternative":"message://current"}
    }]});
    let context = owed_action_context(&state, Some(&names));
    assert_eq!(context["candidate_mutations"], serde_json::json!(["artifact_commit"]));
    assert_eq!(context["advisory_routes"][0]["publication_route"],
        state["remaining"][0]["publication_route"]);
    assert_eq!(context["final_wire_inventory"], "unknown_until_provider_compilation");
}
