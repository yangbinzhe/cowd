//! Operation-local identity and source cuts for the original Goal owner.
//! A frame is evidence for one preparation, never a second durable owner.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::GoalProjection;
use crate::AgenticCompletionRequestProjection;
use crate::{AgenticProgramProjection, ExpectedStreamRevision};

const ASSESSMENT_TAIL_MAX_EVENTS: u64 = 1024;
const ASSESSMENT_TAIL_PAGE_EVENTS: usize = 64;
const ASSESSMENT_TAIL_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AssessmentReadError {
    Identity(&'static str),
    Encoding(String),
    Source(String),
    ConflictingSource {
        stream_id: String,
        expected: u64,
        observed: u64,
    },
}

impl std::fmt::Display for AssessmentReadError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identity(reason) => write!(output, "assessment identity: {reason}"),
            Self::Encoding(reason) => write!(output, "assessment encoding: {reason}"),
            Self::Source(reason) => write!(output, "assessment source unavailable: {reason}"),
            Self::ConflictingSource {
                stream_id,
                expected,
                observed,
            } => write!(
                output,
                "assessment source changed: source={stream_id} expected={expected} actual={observed}"
            ),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AssessmentSourcePins(BTreeMap<String, ExpectedStreamRevision>);

impl AssessmentSourcePins {
    pub(crate) fn pin(
        &mut self,
        source: ExpectedStreamRevision,
    ) -> Result<(), AssessmentReadError> {
        if let Some(previous) = self.0.get(&source.stream_id) {
            if previous.expected_revision != source.expected_revision {
                return Err(AssessmentReadError::ConflictingSource {
                    stream_id: source.stream_id,
                    expected: previous.expected_revision,
                    observed: source.expected_revision,
                });
            }
            return Ok(());
        }
        self.0.insert(source.stream_id.clone(), source);
        Ok(())
    }

    pub(crate) fn sources(&self) -> impl Iterator<Item = &ExpectedStreamRevision> {
        self.0.values()
    }

    pub(crate) fn into_sources(self) -> Vec<ExpectedStreamRevision> {
        self.0.into_values().collect()
    }
}

/// Read one operation-local append-only tail under a single event-count and
/// byte budget. Callers still enforce their own semantic identity over the
/// returned records; this helper only unifies the resource admission policy.
pub(crate) fn read_bounded_event_tail(
    store: &crate::RuntimeEventStore,
    source: &str,
    pinned: u64,
    through: u64,
) -> Result<Vec<crate::DurableRuntimeEvent>, AssessmentReadError> {
    let count = through
        .checked_sub(pinned)
        .ok_or(AssessmentReadError::Source(
            "assessment source cut moved backwards".into(),
        ))?;
    if count > ASSESSMENT_TAIL_MAX_EVENTS {
        return Err(AssessmentReadError::Source(
            "assessment tail event budget exhausted; rebuild original source frame".into(),
        ));
    }
    let mut events = Vec::new();
    let mut after = pinned;
    let mut bytes = 0usize;
    while after < through {
        let page = store
            .list_stream_after(
                source,
                after,
                through,
                ASSESSMENT_TAIL_PAGE_EVENTS,
                ASSESSMENT_TAIL_MAX_BYTES.saturating_sub(bytes),
            )
            .map_err(AssessmentReadError::Source)?;
        if page.is_empty() {
            return Err(AssessmentReadError::Source(
                "assessment tail is incomplete".into(),
            ));
        }
        for event in page {
            if event.stream_id != source || event.sequence != after + 1 || event.sequence > through
            {
                return Err(AssessmentReadError::Source(
                    "assessment tail identity or sequence changed".into(),
                ));
            }
            bytes = bytes
                .checked_add(
                    serde_json::to_vec(&event)
                        .map_err(|error| AssessmentReadError::Encoding(error.to_string()))?
                        .len(),
                )
                .ok_or(AssessmentReadError::Source(
                    "assessment tail byte budget overflow".into(),
                ))?;
            if bytes > ASSESSMENT_TAIL_MAX_BYTES {
                return Err(AssessmentReadError::Source(
                    "assessment tail byte budget exhausted".into(),
                ));
            }
            after = event.sequence;
            events.push(event);
        }
    }
    Ok(events)
}

#[derive(Debug, Clone)]
pub(crate) struct AssessmentSourceFrame {
    program_id: String,
    objective_id: String,
    session_id: String,
    turn_id: String,
    root_id: String,
    program_revision: u64,
    program_digest: String,
    goal_id: String,
    goal_revision: u64,
    goal_digest: String,
    request: Option<AgenticCompletionRequestProjection>,
    pins: AssessmentSourcePins,
}

fn identity_digest(goal: &impl serde::Serialize) -> Result<String, AssessmentReadError> {
    let bytes = serde_json::to_vec(goal)
        .map_err(|error| AssessmentReadError::Encoding(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

impl AssessmentSourceFrame {
    pub(crate) fn new(
        program: &AgenticProgramProjection,
        projection: &GoalProjection,
    ) -> Result<Self, AssessmentReadError> {
        if program.completion_request.is_none()
            || projection.goal.completion != harness_contract::goal::GoalCompletion::Open
            || projection.goal.terminal.is_some()
        {
            return Err(AssessmentReadError::Identity(
                "request does not match the open bound Goal",
            ));
        }
        Self::capture(program, projection)
    }

    pub(crate) fn capture(
        program: &AgenticProgramProjection,
        projection: &GoalProjection,
    ) -> Result<Self, AssessmentReadError> {
        let goal = &projection.goal;
        if !team_quantity_valid(program, super::current_goal_team_bounds(goal)) {
            return Err(AssessmentReadError::Source(
                "Team quantity is inconsistent with its current business contract".into(),
            ));
        }
        let root = program
            .root_execution_id
            .as_ref()
            .ok_or(AssessmentReadError::Identity("missing root binding"))?;
        let binding = goal
            .execution_binding
            .as_ref()
            .ok_or(AssessmentReadError::Identity(
                "missing Goal execution binding",
            ))?;
        if goal.id != format!("goal:{root}")
            || goal.session_id != program.session_id
            || binding.root_execution_id != *root
            || binding.agentic_program_id != program.program_id
            || binding.objective_id != program.objective_id
            || binding.session_id != program.session_id
            || binding.turn_id != program.turn_id
        {
            return Err(AssessmentReadError::Identity(
                "request does not match the open bound Goal",
            ));
        }
        let mut frame = Self {
            program_id: program.program_id.clone(),
            objective_id: program.objective_id.clone(),
            session_id: program.session_id.clone(),
            turn_id: program.turn_id.clone(),
            root_id: root.clone(),
            program_revision: program.revision,
            program_digest: identity_digest(program)?,
            goal_id: goal.id.clone(),
            goal_revision: projection.stream_revision,
            goal_digest: identity_digest(goal)?,
            request: program.completion_request.clone(),
            pins: AssessmentSourcePins::default(),
        };
        frame.pin(ExpectedStreamRevision {
            stream_id: format!("agentic-program:{}", program.program_id),
            expected_revision: program.revision,
        })?;
        frame.pin(ExpectedStreamRevision {
            stream_id: format!("goal:{}", goal.id),
            expected_revision: projection.stream_revision,
        })?;
        Ok(frame)
    }

    pub(crate) fn verify_program(
        &self,
        program: &AgenticProgramProjection,
    ) -> Result<(), AssessmentReadError> {
        if program.program_id != self.program_id
            || program.objective_id != self.objective_id
            || program.session_id != self.session_id
            || program.turn_id != self.turn_id
            || program.root_execution_id.as_ref() != Some(&self.root_id)
            || program.revision != self.program_revision
            || identity_digest(program)? != self.program_digest
            || program.completion_request != self.request
        {
            return Err(AssessmentReadError::Identity(
                "Program or original request changed",
            ));
        }
        Ok(())
    }

    pub(crate) fn verify_goal(&self, goal: &GoalProjection) -> Result<(), AssessmentReadError> {
        if goal.goal.id != self.goal_id
            || goal.stream_revision != self.goal_revision
            || identity_digest(&goal.goal)? != self.goal_digest
        {
            return Err(AssessmentReadError::Identity(
                "Goal/spec/review/source changed",
            ));
        }
        Ok(())
    }

    pub(crate) fn pin(
        &mut self,
        source: ExpectedStreamRevision,
    ) -> Result<(), AssessmentReadError> {
        self.pins.pin(source)
    }

    pub(crate) fn sources(&self) -> impl Iterator<Item = &ExpectedStreamRevision> {
        self.pins.sources()
    }

    pub(crate) fn into_sources(self) -> Vec<ExpectedStreamRevision> {
        self.pins.into_sources()
    }

    #[cfg(test)]
    pub(crate) fn clear_sources_for_test(&mut self) {
        self.pins = AssessmentSourcePins::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bound_pair() -> (AgenticProgramProjection, GoalProjection) {
        use harness_contract::goal::{GoalCompletion, GoalContract, GoalExecutionBinding};
        let mut program = AgenticProgramProjection::empty("program", "objective");
        program.session_id = "session".into();
        program.turn_id = "turn".into();
        program.root_execution_id = Some("root".into());
        program.revision = 39;
        program.completion_request = Some(AgenticCompletionRequestProjection {
            action_id: "complete-original".into(),
            requested_by: "actor".into(),
            program_revision: 38,
            result_refs: vec!["artifact:result".into()],
            primary_artifact_ref: None,
            evidence_refs: vec![],
            unresolved: vec![],
        });
        let goal = GoalContract {
            input_contract: None,
            id: "goal:root".into(),
            session_id: "session".into(),
            objective: "deliver".into(),
            criteria: vec![],
            constraints: vec![],
            phase: "execution".into(),
            evidence_refs: vec![],
            unresolved: vec![],
            blockers: vec![],
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            execution_binding: Some(GoalExecutionBinding {
                objective_id: "objective".into(),
                session_id: "session".into(),
                turn_id: "turn".into(),
                root_execution_id: "root".into(),
                agentic_program_id: "program".into(),
            }),
            spec_revision: 1,
            spec_digest: "spec".into(),
            review_refs: vec![],
            reviews: vec![],
            waiting: None,
            participation_requirement: None,
            obligations: vec![],
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        let projection = GoalProjection {
            progress: super::super::GoalProgressReducer::from_goal(&goal),
            goal,
            stream_revision: 7,
            observations: vec![],
            interventions: vec![],
            intervention_traces: vec![],
        };
        (program, projection)
    }

    #[test]
    fn frame_keeps_original_cuts_and_rejects_rebound_request_or_goal() {
        let (program, goal) = bound_pair();
        let frame = AssessmentSourceFrame::new(&program, &goal).unwrap();
        frame.verify_program(&program).unwrap();
        frame.verify_goal(&goal).unwrap();
        assert_eq!(
            frame.clone().into_sources(),
            vec![
                ExpectedStreamRevision {
                    stream_id: "agentic-program:program".into(),
                    expected_revision: 39
                },
                ExpectedStreamRevision {
                    stream_id: "goal:goal:root".into(),
                    expected_revision: 7
                },
            ]
        );
        let mut changed = program.clone();
        changed.completion_request.as_mut().unwrap().action_id = "other-action".into();
        assert!(frame.verify_program(&changed).is_err());
        changed = program.clone();
        changed.unresolved.push("new unknown".into());
        assert!(
            frame.verify_program(&changed).is_err(),
            "same revision cannot hide changed contents"
        );
        let mut changed_goal = goal.clone();
        changed_goal.goal.spec_digest = "other-spec".into();
        assert!(frame.verify_goal(&changed_goal).is_err());
        changed_goal = goal.clone();
        changed_goal.stream_revision += 1;
        assert!(frame.verify_goal(&changed_goal).is_err());
    }

    #[test]
    fn frame_rejects_missing_or_cross_session_binding_and_closed_goal() {
        let (program, goal) = bound_pair();
        let mut changed = goal.clone();
        changed.goal.execution_binding = None;
        assert!(AssessmentSourceFrame::new(&program, &changed).is_err());
        changed = goal.clone();
        changed.goal.execution_binding.as_mut().unwrap().session_id = "other".into();
        assert!(AssessmentSourceFrame::new(&program, &changed).is_err());
        changed = goal.clone();
        changed.goal.session_id = "other".into();
        assert!(AssessmentSourceFrame::new(&program, &changed).is_err());
        changed = goal;
        changed.goal.completion = harness_contract::goal::GoalCompletion::Satisfied;
        assert!(AssessmentSourceFrame::new(&program, &changed).is_err());
    }

    #[test]
    fn missing_bound_source_is_unknown_and_only_unbound_is_not_applicable() {
        let (mut program, goal) = bound_pair();
        assert!(assess_bound_goal_projection(&program, None).is_err());
        let events = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        assert!(read_bound_goal_assessment(&events, &program).is_err());
        assert!(matches!(
            assess_bound_goal_projection(&program, Some(&goal)).unwrap(),
            BoundGoalAssessment::Bound { .. }
        ));
        let assessment = assess_bound_goal_projection(&program, Some(&goal)).unwrap();
        let mut changed = program.clone();
        changed.turn_id = "another-turn".into();
        assert_eq!(assessment.view(&changed)["readiness"], "unknown");
        assert_eq!(assessment.goal_context(&changed)["status"], "unavailable");
        program.root_execution_id = None;
        assert!(matches!(
            read_bound_goal_assessment(&events, &program).unwrap(),
            BoundGoalAssessment::NotApplicable
        ));
        assert!(assess_bound_goal_projection(&program, Some(&goal)).is_err());
    }

    #[test]
    fn bounded_tail_uses_one_resource_policy_and_fails_closed_above_its_budget() {
        let store = crate::RuntimeEventStore::for_test();
        for index in 0..70 {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: "assessment:tail".into(),
                    scope: crate::RuntimeEventScope::Program,
                    kind: "assessment.process".into(),
                    status: None,
                    actor: Some("assessor".into()),
                    refs: vec![],
                    payload: serde_json::json!({"index": index}),
                })
                .unwrap();
        }
        let head = store.stream_revision("assessment:tail").unwrap();
        assert_eq!(
            read_bounded_event_tail(&store, "assessment:tail", 0, head)
                .unwrap()
                .len(),
            70,
            "the old eight-record liveness barrier no longer truncates a valid tail"
        );
        assert!(
            read_bounded_event_tail(&store, "assessment:tail", 0, 1025)
                .unwrap_err()
                .to_string()
                .contains("event budget exhausted"),
            "a revision gap beyond the shared event budget fails closed"
        );
    }

    #[test]
    fn original_required_obligation_blocks_but_optional_suggestion_does_not() {
        let (mut program, mut goal) = bound_pair();
        program.completion_request = None;
        goal.goal.obligations.push(
            serde_json::from_value(serde_json::json!({
                "obligation_id":"user:source-checked", "required":true,
                "success_predicate":"original source must be checked"
            }))
            .unwrap(),
        );
        let assessment = assess_bound_goal_projection(&program, Some(&goal)).unwrap();
        let view = assessment.view(&program);
        assert_eq!(view["blocked"]["reason"], "required_goal_obligations_open");
        assert_eq!(
            view["blocked"]["obligation_refs"],
            serde_json::json!(["user:source-checked"])
        );
        assert_eq!(view["exit_ready"], false);
        goal.goal.obligations[0].required = false;
        let view = assess_bound_goal_projection(&program, Some(&goal))
            .unwrap()
            .view(&program);
        assert_ne!(view["blocked"]["reason"], "required_goal_obligations_open");
        assert_eq!(
            view["exit_ready"], false,
            "suggestion is not acceptance or ACK"
        );
    }

    #[test]
    fn terminal_suggestion_requires_same_open_goal_without_recorded_gap() {
        let (program, mut goal) = bound_pair();
        let assessment = assess_bound_goal_projection(&program, Some(&goal)).unwrap();
        assert!(assessment.admits_terminal_suggestion(&program));
        let mut other = program.clone();
        other.turn_id = "other-turn".into();
        assert!(!assessment.admits_terminal_suggestion(&other));
        goal.goal.waiting = Some(harness_contract::goal::WaitDescriptor {
            reason_code: "completion_gap".into(),
            reason: "original external input is missing".into(),
            required_refs: vec!["external-input:missing".into()],
            generation: 1,
        });
        let assessment = assess_bound_goal_projection(&program, Some(&goal)).unwrap();
        assert!(assessment.gap().is_some());
        assert!(!assessment.admits_terminal_suggestion(&program));
        assert!(assessment.terminal_action(&program).is_none());
        goal.goal.waiting = None;
        goal.goal.completion = harness_contract::goal::GoalCompletion::Partial;
        let assessment = assess_bound_goal_projection(&program, Some(&goal)).unwrap();
        assert!(!assessment.admits_terminal_suggestion(&program));
    }

    #[test]
    fn conflicting_source_never_replaces_the_original_cut() {
        let mut pins = AssessmentSourcePins::default();
        let source = ExpectedStreamRevision {
            stream_id: "reviewer".into(),
            expected_revision: 4,
        };
        pins.pin(source.clone()).unwrap();
        pins.pin(source).unwrap();
        assert_eq!(pins.sources().count(), 1);
        assert_eq!(
            pins.pin(ExpectedStreamRevision {
                stream_id: "reviewer".into(),
                expected_revision: 5
            }),
            Err(AssessmentReadError::ConflictingSource {
                stream_id: "reviewer".into(),
                expected: 4,
                observed: 5
            })
        );
        assert_eq!(
            pins.into_sources(),
            vec![ExpectedStreamRevision {
                stream_id: "reviewer".into(),
                expected_revision: 4
            }]
        );
    }

    /// A Program with the given team-count contract and team count.
    fn z_program(
        required_teams: u8,
        exact_teams: Option<u8>,
        teams: usize,
    ) -> AgenticProgramProjection {
        let mut program = AgenticProgramProjection::empty("program", "objective");
        program.session_id = "session".into();
        program.turn_id = "turn".into();
        program.root_execution_id = Some("root".into());
        program.required_team_count = required_teams;
        program.exact_team_count = exact_teams;
        for index in 0..teams {
            let team_id = format!("team-{index}");
            program.teams.insert(
                team_id.clone(),
                crate::AgenticTeamProjection {
                    team_id: team_id.clone(),
                    name: team_id.clone(),
                    mission: "deliver".into(),
                    objective: None,
                    topic_ref: format!("topic:{team_id}"),
                    created_by: "root".into(),
                    member_ids: vec![],
                    task_ids: vec![],
                    lifecycle: crate::AgenticTeamLifecycle::Active,
                },
            );
        }
        program
    }

    /// An objective, with or without a participation requirement.
    ///
    /// The participation requirement is the objective's own promise. Its
    /// input contract carries no team-count commitment, exactly as the
    /// #7 objective's does: the promise lives in the participation
    /// requirement, not in the input contract.
    fn z_goal(minimum_team_count: Option<u8>) -> harness_contract::goal::GoalContract {
        use harness_contract::goal::{
            GoalCompletion, GoalContract, GoalExecutionBinding, GoalInputContract,
        };
        GoalContract {
            input_contract: Some(GoalInputContract {
                version: 1,
                principal_id: "principal".into(),
                session_id: "session".into(),
                turn_id: "turn".into(),
                source_ref: "source".into(),
                input_revision: 1,
                content_sha256: "sha".into(),
                commitments: vec![],
                proposed_workspace_scopes: vec![],
            }),
            id: "goal:root".into(),
            session_id: "session".into(),
            objective: "deliver".into(),
            criteria: vec![],
            constraints: vec![],
            phase: "execution".into(),
            evidence_refs: vec![],
            review_refs: vec![],
            reviews: vec![],
            waiting: None,
            participation_requirement: minimum_team_count.map(|minimum| {
                harness_contract::goal::ParticipationRequirement {
                    minimum_team_count: minimum,
                    source_ref: "source".into(),
                }
            }),
            obligations: vec![],
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            unresolved: vec![],
            blockers: vec![],
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            spec_revision: 1,
            spec_digest: "spec".into(),
            execution_binding: Some(GoalExecutionBinding {
                objective_id: "objective".into(),
                session_id: "session".into(),
                turn_id: "turn".into(),
                root_execution_id: "root".into(),
                agentic_program_id: "program".into(),
            }),
        }
    }

    fn z_has_rung(assessment: &CompletionAssessment, rung: &str) -> bool {
        assessment.remaining.iter().any(|step| match (step, rung) {
            (ClosureStep::Team, "team") => true,
            (ClosureStep::PublishTask, "publish_task") => true,
            _ => false,
        })
    }

    /// Z-P1 (the #7 shape, and the discriminating assertion): the objective
    /// promises three teams, and the Program projection still carries its
    /// own default of zero teams because no action has applied the promise
    /// yet. The ladder must read the objective's promise.
    ///
    /// Under the old code this assertion fails: the ladder read the
    /// Program projection's default of zero teams, so the publish_task rung
    /// was absent and the #7 objective published no Task at all.
    #[test]
    fn z_p1_the_ladder_reads_the_objectives_participation_requirement() {
        let program = z_program(0, None, 1);
        let goal = z_goal(Some(1));
        let assessment = CompletionAssessment::for_bound(&program, &goal, None);
        assert_eq!(
            assessment.facts.required_teams, 1,
            "the ladder's fact source must be the objective's participation requirement, not the program projection's default 0"
        );
        assert!(!z_has_rung(&assessment, "team"));
        assert!(z_has_rung(&assessment, "publish_task"));
    }

    /// Z-P2: an objective that promises participation gets the ladder's team
    /// and Task rungs, whatever the Program projection says.
    #[test]
    fn z_p2_an_objective_that_promises_participation_gets_the_ladder() {
        let program = z_program(0, None, 0);
        let goal = z_goal(Some(1));
        let assessment = CompletionAssessment::for_bound(&program, &goal, None);
        assert!(z_has_rung(&assessment, "team"));
        assert!(z_has_rung(&assessment, "publish_task"));
    }

    /// Z-N3: the negative control. An objective that promises no
    /// participation promises none, and the Program projection's own default
    /// stays zero - a promise of one team would invent a collaboration
    /// requirement for every objective.
    ///
    /// This control holds with and without the ladder reading the objective's
    /// promise: it says what an objective without one may never be given, not
    /// which fact the ladder reads.
    #[test]
    fn z_n3_an_objective_without_participation_promises_none() {
        let program = z_program(0, None, 0);
        let goal = z_goal(None);
        let assessment = CompletionAssessment::for_bound(&program, &goal, None);
        assert_eq!(
            assessment.facts.required_teams, 0,
            "an objective without a participation requirement promises no team"
        );
        assert!(!z_has_rung(&assessment, "team"));
        assert!(!z_has_rung(&assessment, "publish_task"));
        // The Program projection's own default is still zero teams: the
        // ladder may read the objective's promise, but no default may become
        // a promise of one team.
        let program_source = include_str!("../../agentic/program.rs");
        assert!(
            program_source.contains("required_team_count: 0,"),
            "the Program projection's default team count must stay zero"
        );
    }

    /// Z-N4: the maximum team count still comes from the Program projection.
    /// The participation requirement states a minimum only, so the ladder's
    /// upper bound stays exactly as it was.
    #[test]
    fn z_n4_the_maximum_team_count_still_comes_from_the_program_projection() {
        let program = z_program(1, Some(3), 0);
        let goal = z_goal(Some(1));
        let assessment = CompletionAssessment::for_bound(&program, &goal, None);
        assert_eq!(assessment.facts.maximum_teams, Some(3));
        assert_eq!(
            assessment.facts.exact_teams,
            Some(3),
            "the exact team count must not be dropped by the participation requirement"
        );
    }

    /// Z-N4b: an explicit zero team count is a prohibition. A topology
    /// without members has no Task to publish.
    #[test]
    fn z_n4b_a_teamless_topology_is_never_asked_for_a_published_task() {
        let program = z_program(0, Some(0), 0);
        let goal = z_goal(Some(1));
        let assessment = CompletionAssessment::for_bound(&program, &goal, None);
        assert!(!z_has_rung(&assessment, "publish_task"));
    }

    /// Z-N2: without an objective, the program's own team-count contract is
    /// still what the ladder reads.
    #[test]
    fn z_n2_without_an_objective_the_program_contract_still_drives_the_ladder() {
        let program = z_program(1, None, 0);
        let assessment = CompletionAssessment::for_program(&program, None);
        assert!(z_has_rung(&assessment, "team"));
        assert!(z_has_rung(&assessment, "publish_task"));
    }

    /// Z-N6: the ladder has one producer. The publish_task rung is pushed in
    /// exactly one place, and the observation side only echoes it.
    #[test]
    fn z_n6_the_ladder_has_one_producer() {
        // The program contract and the objective promise the same team
        // count here, so the rung is produced exactly once whichever fact the
        // ladder reads: this control is about the number of producers, not
        // about which fact the ladder reads.
        let program = z_program(1, None, 0);
        let goal = z_goal(Some(1));
        let assessment = CompletionAssessment::for_bound(&program, &goal, None);
        assert_eq!(
            assessment
                .remaining
                .iter()
                .filter(|step| **step == ClosureStep::PublishTask)
                .count(),
            1,
            "the publish_task rung must be produced exactly once"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompletionTerminalAction {
    RegisterDelivery { prerequisite_task_refs: Vec<String> },
    RequestObjectiveCompletion { result_artifact_refs: Vec<String> },
}

fn team_quantity_valid(
    program: &AgenticProgramProjection,
    current: Option<(u8, Option<u8>)>,
) -> bool {
    current.map_or_else(
        || program.validate_team_quantity_contract().is_ok(),
        |(minimum, maximum)| {
            maximum.is_none_or(|maximum| {
                maximum >= minimum && program.teams.len() <= usize::from(maximum)
            })
        },
    )
}

pub(crate) fn root_agentic_terminal_action(
    program: &crate::AgenticProgramProjection,
) -> Option<CompletionTerminalAction> {
    root_agentic_terminal_action_with_quantity(program, None)
}

fn root_agentic_terminal_action_with_quantity(
    program: &AgenticProgramProjection,
    current: Option<(u8, Option<u8>)>,
) -> Option<CompletionTerminalAction> {
    if !team_quantity_valid(program, current) {
        return None;
    }
    if !matches!(
        program.status,
        crate::AgenticProgramStatus::Open | crate::AgenticProgramStatus::Waiting
    ) {
        return None;
    }
    if !program.unresolved.is_empty() || crate::agentic::issues::completion_gap(program).is_some() {
        return None;
    }
    let prerequisite_task_refs = program
        .tasks
        .values()
        .filter(|task| !task.status.is_retired())
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    if prerequisite_task_refs.is_empty()
        || program.tasks.values().any(|task| {
            !task.status.is_retired() && task.status != crate::AgenticTaskStatus::Accepted
        })
    {
        return None;
    }

    let result_artifact_refs =
        crate::agentic::supervision::completion_ready_result_artifact_refs(program);
    if result_artifact_refs.is_empty() {
        Some(CompletionTerminalAction::RegisterDelivery {
            prerequisite_task_refs,
        })
    } else {
        Some(CompletionTerminalAction::RequestObjectiveCompletion {
            result_artifact_refs,
        })
    }
}

pub(crate) fn completion_view(
    program: &AgenticProgramProjection,
    completion_gap: Option<&str>,
) -> serde_json::Value {
    let assessment = CompletionAssessment::for_program(program, completion_gap);
    render_completion_view(program, completion_gap, &assessment)
}

/// The completion view whose ladder is the objective's participation
/// requirement.
///
/// The ladder's fact source is the objective: its `input_contract` carries
/// the `TeamCount` promise. A program that was never told about that promise
/// (its projection defaults to zero teams) must not decide that no
/// participation was required - that is the seam class Z lives in.
pub(crate) fn completion_view_for_goal(
    program: &AgenticProgramProjection,
    completion_gap: Option<&str>,
    goal: &harness_contract::goal::GoalContract,
) -> serde_json::Value {
    let assessment = CompletionAssessment::for_bound(program, goal, completion_gap);
    render_completion_view(program, completion_gap, &assessment)
}

fn render_completion_view(
    program: &AgenticProgramProjection,
    completion_gap: Option<&str>,
    assessment: &CompletionAssessment,
) -> serde_json::Value {
    let facts = &assessment.facts;
    let teams = facts.teams;
    let required_teams = facts.required_teams;
    let published = facts.published;
    let claimed = facts.claimed;
    let reviewed = facts.reviewed;
    let accepted = facts.accepted;
    let unclaimed = facts.unclaimed;
    let submitted = facts.submitted;
    let pending = facts.pending;
    let issue_blockers = &facts.issue_blockers;
    let unclassified = issue_blockers.len();
    let mut remaining: Vec<serde_json::Value> = Vec::new();
    if assessment.has(ClosureStep::Team) {
        remaining.push(serde_json::json!({
            "step": "team", "outstanding": required_teams - teams,
            "hint": "Runtime requires at least this many Teams; names/roles are yours."
        }));
    }
    if assessment.has(ClosureStep::PublishTask) {
        remaining.push(
            serde_json::json!({"step": "publish_task", "requirement":"required_participation"}),
        );
    }
    if assessment.has(ClosureStep::ClaimTask) {
        remaining.push(serde_json::json!({
            "step": "claim_task", "outstanding": unclaimed
        }));
    }
    if assessment.has(ClosureStep::IndependentReview) {
        remaining.push(serde_json::json!({
            "step": "independent_review", "outstanding": submitted,
            "hint": "A different Agent must review each submitted Task."
        }));
    }
    let mut blocked = serde_json::Value::Null;
    if assessment.blocked == Some(ClosureBlock::TasksNotAccepted) {
        blocked = serde_json::json!({"reason":"tasks_not_accepted","outstanding":pending,
            "hint":"Advance the actual Task state; task_review cannot close an unsubmitted Task. Retired Tasks are not accepted results."});
    }
    if assessment.has(ClosureStep::RegisterDelivery) {
        remaining.push(serde_json::json!({"step":"register_delivery","closing_tool":"artifact_commit",
            "hint":"No Program deliverable is registered. For a real file, discover artifact_publish using tool_search(query=artifact_publish) if deferred; read the actual file, then artifact_publish(source=file,path,sha256,media_type) and artifact_commit(content_ref=<returned immutable selector>,kind,title). For actual response Text use artifact_commit(content_ref=message://current,kind,title). File/tool receipts alone are not registration; never guess a selector or force a Team/Task.",
            "publication_route":{"discovery_tool":"tool_search","query":"artifact_publish",
                "file_publication_tool":"artifact_publish","file_source":"file",
                "file_fields":["path","sha256","media_type"],
                "registration_tool":"artifact_commit","selector":"returned content_ref",
                "body_alternative":"actual response Text + artifact_commit(content_ref=message://current,kind,title)",
                "execution":"explicit_authorized_calls_only","exposure":"requires_actual_native_schema"}}));
    }
    if assessment.has(ClosureStep::ClassifyIssues) {
        remaining.push(serde_json::json!({
            "step": "classify_issues", "outstanding": unclassified,
            "issue_refs": issue_blockers.iter().map(|(reference, _)| reference.clone()).collect::<Vec<_>>(),
            "hint": "Completion is rejected while any entry stays unclassified; dispose them through one message_publish.issue_dispositions."
        }));
    }
    if assessment.has(ClosureStep::RequestCompletion) {
        remaining.push(serde_json::json!({
            "step": "objective_complete_request",
            "hint": "Request the original Objective's governed verification; accepted Tasks do not certify it."
        }));
    }
    if let Some(gap) = completion_gap {
        // The rung must name the action that closes THIS gap. Every gap used to
        // be rendered as `objective_review`, so an `issue_requires_classification`
        // gap was advertised as a review while the completion-gap guidance in the
        // same envelope asked for `message_publish`. The model was handed two
        // mutually exclusive instructions for one gap and satisfied neither.
        let kind = crate::agentic::issues::completion_gap_kind(gap);
        let hint = if kind.is_issue_gap() {
            format!(
                "The Goal verifier records a completion gap, so the Objective is not closable yet: {gap}. \
                 Close it by classifying EVERY unclassified issue_ref listed above through ONE \
                 `message_publish` call carrying `issue_dispositions`. Each entry needs the exact \
                 `issue:` reference, a disposition of `must_resolve`, `disclose` or `resolved`, a \
                 durable `reason_ref`, and `evidence_refs` drawn from artifact://, tool://, task:, \
                 issue: or program: (the issue's own source Task ref is durable). Do not request \
                 completion until this clears."
            )
        } else {
            "The Goal verifier records a completion gap, so the Objective is not closable yet. Close this gap first; do not re-request completion until it clears. Close it by having an Agent READ the delivered results itself in its own execution and then call objective_review: a read by another Agent, by the root, or by an earlier attempt of the same Agent does not count, and submitting the review before those reads only burns the retry budget.".to_string()
        };
        let needs_independent_actor = kind.closing_tool() == "objective_review";
        let optional_route = needs_independent_actor.then(|| serde_json::json!({
            "status":"requires_explicit_independent_actor",
            "team_observation":if program.teams.is_empty() {"absent"} else {"present_admission_unverified"},
            "strategy":"Use a suitable existing Team or explicitly choose a minimal reviewer-only Team, subject to user constraints and authorization. Minimum zero Teams is not a prohibition. This is an optional execution carrier, not a new business deliverable.",
            "admission":"If chosen and authorized, team_create only when needed; agent_invite a distinct read-capable Agent; task_publish(purpose=exploration) with the original criterion and actual current registered result refs. Existing membership alone does not prove reviewer eligibility.",
            "review":"The distinct admitted Agent must really claim the Task and read the current result refs in its own execution, then objective_review with complete current results, read evidence and durable reason.",
            "retirement":"After recorded control work, explicitly task_withdraw with truthful reason_ref/evidence_refs, or task_release if work remains. Retired is not Accepted; do not delete the Task or create a redundant report.",
            "completion":"Independent review does not complete the Goal. Preserve original criterion, manifest and real Goal verifier/CAS; objective_complete_request remains governed.",
            "execution":"optional_explicit_authorized_calls_only",
            "prohibitions":["no_auto_create","no_self_review","no_auto_accept","no_manifest_rewrite"],
            "exposure":"requires_actual_native_schema_and_actor_authorization"
        }));
        remaining.push(serde_json::json!({
            "step": kind.closure_step(),
            "outstanding_gap": gap,
            "closing_tool": kind.closing_tool(),
            "actionability":if needs_independent_actor { "requires_explicit_independent_actor" } else { "requires_actor_and_reads" },
            "optional_route":optional_route,
            "hint": hint
        }));
    }

    if assessment.blocked == Some(ClosureBlock::ReviewReadinessUnknown) {
        blocked = serde_json::json!({"reason":"objective_review_readiness_unknown",
            "hint":"A registered artifact and no reported gap do not prove current original-intent review. Inspect the bound Goal and current review evidence first. If review is missing, a suitable existing Team or explicitly chosen minimal reviewer-only Team with a distinct admitted Agent and typed Exploration Task is an optional authorized route, not business Task debt. Do not auto-create, self-review, autoaccept or infer success."});
    }
    if assessment.blocked == Some(ClosureBlock::RequiredGoalObligations) {
        blocked = serde_json::json!({"reason":"required_goal_obligations_open", "obligation_refs":assessment.required_obligation_refs});
    }
    if assessment.blocked == Some(ClosureBlock::TeamQuantityUnknown) {
        blocked = serde_json::json!({"reason":"team_quantity_unknown"});
    }
    let current = assessment.current();
    let satisfied = &assessment.satisfied;

    serde_json::json!({
        "current": current,
        "exit_ready": false,
        "readiness":"requires_bound_goal_verification",
        "blocked":blocked,
        "suggestions":["Topology is optional unless required by the bound user participation contract. Completion remains a governed request, not a count-derived verdict."],
        "team_quantity": {
            "minimum":facts.required_teams,
            "maximum":facts.maximum_teams,
            "exact":facts.exact_teams,
            "provenance_version":facts.quantity_provenance,
            "valid":facts.quantity_valid
        },
        "completion_gap": completion_gap,
        "facts": {
            "teams": teams,
            "required_teams": required_teams,
            "tasks_published": published,
            "tasks_claimed": claimed,
            "tasks_reviewed": reviewed,
            "tasks_accepted": accepted,
            "unresolved": program.unresolved.len(),
            "unclassified_issues": unclassified,
            "issue_refs": issue_blockers
                .iter()
                .map(|(reference, _)| reference.clone())
                .collect::<Vec<_>>(),
        },
        "satisfied": satisfied,
        "remaining": remaining,
    })
}

/// Pure Program facts and suggested next steps. This is not a Goal verdict,
/// physical-exit certificate or delivery ACK; each remains owned by its source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosureStep {
    Team,
    PublishTask,
    ClaimTask,
    IndependentReview,
    RegisterDelivery,
    ClassifyIssues,
    RequestCompletion,
    GoalGap(crate::agentic::issues::CompletionGapKind),
}

impl ClosureStep {
    fn name(self) -> &'static str {
        match self {
            Self::Team => "team",
            Self::PublishTask => "publish_task",
            Self::ClaimTask => "claim_task",
            Self::IndependentReview => "independent_review",
            Self::RegisterDelivery => "register_delivery",
            Self::ClassifyIssues => "classify_issues",
            Self::RequestCompletion => "objective_complete_request",
            Self::GoalGap(kind) => kind.closure_step(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosureBlock {
    TasksNotAccepted,
    ReviewReadinessUnknown,
    RequiredGoalObligations,
    TeamQuantityUnknown,
}

struct CompletionFacts {
    teams: usize,
    required_teams: usize,
    maximum_teams: Option<u8>,
    exact_teams: Option<u8>,
    /// Whether the program's team count explicitly forbids teams.
    teams_forbidden: bool,
    quantity_provenance: u8,
    quantity_valid: bool,
    published: usize,
    claimed: usize,
    reviewed: usize,
    accepted: usize,
    unclaimed: usize,
    submitted: usize,
    pending: usize,
    issue_blockers: Vec<(String, crate::agentic::issues::CompletionGapKind)>,
}

pub(crate) struct CompletionAssessment {
    facts: CompletionFacts,
    remaining: Vec<ClosureStep>,
    blocked: Option<ClosureBlock>,
    satisfied: Vec<&'static str>,
    completion_requested: bool,
    required_obligation_refs: Vec<String>,
}

impl CompletionAssessment {
    pub(crate) fn for_program(program: &AgenticProgramProjection, gap: Option<&str>) -> Self {
        Self::for_quantity(program, gap, None, None)
    }

    fn for_quantity(
        program: &AgenticProgramProjection,
        gap: Option<&str>,
        current: Option<(u8, Option<u8>)>,
        required_teams: Option<u8>,
    ) -> Self {
        use crate::AgenticTaskStatus as Status;
        let active = program
            .tasks
            .values()
            .filter(|task| !task.status.is_retired())
            .collect::<Vec<_>>();
        let facts = CompletionFacts {
            teams: program.teams.len(),
            required_teams: usize::from(required_teams.unwrap_or_else(|| {
                current.map_or(program.required_team_count, |(minimum, _)| minimum)
            })),
            maximum_teams: current.map_or(program.exact_team_count, |(_, maximum)| maximum),
            // An explicit zero team count is a prohibition, not a missing
            // promise: a topology without members has no Task to publish.
            teams_forbidden: program.exact_team_count == Some(0),
            exact_teams: current.map_or(program.exact_team_count, |(minimum, maximum)| {
                maximum.filter(|maximum| *maximum == minimum)
            }),
            quantity_provenance: if current.is_some() {
                2
            } else {
                program.team_count_contract_version
            },
            quantity_valid: team_quantity_valid(program, current),
            published: program.tasks.len(),
            claimed: program
                .tasks
                .values()
                .filter(|task| task.claimant.is_some())
                .count(),
            reviewed: program
                .tasks
                .values()
                .filter(|task| task.reviewed_by.is_some())
                .count(),
            accepted: program
                .tasks
                .values()
                .filter(|task| task.status == Status::Accepted)
                .count(),
            unclaimed: active
                .iter()
                .filter(|task| matches!(task.status, Status::Published | Status::Rework))
                .count(),
            submitted: active
                .iter()
                .filter(|task| task.status == Status::Submitted)
                .count(),
            pending: active
                .iter()
                .filter(|task| task.status != Status::Accepted)
                .count(),
            issue_blockers: crate::agentic::issues::open_issue_blockers(program),
        };
        let mut remaining = Vec::new();
        if facts.teams < facts.required_teams {
            remaining.push(ClosureStep::Team);
        }
        if facts.published == 0 && facts.required_teams > 0 && !facts.teams_forbidden {
            remaining.push(ClosureStep::PublishTask);
        }
        if facts.unclaimed > 0 {
            remaining.push(ClosureStep::ClaimTask);
        }
        if facts.submitted > 0 {
            remaining.push(ClosureStep::IndependentReview);
        }
        if facts.required_teams == 0 && active.is_empty() && program.artifacts.is_empty() {
            remaining.push(ClosureStep::RegisterDelivery);
        }
        if !facts.issue_blockers.is_empty() {
            remaining.push(ClosureStep::ClassifyIssues);
        }
        let blocked = if !facts.quantity_valid {
            Some(ClosureBlock::TeamQuantityUnknown)
        } else if facts.pending > 0 {
            Some(ClosureBlock::TasksNotAccepted)
        } else if gap.is_none()
            && facts.required_teams == 0
            && active.is_empty()
            && !program.artifacts.is_empty()
            && program.completion_request.is_none()
        {
            Some(ClosureBlock::ReviewReadinessUnknown)
        } else {
            None
        };
        if program.completion_request.is_none()
            && gap.is_none()
            && remaining.is_empty()
            && blocked.is_none()
            && !program.artifacts.is_empty()
        {
            remaining.push(ClosureStep::RequestCompletion);
        }
        if let Some(gap) = gap {
            remaining.push(ClosureStep::GoalGap(
                crate::agentic::issues::completion_gap_kind(gap),
            ));
        }
        let mut satisfied = Vec::new();
        if facts.teams >= facts.required_teams {
            satisfied.push("team");
        }
        if facts.published > 0 {
            satisfied.push("publish_task");
        }
        if facts.unclaimed == 0 && facts.pending == 0 && facts.published > 0 {
            satisfied.push("claim_task");
        }
        if facts.reviewed >= facts.published
            && facts.accepted == facts.published
            && facts.published > 0
        {
            satisfied.push("independent_review");
        }
        if facts.accepted >= facts.published && facts.published > 0 {
            satisfied.push("accepted_task");
        }
        if facts.issue_blockers.is_empty() {
            satisfied.push("classify_issues");
        }
        Self {
            facts,
            remaining,
            blocked,
            satisfied,
            completion_requested: program.completion_request.is_some(),
            required_obligation_refs: Vec::new(),
        }
    }

    fn for_bound(
        program: &AgenticProgramProjection,
        goal: &harness_contract::goal::GoalContract,
        gap: Option<&str>,
    ) -> Self {
        // The objective's participation requirement is the ladder's fact
        // source for the team count: the one source of the participation
        // requirement is the objective's own promise, and it is durable
        // before any action has applied it to the Program. The Program
        // projection carries the same count only after such an action, and
        // its own default is zero - a default, not a promise. An objective
        // that promises no participation promises none, so it must not
        // inherit the projection default either.
        //
        // Only the team count is replaced. The maximum and the exact count
        // keep the fact source they always had: a participation requirement
        // states a minimum only, so no upper bound is taken from it and
        // none is dropped by it.
        let mut assessment = Self::for_quantity(
            program,
            gap,
            super::current_goal_team_bounds(goal),
            goal.participation_requirement
                .as_ref()
                .map(|requirement| requirement.minimum_team_count),
        );
        assessment.required_obligation_refs = goal
            .obligations
            .iter()
            .filter(|obligation| {
                obligation.required
                    && obligation.state
                        != harness_contract::goal::ObjectiveObligationState::Satisfied
            })
            .map(|obligation| obligation.obligation_id.clone())
            .collect();
        if !assessment.required_obligation_refs.is_empty() {
            assessment.blocked = Some(ClosureBlock::RequiredGoalObligations);
            assessment
                .remaining
                .retain(|step| *step != ClosureStep::RequestCompletion);
        } else if goal.completion == harness_contract::goal::GoalCompletion::Open
            && assessment.blocked == Some(ClosureBlock::ReviewReadinessUnknown)
            && current_reviews_satisfy_the_goal(goal)
            && uncovered_delivered_result_refs(program, goal).is_empty()
        {
            assessment.blocked = None;
            assessment.remaining.push(ClosureStep::RequestCompletion);
        }
        assessment
    }

    fn has(&self, step: ClosureStep) -> bool {
        self.remaining.contains(&step)
    }

    pub(crate) fn remaining_names(&self) -> Vec<&'static str> {
        self.remaining.iter().map(|step| step.name()).collect()
    }

    fn current(&self) -> &'static str {
        if self.blocked.is_some() {
            "blocked"
        } else if self.completion_requested {
            "completion_requested"
        } else {
            self.remaining
                .first()
                .map(|step| step.name())
                .unwrap_or("goal_verification_required")
        }
    }
}

pub(crate) fn closure_rungs_for_program(
    program: &AgenticProgramProjection,
) -> Vec<serde_json::Value> {
    completion_view(program, None)["remaining"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Whether the Goal's own records say every current criterion is semantically
/// reviewed and satisfied. Extracted so the Program projection and the ingress
/// gap path cannot disagree about what "satisfied" means.
pub(crate) fn current_reviews_satisfy_the_goal(
    goal: &harness_contract::goal::GoalContract,
) -> bool {
    goal.blockers.is_empty()
        && goal.unresolved.is_empty()
        && goal.obligations.iter().all(|obligation| {
            !obligation.required
                || obligation.state == harness_contract::goal::ObjectiveObligationState::Satisfied
        })
        && goal
            .criteria
            .iter()
            .all(|criterion| match criterion.status {
                harness_contract::goal::AcceptanceStatus::Satisfied => goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == criterion.id)
                    .is_some_and(|review| {
                        review.decision == "satisfied"
                            && review.spec_revision == goal.spec_revision
                            && review.input_manifest_digest == goal.spec_digest
                    }),
                harness_contract::goal::AcceptanceStatus::Waived => {
                    criterion.waiver.as_ref().is_some_and(|waiver| {
                        !waiver.actor.trim().is_empty()
                            && !waiver.reason.trim().is_empty()
                            && !waiver.permission_receipt.trim().is_empty()
                    })
                }
                _ => false,
            })
}

/// Delivered result references no current review covers.
///
/// One live loop had the root cite four artifacts per request while the
/// recorded review covered three; nothing told the model which ref was
/// missing, so it re-issued the same partial review until the observation
/// window expired. Extracted so the Program projection derives the same set
/// from the same rule instead of freezing whatever the reopen event happened
/// to say.
pub(crate) fn uncovered_delivered_result_refs(
    program: &crate::AgenticProgramProjection,
    goal: &harness_contract::goal::GoalContract,
) -> Vec<String> {
    let intent = goal.user_intent_criterion_id.clone().unwrap_or_default();
    let covered = goal
        .reviews
        .iter()
        .rev()
        .find(|review| review.criterion_ref == intent)
        .map(|review| review.result_refs.clone())
        .unwrap_or_default();
    // The objective reviewer's own report is evidence of review, not a
    // delivered result: naming it as uncovered sends the model into an
    // unbounded review-regress. Filter it out with the same delivered-set rule
    // the conclusion gate uses.
    let goal_artifacts = goal
        .evidence_refs
        .iter()
        .filter(|reference| reference.starts_with("artifact:"))
        .cloned()
        .collect::<Vec<_>>();
    let delivered =
        crate::execution_core::goal::delivered_result_refs(program, goal, &goal_artifacts);
    goal.evidence_refs
        .iter()
        .filter(|reference| reference.starts_with("artifact:"))
        .filter(|reference| delivered.contains(reference))
        .filter(|reference| !covered.contains(reference))
        .cloned()
        .collect::<Vec<_>>()
}

/// A `completion_gap` that no longer describes reality must not be reported as
/// one. Shared with the Program projection so a satisfied review clears the
/// gap everywhere at once instead of only on the ingress path.
pub(crate) fn objective_completion_waiting_gap(
    waiting: Option<&harness_contract::goal::WaitDescriptor>,
    current_reviews_satisfied: bool,
    uncovered_result_refs: &[String],
) -> Option<String> {
    let waiting = waiting.filter(|waiting| waiting.reason_code == "completion_gap")?;
    let review_only = !waiting.required_refs.is_empty()
        && waiting.required_refs.iter().all(|reference| {
            reference.starts_with("original_intent_review_required:")
                || reference.starts_with("criterion_review_required:")
                || reference.starts_with("criterion_unsatisfied:")
                || reference
                    == "cannot satisfy a goal until every current criterion has semantic review or required durable evidence"
        });
    if current_reviews_satisfied && review_only {
        return None;
    }
    let mut gap = format!("objective_completion_gap:{}", waiting.reason);
    if !uncovered_result_refs.is_empty() {
        gap.push_str(&format!(
            "; uncovered_result_refs: {}",
            uncovered_result_refs.join(", ")
        ));
    }
    Some(gap)
}

pub(crate) enum BoundGoalAssessment {
    NotApplicable,
    Bound {
        source_frame: AssessmentSourceFrame,
        goal: GoalProjection,
        gap: Option<String>,
    },
}

impl BoundGoalAssessment {
    pub(crate) fn gap(&self) -> Option<&str> {
        match self {
            Self::NotApplicable => None,
            Self::Bound { gap, .. } => gap.as_deref(),
        }
    }

    fn admits_terminal_suggestion(&self, program: &AgenticProgramProjection) -> bool {
        match self {
            Self::NotApplicable => false,
            Self::Bound {
                source_frame,
                goal,
                gap,
            } => {
                source_frame.verify_program(program).is_ok()
                    && goal.goal.completion == harness_contract::goal::GoalCompletion::Open
                    && gap.is_none()
                    && CompletionAssessment::for_bound(program, &goal.goal, gap.as_deref())
                        .blocked
                        .is_none()
            }
        }
    }

    pub(crate) fn terminal_action(
        &self,
        program: &AgenticProgramProjection,
    ) -> Option<CompletionTerminalAction> {
        if !self.admits_terminal_suggestion(program) {
            return None;
        }
        let Self::Bound { goal, .. } = self else {
            return None;
        };
        root_agentic_terminal_action_with_quantity(
            program,
            super::current_goal_team_bounds(&goal.goal),
        )
    }

    pub(crate) fn view(&self, program: &AgenticProgramProjection) -> serde_json::Value {
        if let Self::Bound { source_frame, .. } = self {
            if let Err(error) = source_frame.verify_program(program) {
                return unknown_completion_view(program, &error.to_string());
            }
        }
        let assessment = match self {
            Self::NotApplicable => CompletionAssessment::for_program(program, None),
            Self::Bound { goal, gap, .. } => {
                CompletionAssessment::for_bound(program, &goal.goal, gap.as_deref())
            }
        };
        let mut view = render_completion_view(program, self.gap(), &assessment);
        view["goal"] = self.goal_context(program);
        view["completion_assessment"] = self.inspection();
        view
    }

    pub(crate) fn goal_context(&self, program: &AgenticProgramProjection) -> serde_json::Value {
        if let Self::Bound { source_frame, .. } = self {
            if let Err(error) = source_frame.verify_program(program) {
                return serde_json::json!({"status":"unavailable","reason":error.to_string()});
            }
        }
        let Self::Bound {
            goal: projection, ..
        } = self
        else {
            return serde_json::json!({"status":"not_applicable"});
        };
        let goal = &projection.goal;
        serde_json::json!({
            "status":"known", "goal_ref":goal.id, "stream_revision":projection.stream_revision,
            "spec_revision":goal.spec_revision, "spec_digest":goal.spec_digest,
            "completion":goal.completion, "user_intent_criterion_ref":goal.user_intent_criterion_id,
            "source_intent_ref":goal.source_intent_ref, "participation_requirement":goal.participation_requirement,
            "criteria":goal.criteria.iter().map(|criterion| serde_json::json!({
                "criterion_ref":criterion.id, "status":format!("{:?}",criterion.status).to_lowercase(),
            })).collect::<Vec<_>>(),
            "required_obligations":goal.obligations.iter().filter(|obligation| obligation.required).collect::<Vec<_>>(),
            "blockers":goal.blockers, "unresolved":goal.unresolved, "waiting":goal.waiting,
            "review_readiness":{
                "current_reviews_satisfied":current_reviews_satisfy_the_goal(goal),
                "uncovered_result_refs":uncovered_delivered_result_refs(program, goal),
                "note":"Snapshot of original review predicates; commit still verifies sources, evidence and authorization.",
            },
        })
    }

    pub(crate) fn inspection(&self) -> serde_json::Value {
        match self {
            Self::NotApplicable => serde_json::json!({"applicability":"no_program_root_binding"}),
            Self::Bound {
                source_frame,
                goal,
                gap,
            } => serde_json::json!({
                "applicability":"bound", "goal_id":source_frame.goal_id,
                "program_id":source_frame.program_id, "root_execution_id":source_frame.root_id,
                "session_id":source_frame.session_id, "turn_id":source_frame.turn_id,
                "goal_completion":goal.goal.completion, "completion_gap":gap,
                "sources":source_frame.sources().collect::<Vec<_>>(),
                "execution":"requires_original_execution_source", "delivery":"requires_original_terminal_and_ack",
            }),
        }
    }
}

/// Missing or unreadable bound data is never an empty blocker list. The frame
/// records individual cuts; it does not claim they were an atomic read snapshot.
pub(crate) fn read_bound_goal_assessment(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &AgenticProgramProjection,
) -> Result<BoundGoalAssessment, AssessmentReadError> {
    let Some(root) = program.root_execution_id.as_deref() else {
        return Ok(BoundGoalAssessment::NotApplicable);
    };
    let goal = super::GoalStore::new(std::sync::Arc::clone(store))
        .projection(&format!("goal:{root}"))
        .map_err(AssessmentReadError::Source)?
        .ok_or(AssessmentReadError::Identity("bound Goal is missing"))?;
    assess_bound_goal_projection(program, Some(&goal))
}

pub(crate) fn assess_bound_goal_projection(
    program: &AgenticProgramProjection,
    goal: Option<&GoalProjection>,
) -> Result<BoundGoalAssessment, AssessmentReadError> {
    let Some(goal) = goal else {
        return if program.root_execution_id.is_none() {
            Ok(BoundGoalAssessment::NotApplicable)
        } else {
            Err(AssessmentReadError::Identity("bound Goal is missing"))
        };
    };
    let source_frame = AssessmentSourceFrame::capture(program, &goal)?;
    let gap = goal_completion_gap(program, &goal.goal)?;
    Ok(BoundGoalAssessment::Bound {
        source_frame,
        goal: goal.clone(),
        gap,
    })
}

pub(crate) fn goal_completion_gap(
    program: &AgenticProgramProjection,
    goal: &harness_contract::goal::GoalContract,
) -> Result<Option<String>, AssessmentReadError> {
    let binding = goal
        .execution_binding
        .as_ref()
        .ok_or(AssessmentReadError::Identity(
            "Goal execution binding is missing",
        ))?;
    if program.root_execution_id.as_deref() != Some(binding.root_execution_id.as_str())
        || program.session_id != goal.session_id
        || program.session_id != binding.session_id
        || program.turn_id != binding.turn_id
        || program.objective_id != binding.objective_id
        || program.program_id != binding.agentic_program_id
    {
        return Err(AssessmentReadError::Identity(
            "Goal belongs to a different Program or turn",
        ));
    }
    if goal.completion != harness_contract::goal::GoalCompletion::Open {
        return Ok(None);
    }
    Ok(objective_completion_waiting_gap(
        goal.waiting.as_ref(),
        current_reviews_satisfy_the_goal(goal),
        &uncovered_delivered_result_refs(program, goal),
    ))
}

pub(crate) fn unknown_completion_view(
    program: &AgenticProgramProjection,
    error: &str,
) -> serde_json::Value {
    serde_json::json!({
        "current":"blocked", "exit_ready":false, "readiness":"unknown",
        "blocked":{"reason":"goal_context_unavailable","error":error},
        "remaining":[], "goal":{"status":"unavailable","reason":error},
        "completion_assessment":{"status":"unknown","error":error},
        "program_source":{"program_ref":program.program_id,"revision":program.revision},
    })
}
