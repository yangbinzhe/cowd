//! Prepare, but never independently commit, the Goal half of Program closure.
use super::*;
use crate::AgenticProgramProjection;

pub(crate) struct PreparedProgramConclusion {
    pub goal: GoalContract,
    pub expected_stream_revision: u64,
    pub event: RuntimeTransactionEventInput,
    pub gaps: Vec<String>,
    pub policy_sources: Vec<crate::ExpectedStreamRevision>,
}

fn review_covers_current_goal(
    goal: &GoalContract,
    program: &AgenticProgramProjection,
    review: &harness_contract::goal::ObjectiveReviewRecord,
    result_refs: &[String],
    evidence: &[String],
    policy: Option<&crate::agentic::review_evidence::RootReviewPolicySnapshot>,
    effects: Option<&crate::agentic::review_evidence::RootReviewPolicySnapshot>,
    result_sources: &std::collections::BTreeMap<String, u64>,
) -> bool {
    review.spec_revision == goal.spec_revision
        && review.input_manifest_digest == goal.spec_digest
        && review.decision == "satisfied"
        && review.verification.as_ref().is_some_and(|proof| {
            proof.result_source_revisions.iter().all(|(source, revision)| result_sources.get(source) == Some(revision))
                && proof.reads.iter().all(|read| {
                    (read.result_kind != harness_contract::goal::GoalResultKind::ExternalDecision
                        && !read.result_ref.starts_with("approval:v1:"))
                        || proof.result_source_revisions.contains_key(&format!("approval:{}", read.result_ref))
                })
                && proof.goal_spec_revision == Some(goal.spec_revision)
                && proof.goal_spec_digest.as_deref() == Some(goal.spec_digest.as_str())
                && proof.work_manifest_digest
                    == crate::agentic::review_evidence::work_manifest_digest(program)
                && !proof.reviewer_execution_id.is_empty()
                && !proof.producer_execution_ids.is_empty()
                // Historical delegated proofs predate canonical effect-source
                // verification. They require a fresh review, not a default
                // assumption that an Artifact producer performed no writes.
                && (!proof.producer_execution_ids.iter().any(|producer| producer.starts_with("agent-run:"))
                    || (!proof.effect_source_refs.is_empty() && proof.effect_manifest_digest.is_some()))
                && proof.producer_execution_ids.iter().all(|producer| {
                    !producer.is_empty()
                        && (!proof.independence_required
                            || producer != &proof.reviewer_execution_id)
                })
                && (if proof.independence_required {
                    !review.producer_refs.contains(&review.reviewer_actor)
                } else {
                    proof.reviewer_execution_id
                        == format!(
                            "root-execution:{}",
                            program.root_execution_id.as_deref().unwrap_or_default()
                        )
                        && policy.is_some_and(|policy| {
                            proof.review_policy_digest.as_deref() == Some(policy.digest.as_str())
                        })

                })
                && ((proof.effect_manifest_digest.is_none()
                    && proof.effect_source_refs.is_empty()
                    && effects.is_some_and(|effects| effects.last_effect_cursor == 0)
                    && proof.reads.iter().all(|read| {
                        read.result_kind != harness_contract::goal::GoalResultKind::ToolEffect
                    }))
                    || effects.is_some_and(|effects| {
                        proof.effect_manifest_digest.as_deref() == Some(effects.digest.as_str())
                    }))
                && result_refs.iter().all(|reference| {
                    proof.reads.iter().any(|read| {
                        &read.result_ref == reference
                            && !read.sha256.is_empty()
                            && !read.receipt_refs.is_empty()
                            && (read.result_kind
                                != harness_contract::goal::GoalResultKind::ToolEffect
                                || !read.effect_observation_refs.is_empty())
                    })
                })
        })
        && !review.evidence_refs.is_empty()
        && !review.producer_refs.is_empty()
        && !review.result_refs.is_empty()
        && !result_refs.is_empty()
        && result_refs
            .iter()
            .all(|reference| review.result_refs.contains(reference))
        && review
            .evidence_refs
            .iter()
            .all(|reference| evidence.contains(reference))
}

impl GoalStore {
    pub(crate) fn prepare_program_conclusion(
        &self,
        program: &AgenticProgramProjection,
    ) -> Result<PreparedProgramConclusion, String> {
        let request = program
            .completion_request
            .as_ref()
            .ok_or("missing completion request")?;
        let root = program
            .root_execution_id
            .as_deref()
            .ok_or("missing root binding")?;
        let projection = self
            .projection(&format!("goal:{root}"))?
            .ok_or("missing bound Goal")?;
        let mut goal = projection.goal;
        let mut result_sources = std::collections::BTreeMap::new();
        for source in goal
            .reviews
            .iter()
            .filter_map(|review| review.verification.as_ref())
            .flat_map(|proof| proof.result_source_revisions.keys())
        {
            if !result_sources.contains_key(source) {
                result_sources.insert(
                    source.clone(),
                    self.event_store
                        .stream_revision(source)
                        .map_err(|error| error.to_string())?,
                );
            }
        }
        let binding = goal
            .execution_binding
            .as_ref()
            .ok_or("missing Goal execution binding")?;
        if binding.root_execution_id != root
            || binding.agentic_program_id != program.program_id
            || binding.objective_id != program.objective_id
            || binding.session_id != program.session_id
            || binding.turn_id != program.turn_id
            || goal.completion != GoalCompletion::Open
            || goal.terminal.is_some()
        {
            return Err("completion request does not match the open bound Goal".into());
        }
        let policy = if goal.reviews.iter().any(|review| {
            review
                .verification
                .as_ref()
                .is_some_and(|proof| !proof.independence_required)
        }) {
            crate::agentic::review_evidence::root_self_review_policy(
                &self.event_store,
                program,
                &goal,
            )?
        } else {
            None
        };
        let mut effects = std::collections::BTreeMap::new();
        effects.insert(
            Vec::new(),
            crate::agentic::review_evidence::effect_review_snapshot(
                &self.event_store,
                program,
                Some(&goal),
                &[],
            )?,
        );
        for proof in goal
            .reviews
            .iter()
            .filter_map(|review| review.verification.as_ref())
        {
            if proof.effect_manifest_digest.is_some()
                && !effects.contains_key(&proof.effect_source_refs)
            {
                effects.insert(
                    proof.effect_source_refs.clone(),
                    crate::agentic::review_evidence::effect_review_snapshot(
                        &self.event_store,
                        program,
                        Some(&goal),
                        &proof.effect_source_refs,
                    )?,
                );
            }
        }
        let mut evidence = goal.evidence_refs.clone();
        evidence.extend(projection.progress.evidence_refs.iter().cloned());
        evidence.extend(request.evidence_refs.iter().cloned());
        evidence.extend(
            goal.obligations
                .iter()
                .flat_map(|obligation| obligation.evidence_refs.iter().cloned()),
        );
        evidence.push(format!("execution_graph:{root}"));
        for task in program.tasks.values() {
            evidence.extend(task.evidence_refs.iter().cloned());
            evidence.extend(task.supersede_evidence_refs.iter().cloned());
        }
        evidence.sort();
        evidence.dedup();
        apply_completion_criterion_state(&mut goal, &projection.progress, &evidence);

        // A graph identity proves execution lineage, not semantic coverage of
        // the user's request. Consume the current, independently attributed
        // Objective review rather than manufacturing obligations from Tasks.
        let mut gaps = Vec::new();
        if let Some(participation) = goal.participation_requirement.as_ref() {
            let contributors = program
                .tasks
                .values()
                .filter(|task| {
                    task.status == crate::AgenticTaskStatus::Accepted
                        && task.claimant.is_some()
                        && task.reviewed_by.is_some()
                        && task.claimant != task.reviewed_by
                        && !task.evidence_refs.is_empty()
                })
                .map(|task| &task.team_id)
                .collect::<std::collections::BTreeSet<_>>();
            if contributors.len() < usize::from(participation.minimum_team_count) {
                gaps.push(format!(
                    "original_participation_missing:expected={},actual={}",
                    participation.minimum_team_count,
                    contributors.len()
                ));
            }
        }
        let intent = goal
            .user_intent_criterion_id
            .clone()
            .ok_or("missing original user intent")?;
        let reviewed = goal
            .reviews
            .iter()
            .rev()
            .find(|review| review.criterion_ref == intent)
            .is_some_and(|review| {
                review_covers_current_goal(
                    &goal,
                    program,
                    review,
                    &request.result_refs,
                    &evidence,
                    policy.as_ref(),
                    review
                        .verification
                        .as_ref()
                        .and_then(|proof| effects.get(&proof.effect_source_refs)),
                    &result_sources,
                )
            });
        for obligation in &goal.obligations {
            if obligation.required {
                let current = goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == obligation.obligation_id)
                    .is_some_and(|review| {
                        review_covers_current_goal(
                            &goal,
                            program,
                            review,
                            &obligation.artifact_refs,
                            &evidence,
                            policy.as_ref(),
                            review
                                .verification
                                .as_ref()
                                .and_then(|proof| effects.get(&proof.effect_source_refs)),
                            &result_sources,
                        )
                    });
                if !current {
                    gaps.push(format!(
                        "obligation_review_required:{}",
                        obligation.obligation_id
                    ));
                }
            }
        }

        if let Some(criterion) = goal
            .criteria
            .iter_mut()
            .find(|criterion| criterion.id == intent)
        {
            if criterion.status != AcceptanceStatus::Waived {
                criterion.status = if reviewed {
                    AcceptanceStatus::Satisfied
                } else {
                    AcceptanceStatus::Open
                };
            }
        }
        if !reviewed {
            gaps.push(format!("original_intent_review_required:{intent}"));
        }
        let stale_criteria = goal
            .criteria
            .iter()
            .filter(|criterion| {
                criterion.id != intent && criterion.status == AcceptanceStatus::Satisfied
            })
            .filter(|criterion| {
                !goal
                    .reviews
                    .iter()
                    .rev()
                    .find(|review| review.criterion_ref == criterion.id)
                    .is_some_and(|review| {
                        review_covers_current_goal(
                            &goal,
                            program,
                            review,
                            &review.result_refs,
                            &evidence,
                            policy.as_ref(),
                            review
                                .verification
                                .as_ref()
                                .and_then(|proof| effects.get(&proof.effect_source_refs)),
                            &result_sources,
                        )
                    })
            })
            .map(|criterion| criterion.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for criterion in &mut goal.criteria {
            if stale_criteria.contains(&criterion.id) {
                criterion.status = AcceptanceStatus::Open;
                gaps.push(format!("criterion_review_required:{}", criterion.id));
            }
        }
        for criterion in &goal.criteria {
            if !matches!(
                criterion.status,
                AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
            ) {
                gaps.push(format!("criterion_unsatisfied:{}", criterion.id));
            }
        }
        if let Err(gap) = validate_completion(
            &goal,
            &projection.progress,
            GoalCompletion::Satisfied,
            &evidence,
        ) {
            gaps.push(gap);
        }
        goal.evidence_refs = evidence;
        goal.revision = goal.revision.saturating_add(1);
        let fence = format!(
            "agentic-objective:{}:request:{}",
            program.program_id, request.program_revision
        );
        let (kind, status) = if gaps.is_empty() {
            goal.completion = GoalCompletion::Satisfied;
            goal.phase = "completed".into();
            goal.waiting = None;
            goal.terminal = Some(ObjectiveTerminal {
                kind: ObjectiveTerminalKind::Satisfied,
                terminal_fence: fence.clone(),
                authority_revision: request.program_revision,
                reason: "Current original-objective review and durable acceptance evidence are satisfied".into(),
                evidence_refs: goal.evidence_refs.clone(),
                diagnostics: Vec::new(),
                committed_at_ms: crate::tool_invocation::now_ms(),
            });
            ("goal.completed", "satisfied")
        } else {
            goal.phase = "waiting".into();
            goal.waiting = Some(harness_contract::goal::WaitDescriptor {
                reason_code: "completion_gap".into(),
                reason: gaps.join("; "),
                required_refs: gaps.clone(),
                generation: request.program_revision,
            });
            ("goal.completion_waiting", "open")
        };
        validate_goal(&goal)?;
        let event = goal_event(
            &goal,
            kind,
            status,
            "runtime.objective_supervisor".into(),
            vec![RuntimeEventRef {
                kind: "program".into(),
                id: program.program_id.clone(),
            }],
            serde_json::json!({"goal": goal, "request_revision": request.program_revision}),
            format!("program-conclusion:{fence}"),
        );
        Ok(PreparedProgramConclusion {
            goal,
            expected_stream_revision: projection.stream_revision,
            event,
            gaps,
            policy_sources: {
                let mut sources = result_sources;
                for snapshot in policy.into_iter().chain(effects.into_values()) {
                    for source in
                        std::iter::once(snapshot.source).chain(snapshot.additional_sources)
                    {
                        if sources
                            .insert(source.stream_id.clone(), source.expected_revision)
                            .is_some_and(|revision| revision != source.expected_revision)
                        {
                            return Err(
                                "effect source changed during conclusion preparation".into()
                            );
                        }
                    }
                }
                sources
                    .into_iter()
                    .map(
                        |(stream_id, expected_revision)| crate::ExpectedStreamRevision {
                            stream_id,
                            expected_revision,
                        },
                    )
                    .collect()
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
        MessagePublishInput, ObjectiveCompleteRequestInput, TeamCreateInput,
    };
    use harness_contract::goal::{
        AcceptanceCriterion, GoalExecutionBinding, GoalScope, ObjectiveReviewRecord,
    };

    fn fixture(
        reviewed: bool,
    ) -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        crate::AgentActionService,
        AgentActorBinding,
    ) {
        fixture_with_request(reviewed, true)
    }

    fn fixture_with_request(
        reviewed: bool,
        request_completion: bool,
    ) -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        crate::AgentActionService,
        AgentActorBinding,
    ) {
        let events = Arc::new(RuntimeEventStore::for_test());
        let goals = GoalStore::new(Arc::clone(&events));
        let actions = crate::AgentActionService::new(Arc::clone(&events));
        let actor = AgentActorBinding {
            objective_id: "objective-completion".into(),
            program_id: "program-completion".into(),
            session_id: "session-completion".into(),
            turn_id: "turn-completion".into(),
            root_execution_id: Some("root-completion".into()),
            required_team_count: 0,
            objective_summary: "Preserve original acceptance".into(),
            model_lease: "test".into(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec![],
            actor_id: "root-completion-actor".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let first = actions
            .apply(&AgentActionEnvelope {
                action_id: "open".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: format!("topic:{}", actor.program_id),
                    summary: Some("work began".into()),
                    content_ref: None,
                    refs: vec![],
                    recipients: vec![],
                    intent: None,
                    issue_dispositions: vec![],
                }),
            })
            .unwrap();
        assert_eq!(first.status, AgentActionStatus::Applied);
        if request_completion {
            let requested = actions
                .apply(&AgentActionEnvelope {
                    action_id: "request".into(),
                    actor: actor.clone(),
                    expected_revision: None,
                    action: AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                        result_refs: vec!["tool://result".into()],
                        evidence_refs: vec!["tool://evidence".into()],
                        unresolved: vec![],
                    }),
                })
                .unwrap();
            assert_eq!(requested.status, AgentActionStatus::Applied);
        }
        goals
            .create(GoalContract {
                id: "goal:root-completion".into(),
                session_id: actor.session_id.clone(),
                objective: actor.objective_summary.clone(),
                criteria: vec![AcceptanceCriterion {
                    id: "user_intent".into(),
                    statement: actor.objective_summary.clone(),
                    statement_ref: None,
                    source_refs: vec!["session_message:original".into()],
                    required_evidence: vec!["execution_graph:root-completion".into()],
                    status: AcceptanceStatus::Open,
                    waiver: None,
                }],
                constraints: vec![],
                phase: "execution".into(),
                evidence_refs: vec![],
                unresolved: vec![],
                blockers: vec![],
                scope: GoalScope::UserObjective,
                user_intent_criterion_id: Some("user_intent".into()),
                source_intent_ref: Some("session_message:original".into()),
                execution_binding: Some(GoalExecutionBinding {
                    objective_id: actor.objective_id.clone(),
                    session_id: actor.session_id.clone(),
                    turn_id: actor.turn_id.clone(),
                    root_execution_id: "root-completion".into(),
                    agentic_program_id: actor.program_id.clone(),
                }),
                spec_revision: 1,
                spec_digest: "original-spec".into(),
                review_refs: vec![],
                waiting: None,
                participation_requirement: None,
                obligations: vec![],
                recovery: None,
                terminal: None,
                completion: GoalCompletion::Open,
                revision: 1,
                user_sequence: 1,
                reviews: if reviewed {
                    vec![ObjectiveReviewRecord {
                        review_id: "review".into(),
                        criterion_ref: "user_intent".into(),
                        spec_revision: 1,
                        input_manifest_digest: "original-spec".into(),
                        result_refs: vec!["tool://result".into()],
                        evidence_refs: vec!["tool://evidence".into()],
                        reviewer_actor: "reviewer".into(),
                        reviewer_execution_id: Some("review-run".into()),
                        producer_refs: vec!["producer".into()],
                        decision: "satisfied".into(),
                        reason_ref: "tool://evidence".into(),
                        // Mechanical transaction fixture, not physical-read acceptance evidence.
                        verification: Some(harness_contract::goal::ObjectiveReviewVerification {
                            result_source_revisions: Default::default(),
                            independence_required: true,
                            review_policy_digest: None,
                            effect_manifest_digest: None,
                            effect_source_refs: Vec::new(),
                            goal_spec_revision: Some(1),
                            goal_spec_digest: Some("original-spec".into()),
                            work_manifest_digest:
                                crate::agentic::review_evidence::work_manifest_digest(
                                    &actions.project(&actor.program_id).unwrap(),
                                ),
                            reviewer_execution_id: "review-run".into(),
                            producer_execution_ids: vec!["producer-run".into()],
                            reads: vec![harness_contract::goal::ObjectiveResultReadProof {
                                result_kind: harness_contract::goal::GoalResultKind::Artifact,
                                effect_observation_refs: vec![],
                                result_ref: "tool://result".into(),
                                content_ref: "artifact://fixture".into(),
                                sha256: "fixture-hash".into(),
                                bytes: 1,
                                receipt_refs: vec!["fixture:read".into()],
                            }],
                        }),
                    }]
                } else {
                    vec![]
                },
            })
            .unwrap();
        (events, goals, actions, actor)
    }

    #[test]
    fn historical_delegated_review_without_effect_sources_cannot_verify_goal() {
        let (_, goals, actions, actor) = fixture(true);
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let mut review = goal.reviews[0].clone();
        review.verification.as_mut().unwrap().producer_execution_ids =
            vec!["agent-run:historical".into()];
        assert!(!review_covers_current_goal(
            &goal,
            &program,
            &review,
            &review.result_refs,
            &review.evidence_refs,
            None,
            None,
            &Default::default()
        ));
    }

    #[test]
    fn external_decision_review_requires_current_source_revision_and_final_cas() {
        let (events, goals, actions, actor) = fixture(true);
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let mut review = goal.reviews[0].clone();
        let source = "approval:approval:v1:source:node".to_string();
        review
            .verification
            .as_mut()
            .unwrap()
            .result_source_revisions
            .insert(source.clone(), 1);
        let effects = crate::agentic::review_evidence::effect_review_snapshot(
            &events,
            &program,
            Some(&goal),
            &[],
        )
        .unwrap();
        assert!(review_covers_current_goal(
            &goal,
            &program,
            &review,
            &review.result_refs,
            &review.evidence_refs,
            None,
            Some(&effects),
            &std::collections::BTreeMap::from([(source.clone(), 1)])
        ));
        // A missing or revised source cannot be accepted even if all older
        // artifact and policy assertions still match.
        assert!(!review_covers_current_goal(
            &goal,
            &program,
            &review,
            &review.result_refs,
            &review.evidence_refs,
            None,
            Some(&effects),
            &Default::default()
        ));
        assert!(!review_covers_current_goal(
            &goal,
            &program,
            &review,
            &review.result_refs,
            &review.evidence_refs,
            None,
            Some(&effects),
            &std::collections::BTreeMap::from([(source.clone(), 2)])
        ));
        let mut conclusion = goals.prepare_program_conclusion(&program).unwrap();
        conclusion
            .policy_sources
            .push(crate::ExpectedStreamRevision {
                stream_id: source.clone(),
                expected_revision: 0,
            });
        events
            .append(crate::RuntimeEventInput {
                stream_id: source,
                scope: crate::RuntimeEventScope::Approval,
                kind: "test.concurrent_external_decision_source".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        assert!(actions
            .commit_program_conclusion(&program, conclusion)
            .is_err());
        assert_eq!(
            actions.project(&actor.program_id).unwrap().revision,
            program.revision
        );
        assert!(goals
            .get("goal:root-completion")
            .unwrap()
            .unwrap()
            .terminal
            .is_none());
    }

    #[test]
    fn historical_root_review_without_effect_digest_cannot_hide_a_later_effect() {
        let (events, goals, actions, actor) = fixture(true);
        // Negative historical corruption only: never used to prove a write or
        // successful review. A legacy unclassified effect must invalidate proof.
        events.append(RuntimeEventInput {
            stream_id: format!("session:{}", actor.session_id), scope: RuntimeEventScope::Session,
            kind: "tool.invocation.completed".into(), status: Some("completed".into()),
            actor: Some("conversation_runtime".into()), refs: vec![],
            payload: serde_json::json!({"tool_name":"unclassified_mutation", "test_fault":"legacy-missing-descriptor"}),
        }).unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&program).unwrap();
        assert!(prepared.goal.terminal.is_none());
        assert!(prepared
            .gaps
            .iter()
            .any(|gap| gap.starts_with("original_intent_review_required:")));
    }

    #[test]
    fn graph_identity_cannot_replace_original_review_and_gap_reopens_durably() {
        let (events, goals, actions, actor) = fixture(false);
        let before = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&before).unwrap();
        assert!(prepared.goal.terminal.is_none());
        assert!(prepared
            .gaps
            .iter()
            .any(|gap| gap.starts_with("original_intent_review_required:")));
        actions
            .commit_program_conclusion(&before, prepared)
            .unwrap();
        let restarted = crate::AgentActionService::new(events);
        let after = restarted.project(&actor.program_id).unwrap();
        assert_eq!(after.status, crate::AgenticProgramStatus::Open);
        assert!(after.completion_request.is_none());
        assert!(goals
            .get("goal:root-completion")
            .unwrap()
            .unwrap()
            .waiting
            .is_some());
        let new_work = restarted
            .apply(&AgentActionEnvelope {
                action_id: "fill-gap".into(),
                actor,
                expected_revision: Some(after.revision),
                action: AgentAction::TeamCreate(TeamCreateInput {
                    name: "Required review".into(),
                    mission: "fill original gap".into(),
                    objective: None,
                }),
            })
            .unwrap();
        assert_eq!(new_work.status, AgentActionStatus::Applied);
    }

    #[test]
    fn later_gap_review_supersedes_an_earlier_satisfied_review() {
        let (_, goals, actions, actor) = fixture(true);
        goals
            .revise("goal:root-completion", 1, 2, "review found a gap", |goal| {
                let mut review = goal.reviews.last().unwrap().clone();
                review.review_id = "later-gap".into();
                review.decision = "gap".into();
                goal.reviews.push(review);
                vec!["reviews".into()]
            })
            .unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&program).unwrap();
        assert!(prepared
            .gaps
            .iter()
            .any(|gap| gap.starts_with("original_intent_review_required:")));
        let reopened = actions
            .commit_program_conclusion(&program, prepared)
            .unwrap();
        assert_eq!(reopened.status, crate::AgenticProgramStatus::Open);
        assert!(goals
            .get("goal:root-completion")
            .unwrap()
            .unwrap()
            .terminal
            .is_none());
    }

    #[test]
    fn new_counterevidence_invalidates_a_previous_positive_review() {
        let (_, goals, actions, actor) = fixture_with_request(true, false);
        let before = actions.project(&actor.program_id).unwrap();
        let original = goals.get("goal:root-completion").unwrap().unwrap();
        assert_eq!(
            original.reviews[0]
                .verification
                .as_ref()
                .unwrap()
                .work_manifest_digest,
            crate::agentic::review_evidence::work_manifest_digest(&before)
        );
        // A newly observed issue changes the source manifest even with the same Goal spec.
        let published = actions
            .apply(&AgentActionEnvelope {
                action_id: "new-counterevidence".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: format!("topic:{}", actor.program_id),
                    summary: Some("New evidence contradicts the result".into()),
                    content_ref: None,
                    refs: vec!["tool://counterevidence".into()],
                    recipients: vec![],
                    intent: None,
                    issue_dispositions: vec![],
                }),
            })
            .unwrap();
        assert_eq!(published.status, AgentActionStatus::Applied);
        let requested = actions
            .apply(&AgentActionEnvelope {
                action_id: "request-after-new-evidence".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    result_refs: vec!["tool://result".into()],
                    evidence_refs: vec!["tool://evidence".into()],
                    unresolved: vec![],
                }),
            })
            .unwrap();
        assert_eq!(requested.status, AgentActionStatus::Applied);
        let current = actions.project(&actor.program_id).unwrap();
        assert!(goals
            .prepare_program_conclusion(&current)
            .unwrap()
            .gaps
            .iter()
            .any(|gap| gap.starts_with("original_intent_review_required:")));
    }

    #[test]
    fn goal_cas_conflict_cannot_commit_half_a_program_terminal() {
        let (_, goals, actions, actor) = fixture(true);
        let before = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&before).unwrap();
        assert!(prepared.gaps.is_empty());
        goals
            .revise(
                "goal:root-completion",
                1,
                2,
                "concurrent new requirement",
                |goal| {
                    goal.constraints.push("new requirement".into());
                    vec!["constraints".into()]
                },
            )
            .unwrap();
        assert!(actions
            .commit_program_conclusion(&before, prepared)
            .is_err());
        assert_eq!(
            actions.project(&actor.program_id).unwrap().revision,
            before.revision
        );
        assert!(goals
            .get("goal:root-completion")
            .unwrap()
            .unwrap()
            .terminal
            .is_none());
    }

    #[test]
    fn program_cas_conflict_cannot_commit_half_a_goal_terminal() {
        let (events, goals, actions, actor) = fixture(true);
        let before = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&before).unwrap();
        events
            .append(RuntimeEventInput {
                stream_id: "agentic-program:program-completion".into(),
                scope: RuntimeEventScope::Program,
                kind: "test.concurrent_observation".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        assert!(actions
            .commit_program_conclusion(&before, prepared)
            .is_err());
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        assert_eq!(goal.revision, 1);
        assert!(goal.terminal.is_none());
        assert!(actions
            .project(&actor.program_id)
            .unwrap()
            .objective_verdict
            .is_none());
    }

    #[test]
    fn ordinary_goal_writers_cannot_bypass_the_program_atomic_conclusion() {
        let (events, goals, actions, actor) = fixture(true);
        let goal_id = "goal:root-completion";
        let before = events.stream_revision("goal:goal:root-completion").unwrap();
        let terminal = ObjectiveTerminal {
            kind: ObjectiveTerminalKind::Satisfied,
            terminal_fence: "bypass".into(),
            authority_revision: 1,
            reason: "attempt ordinary completion".into(),
            evidence_refs: vec!["execution_graph:root-completion".into()],
            diagnostics: vec![],
            committed_at_ms: 1,
        };
        for error in [
            goals
                .terminal_event(
                    goal_id,
                    GoalCompletion::Satisfied,
                    terminal.evidence_refs.clone(),
                    terminal.reason.clone(),
                    terminal.terminal_fence.clone(),
                )
                .unwrap_err(),
            goals
                .complete(goal_id, 1, GoalCompletion::Satisfied, "done")
                .unwrap_err(),
            goals.complete_objective(goal_id, 1, terminal).unwrap_err(),
        ] {
            assert!(error.contains("program_terminal_authority"), "{error}");
        }
        assert_eq!(
            events.stream_revision("goal:goal:root-completion").unwrap(),
            before
        );
        let program = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&program).unwrap();
        assert!(prepared.gaps.is_empty());
        actions
            .commit_program_conclusion(&program, prepared)
            .unwrap();
        assert_eq!(
            goals.get(goal_id).unwrap().unwrap().completion,
            GoalCompletion::Satisfied
        );
    }

    #[test]
    fn valid_current_review_commits_goal_and_program_together() {
        let (events, goals, actions, actor) = fixture(true);
        let before = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&before).unwrap();
        let after = actions
            .commit_program_conclusion(&before, prepared)
            .unwrap();
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        assert_eq!(goal.completion, GoalCompletion::Satisfied);
        assert_eq!(after.status, crate::AgenticProgramStatus::Verified);
        assert_eq!(
            after.objective_verdict.as_ref().unwrap().terminal_fence,
            goal.terminal.as_ref().unwrap().terminal_fence
        );
        let goal_event = events.list_stream("goal:goal:root-completion").unwrap();
        let program_event = events
            .list_stream("agentic-program:program-completion")
            .unwrap();
        assert!(!goal_event.is_empty());
        assert_eq!(
            goal_event.last().unwrap().transaction_id,
            program_event.last().unwrap().transaction_id
        );
    }

    #[test]
    fn original_obligations_and_new_spec_cannot_be_erased_by_task_completion() {
        let (_, goals, actions, actor) = fixture(true);
        goals
            .revise(
                "goal:root-completion",
                1,
                2,
                "retain required verification",
                |goal| {
                    goal.obligations
                        .push(harness_contract::goal::ObjectiveObligation {
                            obligation_id: "original-verification".into(),
                            required: true,
                            success_predicate: "verify the user's actual result".into(),
                            producer: Default::default(),
                            evidence_requirement: Default::default(),
                            state: ObjectiveObligationState::Open,
                            artifact_refs: vec![],
                            evidence_refs: vec![],
                            reread_receipts: vec![],
                            verifier_decision: None,
                            diagnostic_code: None,
                        });
                    goal.spec_revision = 2;
                    goal.spec_digest = "new-spec".into();
                    vec![
                        "obligations".into(),
                        "spec_revision".into(),
                        "spec_digest".into(),
                    ]
                },
            )
            .unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&program).unwrap();
        assert!(prepared.goal.terminal.is_none());
        assert!(prepared
            .gaps
            .iter()
            .any(|gap| gap.contains("original-verification")));
        assert!(prepared
            .gaps
            .iter()
            .any(|gap| gap.starts_with("original_intent_review_required:")));
        actions
            .commit_program_conclusion(&program, prepared)
            .unwrap();
        let original = goals.get("goal:root-completion").unwrap().unwrap();
        assert_eq!(original.obligations.len(), 1);
        assert_eq!(
            original.obligations[0].obligation_id,
            "original-verification"
        );
        assert!(original.obligations[0].required);
        assert_eq!(
            original.obligations[0].state,
            ObjectiveObligationState::Open
        );
    }
}
