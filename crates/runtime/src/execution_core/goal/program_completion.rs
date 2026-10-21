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

const COMPLETION_REOPENED_EVENT_KIND: &str = "agentic.completion_reopened";

const COMPLETION_REJECTED_EVENT_KIND: &str = "agentic.completion_rejected";

fn completion_reopen_bound() -> Result<Option<u64>, String> {
    match std::env::var("COWD_COMPLETION_REOPEN_BOUND") {
        Ok(value) => parse_completion_reopen_bound(&value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!(
            "invalid operator completion resource limit: {error}"
        )),
    }
}

fn parse_completion_reopen_bound(value: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or_else(|| {
            "COWD_COMPLETION_REOPEN_BOUND must be an explicit positive resource limit".to_string()
        })
}

fn completion_reopen_count(
    event_store: &crate::RuntimeEventStore,
    program_id: &str,
) -> Result<u64, String> {
    let stream_id = format!("agentic-program:{program_id}");
    let head = event_store
        .stream_revision(&stream_id)
        .map_err(|error| error.to_string())?;
    event_store.count_stream_kinds(
        &stream_id,
        &[
            COMPLETION_REOPENED_EVENT_KIND,
            COMPLETION_REJECTED_EVENT_KIND,
        ],
        head,
    )
}

#[cfg(test)]
#[test]
fn r1_bounded_history_completion_count_is_exact_without_event_hydration() {
    let (store, backend) = crate::content_publication::tests::JournalFault::new_journal();
    let stream = "agentic-program:count";
    let mut expected = 0;
    let events = (0..1000)
        .map(|index| {
            let kind = match index % 7 {
                0 => COMPLETION_REOPENED_EVENT_KIND,
                1 => COMPLETION_REJECTED_EVENT_KIND,
                _ => "agentic.unrelated",
            };
            if index % 7 < 2 {
                expected += 1;
            }
            crate::RuntimeEventInput {
                stream_id: stream.into(),
                scope: crate::RuntimeEventScope::Program,
                kind: kind.into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({"index":index}),
            }
            .into()
        })
        .collect();
    store
        .append_batch_if_revision(stream, 0, "exact-count", events)
        .unwrap();
    backend.reset_history_reads();
    assert_eq!(completion_reopen_count(&store, "count").unwrap(), expected);
    assert_eq!(
        store
            .count_stream_kinds(
                stream,
                &[
                    COMPLETION_REOPENED_EVENT_KIND,
                    COMPLETION_REJECTED_EVENT_KIND
                ],
                0
            )
            .unwrap(),
        0
    );
    assert!(store
        .count_stream_kinds(stream, &[COMPLETION_REOPENED_EVENT_KIND], u64::MAX)
        .is_err());
    assert_eq!(backend.history_reads(), (0, 0));
}

/// The delivered result set: the artifacts accepted work produced. The
/// objective reviewer's own report is evidence of review, not a result, and
/// requiring a review to cover it is an unbounded regress - each review
/// artifact becomes the next uncovered ref, so the Objective can never close.
/// Agents whose Runtime-bound role marks them as the objective reviewer are
/// identified structurally, so this holds regardless of task naming.
pub(crate) fn delivered_result_refs(
    program: &crate::AgenticProgramProjection,
    goal: &GoalContract,
    request_refs: &[String],
) -> Vec<String> {
    // The reviewers are the actors Runtime recorded on the Goal's own review
    // records. That identity is assigned by Runtime and cannot drift: the
    // previous rule matched `agent.role` against the substring "objective
    // review", so a reviewer the model named "independent verifier", "QA" or
    // "事实核验者" was not recognized, its report re-entered the delivered set,
    // and every new review artifact became the next uncovered ref. One live run
    // spun through 14 reviews without a verdict that way.
    //
    // With no recorded review there is no signal, so nothing is excluded:
    // falling back to an explicit gap is safer than silently misjudging the set.
    let objective_reviewers = goal
        .reviews
        .iter()
        .map(|review| review.reviewer_actor.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if objective_reviewers.is_empty() {
        return request_refs.to_vec();
    }
    request_refs
        .iter()
        .filter(|reference| {
            !program.artifacts.values().any(|artifact| {
                artifact.artifact_ref == **reference
                    && objective_reviewers.contains(artifact.committed_by.as_str())
            })
        })
        .cloned()
        .collect()
}

/// Whether the accumulated evidence set contains a reference.
///
/// Runtime records a ref an Agent action changed as `agentic_changed:<ref>`, a
/// provenance marker around the same underlying reference. A reviewer that
/// cites the plain `message:<id>` is citing the same thing, and rejecting it
/// taught the model nothing while keeping the Objective open.
fn evidence_contains(evidence: &[String], reference: &str) -> bool {
    evidence.iter().any(|candidate| {
        candidate == reference
            || candidate
                .strip_prefix("agentic_changed:")
                .is_some_and(|inner| inner == reference)
    })
}

fn review_covers_current_goal(
    goal: &GoalContract,
    program: &AgenticProgramProjection,
    review: &harness_contract::goal::ObjectiveReviewRecord,
    result_refs: &[String],
    evidence: &[String],
    proof_inputs: Option<&crate::agentic::review_evidence::ProofInputSnapshot>,
) -> bool {
    review.spec_revision == goal.spec_revision
        && review.input_manifest_digest == goal.spec_digest
        && review.decision == "satisfied"
        && review.verification.as_ref().is_some_and(|proof| {
            if let Some(inputs) = &proof.input_set {
                return inputs.version == 2
                    && inputs.condition_ref.as_deref() == Some(review.criterion_ref.as_str())
                    && proof.goal_spec_revision == Some(goal.spec_revision)
                    && proof.goal_spec_digest.as_deref() == Some(goal.spec_digest.as_str())
                    && proof_inputs.is_some_and(|snapshot| {
                        snapshot.semantic_digest == inputs.semantic_digest
                            && snapshot.reviewer_matches(program, review)
                            && proof
                                .result_source_revisions
                                .iter()
                                .all(|(stream, revision)| {
                                    snapshot.sources.iter().any(|source| {
                                        &source.stream_id == stream
                                            && source.expected_revision == *revision
                                    })
                                })
                    })
                    && result_refs.iter().all(|reference| {
                        crate::agentic::review_evidence::review_result_identity(program, reference)
                            .is_ok_and(|identity| {
                                inputs.result_refs.iter().any(|reference| {
                                    crate::agentic::review_evidence::review_result_identity(
                                        program, reference,
                                    )
                                    .is_ok_and(|current| current == identity)
                                })
                            })
                    })
                    && !proof.reviewer_execution_id.is_empty()
                    && !proof.producer_execution_ids.is_empty()
                    && proof.producer_execution_ids.iter().all(|producer| {
                        !producer.is_empty()
                            && (!proof.independence_required
                                || producer != &proof.reviewer_execution_id)
                    })
                    && (!proof.independence_required
                        || !review.producer_refs.contains(&review.reviewer_actor))
                    && !inputs.material_events.is_empty()
                    && result_refs.iter().all(|reference| {
                        proof.reads.iter().any(|read| {
                            crate::agentic::review_evidence::review_result_identity(
                                program,
                                &read.result_ref,
                            )
                            .is_ok_and(|identity| {
                                crate::agentic::review_evidence::review_result_identity(
                                    program, reference,
                                )
                                .is_ok_and(|expected| identity == expected)
                            }) && !read.sha256.is_empty()
                                && !read.receipt_refs.is_empty()
                                && (read.result_kind
                                    != harness_contract::goal::GoalResultKind::ToolEffect
                                    || !read.effect_observation_refs.is_empty())
                        })
                    });
            }
            false
        })
        && !review.evidence_refs.is_empty()
        && !review.producer_refs.is_empty()
        && !review.result_refs.is_empty()
        && !result_refs.is_empty()
        && result_refs.iter().all(|reference| {
            crate::agentic::review_evidence::review_result_identity(program, reference).is_ok_and(
                |identity| {
                    review.result_refs.iter().any(|reviewed| {
                        crate::agentic::review_evidence::review_result_identity(program, reviewed)
                            .is_ok_and(|expected| identity == expected)
                    })
                },
            )
        })
        && review
            .evidence_refs
            .iter()
            .all(|reference| evidence_contains(evidence, reference))
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
        // A review's own evidence is what proves it read the results, and it is
        // produced BY the review: the reviewer's `evidence_retrieve` receipts
        // live in its own Session journal and cannot already be in the
        // Program's accumulated set. Requiring them to be there made the check
        // circular - no review could ever be the thing that adds its own proof,
        // so a satisfied review was rejudged as uncovered and the Objective
        // reopened forever. Each review's evidence was already resolved and
        // verified when the review was applied, so it is known durable here.
        for review in &goal.reviews {
            evidence.extend(review.evidence_refs.iter().cloned());
        }
        // The Goal's evidence is read as CONTENT. A logical artifact
        // reference ("artifact:<24 hex>") is one artifact's business
        // identity; the content it is bound to is what a reader can resolve.
        // Keep BOTH identities, so a logical reference is never dressed as a
        // content selector, and refuse a reference that names SEVERAL
        // artifacts: two logical identities over one content are never merged.
        let mut evidence = evidence
            .iter()
            .map(|reference| {
                let bound =
                    crate::agentic::review_evidence::bound_artifact_identities(program, reference)?;
                Ok(if bound.is_empty() {
                    vec![reference.clone()]
                } else {
                    bound
                })
            })
            .collect::<Result<Vec<Vec<String>>, String>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        evidence.sort();
        evidence.dedup();
        apply_completion_criterion_state(&mut goal, &projection.progress, &evidence);
        let conditions = crate::agentic::review_evidence::required_review_conditions(&goal);
        let mut proof_inputs = std::collections::BTreeMap::new();
        let history = self
            .proof_session_queries
            .as_ref()
            .and_then(|queries| queries.get())
            .and_then(|queries| queries.history_reader());
        let process_frame = self
            .process_input_verifier
            .as_ref()
            .map(crate::agentic::review_evidence::ProcessInputVerifier::frame);
        let readers = crate::agentic::review_evidence::ResultContentReaders {
            artifacts: self.proof_artifacts.as_deref(),
            history: history.as_deref(),
            process_inputs: process_frame.as_ref(),
        };
        for condition in conditions {
            let Some(review) = goal
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == condition)
            else {
                continue;
            };
            if review.decision != "satisfied"
                || review.spec_revision != goal.spec_revision
                || review.input_manifest_digest != goal.spec_digest
            {
                continue;
            }
            let Some(proof) = review.verification.as_ref().filter(|proof| {
                proof
                    .input_set
                    .as_ref()
                    .is_some_and(|inputs| inputs.version == 2)
            }) else {
                continue;
            };
            match crate::agentic::review_evidence::validate_proof_inputs(
                &self.event_store,
                program,
                Some(&goal),
                proof,
                readers,
            ) {
                Ok(snapshot) => {
                    let mut results = review.result_refs.clone();
                    if goal.user_intent_criterion_id.as_ref() == Some(&condition) {
                        results.extend(delivered_result_refs(program, &goal, &request.result_refs));
                    }
                    for obligation in goal.obligations.iter().filter(|obligation| {
                        obligation.required && obligation.obligation_id == condition
                    }) {
                        results.extend(obligation.artifact_refs.iter().cloned());
                    }
                    if review_covers_current_goal(
                        &goal,
                        program,
                        review,
                        &results,
                        &evidence,
                        Some(&snapshot),
                    ) {
                        proof_inputs.insert(
                            crate::agentic::review_evidence::review_snapshot_key(proof),
                            snapshot,
                        );
                    }
                }
                Err(error) if error.contains("effect_review_pending") => return Err(error),
                Err(_) => {}
            }
        }

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
        // The objective reviewer's own report is evidence OF review, never a
        // delivered result. One live loop cited it in every completion
        // request, so each new review artifact became the next uncovered ref
        // and the collaboration could never close (14 reviews, 0 verdicts).
        // The delivered result set is what accepted work produced; artifacts
        // committed by the Runtime-bound objective reviewer are excluded.
        let delivered_result_refs = delivered_result_refs(program, &goal, &request.result_refs);
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
                    &delivered_result_refs,
                    &evidence,
                    review.verification.as_ref().and_then(|proof| {
                        proof_inputs
                            .get(&crate::agentic::review_evidence::review_snapshot_key(proof))
                    }),
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
                            review.verification.as_ref().and_then(|proof| {
                                proof_inputs.get(
                                    &crate::agentic::review_evidence::review_snapshot_key(proof),
                                )
                            }),
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
            // Name the cited results no current review covers. A live loop had
            // the root cite four artifacts while the recorded review covered
            // three; without the missing refs the model re-issued the same
            // partial review until the observation window expired.
            let covered = goal
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == intent)
                .map(|review| review.result_refs.clone())
                .unwrap_or_default();
            let mut gap = format!("original_intent_review_required:{intent}");
            let missing = request
                .result_refs
                .iter()
                .filter(|reference| !covered.contains(reference))
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                gap.push_str(&format!("; uncovered_result_refs: {}", missing.join(", ")));
            }
            // A satisfied review that later work invalidated must say so. Naming
            // only "review required" showed the model the same gap string it had
            // already answered once, so it repeated the same action: one live run
            // satisfied the review, had a Task reworked, and then re-issued the
            // identical request four times because nothing told it the earlier
            // satisfaction no longer held.
            let prior_satisfied =
                goal.reviews.iter().rev().find(|review| {
                    review.criterion_ref == intent && review.decision == "satisfied"
                });
            if let Some(prior) = prior_satisfied {
                if !request
                    .result_refs
                    .iter()
                    .all(|reference| prior.result_refs.contains(reference))
                {
                    gap.push_str(&format!(
                        "; previous_satisfied_review_invalidated: it covered [{}] but the \
                         current delivered set is [{}]; read the new results yourself in this \
                         execution and submit one fresh objective_review covering the full \
                         current set",
                        prior.result_refs.join(", "),
                        request.result_refs.join(", ")
                    ));
                }
            }
            gaps.push(gap);
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
                            review.verification.as_ref().and_then(|proof| {
                                proof_inputs.get(
                                    &crate::agentic::review_evidence::review_snapshot_key(proof),
                                )
                            }),
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
        let reopen_budget = completion_reopen_bound()?
            .map(|limit| {
                completion_reopen_count(&self.event_store, &program.program_id)
                    .map(|count| (limit, count))
            })
            .transpose()?;
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
        } else if reopen_budget.is_none_or(|(limit, count)| count < limit) {
            goal.phase = "waiting".into();
            goal.waiting = Some(harness_contract::goal::WaitDescriptor {
                reason_code: "completion_gap".into(),
                reason: gaps.join("; "),
                required_refs: gaps.clone(),
                generation: request.program_revision,
            });
            ("goal.completion_waiting", "open")
        } else {
            let (limit, reopen_count) =
                reopen_budget.ok_or("missing explicit completion resource limit")?;
            goal.completion = GoalCompletion::Blocked;
            goal.phase = "completed".into();
            goal.waiting = None;
            goal.terminal = Some(ObjectiveTerminal {
                kind: ObjectiveTerminalKind::Blocked,
                terminal_fence: fence.clone(),
                authority_revision: request.program_revision,
                reason: format!(
                    "operator-configured completion resource limit {limit} exhausted after {reopen_count} rejection(s); objective remains unmet: {}",
                    gaps.join("; ")
                ),
                evidence_refs: goal.evidence_refs.clone(),
                diagnostics: Vec::new(),
                committed_at_ms: crate::tool_invocation::now_ms(),
            });
            ("goal.blocked", "blocked")
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
                let mut sources = std::collections::BTreeMap::new();
                for snapshot in proof_inputs.into_values() {
                    for source in snapshot.sources {
                        if sources
                            .insert(source.stream_id, source.expected_revision)
                            .is_some_and(|revision| revision != source.expected_revision)
                        {
                            return Err("proof source changed during conclusion preparation".into());
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
    use harness_contract::agent_action::MessagePublishInput;
    #[test]
    fn the_objective_reviewers_own_report_is_not_a_delivered_result() {
        // Requiring a review to cover its own report is an unbounded regress:
        // every review artifact becomes the next uncovered ref, so the
        // Objective can never close (one live loop: 14 reviews, 0 verdicts).
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        for (id, role) in [
            ("agent:producer", "source reader"),
            ("agent:reviewer", "fresh independent objective reviewer"),
        ] {
            program.agents.insert(
                id.to_string(),
                crate::AgentMemberProjection {
                    agent_id: id.to_string(),
                    display_name: id.to_string(),
                    role: role.to_string(),
                    mission: "work".to_string(),
                    required_capabilities: vec![],
                    invited_by: "root".to_string(),
                    membership_ids: vec![],
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                },
            );
        }
        program.artifacts.insert(
            "artifact:delivered".to_string(),
            crate::AgenticArtifactProjection {
                artifact_ref: "artifact:delivered".to_string(),
                content_ref: "artifact://art_delivered".to_string(),
                kind: "report".to_string(),
                title: "delivered fact".to_string(),
                relates_to: vec![],
                committed_by: "agent:producer".to_string(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        program.artifacts.insert(
            "artifact:review-report".to_string(),
            crate::AgenticArtifactProjection {
                artifact_ref: "artifact:review-report".to_string(),
                content_ref: "artifact://art_review".to_string(),
                kind: "report".to_string(),
                title: "objective review report".to_string(),
                relates_to: vec![],
                committed_by: "agent:reviewer".to_string(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        let goal = harness_contract::goal::GoalContract {
            id: "goal:test".to_string(),
            session_id: "session-test".to_string(),
            objective: "objective".to_string(),
            criteria: Vec::new(),
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            execution_binding: None,
            spec_revision: 1,
            spec_digest: String::new(),
            review_refs: Vec::new(),
            reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                review_id: "objective_review:1".to_string(),
                criterion_ref: "user_intent".to_string(),
                spec_revision: 1,
                input_manifest_digest: String::new(),
                result_refs: Vec::new(),
                evidence_refs: Vec::new(),
                reviewer_actor: "agent:reviewer".to_string(),
                reviewer_execution_id: None,
                producer_refs: Vec::new(),
                decision: "satisfied".to_string(),
                reason_ref: String::new(),
                verification: None,
            }],
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: harness_contract::goal::GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        let delivered = delivered_result_refs(
            &program,
            &goal,
            &[
                "artifact:delivered".to_string(),
                "artifact:review-report".to_string(),
            ],
        );
        assert_eq!(delivered, vec!["artifact:delivered".to_string()]);
    }

    /// The exclusion must not depend on what the reviewer is called. The rule
    /// used to match `agent.role` against the substring "objective review", so
    /// a reviewer the model named "independent verifier", "QA" or "事实核验者"
    /// slipped through, its report re-entered the delivered set, and every new
    /// review artifact became the next uncovered ref. The Goal's own
    /// `reviewer_actor` is assigned by Runtime and cannot drift.
    #[test]
    fn the_reviewers_own_report_is_excluded_whatever_the_role_is_named() {
        for role in [
            "independent verifier",
            "QA",
            "事实核验者",
            "fresh independent objective reviewer",
        ] {
            let mut program = crate::AgenticProgramProjection::empty("program", "objective");
            program.agents.insert(
                "agent:reviewer".to_string(),
                crate::AgentMemberProjection {
                    agent_id: "agent:reviewer".to_string(),
                    display_name: "reviewer".to_string(),
                    role: role.to_string(),
                    mission: "review".to_string(),
                    required_capabilities: vec![],
                    invited_by: "root".to_string(),
                    membership_ids: vec![],
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                },
            );
            program.artifacts.insert(
                "artifact:review-report".to_string(),
                crate::AgenticArtifactProjection {
                    artifact_ref: "artifact:review-report".to_string(),
                    content_ref: "artifact://art_review".to_string(),
                    kind: "report".to_string(),
                    title: "review".to_string(),
                    relates_to: vec![],
                    committed_by: "agent:reviewer".to_string(),
                    claim_execution_id: None,
                    claim_generation: None,
                },
            );
            let goal = harness_contract::goal::GoalContract {
                id: "goal:test".to_string(),
                session_id: "session-test".to_string(),
                objective: "objective".to_string(),
                criteria: Vec::new(),
                constraints: Vec::new(),
                phase: "execution".to_string(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                blockers: Vec::new(),
                scope: Default::default(),
                user_intent_criterion_id: None,
                source_intent_ref: None,
                execution_binding: None,
                spec_revision: 1,
                spec_digest: String::new(),
                review_refs: Vec::new(),
                reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                    review_id: "objective_review:1".to_string(),
                    criterion_ref: "user_intent".to_string(),
                    spec_revision: 1,
                    input_manifest_digest: String::new(),
                    result_refs: Vec::new(),
                    evidence_refs: Vec::new(),
                    reviewer_actor: "agent:reviewer".to_string(),
                    reviewer_execution_id: None,
                    producer_refs: Vec::new(),
                    decision: "satisfied".to_string(),
                    reason_ref: String::new(),
                    verification: None,
                }],
                waiting: None,
                participation_requirement: None,
                obligations: Vec::new(),
                recovery: None,
                terminal: None,
                completion: harness_contract::goal::GoalCompletion::Open,
                revision: 1,
                user_sequence: 1,
            };
            let delivered =
                delivered_result_refs(&program, &goal, &["artifact:review-report".to_string()]);
            assert!(
                delivered.is_empty(),
                "role {role:?} must not change the exclusion, got {delivered:?}"
            );
        }
    }

    /// With no recorded review there is no structural signal, so nothing is
    /// excluded. Falling back to an explicit gap is safer than silently
    /// misjudging the delivered set.
    #[test]
    fn without_a_recorded_review_nothing_is_excluded() {
        let program = crate::AgenticProgramProjection::empty("program", "objective");
        let goal = harness_contract::goal::GoalContract {
            id: "goal:test".to_string(),
            session_id: "session-test".to_string(),
            objective: "objective".to_string(),
            criteria: Vec::new(),
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            execution_binding: None,
            spec_revision: 1,
            spec_digest: String::new(),
            review_refs: Vec::new(),
            reviews: Vec::new(),
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: harness_contract::goal::GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        let refs = vec!["artifact:anything".to_string()];
        assert_eq!(delivered_result_refs(&program, &goal, &refs), refs);
    }

    /// Runtime tags a ref an Agent action changed as `agentic_changed:<ref>`.
    /// A reviewer citing the plain ref is citing the same thing, and rejecting it
    /// kept the Objective open while teaching the model nothing.
    #[test]
    fn a_provenance_tagged_ref_satisfies_the_plain_reference() {
        let evidence = vec![
            "agentic_changed:message:fc83095f58e63aa8cad9b887".to_string(),
            "tool://receipt".to_string(),
        ];
        assert!(evidence_contains(
            &evidence,
            "message:fc83095f58e63aa8cad9b887"
        ));
        assert!(evidence_contains(&evidence, "tool://receipt"));
        assert!(!evidence_contains(&evidence, "message:other"));
    }

    /// A review's own read receipts are produced BY the review, so they cannot
    /// already be in the Program's accumulated set. They were resolved and
    /// verified when the review was applied, so the completion gate must accept
    /// them rather than reopen forever.
    #[test]
    fn a_reviews_own_evidence_counts_toward_the_completion_gate() {
        let goal = harness_contract::goal::GoalContract {
            id: "goal:test".to_string(),
            session_id: "session-test".to_string(),
            objective: "objective".to_string(),
            criteria: Vec::new(),
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: Default::default(),
            user_intent_criterion_id: None,
            source_intent_ref: None,
            execution_binding: None,
            spec_revision: 1,
            spec_digest: String::new(),
            review_refs: Vec::new(),
            reviews: vec![harness_contract::goal::ObjectiveReviewRecord {
                review_id: "objective_review:1".to_string(),
                criterion_ref: "user_intent".to_string(),
                spec_revision: 1,
                input_manifest_digest: String::new(),
                result_refs: Vec::new(),
                evidence_refs: vec!["tool://reviewer-read-receipt".to_string()],
                reviewer_actor: "agent:reviewer".to_string(),
                reviewer_execution_id: None,
                producer_refs: Vec::new(),
                decision: "satisfied".to_string(),
                reason_ref: String::new(),
                verification: None,
            }],
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: harness_contract::goal::GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        // Exactly the accumulation the completion gate performs: the review's
        // own receipts are not in the Program's set until they are added.
        let mut evidence = goal.evidence_refs.clone();
        for review in &goal.reviews {
            evidence.extend(review.evidence_refs.iter().cloned());
        }
        assert!(
            evidence_contains(&evidence, "tool://reviewer-read-receipt"),
            "a review's own durable read receipt must count toward the gate"
        );
    }

    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
        ObjectiveCompleteRequestInput, TeamCreateInput,
    };
    use harness_contract::goal::{
        AcceptanceCriterion, GoalExecutionBinding, GoalScope, ObjectiveReviewRecord,
    };

    fn mechanical_fixture(
        historical_review: bool,
    ) -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        crate::AgentActionService,
        AgentActorBinding,
    ) {
        mechanical_fixture_with_request(historical_review, true)
    }

    fn mechanical_fixture_with_request(
        historical_review: bool,
        request_completion: bool,
    ) -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        crate::AgentActionService,
        AgentActorBinding,
    ) {
        mechanical_fixture_with_ids(
            historical_review,
            request_completion,
            "objective-completion".into(),
            "program-completion".into(),
        )
    }

    fn mechanical_fixture_with_ids(
        historical_review: bool,
        request_completion: bool,
        objective_id: String,
        program_id: String,
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
            objective_id,
            program_id,
            session_id: "session-completion".into(),
            turn_id: "turn-completion".into(),
            root_execution_id: Some("root-completion".into()),
            required_team_count: 0,
            exact_team_count: None,
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
                        blocked: false,
                        blocked_reason: None,
                        human_review_required: false,
                        recommended_next_actions: Vec::new(),
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
                reviews: if historical_review {
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
                            input_set: None,
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
                            current_review_effect_key: None,
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

    #[tokio::test]
    async fn historical_delegated_review_without_effect_sources_cannot_verify_goal() {
        let (_, goals, actions, actor) = mechanical_fixture(true);
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
            None
        ));
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, program, prepared, _| {
                let goal = services
                    .goal_store()
                    .get(&prepared.goal.id)
                    .unwrap()
                    .unwrap();
                let mut proof = goal.reviews.last().unwrap().verification.clone().unwrap();
                assert_eq!(proof.input_set.as_ref().unwrap().version, 2);
                let history = services.session_history_reader();
                let readers = crate::agentic::review_evidence::ResultContentReaders {
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: history.as_deref(),
                    process_inputs: None,
                };
                crate::agentic::review_evidence::validate_proof_inputs(
                    services.event_store(),
                    program,
                    Some(&goal),
                    &proof,
                    readers,
                )
                .expect("real effect donor is valid before mutation");
                proof.effect_manifest_digest = None;
                proof.effect_source_refs.clear();
                let error = crate::agentic::review_evidence::validate_proof_inputs(
                    services.event_store(),
                    program,
                    Some(&goal),
                    &proof,
                    readers,
                )
                .err()
                .expect("v2 missing effect facts must fail real validation");
                assert!(!error.contains("unsupported"), "{error}");
            },
        )
        .await;
    }

    #[tokio::test]
    async fn external_decision_review_requires_current_source_revision_and_final_cas() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_approval_before_a(|services, program, prepared, _| {
            let goal = services.goal_store().get(&prepared.goal.id).unwrap().unwrap();
            let history = services.session_history_reader();
            let readers = crate::agentic::review_evidence::ResultContentReaders {
                artifacts: Some(services.artifact_store().as_ref()), history: history.as_deref(), process_inputs: None,
            };
            let proof = crate::agentic::review_evidence::required_review_conditions(&goal).iter()
                .filter_map(|condition| goal.reviews.iter().rev().find(|review| &review.criterion_ref == condition))
                .filter_map(|review| review.verification.as_ref())
                .find(|proof| !proof.result_source_revisions.is_empty()).expect("real selected approval donor");
            let (source, revision) = proof.result_source_revisions.iter().next().unwrap();
            let source = source.clone();
            let revision = *revision;
            assert_eq!(services.event_store().stream_revision(&source).unwrap(), revision);
            assert!(prepared.policy_sources.iter().any(|cut| cut.stream_id == source && cut.expected_revision == revision));
            crate::agentic::review_evidence::validate_proof_inputs(services.event_store(), program,
                Some(&goal), proof, readers).expect("actual current approval is accepted");
            let mut missing = proof.clone();
            missing.result_source_revisions.remove(&source);
            assert!(crate::agentic::review_evidence::validate_proof_inputs(services.event_store(), program,
                Some(&goal), &missing, readers).err().unwrap().contains("external decision source"));
            let mut revised = proof.clone();
            revised.result_source_revisions.insert(source.clone(), revision + 1);
            assert!(crate::agentic::review_evidence::validate_proof_inputs(services.event_store(), program,
                Some(&goal), &revised, readers).err().unwrap().contains("external decision source"));
            let events = services.event_store().clone();
            let fault_source = source.clone();
            let before_goal = goal.clone();
            let error = crate::agentic::execution::reviewed_goal_fixture::with_native_conclusion_append_hook(
                move |transaction| {
                    assert!(transaction.expected_streams.iter().any(|cut|
                        cut.stream_id == fault_source && cut.expected_revision == revision));
                    events.append(RuntimeEventInput { stream_id: fault_source, scope: RuntimeEventScope::Approval,
                        kind: "test.concurrent_external_decision_source".into(), status: None, actor: None,
                        refs: vec![], payload: serde_json::json!({}) }).unwrap();
                },
                || services.agent_action_service().commit_program_conclusion(program, prepared),
            ).err().expect("actual final approval storage CAS must fail");
            match error {
                crate::AgentActionServiceError::Store(crate::RuntimeEventStoreError::StaleRevision {
                    stream_id, expected, actual,
                }) => { assert_eq!(stream_id, source); assert_eq!(expected, revision); assert_eq!(actual, revision + 1); }
                error => panic!("wrong failure layer: {error}"),
            }
            assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), before_goal);
            assert!(services.agent_action_service().project(&program.program_id).unwrap().objective_verdict.is_none());
        }).await;
        crate::agentic::execution::reviewed_goal_fixture::with_native_approval_before_a(
            |services, program, prepared, _| {
                let after = services
                    .agent_action_service()
                    .commit_program_conclusion(program, prepared)
                    .unwrap();
                assert_eq!(after.status, crate::AgenticProgramStatus::Verified);
            },
        )
        .await;
    }

    #[tokio::test]
    async fn a_satisfied_review_that_later_work_invalidated_says_so_in_the_gap() {
        // Naming only "review required" showed the model the same gap string it
        // had already answered once, so it repeated the same action: one live run
        // satisfied the review, had a Task reworked, and then re-issued the
        // identical completion request four times.
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
            exact_team_count: None,
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
        actions
            .apply(&AgentActionEnvelope {
                action_id: "open".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    result_refs: vec!["tool://result".into()],
                    evidence_refs: vec!["tool://evidence".into()],
                    unresolved: vec![],
                    blocked: false,
                    blocked_reason: None,
                    human_review_required: false,
                    recommended_next_actions: vec![],
                }),
            })
            .unwrap();
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
                // The prior satisfaction covered a different delivered set than the
                // one the active request now cites: later work invalidated it.
                reviews: vec![ObjectiveReviewRecord {
                    review_id: "prior".into(),
                    criterion_ref: "user_intent".into(),
                    spec_revision: 1,
                    input_manifest_digest: "original-spec".into(),
                    result_refs: vec!["tool://superseded".into()],
                    evidence_refs: vec!["tool://evidence".into()],
                    reviewer_actor: "reviewer".into(),
                    reviewer_execution_id: None,
                    producer_refs: vec!["agent:producer".into()],
                    decision: "satisfied".into(),
                    reason_ref: "artifact://prior-report".into(),
                    verification: None,
                }],
            })
            .unwrap();
        let program = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&program).unwrap();
        assert!(prepared.goal.terminal.is_none());
        let gap = prepared
            .gaps
            .iter()
            .find(|gap| gap.starts_with("original_intent_review_required:"))
            .expect("the intent gap");
        assert!(
            gap.contains("previous_satisfied_review_invalidated"),
            "the gap must announce that the earlier satisfaction no longer holds: {gap}"
        );
        assert!(
            gap.contains("tool://superseded") && gap.contains("tool://result"),
            "it must name both the old and the current set: {gap}"
        );
        // A satisfaction that still covers the request is not called invalidated.
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, program, prepared, _| {
                assert!(prepared.gaps.is_empty());
                assert!(
                    prepared
                        .gaps
                        .iter()
                        .all(|gap| !gap.contains("previous_satisfied_review_invalidated")),
                    "a still-current satisfaction is not invalidated: {:?}",
                    prepared.gaps
                );
                let after = services
                    .agent_action_service()
                    .commit_program_conclusion(program, prepared)
                    .unwrap();
                assert_eq!(after.status, crate::AgenticProgramStatus::Verified);
            },
        )
        .await;
    }

    #[tokio::test]
    async fn historical_root_review_without_effect_digest_cannot_hide_a_later_effect() {
        let (events, goals, actions, actor) = mechanical_fixture(true);
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
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(|services, program, prepared, _| {
            let goal = services.goal_store().get(&prepared.goal.id).unwrap().unwrap();
            let proof = goal.reviews.last().unwrap().verification.as_ref().unwrap();
            assert_eq!(proof.input_set.as_ref().unwrap().version, 2);
            let history = services.session_history_reader();
            let readers = crate::agentic::review_evidence::ResultContentReaders {
                artifacts: Some(services.artifact_store().as_ref()), history: history.as_deref(), process_inputs: None,
            };
            crate::agentic::review_evidence::validate_proof_inputs(services.event_store(), program,
                Some(&goal), proof, readers).expect("unmodified v2 effect donor");
            services.event_store().append(RuntimeEventInput {
                stream_id: format!("session:{}", program.session_id), scope: RuntimeEventScope::Session,
                kind: "tool.invocation.completed".into(), status: Some("completed".into()),
                actor: Some("conversation_runtime".into()), refs: vec![],
                payload: serde_json::json!({"tool_name":"unclassified_mutation","test_fault":"legacy-missing-descriptor"}),
            }).unwrap();
            let error = crate::agentic::review_evidence::validate_proof_inputs(services.event_store(), program,
                Some(&goal), proof, readers).err().expect("actual unknown effect cannot be hidden by a v2 donor");
            assert!(!error.contains("unsupported"), "{error}");
            assert!(services.agent_action_service().project(&program.program_id).unwrap().objective_verdict.is_none());
        }).await;
    }

    #[test]
    fn graph_identity_cannot_replace_original_review_and_gap_reopens_durably() {
        let (events, goals, actions, actor) = mechanical_fixture(false);
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
    fn reconciling_an_existing_completion_request_reopens_goal_for_missing_review() {
        let (events, goals, actions, actor) = mechanical_fixture(false);
        let before = actions.project(&actor.program_id).unwrap();
        assert_eq!(
            before.status,
            crate::AgenticProgramStatus::CompletionRequested
        );
        assert!(before.completion_request.is_some());
        assert!(goals
            .get("goal:root-completion")
            .unwrap()
            .unwrap()
            .waiting
            .is_none());

        let supervisor =
            crate::execution_core::goal::ObjectiveSupervisor::new(Arc::new(GoalStore::new(events)));
        let reconciled = crate::agentic::supervision::reconcile_completion_request(
            &actions,
            &supervisor,
            &actor.program_id,
        )
        .unwrap()
        .expect("existing request must be reduced");

        assert_eq!(reconciled.status, crate::AgenticProgramStatus::Open);
        assert!(reconciled.completion_request.is_none());
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        assert_eq!(goal.completion, GoalCompletion::Open);
        assert_eq!(
            goal.waiting
                .as_ref()
                .map(|waiting| waiting.reason_code.as_str()),
            Some("completion_gap")
        );
        assert!(goal
            .waiting
            .as_ref()
            .expect("waiting")
            .required_refs
            .iter()
            .any(|reference| reference.starts_with("original_intent_review_required:user_intent")));
        // The gap names the cited results no review covers, so the model can
        // complete the review instead of re-issuing a partial one.
        let mut goal = goal;
        let required_refs = goal.waiting.take().expect("waiting").required_refs;
        assert!(required_refs
            .iter()
            .any(|reference| reference.contains("uncovered_result_refs: tool://result")));
    }

    #[tokio::test]
    async fn later_gap_review_supersedes_an_earlier_satisfied_review() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, before, baseline, envelope| {
                let goals = services.goal_store();
                let actions = services.agent_action_service();
                let actor = &envelope.actor;
                let goal_id = baseline.goal.id.clone();
                let current_goal = goals.get(&goal_id).unwrap().unwrap();
                assert!(baseline.gaps.is_empty());

                goals
                    .revise(
                        goal_id.as_str(),
                        current_goal.revision,
                        current_goal.user_sequence + 1,
                        "review found a gap",
                        |goal| {
                            let mut review = goal.reviews.last().unwrap().clone();
                            review.review_id = "later-gap".into();
                            review.decision = "gap".into();
                            goal.reviews.push(review);
                            vec!["reviews".into()]
                        },
                    )
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
                    .get(goal_id.as_str())
                    .unwrap()
                    .unwrap()
                    .terminal
                    .is_none());
            },
        )
        .await;
    }

    #[tokio::test]
    async fn new_counterevidence_invalidates_a_previous_positive_review() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, program, baseline, envelope| {
                assert!(baseline.gaps.is_empty());
                let actions = services.agent_action_service();
                let original = services
                    .goal_store()
                    .get(&baseline.goal.id)
                    .unwrap()
                    .unwrap();
                let note = services
                    .event_store()
                    .event_by_idempotency_key(
                        &format!("agentic-program:{}", program.program_id),
                        "agent-action:mechanical-unrelated-note",
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(note.status.as_deref(), Some("applied"));
                let current = actions.project(&program.program_id).unwrap();
                assert!(
                    services
                        .goal_store()
                        .prepare_program_conclusion(&current)
                        .unwrap()
                        .gaps
                        .is_empty(),
                    "the actual unrelated MessagePublish preserves selected current proof"
                );
                let goal_id = original.id.clone();
                services
                    .goal_store()
                    .revise(
                        &goal_id,
                        original.revision,
                        original.user_sequence + 1,
                        "new acceptance risk",
                        |goal| {
                            goal.spec_revision += 1;
                            goal.spec_digest = "actual changed acceptance policy".into();
                            vec!["spec_revision".into(), "spec_digest".into()]
                        },
                    )
                    .unwrap();
                let prepared = services
                    .goal_store()
                    .prepare_program_conclusion(&current)
                    .unwrap();
                assert!(prepared
                    .gaps
                    .iter()
                    .any(|gap| gap.starts_with("original_intent_review_required:")));
                assert!(prepared.goal.terminal.is_none());
            },
        )
        .await;
    }

    #[tokio::test]
    async fn goal_cas_conflict_cannot_commit_half_a_program_terminal() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(|services, before, baseline, envelope| {
        let goals = services.goal_store();
        let actions = services.agent_action_service();
        let actor = &envelope.actor;
        let goal_id = baseline.goal.id.clone();
        let goal_stream_id = baseline.event.event.stream_id.clone();
        let current_goal = goals.get(&goal_id).unwrap().unwrap();
        assert!(baseline.gaps.is_empty());

        let before = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&before).unwrap();
        assert!(prepared.gaps.is_empty());
        let concurrent_goals = Arc::clone(goals);
        let fault_goal_id = goal_id.clone();
        let source = goal_stream_id.clone();
        let expected = prepared.expected_stream_revision;
        let error = crate::agentic::execution::reviewed_goal_fixture::with_native_conclusion_append_hook(
            move |transaction| {
                assert!(transaction.expected_streams.iter().any(|cut|
                    cut.stream_id == source && cut.expected_revision == expected));
                concurrent_goals.revise(&fault_goal_id, current_goal.revision,
                    current_goal.user_sequence + 1, "concurrent new requirement", |goal| {
                        goal.constraints.push("new requirement".into());
                        vec!["constraints".into()]
                    }).unwrap();
            },
            || actions.commit_program_conclusion(&before, prepared),
        ).err().expect("actual Goal storage CAS");
        match error {
            crate::AgentActionServiceError::Store(crate::RuntimeEventStoreError::StaleRevision {
                stream_id, expected: found, actual,
            }) => { assert_eq!(stream_id, goal_stream_id); assert_eq!(found, expected); assert_eq!(actual, expected + 1); }
            error => panic!("wrong failure layer: {error}"),
        }
        assert_eq!(
            actions.project(&actor.program_id).unwrap().revision,
            before.revision
        );
        assert!(
            goals
                .get(goal_id.as_str())
                .unwrap()
                .unwrap()
                .terminal
                .is_none()
        );

        }).await;
    }

    #[tokio::test]
    async fn real_a_verdict_is_bound_into_b_with_goal_and_program_source_fences() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, program, prepared, _| {
                let committed = services
                    .agent_action_service()
                    .commit_program_conclusion(program, prepared)
                    .unwrap();
                let goal = services
                    .goal_store()
                    .get(&committed.objective_verdict.as_ref().unwrap().goal_id)
                    .unwrap()
                    .unwrap();
                services
                    .event_store()
                    .append(RuntimeEventInput {
                        stream_id: format!("agentic-program:{}", program.program_id),
                        scope: RuntimeEventScope::Program,
                        kind: "agentic.task_lease_renewed".into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({}),
                    })
                    .unwrap();
                crate::agentic::execution::reviewed_goal_fixture::assert_native_sources_final_b(
                    services,
                    services.commit_service(),
                    &goal,
                    false,
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn program_cas_conflict_cannot_commit_half_a_goal_terminal() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, before, baseline, envelope| {
                let events = services.event_store();
                let goals = services.goal_store();
                let actions = services.agent_action_service();
                let actor = &envelope.actor;
                let goal_id = baseline.goal.id.clone();
                let program_stream_id = format!("agentic-program:{}", before.program_id);
                let current_goal = goals.get(&goal_id).unwrap().unwrap();
                assert!(baseline.gaps.is_empty());

                let before = actions.project(&actor.program_id).unwrap();
                let prepared = goals.prepare_program_conclusion(&before).unwrap();
                events
                    .append(RuntimeEventInput {
                        stream_id: program_stream_id.as_str().into(),
                        scope: RuntimeEventScope::Program,
                        kind: "test.concurrent_observation".into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({}),
                    })
                    .unwrap();
                let error = actions
                    .commit_program_conclusion(&before, prepared)
                    .err()
                    .expect("non-lease Program fence must reject the valid v2 prepared A");
                match error {
                    crate::AgentActionServiceError::Store(
                        crate::RuntimeEventStoreError::StaleRevision {
                            stream_id,
                            expected,
                            actual,
                        },
                    ) => {
                        assert_eq!(stream_id, program_stream_id);
                        assert_eq!(expected, before.revision);
                        assert_eq!(actual, before.revision + 1);
                    }
                    error => panic!("wrong Program fence failure layer: {error}"),
                }
                let goal = goals.get(goal_id.as_str()).unwrap().unwrap();
                assert_eq!(goal.revision, current_goal.revision);
                assert!(goal.terminal.is_none());
                assert!(actions
                    .project(&actor.program_id)
                    .unwrap()
                    .objective_verdict
                    .is_none());
            },
        )
        .await;
    }

    #[tokio::test]
    async fn lease_renewals_do_not_starve_an_already_verified_current_goal() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(|services, before, baseline, envelope| {
        let events = services.event_store();
        let goals = services.goal_store();
        let actions = services.agent_action_service();
        let actor = &envelope.actor;
        let goal_id = baseline.goal.id.clone();
        assert!(baseline.gaps.is_empty());

        let before = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&before).unwrap();
        assert!(prepared.gaps.is_empty());
        for renewal in 0..25 {
            events.append(RuntimeEventInput {
                stream_id: format!("agentic-program:{}", actor.program_id),
                scope: RuntimeEventScope::Program,
                kind: "agentic.task_lease_renewed".into(),
                status: Some("renewed".into()), actor: Some("worker".into()), refs: vec![],
                payload: serde_json::json!({"task_ref":"optional-task", "agent_id":"worker", "execution_id":format!("worker-{renewal}")}),
            }).unwrap();
        }
        let committed = actions.commit_program_conclusion(&before, prepared).unwrap();
        assert_eq!(committed.status, crate::AgenticProgramStatus::Verified);
        assert_eq!(committed.revision, before.revision + 26);
        assert_eq!(goals.get(goal_id.as_str()).unwrap().unwrap().completion,
            GoalCompletion::Satisfied);

        }).await;
    }

    #[tokio::test]
    async fn lease_renewal_rebase_cannot_bypass_a_new_goal_revision() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(|services, before, baseline, envelope| {
        let events = services.event_store();
        let goals = services.goal_store();
        let actions = services.agent_action_service();
        let actor = &envelope.actor;
        let goal_id = baseline.goal.id.clone();
        let current_goal = goals.get(&goal_id).unwrap().unwrap();
        assert!(baseline.gaps.is_empty());

        let before = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&before).unwrap();
        events.append(RuntimeEventInput {
            stream_id: format!("agentic-program:{}", actor.program_id),
            scope: RuntimeEventScope::Program, kind: "agentic.task_lease_renewed".into(),
            status: Some("renewed".into()), actor: None, refs: vec![],
            payload: serde_json::json!({"task_ref":"task", "agent_id":"worker", "execution_id":"run"}),
        }).unwrap();
        goals.revise(goal_id.as_str(), current_goal.revision, current_goal.user_sequence + 1, "new user requirement", |goal| {
            goal.objective = "A revised objective must be reviewed again".into();
            vec!["objective".into()]
        }).unwrap();
        assert!(actions.commit_program_conclusion(&before, prepared).is_err());
        let current = goals.get(goal_id.as_str()).unwrap().unwrap();
        assert_eq!(current.revision, current_goal.revision + 1);
        assert!(current.terminal.is_none());
        assert!(actions.project(&actor.program_id).unwrap().objective_verdict.is_none());

        }).await;
    }

    #[tokio::test]
    async fn ordinary_goal_writers_cannot_bypass_the_program_atomic_conclusion() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, before, baseline, envelope| {
                let events = services.event_store();
                let goals = services.goal_store();
                let actions = services.agent_action_service();
                let actor = &envelope.actor;
                let goal_id = baseline.goal.id.clone();
                let goal_stream_id = baseline.event.event.stream_id.clone();
                let current_goal = goals.get(&goal_id).unwrap().unwrap();
                assert!(baseline.gaps.is_empty());

                let goal_id = goal_id.as_str();
                let before = events.stream_revision(goal_stream_id.as_str()).unwrap();
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
                        .complete(
                            goal_id,
                            current_goal.revision,
                            GoalCompletion::Satisfied,
                            "done",
                        )
                        .unwrap_err(),
                    goals
                        .complete_objective(goal_id, current_goal.revision, terminal)
                        .unwrap_err(),
                ] {
                    assert!(error.contains("program_terminal_authority"), "{error}");
                }
                assert_eq!(
                    events.stream_revision(goal_stream_id.as_str()).unwrap(),
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
            },
        )
        .await;
    }

    #[tokio::test]
    async fn valid_current_review_commits_goal_and_program_together() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, before, baseline, envelope| {
                let events = services.event_store();
                let goals = services.goal_store();
                let actions = services.agent_action_service();
                let actor = &envelope.actor;
                let goal_id = baseline.goal.id.clone();
                let goal_stream_id = baseline.event.event.stream_id.clone();
                let program_stream_id = format!("agentic-program:{}", before.program_id);
                assert!(baseline.gaps.is_empty());

                let before = actions.project(&actor.program_id).unwrap();
                let prepared = goals.prepare_program_conclusion(&before).unwrap();
                let after = actions
                    .commit_program_conclusion(&before, prepared)
                    .unwrap();
                let goal = goals.get(goal_id.as_str()).unwrap().unwrap();
                assert_eq!(goal.completion, GoalCompletion::Satisfied);
                assert_eq!(after.status, crate::AgenticProgramStatus::Verified);
                assert_eq!(
                    after.objective_verdict.as_ref().unwrap().terminal_fence,
                    goal.terminal.as_ref().unwrap().terminal_fence
                );
                let goal_event = events.list_stream(goal_stream_id.as_str()).unwrap();
                let program_event = events.list_stream(program_stream_id.as_str()).unwrap();
                assert!(!goal_event.is_empty());
                assert_eq!(
                    goal_event.last().unwrap().transaction_id,
                    program_event.last().unwrap().transaction_id
                );
            },
        )
        .await;
    }

    #[tokio::test]
    async fn original_obligations_and_new_spec_cannot_be_erased_by_task_completion() {
        crate::agentic::execution::reviewed_goal_fixture::with_native_goal_before_a(
            |services, before, baseline, envelope| {
                let goals = services.goal_store();
                let actions = services.agent_action_service();
                let actor = &envelope.actor;
                let goal_id = baseline.goal.id.clone();
                let current_goal = goals.get(&goal_id).unwrap().unwrap();
                assert!(baseline.gaps.is_empty());

                goals
                    .revise(
                        goal_id.as_str(),
                        current_goal.revision,
                        current_goal.user_sequence + 1,
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
                let original = goals.get(goal_id.as_str()).unwrap().unwrap();
                assert_eq!(
                    original.obligations.len(),
                    current_goal.obligations.len() + 1
                );
                assert_eq!(
                    original.obligations.last().unwrap().obligation_id,
                    "original-verification"
                );
                assert!(original.obligations.last().unwrap().required);
                assert_eq!(
                    original.obligations.last().unwrap().state,
                    ObjectiveObligationState::Open
                );
            },
        )
        .await;
    }

    #[test]
    fn repeated_completion_rejections_do_not_invent_a_default_business_round_terminal() {
        let (events, goals, actions, actor) = mechanical_fixture(false);
        assert_eq!(completion_reopen_bound().unwrap(), None);
        for attempt in 0..20 {
            events.append(RuntimeEventInput {
                stream_id: format!("agentic-program:{}", actor.program_id), scope: RuntimeEventScope::Program,
                kind: COMPLETION_REJECTED_EVENT_KIND.into(), status: Some("rejected".into()), actor: Some(actor.actor_id.clone()),
                refs: vec![], payload: serde_json::json!({"program_id":actor.program_id,"gap":"review still required","request_action_id":format!("rejection-{attempt}")}),
            }).unwrap();
        }
        let program = actions.project(&actor.program_id).unwrap();
        let prepared = goals.prepare_program_conclusion(&program).unwrap();
        assert!(prepared.goal.terminal.is_none());
        assert_eq!(prepared.goal.completion, GoalCompletion::Open);
        assert!(prepared.goal.waiting.is_some());
        assert!(!prepared.gaps.is_empty());
        actions
            .commit_program_conclusion(&program, prepared)
            .unwrap();
        let goal = goals.get("goal:root-completion").unwrap().unwrap();
        assert_eq!(goal.completion, GoalCompletion::Open);
        assert!(goal.terminal.is_none());
        assert!(actions
            .project(&actor.program_id)
            .unwrap()
            .completion_request
            .is_none());
    }

    #[test]
    fn completion_reopen_resource_limit_is_explicit_and_invalid_values_do_not_fail_open() {
        assert_eq!(parse_completion_reopen_bound("7").unwrap(), 7);
        assert!(parse_completion_reopen_bound("0").is_err());
        assert!(parse_completion_reopen_bound("invalid").is_err());
        assert!(parse_completion_reopen_bound("-1").is_err());
    }

    #[test]
    fn completion_reopen_count_is_zero_without_reopen_events() {
        let store = crate::RuntimeEventStore::for_test();
        assert_eq!(
            completion_reopen_count(&store, "absent-program").unwrap(),
            0
        );
    }
}
