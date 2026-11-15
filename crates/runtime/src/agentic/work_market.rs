use harness_contract::agent_action::{
    AgentActionEnvelope, AgentAttemptMode, TaskAttemptDispatchInput, TaskAttemptFailInput,
    TaskClaimInput, TaskPublishInput, TaskReleaseInput, TaskReviewDecision, TaskReviewInput,
    TaskSubmitInput, TaskSupersedeInput, TaskWithdrawInput,
};

use super::program::{
    AgenticProgramProjection, AgenticReworkDirective, AgenticTaskAttemptProjection,
    AgenticTaskObligation, AgenticTaskProjection, AgenticTaskRetirement, AgenticTaskStatus,
};

// Claims use a short rolling fence. Runtime renews the fence while the
// corresponding physical Agent graph is active; after a crash the absence of
// heartbeats makes the work reclaimable without imposing a task-quality
// deadline on healthy long-running work.
//
// 90s is the default. Each renewal is a full durable `task_claim` action, so on
// a large long-running collaboration the renewal rate dominates applied
// actions. `COWD_CLAIM_LEASE_SECS` raises the lease (and therefore lengthens the
// renewal interval) to cut that churn; crash recovery still reclaims an
// abandoned claim via the stranded-attempt sweep once the lease lapses.
pub(crate) fn claim_lease_ms() -> u64 {
    std::env::var("COWD_CLAIM_LEASE_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(90)
        .saturating_mul(1_000)
}

/// An expired lease may only be renewed by its still-active original attempt.
pub(crate) fn expired_original_claim(
    task: &AgenticTaskProjection,
    agent_id: &str,
    execution_id: &str,
    membership_id: &str,
    now_ms: u64,
) -> bool {
    task.status == AgenticTaskStatus::Claimed
        && task.claimant.as_deref() == Some(agent_id)
        && task.claim_execution_id.as_deref() == Some(execution_id)
        && task.lease_expires_at_ms.is_some_and(|expires| expires <= now_ms)
        && task.cancel_requested_by.is_none()
        && task.pending_retirement.is_none()
        && task.active_attempts.get(execution_id).is_some_and(|attempt| {
            attempt.execution_id == execution_id
                && attempt.agent_id == agent_id
                && attempt.membership_id == membership_id
                && attempt.mode == AgentAttemptMode::Execute
                && attempt.generation.checked_add(1) == Some(task.claim_generation)
        })
}

/// Consecutive claims of one Task with no progress in between.
///
/// Run mon7's final task expired its lease and the model re-claimed it four
/// times over fourteen minutes, submitting the same committed artifact every
/// time, and never reached the completion request again. A renewal costs one
/// durable action and moves nothing, so nothing counted them.
///
/// The streak resets on progress - a delivered result, a review, the claim
/// ending - so a healthy Task never accumulates one and this bound is never
/// reached by work that is actually advancing. Pure so the contract is
/// testable: unset means 3, `0` disables the refusal, an unparseable value
/// falls back rather than silently removing the discipline.
pub(crate) fn claim_renewal_bound() -> Option<u32> {
    match std::env::var("COWD_TASK_RECLAIM_LIMIT") {
        Err(_) => Some(3),
        Ok(value) => match value.trim().parse::<u32>() {
            Ok(0) => None,
            Ok(bound) => Some(bound),
            Err(_) => Some(3),
        },
    }
}

/// 032 K1 (B1-B3): the single compilation of a Task's acceptance face into
/// its criterion record. Prose stays authoritative (B2); each machine check
/// compiles to a hint-ranked criterion of the same record, and the publish
/// provenance travels with every entry (B3). This is the only place prose or
/// checks become criteria: the rework anchor, the K2 review input pack, and
/// every completion read-out consume this list instead of re-splitting text -
/// the two live runs whose reviewers invented standards (the "documented
/// domains" and the counterexample construction) are the named failures this
/// single source closes.
pub(crate) fn compile_task_obligations(
    acceptance: &str,
    acceptance_checks: &[String],
    criterion_source: harness_contract::agent_action::CriterionSource,
) -> Vec<AgenticTaskObligation> {
    let mut obligations = vec![AgenticTaskObligation {
        key: "acceptance".to_string(),
        text: acceptance.trim().to_string(),
        authoritative: true,
        criterion_source,
    }];
    obligations.extend(acceptance_checks.iter().enumerate().map(|(index, check)| {
        AgenticTaskObligation {
            key: format!("check:{index}"),
            text: check.trim().to_string(),
            authoritative: false,
            criterion_source,
        }
    }));
    obligations
}

/// 033 S8: the key of the review obligation a submitted Task owes.
pub(crate) const REVIEW_PENDING_OBLIGATION_KEY: &str = "review_pending";

/// 033 S8: the single source of the review obligation.
///
/// A submitted Task owes a review: the acceptance record compiled at publish
/// is not judged until a reviewer says so. The obligation is compiled here,
/// beside the acceptance obligations, so the Task's obligation record has one
/// source - and it is not an operator criterion (`authoritative = false`): it
/// is a duty the Runtime owes the Program, not a standard a reviewer may
/// paraphrase.
pub(crate) fn compile_review_pending_obligation(
    criterion_source: harness_contract::agent_action::CriterionSource,
) -> AgenticTaskObligation {
    AgenticTaskObligation {
        key: REVIEW_PENDING_OBLIGATION_KEY.to_string(),
        text: "the submitted work must be reviewed against the compiled acceptance record"
            .to_string(),
        authoritative: false,
        criterion_source,
    }
}

/// 033 S8: the terminal obligation list of a Task, with each obligation's
/// status, and the family that blocked it.
///
/// The status is derived, never stored twice: a Task whose review was
/// accepted has satisfied its obligations; a Task that is blocked on a
/// structural fault (a delegated child's receipt that never settled, a read
/// with no original physical extent) has a *failed* obligation and the
/// family's name travels with it - the review could not be judged because
/// the evidence never settled, which is not the model's failure. A Task that
/// is merely waiting keeps the obligation pending.
pub(crate) fn terminal_obligation_list(task: &AgenticTaskProjection) -> Vec<serde_json::Value> {
    let structural_family = task
        .terminal_note
        .as_deref()
        .and_then(|note| note.split("failure_family=").nth(1))
        .and_then(|rest| rest.split(';').next())
        .map(str::trim)
        .filter(|family| {
            !family.is_empty() && *family != "unclassified" && *family != "model_refusal"
        });
    let accepted = task.status == AgenticTaskStatus::Accepted && task.reviews_accepted > 0;
    // 033 S8 (R102 BLOCK-3): the structural failure is marked on the review
    // duty alone. The acceptance obligations keep their own derivation - a
    // structural stall says the review could not be judged, not that the
    // operator's criteria went unsatisfied.
    let mut entries = task
        .obligations
        .iter()
        .map(|obligation| {
            let status = if accepted { "satisfied" } else { "pending" };
            serde_json::json!({
                "key": obligation.key,
                "text": obligation.text,
                "authoritative": obligation.authoritative,
                "criterion_source": obligation.criterion_source,
                "status": status,
                "failure_family": serde_json::Value::Null,
            })
        })
        .collect::<Vec<_>>();
    if let Some(obligation) = &task.review_obligation {
        let status = if accepted {
            "satisfied"
        } else if task.status == AgenticTaskStatus::Blocked
            && structural_family.is_some()
        {
            "failed_structural"
        } else {
            "pending"
        };
        entries.push(serde_json::json!({
            "key": obligation.key,
            "text": obligation.text,
            "authoritative": obligation.authoritative,
            "criterion_source": obligation.criterion_source,
            "status": status,
            "failure_family": structural_family,
        }));
    }
    entries
}

/// 033 S7: the family a failed attempt belongs to.
///
/// The families are the ones the live B-family runs actually produced, so the
/// account a failure is charged to is a fact about the failure and not a
/// guess: a delegated child's receipt that never settles, a read that has no
/// original physical extent, and a composite read whose child acquisitions
/// never completed are all structural - the model cannot fix them by trying
/// again. A refusal the model could have avoided (a mis-cited receipt) is
/// the model's own failure and stays charged to the model. An unclassified
/// failure is charged to the model, the conservative side of the ledger.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AttemptFailureFamily {
    /// The delegated child's receipt never settled ("original child receipt
    /// remains unsettled", "original tool effect remains unknown").
    ReceiptUnsettled,
    /// The original review owner exhausted its pinned-source rebuild budget.
    ReviewCutExhausted,
    /// The read has no original physical extent ("proof_actual_input_extent
    /// _unsupported").
    PhysicalExtentUnsupported,
    /// The composite read's child acquisitions never completed.
    CompositeAcquisitionsIncomplete,
    /// A refusal the model could have avoided (a mis-cited receipt).
    ModelRefusal,
    /// Nothing in the failure names a known family.
    Unclassified,
}

impl AttemptFailureFamily {
    /// The name that goes into the exhaustion note.
    pub(crate) const fn note_name(self) -> &'static str {
        match self {
            Self::ReceiptUnsettled => "receipt_unsettled",
            Self::ReviewCutExhausted => "review_cut_exhausted",
            Self::PhysicalExtentUnsupported => "physical_extent_unsupported",
            Self::CompositeAcquisitionsIncomplete => "composite_acquisitions_incomplete",
            Self::ModelRefusal => "model_refusal",
            Self::Unclassified => "unclassified",
        }
    }

    /// A structural fault is not the model's output problem: it must not be
    /// charged to the model's attempt budget.
    pub(crate) const fn is_structural(self) -> bool {
        matches!(
            self,
            Self::ReceiptUnsettled
                | Self::ReviewCutExhausted
                | Self::PhysicalExtentUnsupported
                | Self::CompositeAcquisitionsIncomplete
        )
    }
}

/// 033 S7: classify a failed attempt by the family its reason names.
pub(crate) fn attempt_failure_family(reason: &str) -> AttemptFailureFamily {
    if reason.contains("original composite child acquisitions incomplete") {
        AttemptFailureFamily::CompositeAcquisitionsIncomplete
    } else if reason.contains("proof_actual_input_extent_unsupported") {
        AttemptFailureFamily::PhysicalExtentUnsupported
    } else if reason.contains("original child receipt remains unsettled")
        || reason.contains("original tool effect remains unknown")
    {
        AttemptFailureFamily::ReceiptUnsettled
    } else if reason.contains("has no canonical durable receipt")
        || reason.contains("one edit away from the cited one")
    {
        AttemptFailureFamily::ModelRefusal
    } else {
        AttemptFailureFamily::Unclassified
    }
}

/// 032 K3 (B10): the one spelling of a Task's blocked-on-exhaustion note.
/// Failure family only - a Blocked Task never carries a satisfied bit. The
/// bound interpolates from the same constant the decision used, so a budget
/// change cannot leave a stale literal behind (the T1b single-token lesson).
///
/// 033 S7 (causal accounting): the note also names the family and the two
/// accounts, so the root can see on its own face that the exhaustion was a
/// structural fault rather than three model failures. The key=value pairs
/// are the machine-readable part; the prose around them stays readable.
fn blocked_exhaustion_note(
    dimension: &str,
    bound: u8,
    last_failure: Option<&str>,
    family: AttemptFailureFamily,
    model_attempts_charged: u8,
    structural_stalls: u8,
) -> String {
    format!(
        "task_blocked: {dimension} exhausted at bounded {bound}; failure_family={family}; \
account={account}; model_attempts_charged={model_attempts_charged}; \
structural_stalls={structural_stalls}; last failure: {last}; remaining: not accepted",
        family = family.note_name(),
        account = if family.is_structural() { "structural" } else { "model" },
        last = last_failure.unwrap_or("none recorded"),
    )
}

/// The bound as a pure function of the configured value.
#[cfg(test)]
pub(crate) fn claim_renewal_bound_from(raw: Option<&str>) -> Option<u32> {
    match raw {
        None => Some(3),
        Some(value) => match value.trim().parse::<u32>() {
            Ok(0) => None,
            Ok(bound) => Some(bound),
            Err(_) => Some(3),
        },
    }
}

// Bounded failure progress. Retries stay evidence-driven, but a Task that keeps
// failing with *different* reasons can otherwise re-enter Rework forever and
// park the root on the Program barrier (G56/G57 root non-finalization). After
// this many physical/review failures the Task is forced `Blocked` so the
// Program can reach an explicit terminal instead of an unbounded loop. A
// materially improved retry still gets its opportunities before the bound.
pub(crate) const MAX_TASK_EXECUTION_ATTEMPTS: u8 = 3;
pub(crate) const MAX_TASK_REVIEW_ATTEMPTS: u8 = 3;
/// Bounded review *rounds*. A rework verdict returns the Task to the market, and
/// the re-executed Task must be reviewable again in its own physical execution:
/// a dispatched Agent is identified by `digest(program | task | agent | mode |
/// generation)` and the Review mode's generation is `review_generation`, so a
/// verdict that does not advance it makes every later review round collide with
/// the first round's graph and be swallowed as an existing durable admission.
/// Once the rounds are counted they must also be bounded, otherwise a reviewer
/// that keeps sending the same Task back parks the root on the Program barrier
/// forever. This counts verdicts only; physical review attempts keep their own
/// separate `MAX_TASK_REVIEW_ATTEMPTS` bound.
pub(crate) const MAX_TASK_REVIEW_ROUNDS: u8 = 3;

pub(crate) fn apply_task_publish(
    projection: &mut AgenticProgramProjection,
    _envelope: &AgentActionEnvelope,
    input: &TaskPublishInput,
    entity_ref: Option<&str>,
) {
    let Some(task_id) = entity_ref else {
        return;
    };
    let criterion_source = derived_criterion_source(projection, input);
    let obligations =
        compile_task_obligations(&input.acceptance, &input.acceptance_checks, criterion_source);
    projection.tasks.insert(
        task_id.to_string(),
        AgenticTaskProjection {
            task_id: task_id.to_string(),
            team_id: input.team_ref.clone(),
            title: input.title.trim().to_string(),
            objective: input.objective.trim().to_string(),
            acceptance: input.acceptance.trim().to_string(),
            acceptance_checks: input.acceptance_checks.clone(),
            criterion_source,
            obligations,
            rework_directives: Vec::new(),
            unsettled_released_attempts: Vec::new(),
            terminal_note: None,
            required_capabilities: input.required_capabilities.clone(),
            obligation_refs: input.obligation_refs.clone(),
            purpose: input.purpose,
            execution_requirements: input.execution_requirements.clone(),
            expertise_hints: input.expertise_hints.clone(),
            depends_on: input.depends_on.clone(),
            status: AgenticTaskStatus::Published,
            claimant: None,
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
            reviewed_by: None,
            failed_attempts: 0,
            review_generation: 0,
            failed_review_attempts: 0,
            structural_execution_stalls: 0,
            structural_review_stalls: 0,
            reviews_applied: 0,
            review_obligation: None,
            reviews_accepted: 0,
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
    if let Some(rounds) = projection.applied_rework_rounds.as_mut() {
        rounds.insert(task_id.to_string(), 0);
    }
    if let Some(team) = projection.teams.get_mut(&input.team_ref) {
        team.task_ids.push(task_id.to_string());
    }
    // An invented standard is labelled where it is written, not where it is
    // enforced. The waste happens at authoring time - the reviewer scans for
    // hazards nobody asked about - so the fact has to be visible to the author
    // at the moment of writing and to the reviewer at the moment of judging.
    //
    // The label is derived from the operator's own objective, never declared by
    // the author. A live run wrote "including exact file path, line reference,
    // and quoted source text" into its own acceptance, a reviewer enforced it
    // line by line, and the declared `criterion_source` stayed at its default
    // for the whole run - asking an author to report that it invented a
    // standard is asking it to incriminate itself, so the field is never set.
    // The label fires on the derived value, which already honours an explicit
    // declaration: a model that reports its own standard as invented is
    // believed, and one that says nothing is judged against the operator.
    if derived_criterion_source(projection, input)
        == harness_contract::agent_action::CriterionSource::AgentInvented
    {
        let marker = invented_form_marker(projection, input)
            .unwrap_or("a standard the model set for itself");
        projection.program_warnings.push(format!(
            "Task `{task_id}` requires `{marker}`, which the operator never asked for. \
             The operator's requirement is: {}. A reviewer must not rework the Task on \
             that added standard alone.",
            operator_requirement_summary(projection)
        ));
    }
}

/// Form requirements a reviewer commonly enforces that the operator rarely
/// states. Derived from the same enumeration `order_contract` carries, so the
/// authoring discipline and the detection cannot drift apart.
const INVENTED_FORM_MARKERS: [&str; 8] = [
    "line reference",
    "quoted source",
    "exact file path",
    "line-level",
    "todo",
    "fixme",
    "unwrap",
    "api survey",
];

/// The provenance of a Task's acceptance statement.
///
/// A declared value wins: a model that reports its own standard as invented is
/// believed. Otherwise the value is derived by comparing the acceptance against
/// the operator's objective, because the author cannot be the source of truth
/// about its own invention.
fn derived_criterion_source(
    projection: &AgenticProgramProjection,
    input: &TaskPublishInput,
) -> harness_contract::agent_action::CriterionSource {
    use harness_contract::agent_action::CriterionSource;
    if input.criterion_source == CriterionSource::AgentInvented {
        return CriterionSource::AgentInvented;
    }
    if invented_form_marker(projection, input).is_some() {
        return CriterionSource::AgentInvented;
    }
    CriterionSource::OperatorDerived
}

/// A form requirement in this Task's acceptance that the operator never stated,
/// or `None` when every requirement traces back to the operator.
///
/// A marker counts as the operator's only when it appears in the operator's own
/// text. Anchors are literal: source paths, counts, named deliverables, and the
/// markers the operator itself used. Nothing here interprets meaning, so an
/// objective that cannot be anchored yields no marker and fails open.
fn invented_form_marker(
    projection: &AgenticProgramProjection,
    input: &TaskPublishInput,
) -> Option<&'static str> {
    let objective = projection.objective_summary.to_ascii_lowercase();
    let acceptance = input.acceptance.to_ascii_lowercase();
    // Nothing to compare against means nothing can be called invented. An
    // objective with no recognisable anchor is not evidence that a requirement
    // was added, and guessing would refuse legitimate rework.
    if !objective_has_anchors(&objective) {
        return None;
    }
    INVENTED_FORM_MARKERS
        .into_iter()
        .find(|marker| acceptance.contains(marker) && !objective.contains(marker))
}

/// Whether the operator's objective states anything checkable at all: a source
/// path, a count, a named deliverable, or one of the form requirements itself.
fn objective_has_anchors(objective: &str) -> bool {
    if objective.trim().is_empty() {
        return false;
    }
    let names_a_path = objective.contains('/') && objective.contains('.');
    let names_a_count = [
        "三个",
        "至少三个",
        "至少",
        "1 个",
        "2 个",
        "3 个",
        "one ",
        "two ",
        "three ",
    ]
    .iter()
    .any(|count| objective.contains(count));
    let names_a_deliverable = [".md", ".html", ".txt", ".json", ".rs"]
        .iter()
        .any(|extension| objective.contains(extension));
    let states_a_form_requirement = INVENTED_FORM_MARKERS
        .iter()
        .any(|marker| objective.contains(marker));
    names_a_path || names_a_count || names_a_deliverable || states_a_form_requirement
}

/// What the operator actually asked for, quoted back so the author and the
/// reviewer read the same comparison instead of an assertion.
fn operator_requirement_summary(projection: &AgenticProgramProjection) -> String {
    let summary = projection.objective_summary.trim();
    if summary.is_empty() {
        return "not recorded on this Program".to_string();
    }
    const LIMIT: usize = 240;
    if summary.chars().count() <= LIMIT {
        return summary.to_string();
    }
    let mut shortened: String = summary.chars().take(LIMIT).collect();
    shortened.push_str("...");
    shortened
}

/// Persist executor admission before a physical graph is started.
///
/// This is an effect-outbox record and, for an Execute dispatch, the one
/// writer of the Task's claim fence.  The dispatch decision already named the
/// Agent and the physical graph that will run this Task, so the fence is
/// written from that decision instead of being re-derived from a separate
/// `task_claim` action: the Agent still decides whether to submit, review,
/// release, or replan when it runs, but the admission it was dispatched under
/// is already durable.
///
/// A Review or Coordination dispatch writes no claim.  The reviewer keeps the
/// read path it has today, and the coordination consumer keeps its own
/// consumption record; a second claim writer for those modes would give the
/// Task two names for one execution.
pub(crate) fn apply_task_attempt_dispatch(
    projection: &mut AgenticProgramProjection,
    input: &TaskAttemptDispatchInput,
    applied_at_ms: u64,
) {
    let Some(task) = projection.tasks.get_mut(&input.task_ref) else {
        return;
    };
    task.active_attempts.insert(
        input.execution_id.clone(),
        AgenticTaskAttemptProjection {
            execution_id: input.execution_id.clone(),
            agent_id: input.agent_ref.clone(),
            membership_id: input.membership_id.clone(),
            mode: input.mode,
            generation: input.generation,
        },
    );
    if input.mode == AgentAttemptMode::Execute {
        // The fence names exactly the attempt that was just admitted, and its
        // generation is that attempt's generation plus one - the same equation
        // the admission authority checks when it decides an effect.
        task.status = AgenticTaskStatus::Claimed;
        task.claimant = Some(input.agent_ref.clone());
        task.claim_execution_id = Some(input.execution_id.clone());
        task.claim_generation = input.generation.saturating_add(1);
        // The claim time is part of the fence, not a decoration: a publication
        // is authenticated against the claim that was live when it was made,
        // and a claim without a time cannot be one.
        task.claimed_at_ms = Some(applied_at_ms);
        task.lease_expires_at_ms = Some(applied_at_ms.saturating_add(claim_lease_ms()));
    }
    if input.mode == AgentAttemptMode::Coordination {
        if let Some(entry) = projection
            .topics
            .values_mut()
            .flatten()
            .find(|entry| entry.revision == input.generation)
        {
            entry.coordination = Some(super::program::AgenticCoordinationConsumption {
                execution_id: input.execution_id.clone(),
                agent_id: input.agent_ref.clone(),
                membership_id: input.membership_id.clone(),
                settled: false,
                reason: None,
            });
        }
    }
}

pub(crate) fn apply_task_attempt_fail(
    projection: &mut AgenticProgramProjection,
    input: &TaskAttemptFailInput,
) {
    apply_task_attempt_fail_with_origin(projection, input, false);
}

pub(crate) fn apply_task_attempt_fail_with_origin(
    projection: &mut AgenticProgramProjection,
    input: &TaskAttemptFailInput,
    trusted_review_cut: bool,
) {
    if input.mode == AgentAttemptMode::Coordination {
        projection.settle_coordination(&input.execution_id, Some(input.reason.clone()));
    }
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        let attempt = task.active_attempts.remove(&input.execution_id);
        if task.status == AgenticTaskStatus::CancelRequested {
            if task.claim_execution_id.as_deref() == Some(input.execution_id.as_str()) {
                task.claim_execution_id = None;
            }
            if task.active_attempts.is_empty() && task.claim_execution_id.is_none() {
                finalize_pending_retirement(task);
            }
            return;
        }
        if input.mode == AgentAttemptMode::Coordination {
            return;
        }
        // Retries are controlled by evidence of progress, not an arbitrary
        // attempt counter.  A second identical durable failure has supplied
        // no new fact or strategy, so re-dispatching it would spend provider
        // capacity in a loop.  Keep the Task visibly blocked for an Agent to
        // replan/supersede; a materially different failure can receive one
        // new runtime retry opportunity.
        let repeats_same_failure = input.retryable
            && task
                .last_failure
                .as_deref()
                .is_some_and(|previous| previous == input.reason);
        task.last_failure = Some(input.reason.clone());
        match input.mode {
            AgentAttemptMode::Coordination => {
                unreachable!("coordination settles without business failure")
            }
            AgentAttemptMode::Execute => {
                // A failure before the first claim still consumes a physical
                // opportunity. Give recovery a fresh graph identity without
                // forging the Agent's claim or reusing its old graph.
                if task.claim_execution_id.is_none()
                    && attempt
                        .as_ref()
                        .is_some_and(|attempt| attempt.generation == task.claim_generation)
                {
                    task.claim_generation = task.claim_generation.saturating_add(1);
                }
                // 033 S7 (causal accounting): a structural fault is charged
                // to the structural account, not to the model's attempt
                // budget. The bound still covers both accounts, so a stall
                // can never run forever, but the model is no longer charged
                // for a fault it cannot fix.
                let family = attempt_failure_family(input.reason.as_str());
                if family.is_structural() {
                    task.structural_execution_stalls =
                        task.structural_execution_stalls.saturating_add(1);
                } else {
                    task.failed_attempts = task.failed_attempts.saturating_add(1);
                }
                let attempts_exhausted = task
                    .failed_attempts
                    .saturating_add(task.structural_execution_stalls)
                    >= MAX_TASK_EXECUTION_ATTEMPTS;
                task.claimant = None;
                task.claim_execution_id = None;
                task.claimed_at_ms = None;
                task.lease_expires_at_ms = None;
                task.status = if attempts_exhausted {
                    task.terminal_note = Some(blocked_exhaustion_note(
                        "execution attempts",
                        MAX_TASK_EXECUTION_ATTEMPTS,
                        task.last_failure.as_deref(),
                        family,
                        task.failed_attempts,
                        task.structural_execution_stalls,
                    ));
                    AgenticTaskStatus::Blocked
                } else if input.retryable && !repeats_same_failure {
                    AgenticTaskStatus::Rework
                } else if input.retryable {
                    task.terminal_note = Some(blocked_exhaustion_note(
                        "repeated identical failure",
                        1,
                        task.last_failure.as_deref(),
                        family,
                        task.failed_attempts,
                        task.structural_execution_stalls,
                    ));
                    AgenticTaskStatus::Blocked
                } else {
                    // A non-retryable physical failure is a scheduling
                    // checkpoint, not a business terminal. Returning the
                    // work to Published lets the root change the assignee,
                    // split the work or publish replacement work.
                    AgenticTaskStatus::Published
                };
            }
            AgentAttemptMode::Review => {
                // 033 S7 (causal accounting): the review path is where the
                // B-family ran out - three "model failures" that were all
                // structural. A structural stall is charged to the structural
                // account; the model's review budget only pays for the
                // failures the model caused.
                let family = if trusted_review_cut {
                    AttemptFailureFamily::ReviewCutExhausted
                } else {
                    attempt_failure_family(input.reason.as_str())
                };
                if family.is_structural() {
                    task.structural_review_stalls =
                        task.structural_review_stalls.saturating_add(1);
                } else {
                    task.failed_review_attempts = task.failed_review_attempts.saturating_add(1);
                }
                task.review_generation = task.review_generation.saturating_add(1);
                let reviews_exhausted = task
                    .failed_review_attempts
                    .saturating_add(task.structural_review_stalls)
                    >= MAX_TASK_REVIEW_ATTEMPTS;
                task.status = if reviews_exhausted {
                    task.terminal_note = Some(blocked_exhaustion_note(
                        "review attempts",
                        MAX_TASK_REVIEW_ATTEMPTS,
                        task.last_failure.as_deref(),
                        family,
                        task.failed_review_attempts,
                        task.structural_review_stalls,
                    ));
                    AgenticTaskStatus::Blocked
                } else if input.retryable && !repeats_same_failure {
                    AgenticTaskStatus::Submitted
                } else if input.retryable {
                    task.terminal_note = Some(blocked_exhaustion_note(
                        "repeated identical review failure",
                        1,
                        task.last_failure.as_deref(),
                        family,
                        task.failed_review_attempts,
                        task.structural_review_stalls,
                    ));
                    AgenticTaskStatus::Blocked
                } else {
                    // The root can invite a new reviewer or replan the work;
                    // only ObjectiveSupervisor may create a business terminal.
                    AgenticTaskStatus::Published
                };
            }
        }
    }
}

pub(crate) fn apply_task_claim(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskClaimInput,
    applied_at_ms: u64,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        let renewal = task.status == AgenticTaskStatus::Claimed
            && task.claimant.as_deref()
                == envelope
                    .actor
                    .agent_id
                    .as_deref()
                    .or(Some(envelope.actor.actor_id.as_str()))
            && task.claim_execution_id == envelope.actor.execution_id;
        if !renewal {
            task.claim_generation = task.claim_generation.saturating_add(1);
            task.claim_renewal_streak = 1;
        } else {
            // A renewal with nothing new in between is one more turn of the
            // same wheel. The validator refuses the claim past the bound; the
            // streak itself is reset by progress (a new deliverable or a
            // status change), never by time.
            task.claim_renewal_streak = task.claim_renewal_streak.saturating_add(1);
        }
        task.status = AgenticTaskStatus::Claimed;
        task.claimant = Some(
            envelope
                .actor
                .agent_id
                .clone()
                .unwrap_or_else(|| envelope.actor.actor_id.clone()),
        );
        task.claim_execution_id
            .clone_from(&envelope.actor.execution_id);
        task.claimed_at_ms = Some(applied_at_ms);
        task.lease_expires_at_ms = Some(applied_at_ms.saturating_add(claim_lease_ms()));
    }
}

pub(crate) fn apply_task_release(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskReleaseInput,
) {
    let coordination = projection
        .coordination_attempt(envelope.actor.execution_id.as_deref().unwrap_or(""))
        .is_some();
    if coordination {
        projection.settle_coordination(envelope.actor.execution_id.as_deref().unwrap_or(""), None);
    }
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        // The claim ends here, so its renewal streak ends with it.
        task.claim_renewal_streak = 0;
        if let Some(execution_id) = envelope.actor.execution_id.as_deref() {
            task.active_attempts.remove(execution_id);
        }
        if task.status == AgenticTaskStatus::CancelRequested {
            if task.claim_execution_id == envelope.actor.execution_id {
                task.claim_execution_id = None;
            }
            if task.active_attempts.is_empty() && task.claim_execution_id.is_none() {
                finalize_pending_retirement(task);
            }
            return;
        }
        if coordination {
            return;
        }
        task.status = AgenticTaskStatus::Published;
        task.claimant = None;
        task.claim_execution_id = None;
        task.claimed_at_ms = None;
        task.lease_expires_at_ms = None;
    }
}

pub(crate) fn apply_task_supersede(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskSupersedeInput,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        if task.status == AgenticTaskStatus::Claimed || !task.active_attempts.is_empty() {
            task.claim_generation = task.claim_generation.saturating_add(1);
            task.status = AgenticTaskStatus::CancelRequested;
            task.pending_retirement = Some(AgenticTaskRetirement::Superseded);
            task.cancel_requested_by = Some(envelope.actor.actor_id.clone());
            task.cancel_reason_ref = None;
            task.cancel_evidence_refs.clone_from(&input.evidence_refs);
        } else {
            task.status = AgenticTaskStatus::Superseded;
        }
        task.replacement_task_refs
            .clone_from(&input.replacement_task_refs);
        task.supersede_evidence_refs
            .clone_from(&input.evidence_refs);
        task.superseded_reason = Some(input.reason.clone());
        task.superseded_by = Some(envelope.actor.actor_id.clone());
    }
}

/// Withdrawal preserves the Task and all downstream dependency diagnostics.
/// It never converts removal into a successful dependency result.
pub(crate) fn apply_task_withdraw(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskWithdrawInput,
) {
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        if task.status == AgenticTaskStatus::Claimed || !task.active_attempts.is_empty() {
            task.claim_generation = task.claim_generation.saturating_add(1);
            task.status = AgenticTaskStatus::CancelRequested;
            task.pending_retirement = Some(AgenticTaskRetirement::Withdrawn);
            task.cancel_requested_by = Some(envelope.actor.actor_id.clone());
            task.cancel_reason_ref = Some(input.reason_ref.clone());
            task.cancel_evidence_refs.clone_from(&input.evidence_refs);
        } else {
            task.status = AgenticTaskStatus::Withdrawn;
            task.claimant = None;
            task.claim_execution_id = None;
            task.claimed_at_ms = None;
            task.lease_expires_at_ms = None;
            task.last_failure = Some(format!("withdrawn: {}", input.reason_ref));
            task.cancel_evidence_refs.clone_from(&input.evidence_refs);
            task.cancel_requested_by = Some(envelope.actor.actor_id.clone());
            task.cancel_reason_ref = Some(input.reason_ref.clone());
        }
    }
}

fn finalize_pending_retirement(task: &mut AgenticTaskProjection) {
    if !task.active_attempts.is_empty() {
        return;
    }
    let retirement = task.pending_retirement.take();
    task.status = match retirement {
        Some(AgenticTaskRetirement::Withdrawn) => AgenticTaskStatus::Withdrawn,
        Some(AgenticTaskRetirement::Superseded) => AgenticTaskStatus::Superseded,
        None => AgenticTaskStatus::Blocked,
    };
    task.claimant = None;
    task.claim_execution_id = None;
    task.claimed_at_ms = None;
    task.lease_expires_at_ms = None;
}

/// A superseded dependency is satisfied only when every concrete successor is
/// itself accepted (or recursively superseded by accepted successors). This
/// preserves downstream dependencies without rewriting already-journaled Task
/// contracts and never treats retirement itself as successful work.
pub(crate) fn task_dependency_satisfied(
    projection: &AgenticProgramProjection,
    task_ref: &str,
) -> bool {
    fn visit(
        projection: &AgenticProgramProjection,
        task_ref: &str,
        visiting: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        let Some(task) = projection.tasks.get(task_ref) else {
            return false;
        };
        match task.status {
            AgenticTaskStatus::Accepted => true,
            AgenticTaskStatus::Superseded => {
                if task.replacement_task_refs.is_empty() || !visiting.insert(task_ref.to_string()) {
                    return false;
                }
                let satisfied = task
                    .replacement_task_refs
                    .iter()
                    .all(|replacement| visit(projection, replacement, visiting));
                visiting.remove(task_ref);
                satisfied
            }
            _ => false,
        }
    }

    visit(projection, task_ref, &mut std::collections::BTreeSet::new())
}

pub(crate) fn task_depends_on(
    projection: &AgenticProgramProjection,
    task_ref: &str,
    dependency_ref: &str,
) -> bool {
    fn visit(
        projection: &AgenticProgramProjection,
        task_ref: &str,
        dependency_ref: &str,
        visiting: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        let Some(task) = projection.tasks.get(task_ref) else {
            return false;
        };
        if task.depends_on.iter().any(|item| item == dependency_ref) {
            return true;
        }
        if !visiting.insert(task_ref.to_string()) {
            return false;
        }
        let found = task
            .depends_on
            .iter()
            .any(|dependency| visit(projection, dependency, dependency_ref, visiting));
        visiting.remove(task_ref);
        found
    }

    visit(
        projection,
        task_ref,
        dependency_ref,
        &mut std::collections::BTreeSet::new(),
    )
}

pub(crate) fn apply_task_submit(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskSubmitInput,
) {
    // One-shot deliverable: the host persisted the selected non-empty response
    // block as durable content, so the canonical Task artifact is synthesized
    // here through the same projection shape as `artifact_commit`. The id is a
    // pure function of (program, actor, action, kind), so replay is stable.
    // 033 S8 (R102 rework): a submitted Task owes a review. The duty lives
    // in its own projection slot, not in the acceptance obligation list: the
    // work manifests carry the acceptance record, and a workflow duty is not
    // semantic work for the worker.
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        if task.review_obligation.is_none() {
            task.review_obligation =
                Some(compile_review_pending_obligation(task.criterion_source));
        }
    }
    let mut artifact_refs = input.artifact_refs.clone();
    if artifact_refs.is_empty() {
        if let Some(content_ref) = input
            .deliverable
            .as_ref()
            .and_then(|deliverable| deliverable.resolved_content_ref.as_deref())
        {
            let artifact_ref = one_shot_artifact_ref(envelope);
            let (kind, title) = input
                .deliverable
                .as_ref()
                .map(|deliverable| {
                    (
                        if deliverable.kind.trim().is_empty() {
                            "report".to_string()
                        } else {
                            deliverable.kind.clone()
                        },
                        deliverable.title.clone(),
                    )
                })
                .unwrap_or_else(|| ("report".to_string(), String::new()));
            let active_claim = projection
                .tasks
                .get(&input.task_ref)
                .map(|task| (task.claim_execution_id.clone(), Some(task.claim_generation)));
            projection.artifacts.insert(
                artifact_ref.clone(),
                super::program::AgenticArtifactProjection {
                    artifact_ref: artifact_ref.clone(),
                    content_ref: content_ref.to_string(),
                    kind,
                    title,
                    relates_to: vec![input.task_ref.clone()],
                    committed_by: envelope.actor.actor_id.clone(),
                    claim_execution_id: active_claim
                        .as_ref()
                        .and_then(|(execution_id, _)| execution_id.clone()),
                    claim_generation: active_claim
                        .as_ref()
                        .and_then(|(_, generation)| *generation),
                },
            );
            artifact_refs.push(artifact_ref);
        }
    }
    let mut evidence_refs = input.evidence_refs.clone();
    evidence_refs.extend(artifact_refs.iter().filter_map(|artifact_ref| {
        projection
            .artifacts
            .get(artifact_ref)
            .map(|artifact| artifact.content_ref.clone())
    }));
    if evidence_refs.is_empty() {
        // The deliverable itself is durable evidence: keep the submission
        // substantive without asking a weak model for a second selector.
        evidence_refs.extend(artifact_refs.iter().filter_map(|artifact_ref| {
            projection
                .artifacts
                .get(artifact_ref)
                .map(|artifact| artifact.content_ref.clone())
        }));
    }
    evidence_refs.sort();
    evidence_refs.dedup();
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        if let Some(execution_id) = envelope.actor.execution_id.as_deref() {
            task.active_attempts.remove(execution_id);
        }
        task.status = AgenticTaskStatus::Submitted;
        task.artifact_refs.clone_from(&artifact_refs);
        task.evidence_refs = evidence_refs;
        task.unresolved.clone_from(&input.unresolved);
        // A delivered result is progress: the renewal streak exists to count
        // re-claims with nothing new in between, and this is something new.
        task.claim_renewal_streak = 0;
    }
}

/// Deterministic artifact id for a one-shot Task deliverable. Mirrors the
/// canonical `artifact_commit` entity id derivation (program, actor, action,
/// kind) so replays and rebuilds resolve to the same artifact.
fn one_shot_artifact_ref(envelope: &AgentActionEnvelope) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(
        format!(
            "{}|{}|{}|{}",
            envelope.actor.program_id,
            envelope.actor.actor_id,
            envelope.action_id,
            envelope.action.kind()
        )
        .as_bytes(),
    );
    let encoded = format!("{digest:x}");
    format!("artifact:{}", &encoded[..24])
}

pub(crate) fn apply_task_review(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskReviewInput,
) {
    // The durable event owner chooses the versioned path; direct reducer
    // callers without a recovered count retain the historical rule.
    let verdict_rule = projection.applied_rework_rounds.as_ref()
        .is_some_and(|rounds| rounds.contains_key(&input.task_ref));
    apply_task_review_with_round_rule(projection, envelope, input, verdict_rule)
        .expect("known verdict round count");
}

pub(crate) fn apply_task_review_with_round_rule(
    projection: &mut AgenticProgramProjection,
    envelope: &AgentActionEnvelope,
    input: &TaskReviewInput,
    verdict_rule: bool,
) -> Result<(), &'static str> {
    let is_rework = matches!(input.decision, TaskReviewDecision::Challenge | TaskReviewDecision::Rework);
    if verdict_rule && is_rework && projection.tasks.contains_key(&input.task_ref)
        && projection.applied_rework_rounds.as_ref().and_then(|rounds| rounds.get(&input.task_ref)).is_none()
    {
        return Err("review_rework_history_unknown");
    }
    let rework_round = if is_rework && projection.tasks.contains_key(&input.task_ref) {
        projection.applied_rework_rounds.as_mut().and_then(|rounds| rounds.get_mut(&input.task_ref)).map(|rounds| {
            *rounds = rounds.saturating_add(1).min(MAX_TASK_REVIEW_ROUNDS);
            *rounds
        })
    } else { None };
    if let Some(task) = projection.tasks.get_mut(&input.task_ref) {
        // A review is the task moving forward; the renewal streak resets.
        task.claim_renewal_streak = 0;
        if let Some(execution_id) = envelope.actor.execution_id.as_deref() {
            task.active_attempts.remove(execution_id);
        }
        task.reviewed_by = Some(envelope.actor.actor_id.clone());
        // 033 S8: the two review measures sit side by side - every applied
        // verdict (the old measure) and the applied verdicts that were an
        // acceptance (the new one). The old count keeps its meaning.
        task.reviews_applied = task.reviews_applied.saturating_add(1);
        if matches!(input.decision, TaskReviewDecision::Accept) {
            task.reviews_accepted = task.reviews_accepted.saturating_add(1);
        }
        task.review_reason = Some(input.reason.clone());
        task.evidence_refs
            .extend(input.evidence_refs.iter().cloned());
        task.evidence_refs.sort();
        task.evidence_refs.dedup();
        match input.decision {
            TaskReviewDecision::Accept => {
                task.status = AgenticTaskStatus::Accepted;
            }
            TaskReviewDecision::Challenge | TaskReviewDecision::Rework => {
                task.claimant = None;
                task.claim_execution_id = None;
                task.claimed_at_ms = None;
                task.lease_expires_at_ms = None;
                // 032 K3 (B8): releasing stale attempts must not erase them.
                // Every attempt that leaves the active set without a durable
                // terminal keeps its full identity in the unsettled list -
                // the new round cannot inherit or launder an effect nobody
                // observed settled, and the generation fence already refuses
                // its late settlements (the ledger's no-redemption rule).
                task.unsettled_released_attempts
                    .extend(task.active_attempts.values().cloned());
                task.active_attempts.clear();
                if !input.gaps.is_empty() || !input.directed_actions.is_empty() {
                    // The directive is a durable fact for the next claim and
                    // for the root's dispatch decisions. Nothing here
                    // dispatches: MissionVerdict never becomes an admission
                    // path (v2 audit red line three).
                    task.rework_directives.push(AgenticReworkDirective {
                        source_action_id: envelope.action_id.clone(),
                        gaps: input.gaps.clone(),
                        directed_actions: input.directed_actions.clone(),
                    });
                }
                // The reworked Task re-enters the market, so its next review is a
                // new round: advance the review generation or the next review
                // round reuses this round's graph identity. Past the bound the
                // Task is forced Blocked with the named exhaustion note: the
                // rework loop stops (the control phase refuses further claims
                // and reviews on a Blocked Task), while the completion barrier
                // keeps counting the Task as not-accepted until the root
                // retires it (supersede/withdraw) or replans with a
                // replacement - the note names which bound was reached.
                // (032 K3 corrected this comment: the earlier wording claimed
                // the barrier "already treats Blocked as a release condition",
                // which the supervision gate contradicts - ledger finding H2.)
                task.review_generation = task.review_generation.saturating_add(1);
                let rounds_exhausted = if verdict_rule {
                    rework_round.is_some_and(|round| round >= MAX_TASK_REVIEW_ROUNDS)
                } else {
                    task.review_generation >= u64::from(MAX_TASK_REVIEW_ROUNDS)
                };
                task.status = if rounds_exhausted {
                    task.terminal_note = Some(blocked_exhaustion_note(
                        "review rounds",
                        MAX_TASK_REVIEW_ROUNDS,
                        Some(input.reason.as_str()),
                        attempt_failure_family(input.reason.as_str()),
                        task.failed_review_attempts,
                        task.structural_review_stalls,
                    ));
                    AgenticTaskStatus::Blocked
                } else {
                    AgenticTaskStatus::Rework
                };
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use harness_contract::agent_action::{
        AgentAction, AgentActorBinding, AgentActorKind, AgentAttemptMode, TaskAttemptDispatchInput,
        TaskAttemptFailInput, TaskClaimInput, TaskWithdrawInput,
    };

    use super::*;

    fn claim_envelope(execution_id: &str) -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: format!("claim:{execution_id}"),
            actor: AgentActorBinding {
                original_execution: None,
                objective_id: "objective".to_string(),
                program_id: "program".to_string(),
                session_id: "session".to_string(),
                turn_id: "turn".to_string(),
                root_execution_id: Some("root".to_string()),
                required_team_count: 1,
                exact_team_count: None,
                objective_summary: "long-running task".to_string(),
                model_lease: "model".to_string(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: "agent".to_string(),
                kind: AgentActorKind::Agent,
                execution_id: Some(execution_id.to_string()),
                team_id: Some("team".to_string()),
                agent_id: Some("agent".to_string()),
            },
            expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput {
                task_ref: "task".to_string(),
                reason: None,
            }),
        }
    }

    #[test]
    fn renewals_accumulate_the_streak_and_a_fresh_claim_resets_it() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task".to_string(),
            AgenticTaskProjection {
                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "long task".to_string(),
                objective: "finish without an arbitrary wall timeout".to_string(),
                acceptance: "durable evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                required_capabilities: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Published,
                claimant: None,
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
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                structural_execution_stalls: 0,
                structural_review_stalls: 0,
                reviews_applied: 0,
                review_obligation: None,
                reviews_accepted: 0,
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
        let envelope = claim_envelope("exec-1");
        apply_task_claim(&mut projection, &envelope, &claim_input(), 1_000);
        // The same execution renewing is one more turn of the same wheel.
        apply_task_claim(&mut projection, &envelope, &claim_input(), 2_000);
        apply_task_claim(&mut projection, &envelope, &claim_input(), 3_000);
        // A different execution is a fresh claim, not a renewal.
        let other = claim_envelope("exec-2");
        apply_task_claim(&mut projection, &other, &claim_input(), 4_000);
        assert_eq!(projection.tasks["task"].claim_renewal_streak, 1);
        // A fresh claim by a different execution advances the generation.
        assert_eq!(projection.tasks["task"].claim_generation, 2);
    }

    #[test]
    fn a_delivered_result_resets_the_renewal_streak() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task".to_string(),
            AgenticTaskProjection {
                claim_renewal_streak: 0,
                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "long task".to_string(),
                objective: "finish".to_string(),
                acceptance: "durable evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                required_capabilities: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Claimed,
                claimant: Some("agent".to_string()),
                claim_generation: 1,
                claim_execution_id: Some("exec-1".to_string()),
                claimed_at_ms: Some(1_000),
                lease_expires_at_ms: Some(91_000),
                active_attempts: Default::default(),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                structural_execution_stalls: 0,
                structural_review_stalls: 0,
                reviews_applied: 0,
                review_obligation: None,
                reviews_accepted: 0,
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
        // Mon7's shape: two renewals, then a submission finally lands. The
        // delivered result is progress and the streak must clear.
        let envelope = claim_envelope("exec-1");
        let submit = AgentActionEnvelope {
            action: AgentAction::TaskSubmit(harness_contract::agent_action::TaskSubmitInput {
                task_ref: "task".to_string(),
                artifact_refs: vec!["artifact:fresh".to_string()],
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                deliverable: None,
            }),
            ..envelope.clone()
        };
        apply_task_submit(&mut projection, &submit, &submit_input());
        assert_eq!(projection.tasks["task"].claim_renewal_streak, 0);
        assert_eq!(
            projection.tasks["task"].status,
            AgenticTaskStatus::Submitted
        );
    }

    #[test]
    fn the_reclaim_bound_honors_its_documented_contract() {
        assert_eq!(claim_renewal_bound_from(None), Some(3));
        assert_eq!(claim_renewal_bound_from(Some("5")), Some(5));
        // The documented `0 disables` must disable.
        assert_eq!(claim_renewal_bound_from(Some("0")), None);
        // Unparseable falls back rather than removing the discipline.
        assert_eq!(claim_renewal_bound_from(Some("")), Some(3));
        assert_eq!(claim_renewal_bound_from(Some("abc")), Some(3));
    }

    fn claim_input() -> TaskClaimInput {
        TaskClaimInput {
            task_ref: "task".to_string(),
            reason: None,
        }
    }

    fn submit_input() -> harness_contract::agent_action::TaskSubmitInput {
        harness_contract::agent_action::TaskSubmitInput {
            task_ref: "task".to_string(),
            artifact_refs: vec!["artifact:fresh".to_string()],
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            deliverable: None,
        }
    }

    #[test]
    fn same_execution_claim_heartbeat_renews_without_advancing_generation() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task".to_string(),
            AgenticTaskProjection {
                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "long task".to_string(),
                objective: "finish without an arbitrary wall timeout".to_string(),
                acceptance: "durable evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Published,
                claimant: None,
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
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                structural_execution_stalls: 0,
                structural_review_stalls: 0,
                reviews_applied: 0,
                review_obligation: None,
                reviews_accepted: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
        let envelope = claim_envelope("execution");
        let input = match &envelope.action {
            AgentAction::TaskClaim(input) => input,
            _ => unreachable!(),
        };
        apply_task_claim(&mut projection, &envelope, input, 10);
        let first_expiry = projection.tasks["task"]
            .lease_expires_at_ms
            .expect("initial lease");
        apply_task_claim(&mut projection, &envelope, input, first_expiry - 1);
        let task = &projection.tasks["task"];
        assert_eq!(task.claim_generation, 1);
        assert_eq!(task.claim_execution_id.as_deref(), Some("execution"));
        assert!(task.lease_expires_at_ms.expect("renewed lease") > first_expiry);
    }

    #[test]
    fn distinct_execution_failures_are_bounded_to_blocked() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert(
            "task".to_string(),
            AgenticTaskProjection {
                claim_renewal_streak: 0,
                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "task".to_string(),
                objective: "objective".to_string(),
                acceptance: "durable evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Rework,
                claimant: None,
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                active_attempts: Default::default(),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                structural_execution_stalls: 0,
                structural_review_stalls: 0,
                reviews_applied: 0,
                review_obligation: None,
                reviews_accepted: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
        // Distinct reasons defeat the same-failure veto, so the explicit bound
        // must terminalize the Task instead of re-entering Rework forever.
        for attempt in 1..=MAX_TASK_EXECUTION_ATTEMPTS {
            let input = TaskAttemptFailInput {
                task_ref: "task".to_string(),
                execution_id: format!("execution-{attempt}"),
                mode: AgentAttemptMode::Execute,
                reason: format!("distinct failure {attempt}"),
                retryable: true,
            };
            apply_task_attempt_fail(&mut projection, &input);
        }
        let task = &projection.tasks["task"];
        assert_eq!(task.failed_attempts, MAX_TASK_EXECUTION_ATTEMPTS);
        assert_eq!(
            task.status,
            AgenticTaskStatus::Blocked,
            "bounded distinct failures must reach a terminal instead of looping"
        );
    }

    #[test]
    fn superseded_dependency_waits_for_every_split_successor() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        let base = AgenticTaskProjection {
            task_id: "source".to_string(),
            team_id: "team".to_string(),
            title: "source".to_string(),
            objective: "objective".to_string(),
            acceptance: "accepted evidence".to_string(),
            acceptance_checks: Vec::new(),
            criterion_source: Default::default(),
            obligations: Vec::new(),
            rework_directives: Vec::new(),
            unsettled_released_attempts: Vec::new(),
            terminal_note: None,
            required_capabilities: Vec::new(),
            depends_on: Vec::new(),
            status: AgenticTaskStatus::Superseded,
            claimant: None,
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
            reviewed_by: None,
            failed_attempts: 1,
            review_generation: 0,
            failed_review_attempts: 0,
            structural_execution_stalls: 0,
            structural_review_stalls: 0,
            reviews_applied: 0,
            review_obligation: None,
            reviews_accepted: 0,
            last_failure: Some("disproved".to_string()),
            replacement_task_refs: vec!["part-a".to_string(), "part-b".to_string()],
            supersede_evidence_refs: vec!["tool://failure".to_string()],
            superseded_reason: Some("split".to_string()),
            superseded_by: Some("lead".to_string()),
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
            cancel_requested_by: None,
            cancel_reason_ref: None,
            cancel_evidence_refs: Vec::new(),
            pending_retirement: None,
        };
        projection.tasks.insert("source".to_string(), base.clone());
        for (id, status) in [
            ("part-a", AgenticTaskStatus::Accepted),
            ("part-b", AgenticTaskStatus::Published),
        ] {
            let mut successor = base.clone();
            successor.task_id = id.to_string();
            successor.status = status;
            successor.replacement_task_refs.clear();
            successor.supersede_evidence_refs.clear();
            successor.superseded_reason = None;
            successor.superseded_by = None;
            projection.tasks.insert(id.to_string(), successor);
        }

        assert!(!task_dependency_satisfied(&projection, "source"));
        projection.tasks.get_mut("part-b").expect("part b").status = AgenticTaskStatus::Accepted;
        assert!(task_dependency_satisfied(&projection, "source"));
    }

    #[test]
    fn retirement_waits_for_every_admitted_physical_attempt_to_settle() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        let task = AgenticTaskProjection {
            claim_renewal_streak: 0,
            task_id: "task".to_string(),
            team_id: "team".to_string(),
            title: "cancel safely".to_string(),
            objective: "prove effect fencing".to_string(),
            acceptance: "no orphaned execution".to_string(),
            acceptance_checks: Vec::new(),
            criterion_source: Default::default(),
            obligations: Vec::new(),
            rework_directives: Vec::new(),
            unsettled_released_attempts: Vec::new(),
            terminal_note: None,
            required_capabilities: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
            depends_on: Vec::new(),
            status: AgenticTaskStatus::Published,
            claimant: None,
            claim_generation: 0,
            claim_execution_id: None,
            claimed_at_ms: None,
            lease_expires_at_ms: None,
            active_attempts: Default::default(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            review_reason: None,
            reviewed_by: None,
            failed_attempts: 0,
            review_generation: 0,
            failed_review_attempts: 0,
            structural_execution_stalls: 0,
            structural_review_stalls: 0,
            reviews_applied: 0,
            review_obligation: None,
            reviews_accepted: 0,
            last_failure: None,
            replacement_task_refs: Vec::new(),
            supersede_evidence_refs: Vec::new(),
            superseded_reason: None,
            superseded_by: None,
            cancel_requested_by: None,
            cancel_reason_ref: None,
            cancel_evidence_refs: Vec::new(),
            pending_retirement: None,
        };
        projection.tasks.insert(task.task_id.clone(), task.clone());
        for execution_id in ["execute-graph", "review-graph"] {
            apply_task_attempt_dispatch(
                &mut projection,
                &TaskAttemptDispatchInput {
                    task_ref: "task".to_string(),
                    execution_id: execution_id.to_string(),
                    agent_ref: "agent".to_string(),
                    membership_id: "membership:agent:team".to_string(),
                    mode: if execution_id == "execute-graph" {
                        AgentAttemptMode::Execute
                    } else {
                        AgentAttemptMode::Review
                    },
                    generation: 0,
                },
                1_000,
            );
        }
        let withdrawal = TaskWithdrawInput {
            task_ref: "task".to_string(),
            reason_ref: "artifact://withdrawal-reason".to_string(),
            evidence_refs: Vec::new(),
        };
        apply_task_withdraw(
            &mut projection,
            &claim_envelope("execute-graph"),
            &withdrawal,
        );
        assert_eq!(
            projection.tasks["task"].status,
            AgenticTaskStatus::CancelRequested
        );
        apply_task_attempt_fail(
            &mut projection,
            &TaskAttemptFailInput {
                task_ref: "task".to_string(),
                execution_id: "execute-graph".to_string(),
                mode: AgentAttemptMode::Execute,
                reason: "cancelled".to_string(),
                retryable: false,
            },
        );
        assert_eq!(
            projection.tasks["task"].status,
            AgenticTaskStatus::CancelRequested
        );
        assert_eq!(projection.tasks["task"].active_attempts.len(), 1);
        apply_task_attempt_fail(
            &mut projection,
            &TaskAttemptFailInput {
                task_ref: "task".to_string(),
                execution_id: "review-graph".to_string(),
                mode: AgentAttemptMode::Review,
                reason: "cancelled".to_string(),
                retryable: false,
            },
        );
        assert_eq!(
            projection.tasks["task"].status,
            AgenticTaskStatus::Withdrawn
        );
        assert!(projection.tasks["task"].active_attempts.is_empty());
    }
}

/// The seven paths that end a Task's claim, and the state each one leaves.
/// A fence that is written once has to be ended exactly once too: these
/// assertions say what every ending leaves behind, so a path that forgets to
/// release the claim - or a path that releases it early - cannot pass
/// unnoticed.
#[cfg(test)]
mod post_state_tests {
    use super::*;
    use harness_contract::agent_action::{
        AgentAction, AgentActorBinding, AgentActorKind, AgentAttemptMode, MessagePublishInput,
        TaskAttemptFailInput, TaskIntent, TaskIntentKind, TaskOneShotDeliverable,
        TaskReleaseInput, TaskReviewDecision, TaskReviewInput, TaskSubmitInput, TaskSupersedeInput,
        TaskWithdrawInput,
    };

    fn task() -> AgenticTaskProjection {
        AgenticTaskProjection {
            task_id: "task".to_string(),
            team_id: "team".to_string(),
            title: "task".to_string(),
            objective: "objective".to_string(),
            acceptance: "durable evidence".to_string(),
            acceptance_checks: Vec::new(),
            criterion_source: Default::default(),
            obligations: Vec::new(),
            rework_directives: Vec::new(),
            unsettled_released_attempts: Vec::new(),
            terminal_note: None,
            required_capabilities: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
            depends_on: Vec::new(),
            status: AgenticTaskStatus::Published,
            claimant: None,
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
            reviewed_by: None,
            failed_attempts: 0,
            review_generation: 0,
            failed_review_attempts: 0,
            structural_execution_stalls: 0,
            structural_review_stalls: 0,
            reviews_applied: 0,
            review_obligation: None,
            reviews_accepted: 0,
            last_failure: None,
            replacement_task_refs: Vec::new(),
            supersede_evidence_refs: Vec::new(),
            superseded_reason: None,
            superseded_by: None,
            cancel_requested_by: None,
            cancel_reason_ref: None,
            cancel_evidence_refs: Vec::new(),
            pending_retirement: None,
        }
    }

    fn envelope(execution_id: &str, agent_id: &str, action: AgentAction) -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: format!("action:{execution_id}:{agent_id}"),
            actor: AgentActorBinding {
                original_execution: None,
                objective_id: "objective".to_string(),
                program_id: "program".to_string(),
                session_id: "session".to_string(),
                turn_id: "turn".to_string(),
                root_execution_id: Some("root".to_string()),
                required_team_count: 1,
                exact_team_count: None,
                objective_summary: "objective".to_string(),
                model_lease: "model".to_string(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: agent_id.to_string(),
                kind: AgentActorKind::Agent,
                execution_id: Some(execution_id.to_string()),
                team_id: Some("team".to_string()),
                agent_id: Some(agent_id.to_string()),
            },
            expected_revision: None,
            action,
        }
    }

    /// A Task whose Execute dispatch has already written the fence.
    fn dispatched() -> AgenticProgramProjection {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert("task".to_string(), task());
        apply_task_attempt_dispatch(
            &mut projection,
            &TaskAttemptDispatchInput {
                task_ref: "task".to_string(),
                execution_id: "execute-graph".to_string(),
                agent_ref: "agent".to_string(),
                membership_id: "membership:agent:team".to_string(),
                mode: AgentAttemptMode::Execute,
                generation: 0,
            },
            1_000,
        );
        projection
    }

    fn attempt_fail(reason: &str, retryable: bool) -> TaskAttemptFailInput {
        TaskAttemptFailInput {
            task_ref: "task".to_string(),
            execution_id: "execute-graph".to_string(),
            mode: AgentAttemptMode::Execute,
            reason: reason.to_string(),
            retryable,
        }
    }

    /// The generation equation in one named place: the fence belongs to the
    /// attempt that holds it, one generation ahead of it.  A live pair is the
    /// claim's *own* attempt - a Task whose submitter already submitted has
    /// no live attempt for its claim, and the equation then applies to nobody
    /// else's attempt either.
    fn claim_generation_matches_its_attempt(task: &AgenticTaskProjection) -> Option<bool> {
        let attempt = task
            .active_attempts
            .get(task.claim_execution_id.as_deref()?)?;
        Some(attempt.generation + 1 == task.claim_generation)
    }

    /// Path 1 - a failed attempt takes the fence with it and returns the Task
    /// to the market.
    #[test]
    fn a_failed_attempt_leaves_no_claim_behind() {
        let mut projection = dispatched();
        apply_task_attempt_fail(&mut projection, &attempt_fail("the worker died", true));
        let task = &projection.tasks["task"];
        assert!(task.active_attempts.is_empty());
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
        assert_eq!(task.status, AgenticTaskStatus::Rework);
    }

    /// Path 2 - an explicit release ends the fence and hands the Task back.
    #[test]
    fn a_released_task_carries_no_claim() {
        let mut projection = dispatched();
        let input = TaskReleaseInput {
            task_ref: "task".to_string(),
            reason: "the work is not needed".to_string(),
            expected_claim_generation: None,
        };
        apply_task_release(
            &mut projection,
            &envelope(
                "execute-graph",
                "agent",
                AgentAction::TaskRelease(input.clone()),
            ),
            &input,
        );
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Published);
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
        assert_eq!(task.claim_renewal_streak, 0);
        assert!(task.active_attempts.is_empty());
    }

    /// Path 3 - an accepted Task keeps the claim that produced it.  The
    /// acceptance ends the work, not the fence: the claim still names the
    /// execution that wrote the deliverable.
    #[test]
    fn an_accepted_task_keeps_the_claim_that_produced_it() {
        let mut projection = dispatched();
        let submit = TaskSubmitInput {
            task_ref: "task".to_string(),
            artifact_refs: vec!["artifact:deliverable".to_string()],
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            deliverable: None,
        };
        apply_task_submit(
            &mut projection,
            &envelope(
                "execute-graph",
                "agent",
                AgentAction::TaskSubmit(submit.clone()),
            ),
            &submit,
        );
        let review = TaskReviewInput {
            task_ref: "task".to_string(),
            decision: TaskReviewDecision::Accept,
            reason: "the evidence is there".to_string(),
            operator_requirement: None,
            evidence_refs: Vec::new(),
        gaps: Vec::new(),
        directed_actions: Vec::new(),
        };
        apply_task_review(
            &mut projection,
            &envelope(
                "review-graph",
                "reviewer",
                AgentAction::TaskReview(review.clone()),
            ),
            &review,
        );
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Accepted);
        assert_eq!(task.claimant.as_deref(), Some("agent"));
        assert_eq!(task.claim_execution_id.as_deref(), Some("execute-graph"));
        assert_eq!(task.claimed_at_ms, Some(1_000));
        assert_eq!(task.lease_expires_at_ms, Some(1_000 + claim_lease_ms()));
    }

    /// Path 5 - a withdrawal is two stages: the cancellation is the
    /// intermediate state, the retirement is the end of it.  The claim is
    /// released when the Task is actually retired.
    #[test]
    fn a_withdrawn_task_ends_retired_with_no_claim() {
        let mut projection = dispatched();
        let input = TaskWithdrawInput {
            task_ref: "task".to_string(),
            reason_ref: "artifact://withdrawal-reason".to_string(),
            evidence_refs: Vec::new(),
        };
        apply_task_withdraw(
            &mut projection,
            &envelope(
                "execute-graph",
                "agent",
                AgentAction::TaskWithdraw(input.clone()),
            ),
            &input,
        );
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::CancelRequested);
        assert_eq!(
            task.pending_retirement,
            Some(AgenticTaskRetirement::Withdrawn)
        );
        apply_task_attempt_fail(&mut projection, &attempt_fail("cancelled", false));
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Withdrawn);
        assert_eq!(task.pending_retirement, None);
        assert!(task.active_attempts.is_empty());
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
    }

    /// Path 7 - a supersession is the same two stages as a withdrawal.
    #[test]
    fn a_superseded_task_ends_retired_with_no_claim() {
        let mut projection = dispatched();
        let input = TaskSupersedeInput {
            task_ref: "task".to_string(),
            replacement_task_refs: vec!["task:next".to_string()],
            reason: "the plan changed".to_string(),
            evidence_refs: Vec::new(),
        };
        apply_task_supersede(
            &mut projection,
            &envelope(
                "execute-graph",
                "agent",
                AgentAction::TaskSupersede(input.clone()),
            ),
            &input,
        );
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::CancelRequested);
        assert_eq!(
            task.pending_retirement,
            Some(AgenticTaskRetirement::Superseded)
        );
        apply_task_attempt_fail(&mut projection, &attempt_fail("cancelled", false));
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Superseded);
        assert_eq!(task.pending_retirement, None);
        assert!(task.active_attempts.is_empty());
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
    }

    /// B9 - the residual claim of an accepted Task is load-bearing, not
    /// decoration.  The artifact was stamped with the claim that was live
    /// when it was published, and the publication provenance gate reads the
    /// Task's claim against the artifact's: clearing the claim on acceptance
    /// would orphan every deliverable published under it.
    #[test]
    fn the_accepted_residual_claim_is_the_one_the_artifact_was_stamped_with() {
        let mut projection = dispatched();
        let submit = TaskSubmitInput {
            task_ref: "task".to_string(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            deliverable: Some(TaskOneShotDeliverable {
                block_index: 0,
                kind: "report".to_string(),
                title: "report".to_string(),
                resolved_content_ref: Some("artifact://content".to_string()),
            }),
        };
        apply_task_submit(
            &mut projection,
            &envelope(
                "execute-graph",
                "agent",
                AgentAction::TaskSubmit(submit.clone()),
            ),
            &submit,
        );
        let artifact_ref = projection.tasks["task"]
            .artifact_refs
            .first()
            .cloned()
            .expect("the submitted artifact");
        let review = TaskReviewInput {
            task_ref: "task".to_string(),
            decision: TaskReviewDecision::Accept,
            reason: "the evidence is there".to_string(),
            operator_requirement: None,
            evidence_refs: Vec::new(),
        gaps: Vec::new(),
        directed_actions: Vec::new(),
        };
        apply_task_review(
            &mut projection,
            &envelope(
                "review-graph",
                "reviewer",
                AgentAction::TaskReview(review.clone()),
            ),
            &review,
        );
        let task = &projection.tasks["task"];
        let artifact = &projection.artifacts[&artifact_ref];
        assert_eq!(task.status, AgenticTaskStatus::Accepted);
        assert_eq!(task.claimant.as_deref(), Some("agent"));
        assert_eq!(task.claim_execution_id.as_deref(), Some("execute-graph"));
        assert_eq!(artifact.claim_execution_id.as_deref(), Some("execute-graph"));
        assert_eq!(artifact.claim_generation, Some(task.claim_generation));
        assert_eq!(task.claimed_at_ms, Some(1_000));
        let lease = task.lease_expires_at_ms.expect("the residual lease");
        assert!(lease > 1_000 && lease < u64::MAX, "the lease is finite");
        // The submit removed the submitter's attempt, so the claim has no
        // live attempt of its own: the equation applies to nobody.
        assert_eq!(claim_generation_matches_its_attempt(task), None);
        // Nothing holds the accepted Task open.
        assert_eq!(task.pending_retirement, None);
        assert_eq!(task.cancel_requested_by, None);
        assert!(task.active_attempts.is_empty());
    }

    /// B5 - a review dispatch must not put a second name on the work.  The
    /// Task keeps the claim that names the executor, the artifact keeps the
    /// same execution, and the reviewer's attempt does not take either over.
    #[test]
    fn a_review_dispatch_leaves_the_artifact_and_the_task_naming_the_same_execution() {
        let mut projection = dispatched();
        let submit = TaskSubmitInput {
            task_ref: "task".to_string(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            deliverable: Some(TaskOneShotDeliverable {
                block_index: 0,
                kind: "report".to_string(),
                title: "report".to_string(),
                resolved_content_ref: Some("artifact://content".to_string()),
            }),
        };
        apply_task_submit(
            &mut projection,
            &envelope(
                "execute-graph",
                "agent",
                AgentAction::TaskSubmit(submit.clone()),
            ),
            &submit,
        );
        let artifact_ref = projection.tasks["task"]
            .artifact_refs
            .first()
            .cloned()
            .expect("the submitted artifact");
        apply_task_attempt_dispatch(
            &mut projection,
            &TaskAttemptDispatchInput {
                task_ref: "task".to_string(),
                execution_id: "review-graph".to_string(),
                agent_ref: "reviewer".to_string(),
                membership_id: "membership:reviewer:team".to_string(),
                mode: AgentAttemptMode::Review,
                generation: 0,
            },
            2_000,
        );
        let task = &projection.tasks["task"];
        let artifact = &projection.artifacts[&artifact_ref];
        assert_eq!(task.claimant.as_deref(), Some("agent"));
        assert_eq!(task.claim_execution_id.as_deref(), Some("execute-graph"));
        assert_eq!(artifact.claim_execution_id.as_deref(), Some("execute-graph"));
        assert_eq!(artifact.claim_generation, Some(task.claim_generation));
        // The reviewer's attempt is live; the claim's own attempt is not, so
        // the equation has no pair to hold.
        assert_eq!(claim_generation_matches_its_attempt(task), None);
        assert_eq!(
            task.active_attempts.get("review-graph").map(|a| a.mode),
            Some(AgentAttemptMode::Review)
        );
    }

    /// The dispatched executor that does not take the Task must not leave a
    /// claim hanging.  Whatever it said on the topic - including a decline it
    /// published through the message channel - the executor's exit settles
    /// the attempt, the fence goes with it, and the Task is back in the
    /// market instead of held by a claim nobody can renew.
    #[test]
    fn a_dispatched_executor_that_leaves_leaves_no_hanging_claim() {
        let mut projection = dispatched();
        let decline = MessagePublishInput {
            topic_ref: "topic:program".to_string(),
            summary: Some("this task does not fit my role".to_string()),
            content_ref: None,
            refs: vec!["task".to_string()],
            recipients: Vec::new(),
            intent: Some(TaskIntent {
                task_ref: "task".to_string(),
                kind: TaskIntentKind::Decline,
                reason_ref: None,
                requested_capability_refs: Vec::new(),
            }),
            issue_dispositions: Vec::new(),
        };
        let revision = projection.revision + 1;
        super::super::topic::apply_message_publish(
            &mut projection,
            &envelope(
                "execute-graph",
                "agent",
                AgentAction::MessagePublish(decline.clone()),
            ),
            &decline,
            Some("topic:program:1"),
            revision,
        );
        apply_task_attempt_fail(&mut projection, &attempt_fail("the executor left", false));
        let task = &projection.tasks["task"];
        assert!(task.active_attempts.is_empty());
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
        assert_eq!(task.status, AgenticTaskStatus::Published);
        assert_eq!(task.pending_retirement, None);
    }

    /// Path 6 - the stranded-attempt sweep settles a lapsed attempt through
    /// the same failure action, so a lease that already ran out does not keep
    /// the claim alive: the sweep does not clear the claim itself, the
    /// settlement it emits does.
    #[test]
    fn an_expired_lease_does_not_keep_the_claim_alive() {
        let mut projection = dispatched();
        projection
            .tasks
            .get_mut("task")
            .expect("task")
            .lease_expires_at_ms = Some(500);
        apply_task_attempt_fail(
            &mut projection,
            &attempt_fail("startup recovered a terminal Agent graph", false),
        );
        let task = &projection.tasks["task"];
        assert!(task.active_attempts.is_empty());
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
    }
}

/// The dispatch decision already named the Agent and the physical graph that
/// will run the Task, so it is the one writer of the Task's claim fence.  A
/// second writer - the Agent's own `task_claim` - would let the admission be
/// re-derived from a different ledger than the dispatch that produced it.
#[cfg(test)]
mod dispatch_fence_tests {
    use super::*;
    use harness_contract::agent_action::{AgentAction, AgentAttemptMode, TaskClaimInput};

    fn published_task() -> AgenticTaskProjection {
        AgenticTaskProjection {
            task_id: "task".to_string(),
            team_id: "team".to_string(),
            title: "task".to_string(),
            objective: "objective".to_string(),
            acceptance: "durable evidence".to_string(),
            acceptance_checks: Vec::new(),
            criterion_source: Default::default(),
            obligations: Vec::new(),
            rework_directives: Vec::new(),
            unsettled_released_attempts: Vec::new(),
            terminal_note: None,
            required_capabilities: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
            depends_on: Vec::new(),
            status: AgenticTaskStatus::Published,
            claimant: None,
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
            reviewed_by: None,
            failed_attempts: 0,
            review_generation: 0,
            failed_review_attempts: 0,
            structural_execution_stalls: 0,
            structural_review_stalls: 0,
            reviews_applied: 0,
            review_obligation: None,
            reviews_accepted: 0,
            last_failure: None,
            replacement_task_refs: Vec::new(),
            supersede_evidence_refs: Vec::new(),
            superseded_reason: None,
            superseded_by: None,
            cancel_requested_by: None,
            cancel_reason_ref: None,
            cancel_evidence_refs: Vec::new(),
            pending_retirement: None,
        }
    }

    fn claim_envelope(execution_id: &str) -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: format!("claim:{execution_id}"),
            actor: harness_contract::agent_action::AgentActorBinding {
                original_execution: None,
                objective_id: "objective".to_string(),
                program_id: "program".to_string(),
                session_id: "session".to_string(),
                turn_id: "turn".to_string(),
                root_execution_id: Some("root".to_string()),
                required_team_count: 1,
                exact_team_count: None,
                objective_summary: "long-running task".to_string(),
                model_lease: "model".to_string(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: "agent".to_string(),
                kind: harness_contract::agent_action::AgentActorKind::Agent,
                execution_id: Some(execution_id.to_string()),
                team_id: Some("team".to_string()),
                agent_id: Some("agent".to_string()),
            },
            expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput {
                task_ref: "task".to_string(),
                reason: None,
            }),
        }
    }

    fn dispatch(mode: AgentAttemptMode, execution_id: &str, generation: u64) -> TaskAttemptDispatchInput {
        TaskAttemptDispatchInput {
            task_ref: "task".to_string(),
            execution_id: execution_id.to_string(),
            agent_ref: "agent".to_string(),
            membership_id: "membership:agent:team".to_string(),
            mode,
            generation,
        }
    }

    /// The five fields of the fence, and the generation equation the admission
    /// authority checks: the fence belongs to the attempt that was admitted.
    #[test]
    fn the_execute_dispatch_writes_the_claim_fence() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert("task".to_string(), published_task());
        apply_task_attempt_dispatch(
            &mut projection,
            &dispatch(AgentAttemptMode::Execute, "execute-graph", 4),
            1_000,
        );
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Claimed);
        assert_eq!(task.claimant.as_deref(), Some("agent"));
        assert_eq!(task.claim_execution_id.as_deref(), Some("execute-graph"));
        assert_eq!(task.claim_generation, 5);
        assert_eq!(task.claimed_at_ms, Some(1_000));
        assert_eq!(task.lease_expires_at_ms, Some(1_000 + claim_lease_ms()));
        let attempt = task
            .active_attempts
            .get("execute-graph")
            .expect("the dispatched attempt");
        assert_eq!(attempt.generation + 1, task.claim_generation);
        assert_eq!(attempt.mode, AgentAttemptMode::Execute);
    }

    /// The writer stays single.  The Agent's own `task_claim` after the
    /// dispatch is a renewal of that fence, not a second claim: it cannot move
    /// the generation, so the equation that binds the fence to its attempt
    /// survives it, and the only thing it may change is how long the fence
    /// holds.
    #[test]
    fn the_agents_own_claim_after_the_dispatch_is_a_renewal() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert("task".to_string(), published_task());
        apply_task_attempt_dispatch(
            &mut projection,
            &dispatch(AgentAttemptMode::Execute, "execute-graph", 4),
            1_000,
        );
        let envelope = claim_envelope("execute-graph");
        let input = match &envelope.action {
            AgentAction::TaskClaim(input) => input,
            _ => unreachable!(),
        };
        apply_task_claim(&mut projection, &envelope, input, 2_000);
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Claimed);
        assert_eq!(task.claim_generation, 5);
        assert_eq!(task.claimant.as_deref(), Some("agent"));
        assert_eq!(task.claim_execution_id.as_deref(), Some("execute-graph"));
        assert_eq!(task.lease_expires_at_ms, Some(2_000 + claim_lease_ms()));
        assert_eq!(task.claim_renewal_streak, 1);
    }

    /// Only the Execute dispatch writes a claim.  A reviewer has no fence of
    /// its own: its reads keep going down the review path, and the Task keeps
    /// the one claim that names the execution that will do the work.
    #[test]
    fn a_review_dispatch_writes_no_claim() {
        let mut projection = AgenticProgramProjection::empty("program", "objective");
        projection.tasks.insert("task".to_string(), published_task());
        apply_task_attempt_dispatch(
            &mut projection,
            &dispatch(AgentAttemptMode::Review, "review-graph", 0),
            1_000,
        );
        let task = &projection.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Published);
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claim_generation, 0);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
        assert_eq!(
            task.active_attempts.get("review-graph").map(|a| a.mode),
            Some(AgentAttemptMode::Review)
        );
    }
}

#[cfg(test)]
mod criterion_source_tests {
    use super::*;
    use harness_contract::agent_action::AgentAction;

    fn publish_input(acceptance: &str) -> TaskPublishInput {
        TaskPublishInput {
            team_ref: "team:1".to_string(),
            title: "Research".to_string(),
            objective: "Read source".to_string(),
            acceptance: acceptance.to_string(),
            acceptance_checks: Vec::new(),
            criterion_source: Default::default(),
            required_capabilities: vec!["read".to_string()],
            depends_on: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }
    }

    fn program_with_objective(objective: &str) -> AgenticProgramProjection {
        let mut program = AgenticProgramProjection::empty("program-1", "objective-1");
        program.objective_summary = objective.to_string();
        program
    }

    #[test]
    fn r1_review_projection_replay_never_clears_runtime_breakers() {
        use crate::repetition_guard::*;

        let mut envelope = envelope();
        envelope.actor.session_id = "r1-projection-replay-session".into();
        envelope.actor.turn_id = "r1-projection-replay-turn".into();
        let store = crate::RuntimeEventStore::for_test();
        let scoped = store.repetition_scope(&envelope.actor.session_id, &envelope.actor.turn_id);
        let unscoped = RepetitionScope::turn(&envelope.actor.session_id, &envelope.actor.turn_id);
        for scope in [&scoped, &unscoped] {
            for _ in 0..repetition_bound() {
                record_tool_failure(scope, "read_file", "input", &[],
                    crate::tool_invocation::ToolFailureKind::ExecutionError,
                    "graph run:abc123 is missing");
            }
        }
        let mut program = program_with_objective("Read source");
        apply_task_publish(&mut program, &envelope, &publish_input("source evidence"), Some("task"));
        let review = TaskReviewInput {
            task_ref: "task".into(),
            decision: TaskReviewDecision::Accept,
            reason: "source evidence".into(),
            operator_requirement: None,
            evidence_refs: Vec::new(),
        gaps: Vec::new(),
        directed_actions: Vec::new(),
        };
        for _ in 0..2 {
            apply_task_review(&mut program, &envelope, &review);
            assert_eq!(program.tasks["task"].status, AgenticTaskStatus::Accepted);
            for scope in [&scoped, &unscoped] {
                assert!(cause_blocked_for_tool(scope, "read_file"));
            }
        }
        clear_turn(&scoped);
        clear_turn(&unscoped);
    }

    /// The operator asked for one verifiable fact per file. A Task that also
    /// demands line references and quoted source has added a standard, and the
    /// author will never say so - a live run proved the declared field stays at
    /// its default while the reviewer enforces the addition line by line.
    #[test]
    fn an_invented_form_requirement_is_derived_as_agent_invented() {
        let program = program_with_objective(
            "Read crates/runtime/src/lib.rs, crates/tools/src/lib.rs and crates/memory/src/lib.rs. \
             Give one verifiable fact per file (file path + fact).",
        );
        let input =
            publish_input("must include exact file path, line reference, and quoted source text");
        assert_eq!(
            derived_criterion_source(&program, &input),
            harness_contract::agent_action::CriterionSource::AgentInvented
        );
        assert_eq!(
            invented_form_marker(&program, &input),
            Some("line reference")
        );
    }

    /// An operator who asked for the form gets it: the marker is in the
    /// operator's own text, so requiring it is not an invention.
    #[test]
    fn an_operator_stated_form_requirement_stays_operator_derived() {
        let program = program_with_objective(
            "Read crates/runtime/src/lib.rs. Give one verifiable fact per file, \
             including exact file path, line reference, and quoted source text.",
        );
        let input =
            publish_input("must include exact file path, line reference, and quoted source text");
        assert_eq!(
            derived_criterion_source(&program, &input),
            harness_contract::agent_action::CriterionSource::OperatorDerived
        );
        assert_eq!(invented_form_marker(&program, &input), None);
    }

    /// An objective that cannot be anchored must not be second-guessed: with no
    /// anchors there is no basis for calling anything invented.
    #[test]
    fn an_objective_without_anchors_fails_open() {
        let program = program_with_objective("");
        let input = publish_input("must include exact file path, line reference");
        assert_eq!(
            derived_criterion_source(&program, &input),
            harness_contract::agent_action::CriterionSource::OperatorDerived
        );
        assert_eq!(invented_form_marker(&program, &input), None);
    }

    /// A declared value is still believed: a model that reports its own standard
    /// as invented is not overridden by the derivation.
    #[test]
    fn a_declared_invented_source_is_respected() {
        let program = program_with_objective("Read crates/runtime/src/lib.rs.");
        let mut input = publish_input("one verifiable fact");
        input.criterion_source = harness_contract::agent_action::CriterionSource::AgentInvented;
        assert_eq!(
            derived_criterion_source(&program, &input),
            harness_contract::agent_action::CriterionSource::AgentInvented
        );
    }

    /// The T2b dead case, pinned: the operator asked for a fact per file, the
    /// Task added a form requirement, and the reviewer enforced it into a rework
    /// loop that the deep monitor had to stop.
    #[test]
    fn the_t2b_rework_loop_would_now_be_labelled() {
        let program = program_with_objective(
            "Read crates/runtime/src/lib.rs, crates/tools/src/lib.rs and crates/memory/src/lib.rs. \
             Give one verifiable fact per file (file path + fact), and list at least three \
             complete crates/.../*.rs paths actually read.",
        );
        let input = publish_input(
            "Each finding must include exact file path, line reference, and quoted source text.",
        );
        let mut program = program;
        let task_id = "task:t2b".to_string();
        apply_task_publish(&mut program, &envelope(), &input, Some(&task_id));
        let task = program.tasks.get(&task_id).expect("durable Task");
        assert_eq!(
            task.criterion_source,
            harness_contract::agent_action::CriterionSource::AgentInvented,
            "the added form requirement must be derived, not declared"
        );
        assert!(
            program
                .program_warnings
                .iter()
                .any(|warning| warning.contains("line reference") && warning.contains("operator")),
            "the label must quote the operator's requirement for both sides: {:?}",
            program.program_warnings
        );
    }

    fn envelope() -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: "publish".to_string(),
            actor: harness_contract::agent_action::AgentActorBinding {
                original_execution: None,
                objective_id: "objective-1".to_string(),
                program_id: "program-1".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                root_execution_id: Some("root-1".to_string()),
                required_team_count: 0,
                exact_team_count: None,
                objective_summary: String::new(),
                model_lease: "test".to_string(),
                permission_ceiling: None,
                resource_scopes: Vec::new(),
                actor_id: "root".to_string(),
                kind: harness_contract::agent_action::AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
            expected_revision: None,
            action: AgentAction::TaskPublish(publish_input("one verifiable fact")),
        }
    }

    // ---- 032 K1/K3 (e1) contract fixtures. On the red tree (pre-wiring)
    // these fail exactly where the old behaviour is wrong: no compiled record,
    // erased attempts, silent Blocked, dropped directives. ----

    fn k3_review(reason: &str, gaps: &[&str], actions: &[&str]) -> TaskReviewInput {
        TaskReviewInput {
            task_ref: "task".to_string(),
            decision: TaskReviewDecision::Rework,
            reason: reason.to_string(),
            operator_requirement: None,
            evidence_refs: Vec::new(),
            gaps: gaps.iter().map(|item| item.to_string()).collect(),
            directed_actions: actions.iter().map(|item| item.to_string()).collect(),
        }
    }

    #[test]
    fn k1_a_publish_compiles_the_single_criterion_record() {
        let mut program = program_with_objective("Read source");
        let mut input = publish_input("the answer quotes Cargo.toml exactly");
        input.acceptance_checks = vec!["cargo.toml sha matches the read".to_string()];
        let mut env = envelope();
        env.action = AgentAction::TaskPublish(input.clone());
        super::apply_task_publish(&mut program, &env, &input, Some("task"));
        let task = &program.tasks["task"];
        let expected: std::collections::BTreeSet<_> = std::iter::once("acceptance".to_string())
            .chain(input.acceptance_checks.iter().enumerate().map(|(index, _)| format!("check:{index}")))
            .collect();
        let mut actual = std::collections::BTreeSet::new();
        for obligation in &task.obligations {
            assert!(actual.insert(obligation.key.clone()), "each criterion is compiled once");
        }
        assert_eq!(actual, expected);
        assert_eq!(task.obligations[0].key, "acceptance");
        assert!(task.obligations[0].authoritative, "the prose stays the authoritative criterion");
        assert_eq!(task.obligations[1].key, "check:0");
        assert!(!task.obligations[1].authoritative, "checks are hints of the same record, never a second standard");
        assert_eq!(
            task.obligations[0].criterion_source, task.criterion_source,
            "publish provenance travels with every criterion (B3)"
        );
        let again = super::compile_task_obligations(
            &input.acceptance, &input.acceptance_checks, input.criterion_source,
        );
        assert_eq!(again, task.obligations, "the record is a pure function of the durable publish face");
    }

    #[test]
    fn k3_a_rework_releases_stale_attempts_as_named_unsettlement() {
        use harness_contract::agent_action::AgentAttemptMode;
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        let task = program.tasks.get_mut("task").unwrap();
        task.status = AgenticTaskStatus::Submitted;
        program.tasks.get_mut("task").unwrap().active_attempts.insert(
            "exec-old".to_string(),
            crate::agentic::program::AgenticTaskAttemptProjection {
                execution_id: "exec-old".to_string(),
                agent_id: "agent:worker".to_string(),
                membership_id: "m-1".to_string(),
                mode: AgentAttemptMode::Execute,
                generation: 0,
            },
        );
        let review = k3_review("source evidence not delivered", &[], &[]);
        let mut env = envelope();
        env.action = AgentAction::TaskReview(review.clone());
        env.action_id = "review-1".to_string();
        super::apply_task_review(&mut program, &env, &review);
        let task = &program.tasks["task"];
        assert!(task.active_attempts.is_empty());
        let released = &task.unsettled_released_attempts;
        assert_eq!(released.len(), 1, "B8: the released attempt must survive the clear as a named fact");
        assert_eq!(released[0].execution_id, "exec-old");
        assert_eq!(released[0].agent_id, "agent:worker");
        assert_eq!(released[0].generation, 0, "identity kept: no later round can launder this effect");
    }

    #[test]
    fn k3_b_each_blocked_arm_names_its_exhaustion() {
        use harness_contract::agent_action::{AgentAttemptMode, TaskAttemptFailInput};
        let fail = |attempt: u8, mode| TaskAttemptFailInput {
            task_ref: "task".to_string(),
            execution_id: format!("exec-{attempt}"),
            mode,
            reason: format!("distinct failure {attempt}"),
            retryable: true,
        };
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        program.tasks.get_mut("task").unwrap().status = AgenticTaskStatus::Claimed;
        for attempt in 1..=3 {
            super::apply_task_attempt_fail(&mut program, &fail(attempt, AgentAttemptMode::Execute));
        }
        let task = &program.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Blocked);
        let note = task.terminal_note.as_deref().expect("B10: execution exhaustion must be named");
        assert!(note.contains("execution attempts exhausted at bounded 3"), "{note}");
        assert!(note.contains("distinct failure 3"), "the note names the last failure: {note}");

        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        for attempt in 1..=3 {
            super::apply_task_attempt_fail(&mut program, &fail(attempt, AgentAttemptMode::Review));
        }
        let task = &program.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Blocked);
        assert!(task.terminal_note.as_deref().expect("review exhaustion named").contains("review attempts exhausted at bounded 3"));

        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        for round in 1..=3 {
            let review = k3_review("source evidence incomplete", &[], &[]);
            let mut env = envelope();
            env.action = AgentAction::TaskReview(review.clone());
            env.action_id = format!("review-{round}");
            program.tasks.get_mut("task").unwrap().status = AgenticTaskStatus::Submitted;
            super::apply_task_review(&mut program, &env, &review);
        }
        let task = &program.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Blocked, "the third rework round must stop the loop");
        let note = task.terminal_note.as_deref().expect("rounds exhaustion named");
        assert!(note.contains("review rounds exhausted at bounded 3"), "{note}");
        assert!(!note.contains("satisfied"), "failure family only (B10): {note}");
    }

    #[test]
    fn gb3_p2_only_applied_rework_counts_as_business_round() {
        use harness_contract::agent_action::{AgentAttemptMode, TaskAttemptFailInput};
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        for attempt in 1..=2 {
            super::apply_task_attempt_fail(&mut program, &TaskAttemptFailInput {
                task_ref: "task".into(), execution_id: format!("exec-{attempt}"), mode: AgentAttemptMode::Review,
                reason: format!("proof_actual_input_extent_unsupported: distinct {attempt}"), retryable: true,
            });
        }
        assert_eq!(program.tasks["task"].review_generation, 2);
        assert_eq!(program.tasks["task"].structural_review_stalls, 2);
        for round in 1..=3 {
            let review = k3_review("source evidence incomplete", &[], &[]);
            let mut env = envelope();
            env.action = AgentAction::TaskReview(review.clone());
            env.action_id = format!("gb3-rework-{round}");
            program.tasks.get_mut("task").unwrap().status = AgenticTaskStatus::Submitted;
            super::apply_task_review(&mut program, &env, &review);
            let task = &program.tasks["task"];
            assert_eq!(task.review_generation, 2 + round);
            assert_eq!(task.status, if round == 3 { AgenticTaskStatus::Blocked } else { AgenticTaskStatus::Rework });
            assert_eq!(task.failed_review_attempts, 0);
            assert_eq!(task.structural_review_stalls, 2);
        }
    }

    #[test]
    fn gb3_p2_unavailable_history_cannot_apply_new_rework() {
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        program.applied_rework_rounds = None;
        let before = program.clone();
        let review = k3_review("source evidence incomplete", &[], &[]);
        let mut env = envelope();
        env.action = AgentAction::TaskReview(review.clone());
        assert_eq!(super::apply_task_review_with_round_rule(&mut program, &env, &review, true), Err("review_rework_history_unknown"));
        assert_eq!(program, before);
        program.applied_rework_rounds = Some(std::collections::BTreeMap::new());
        let before_missing_task = program.clone();
        assert_eq!(super::apply_task_review_with_round_rule(&mut program, &env, &review, true), Err("review_rework_history_unknown"));
        assert_eq!(program, before_missing_task);
    }

    #[test]
    fn gb3_p1_typed_cut_origin_not_reason_controls_real_failure_reducer() {
        use harness_contract::agent_action::{AgentAttemptMode, TaskAttemptFailInput};
        for trusted in [false, true] {
            let mut program = program_with_objective("Read source");
            super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
            let fail = TaskAttemptFailInput { task_ref: "task".into(), execution_id: "graph".into(),
                mode: AgentAttemptMode::Review, reason: "review_window_unknown: review cut rebuild exhausted".into(), retryable: true };
            super::apply_task_attempt_fail_with_origin(&mut program, &fail, trusted);
            let task = &program.tasks["task"];
            assert_eq!((task.failed_review_attempts, task.structural_review_stalls), if trusted { (0, 1) } else { (1, 0) });
            assert_eq!(task.review_generation, 1);
        }
    }

    #[test]
    fn gb3_real_attempt_owner_classifies_and_bounds_both_modes() {
        use harness_contract::agent_action::{AgentAttemptMode, TaskAttemptFailInput};
        let cases = [
            ("original composite child acquisitions incomplete", "composite_acquisitions_incomplete", true),
            ("proof_actual_input_extent_unsupported", "physical_extent_unsupported", true),
            ("original child receipt remains unsettled", "receipt_unsettled", true),
            ("has no canonical durable receipt", "model_refusal", false),
            ("ordinary model failure", "unclassified", false),
        ];
        for mode in [AgentAttemptMode::Execute, AgentAttemptMode::Review] {
            for (reason, family, structural) in cases {
                let mut program = program_with_objective("Read source");
                super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
                for attempt in 1..=3 {
                    let fail = TaskAttemptFailInput {
                        task_ref: "task".into(), execution_id: format!("exec-{attempt}"), mode,
                        reason: format!("{reason} (attempt {attempt})"), retryable: true,
                    };
                    super::apply_task_attempt_fail(&mut program, &fail);
                    let task = &program.tasks["task"];
                    let (model, stall) = match mode {
                        AgentAttemptMode::Execute => (task.failed_attempts, task.structural_execution_stalls),
                        AgentAttemptMode::Review => (task.failed_review_attempts, task.structural_review_stalls),
                        _ => unreachable!(),
                    };
                    assert_eq!((model, stall), if structural { (0, attempt) } else { (attempt, 0) });
                    assert_eq!(task.status == AgenticTaskStatus::Blocked, attempt == 3);
                }
                let note = program.tasks["task"].terminal_note.as_deref().expect("exhaustion note");
                assert!(note.contains(&format!("failure_family={family}")), "{note}");
                assert!(note.contains(if structural { "account=structural" } else { "account=model" }), "{note}");
                assert!(note.contains(if structural { "model_attempts_charged=0" } else { "model_attempts_charged=3" }), "{note}");
                assert!(note.contains(if structural { "structural_stalls=3" } else { "structural_stalls=0" }), "{note}");
            }
        }
    }

    /// 033 S7 (causal accounting): a structural stall is charged to the
    /// structural account, not to the model's attempt budget, and the
    /// exhaustion note names the family and both accounts so the root can
    /// read on its own face that the exhaustion was not three model
    /// failures.
    ///
    /// The old shape (033 pre-S7) was:
    ///   task_blocked: review attempts exhausted at bounded 3; last failure:
    ///   <prose>; remaining: not accepted
    /// - no family, no account, and every structural stall charged to the
    /// model.
    #[test]
    fn s7_structural_review_stalls_are_charged_to_the_structural_account() {
        use harness_contract::agent_action::{AgentAttemptMode, TaskAttemptFailInput};
        let structural = "agent action event store failed: proof_actual_input_extent_unsupported: actual read lacks an original physical extent; original child receipt remains unsettled";
        let fail = |attempt: u8, reason: String| TaskAttemptFailInput {
            task_ref: "task".to_string(),
            execution_id: format!("exec-{attempt}"),
            mode: AgentAttemptMode::Review,
            reason,
            retryable: true,
        };
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        for attempt in 1..=3 {
            super::apply_task_attempt_fail(
                &mut program,
                &fail(attempt, format!("{structural} (attempt {attempt})")),
            );
        }
        let task = &program.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Blocked);
        assert_eq!(
            task.failed_review_attempts, 0,
            "a structural stall is not the model's review budget"
        );
        assert_eq!(
            task.structural_review_stalls, 3,
            "the structural account carries the stall"
        );
        let note = task.terminal_note.as_deref().expect("the exhaustion is named");
        assert!(note.contains("review attempts exhausted at bounded 3"), "{note}");
        assert!(
            note.contains("failure_family=physical_extent_unsupported"),
            "the note names the family: {note}"
        );
        assert!(note.contains("account=structural"), "{note}");
        assert!(note.contains("model_attempts_charged=0"), "{note}");
        assert!(note.contains("structural_stalls=3"), "{note}");

        // The model's own failures stay on the model's account.
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        for attempt in 1..=3 {
            super::apply_task_attempt_fail(
                &mut program,
                &fail(attempt, format!("distinct model failure {attempt}")),
            );
        }
        let task = &program.tasks["task"];
        assert_eq!(task.failed_review_attempts, 3, "the model's failures stay on the model account");
        assert_eq!(task.structural_review_stalls, 0);
        let note = task.terminal_note.as_deref().expect("the exhaustion is named");
        assert!(note.contains("account=model"), "{note}");
        assert!(note.contains("failure_family=unclassified"), "{note}");
        assert!(note.contains("model_attempts_charged=3"), "{note}");
    }

    /// 033 S8: a submitted Task owes a review, the obligation enters the
    /// compiled record, the two review measures sit side by side, and the
    /// terminal obligation list names the family that blocked the Task.
    ///
    /// The old shape (033 pre-S8): a submission left the Task's obligation
    /// record at the publish-time acceptance criteria only, the review left
    /// no count the root could read, and a Task blocked by a structural fault
    /// read as "not verifiable" with no obligation and no family.
    #[test]
    fn s8_a_submission_owes_a_review_and_the_terminal_list_names_the_family() {
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(
            &mut program,
            &envelope(),
            &publish_input("source evidence"),
            Some("task"),
        );
        let before = program.tasks["task"].obligations.len();
        let submit = AgentAction::TaskSubmit(
            serde_json::from_value(serde_json::json!({
                "task_ref": "task",
                "artifact_refs": [],
                "evidence_refs": [],
                "unresolved": []
            }))
            .unwrap(),
        );
        let mut env = envelope();
        env.action = submit.clone();
        env.action_id = "submit-1".to_string();
        super::apply_task_submit(&mut program, &env, match &submit {
            AgentAction::TaskSubmit(input) => input,
            _ => unreachable!("submit action"),
        });
        let task = &program.tasks["task"];
        assert_eq!(
            task.obligations.len(),
            before,
            "the review duty is not part of the acceptance record: it is a duty, not a criterion"
        );
        assert!(
            task.review_obligation.is_some(),
            "the submission owes a review: {:?}",
            task.review_obligation
        );
        assert_eq!(
            task.review_obligation.as_ref().map(|o| o.key.clone()),
            Some(super::REVIEW_PENDING_OBLIGATION_KEY.to_string()),
            "the review duty is named"
        );
        // A second submission does not duplicate the duty.
        let mut env = envelope();
        env.action = submit.clone();
        env.action_id = "submit-2".to_string();
        super::apply_task_submit(&mut program, &env, match &submit {
            AgentAction::TaskSubmit(input) => input,
            _ => unreachable!("submit action"),
        });
        assert!(
            program.tasks["task"]
                .review_obligation
                .as_ref()
                .map(|o| o.key.clone())
                == Some(super::REVIEW_PENDING_OBLIGATION_KEY.to_string()),
            "the review duty is compiled once"
        );

        // The terminal list: pending while the review is open.
        let list = super::terminal_obligation_list(&program.tasks["task"]);
        assert!(!list.is_empty(), "the terminal obligation list is not empty");
        let review = list
            .iter()
            .find(|entry| entry["key"] == super::REVIEW_PENDING_OBLIGATION_KEY)
            .expect("the review duty is in the terminal list");
        assert_eq!(review["status"], "pending", "{review}");

        // A structural block names its family instead of reading as the
        // model's failure.
        let mut blocked = program.tasks["task"].clone();
        blocked.status = AgenticTaskStatus::Blocked;
        blocked.terminal_note = Some(
            "task_blocked: review attempts exhausted at bounded 3; failure_family=receipt_unsettled; account=structural; model_attempts_charged=0; structural_stalls=3; last failure: none recorded; remaining: not accepted".into(),
        );
        let list = super::terminal_obligation_list(&blocked);
        let review = list
            .iter()
            .find(|entry| entry["key"] == super::REVIEW_PENDING_OBLIGATION_KEY)
            .expect("the review obligation is in the terminal list");
        assert_eq!(review["status"], "failed_structural", "{review}");
        assert_eq!(review["failure_family"], "receipt_unsettled", "{review}");
        // 033 S8 (BLOCK-3): the structural failure is marked on the review
        // duty alone. The acceptance obligations keep their own derivation -
        // a stall that stopped the review from being judged says nothing about
        // whether the operator's criteria were met.
        for entry in list
            .iter()
            .filter(|entry| entry["key"] != super::REVIEW_PENDING_OBLIGATION_KEY)
        {
            assert_eq!(
                entry["status"], "pending",
                "an acceptance obligation keeps its own derivation: {entry}"
            );
            assert!(
                entry["failure_family"].is_null(),
                "the structural family is not projected onto an acceptance obligation: {entry}"
            );
        }
    }

    /// 033 S8: the two review measures sit side by side - every applied
    /// verdict (the old measure) and the applied verdicts that were an
    /// acceptance (the new one).
    #[test]
    fn s8_b_the_two_review_measures_are_reported_side_by_side() {
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(
            &mut program,
            &envelope(),
            &publish_input("source evidence"),
            Some("task"),
        );
        program.tasks.get_mut("task").unwrap().status = AgenticTaskStatus::Submitted;
        let mut env = envelope();
        env.action = AgentAction::TaskReview(k3_review("source evidence incomplete", &[], &[]));
        env.action_id = "review-rework".to_string();
        super::apply_task_review(&mut program, &env, match &env.action {
            AgentAction::TaskReview(input) => input,
            _ => unreachable!("review action"),
        });
        let task = &program.tasks["task"];
        assert_eq!(task.reviews_applied, 1, "every applied verdict is counted");
        assert_eq!(
            task.reviews_accepted, 0,
            "a rework is not an acceptance, and the old measure still counts it"
        );
    }

    #[test]
    fn k3_c_directives_are_durable_and_dispatch_inert() {
        let mut program = program_with_objective("Read source");
        super::apply_task_publish(&mut program, &envelope(), &publish_input("source evidence"), Some("task"));
        let task = program.tasks.get_mut("task").unwrap();
        task.status = AgenticTaskStatus::Submitted;
        let review = k3_review("source evidence misses the quote", &["quote section absent"], &["re-read Cargo.toml and quote line 12"]);
        let mut env = envelope();
        env.action = AgentAction::TaskReview(review.clone());
        env.action_id = "review-1".to_string();
        let action_id = env.action_id.clone();
        super::apply_task_review(&mut program, &env, &review);
        let task = &program.tasks["task"];
        assert_eq!(task.rework_directives.len(), 1, "K3: the gaps channel must be durable");
        assert_eq!(task.rework_directives[0].source_action_id, action_id);
        assert_eq!(task.rework_directives[0].gaps, vec!["quote section absent".to_string()]);
        assert_eq!(
            task.rework_directives[0].directed_actions,
            vec!["re-read Cargo.toml and quote line 12".to_string()]
        );
        assert!(task.claimant.is_none() && task.active_attempts.is_empty(),
            "a directive names next work; it never dispatches or claims by itself");
        // A verdict without directives leaves the channel untouched (default
        // payloads keep their exact old behaviour).
        let plain = k3_review("source evidence accepted on rerun", &[], &[]);
        program.tasks.get_mut("task").unwrap().status = AgenticTaskStatus::Submitted;
        let mut env = envelope();
        env.action = AgentAction::TaskReview(plain.clone());
        env.action_id = "review-2".to_string();
        super::apply_task_review(&mut program, &env, &plain);
        assert_eq!(program.tasks["task"].rework_directives.len(), 1, "no gaps, no new directive");
    }

}
