//! The single authoritative Agent-first order contract.
//!
//! Rules propagated to every actor (root, delegated executor, independent
//! reviewer, receipt hints) are defined here once and referenced from each
//! prompt-generation site. Before this module the same rule was re-worded at
//! independent sites, so a rule added at one layer never reached the others:
//! one live run inflated "one verifiable fact per file" into three invented
//! acceptance criteria, and reviewers enforced them with line-by-line re-reads
//! and rework cycles that burned 300+ steps without a completion request. A
//! rule that exists in one place cannot drift.

/// Task authoring discipline for the root: acceptance criteria restate the
/// operator stated requirements and nothing more.
pub const TASK_AUTHORING_DISCIPLINE: &str = "Acceptance criteria must restate the operator stated requirements and nothing more: do not invent extra rungs (no line-level hazard scans, no API summaries, no structure write-ups the objective never asked for). State the required outcome, not guessed tool argument shapes. A Task's artifact_refs can contain only its own claimant-bound deliverables, never peer-produced dependency artifacts. Runtime closure actions are protocol obligations, not extra user deliverable or report-format requirements; use agent_invented provenance for requirements the operator did not state. If a Task's sole outcome is an objective_review or control-plane action, rather than a new user deliverable, use purpose=exploration and record the real action; an extra delivery report would change the work manifest and invalidate that review. A reviewer that finds the operator stated requirement met must accept.";

/// Reviewer scope discipline: rework only for missing operator-required
/// deliverables, never for self-invented standards.
pub const REVIEWER_SCOPE_DISCIPLINE: &str = "For rework or challenge, quote the exact relevant original operator wording in operator_requirement; copy it from the Goal objective in state_inspect (or the frozen Program objective when no Goal exists). Explain the missing deliverable in reason. Scope discipline: the criteria operationalize the Task objective; rework is permitted only when the submission lacks a deliverable the objective itself requires. Never reject on self-invented standards - if the objective asks for one verifiable fact, do not demand API surveys, line-level hazard scans or structure write-ups; record such observations as notes or post-accept hints instead. Verify an action or state claim against actual durable Runtime receipts and state, not whether a narrative report contains command JSON. Never require simulated tool markup as proof of execution.";

pub const OBJECTIVE_REVIEW_SCOPE_DISCIPLINE: &str = "Preserve accepted work and do not repeat Task reviews: an Objective review settles the open criterion only. The reviewer must be distinct from the Runtime-resolved producer of every result it reviews. Root-authored content is not self-reviewable. After registering the real result, use an existing Team for a reviewer when possible; if none exists, the model may explicitly choose a minimal Team solely as the current production reviewer admission container. Invite a distinct read-capable Agent and publish a review-only Task with purpose=exploration naming the current criterion and exact result refs; do not redo the file work or demand a new delivery report. That Agent must claim through its real execution, read the complete current results and required effect evidence, then submit objective_review with those read receipts and a durable reason_ref. Keep the same reviewer for a retriable read/review step; after its action settles, retire the exploration execution through its existing lifecycle. This is an available route, never an automatic Team, claim, review or acceptance.";

pub const TASK_REPORT_SUBMISSION_CONTRACT: &str = "For an ordinary Task report, prefer task_submit.deliverable with non-empty normal response Text or the latest saved report from this same Task, Agent, claim and attempt, and leave artifact_refs empty. It is valid to write normal report Text before a subsequent submit call; Text alone is not Task completion. artifact_refs is only for your own deliverables bound to this Task and active claim; non-empty artifact_refs overrides deliverable. Peer-produced dependency artifacts are supporting evidence, not your new deliverable: read their retained content and cite the returned tool:// receipts or artifact:// content selectors in evidence_refs. Cite real tool:// or artifact:// observations, never action receipt: IDs. Use artifact_commit for existing file or multipart artifacts. Explicit current_message_block:<index> selects only a Text block of the current response; out-of-range and empty blocks stay rejected. Never submit hidden reasoning or unrelated content.";

pub(crate) const ARTIFACT_CONTENT_REF_CONTRACT: &str = "Select an existing deliverable body with artifact_commit.content_ref. message://current requires non-empty normal Text in this same model response. current_message_block:<zero-based-index> must select a real non-empty Text block in this response; no Text or an out-of-range block stays unresolved and rejected. For a file, read its exact bytes/hash, discover artifact_publish with tool_search if it is deferred, and use its native schema with source=file, path, sha256 and media_type. Copy the returned immutable content_ref into artifact_commit; a file hash is not an artifact selector and must never be prefixed with artifact://. A stored Session Text block may be published using source=message_block and its actual message_id, block_index and block digest. Root artifact_commit opens the existing bound Program without creating a Team or a Task. A file path is not an implicit file read. Never select tool parameters or hidden reasoning as the body. This selection does not require creating a Task report or any deliverable the operator did not request.";

pub const EXPLORATION_CLOSEOUT_CONTRACT: &str = "This Task has typed purpose=exploration. Follow its actual objective and acceptance, not a mandatory report workflow. Execute any required control action through the authorized real tool and inspect its durable result; final prose is not an action. A pure objective_review or control Task need not create an additional report artifact. Once its control work is recorded, explicitly retire your own Task through authorized task_withdraw with a truthful reason_ref and real evidence_refs, or task_release when work must remain available. Withdrawal/release is not acceptance and never satisfies an unmet user obligation. If the actual objective requires a deliverable, submit that deliverable normally. Do not create a report or publish a redundant topic message just to restate an already current objective review: new work evidence changes the reviewed manifest. Preserve unresolved work and stop honestly if authorization or evidence is missing.";

pub(crate) fn worker_terminal_action_ids(
    mode: &str,
    purpose: harness_contract::agent_action::TaskPurpose,
) -> &'static [&'static str] {
    use harness_contract::agent_action::*;
    match mode {
        "review" => &[TASK_REVIEW_TOOL_ID],
        "coordination" => &[MESSAGE_PUBLISH_TOOL_ID],
        _ if purpose == TaskPurpose::Exploration => &[
            TASK_SUBMIT_TOOL_ID, TASK_WITHDRAW_TOOL_ID, TASK_RELEASE_TOOL_ID, TASK_SUPERSEDE_TOOL_ID,
        ],
        _ => &[TASK_SUBMIT_TOOL_ID],
    }
}

pub(crate) fn worker_closeout_action_ids(
    mode: &str,
    purpose: harness_contract::agent_action::TaskPurpose,
) -> Vec<&'static str> {
    use harness_contract::agent_action::*;
    let mut actions = worker_terminal_action_ids(mode, purpose).to_vec();
    if mode == "execute" {
        actions.extend([ARTIFACT_COMMIT_TOOL_ID, "artifact_publish"]);
        if purpose == TaskPurpose::Exploration {
            actions.extend([OBJECTIVE_REVIEW_TOOL_ID, TASK_REVIEW_TOOL_ID, MESSAGE_PUBLISH_TOOL_ID]);
        }
    }
    actions
}

#[cfg(test)]
#[path = "order_contract/own823c_tests.rs"]
mod own823c_tests;
