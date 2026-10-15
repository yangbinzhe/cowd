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
pub const TASK_AUTHORING_DISCIPLINE: &str = "Acceptance criteria must restate the operator stated requirements and nothing more: do not invent extra rungs (no line-level hazard scans, no API summaries, no structure write-ups the objective never asked for). A reviewer that finds the operator stated requirement met must accept.";

/// Reviewer scope discipline: rework only for missing operator-required
/// deliverables, never for self-invented standards.
pub const REVIEWER_SCOPE_DISCIPLINE: &str = "For rework or challenge, quote the exact relevant original operator wording in operator_requirement; copy it from the Goal objective in state_inspect (or the frozen Program objective when no Goal exists). Explain the missing deliverable in reason. Scope discipline: the criteria operationalize the Task objective; rework is permitted only when the submission lacks a deliverable the objective itself requires. Never reject on self-invented standards - if the objective asks for one verifiable fact, do not demand API surveys, line-level hazard scans or structure write-ups; record such observations as notes or post-accept hints instead.";

/// Evidence economy for every actor: reads are durable receipts, not
/// conversations; batch independent work; prefer ranged reads.
pub const EVIDENCE_ECONOMY: &str = "Never re-read a file an accepted Task already read: open its durable receipt with evidence_retrieve (same bytes, hash-verified). Batch every independent read/search into one response or one tool_orchestrate program; prefer ranged reads over whole files.";

pub const OBJECTIVE_REVIEW_SCOPE_DISCIPLINE: &str = "Preserve accepted work and do not repeat Task reviews: an Objective review settles the open criterion only. The reviewer must be distinct from the Runtime-resolved producer of every result it reviews.";

pub const TASK_REPORT_SUBMISSION_CONTRACT: &str = "For an ordinary Task report, prefer task_submit.deliverable with non-empty normal response Text or the latest saved report from this same Task, Agent, claim and attempt. It is valid to write normal report Text before a subsequent submit call; Text alone is not Task completion. Cite real tool:// or artifact:// observations, never action receipt: IDs. Use artifact_commit for existing file or multipart artifacts. Explicit current_message_block:<index> selects only a Text block of the current response; out-of-range and empty blocks stay rejected. Never submit hidden reasoning or unrelated content.";
