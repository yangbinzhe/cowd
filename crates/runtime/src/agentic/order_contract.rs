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
pub const REVIEWER_SCOPE_DISCIPLINE: &str = "Scope discipline: the criteria operationalize the Task objective; rework is permitted only when the submission lacks a deliverable the objective itself requires. Never reject on self-invented standards - if the objective asks for one verifiable fact, do not demand API surveys, line-level hazard scans or structure write-ups; record such observations as notes or post-accept hints instead.";

/// Evidence economy for every actor: reads are durable receipts, not
/// conversations; batch independent work; prefer ranged reads.
pub const EVIDENCE_ECONOMY: &str = "Never re-read a file an accepted Task already read: open its durable receipt with evidence_retrieve (same bytes, hash-verified). Batch every independent read/search into one response or one tool_orchestrate program; prefer ranged reads over whole files.";

/// Reading disciplines the operator states alongside a read requirement.
///
/// An operator objective routinely carries two instructions that pull in opposite
/// directions: "list at least three complete source paths you actually read" and
/// "tool output may be truncated; that is normal, do not re-read whole files, keep
/// going". A reviewer that reads the first as "every file must be read in full"
/// will send back work that satisfied the second. Run one20 did exactly this: a
/// report citing three paths, one of them read as lines 1-1000 of 3084, was
/// reworked for not being "complete".
///
/// These phrases name a reading the operator explicitly ruled out.
///
/// They are enumerated rather than interpreted for the same reason
/// `INVENTED_FORM_MARKERS` is: a marker list catches the common case and fails
/// open on everything else, which costs a return to today's behaviour instead of
/// a wrong refusal. English-only would be too narrow - the run that motivated
/// this wrote its reason in Chinese - so the structural signal below carries the
/// cross-language case.
pub const OPERATOR_READING_DISCIPLINES: &[(&str, &str)] = &[
    (
        "not completely read",
        "the operator states tool output may be truncated and that truncation is normal, not a defect",
    ),
    (
        "not a complete read",
        "the operator states tool output may be truncated and that truncation is normal, not a defect",
    ),
    (
        "did not read the whole",
        "the operator forbids re-reading whole files and prefers ranged reads",
    ),
    (
        "must read the whole",
        "the operator forbids re-reading whole files and prefers ranged reads",
    ),
    (
        "re-read the whole",
        "the operator forbids re-reading whole files and prefers ranged reads",
    ),
    (
        "complete file read",
        "the operator prefers ranged reads over whole files",
    ),
    (
        "整文件",
        "the operator prefers ranged reads over whole files",
    ),
    (
        "未完整读取",
        "the operator states tool output may be truncated and that truncation is normal, not a defect",
    ),
    (
        "非完整读取",
        "the operator states tool output may be truncated and that truncation is normal, not a defect",
    ),
];

/// The structural signal: a reviewer counted a partial read against a total and
/// called the shortfall a defect - "lines 1-1000 of 3084", "前1000行（共3084行）",
/// "1000/3084 lines". Comparing a ranged read to a whole file is precisely the
/// reading the operator ruled out, and the pattern survives rewording and
/// translation because it is arithmetic, not prose.
fn contradicted_by_partial_read_arithmetic(reason: &str) -> bool {
    // Two numbers with a separator between them, where the second is larger and
    // both are line counts. Anchored on a line-count word so a ratio like "3/5
    // tasks" is not mistaken for a read.
    let lowered = reason.to_ascii_lowercase();
    let line_word = ["行", "line"];
    if !line_word.iter().any(|word| lowered.contains(word)) {
        return false;
    }
    let numbers = lowered
        .split(|ch: char| !ch.is_ascii_digit())
        .filter_map(|part| part.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    numbers.windows(2).any(|pair| pair[0] < pair[1])
}

/// The operator reading discipline a reviewer's rework reason contradicts, if any.
///
/// Returns the discipline's own words so the refusal can name it. A reason that
/// contradicts nothing returns `None` and the rework proceeds untouched - this
/// check refuses a refusal, so it must never become a way to block legitimate
/// rework.
pub fn contradicted_reading_discipline(reason: &str) -> Option<&'static str> {
    let lowered = reason.to_ascii_lowercase();
    if let Some((_, discipline)) = OPERATOR_READING_DISCIPLINES
        .iter()
        .find(|(marker, _)| lowered.contains(marker))
    {
        return Some(discipline);
    }
    if contradicted_by_partial_read_arithmetic(reason) {
        return Some(
            "the operator states tool output may be truncated and that truncation is normal, not a defect",
        );
    }
    None
}

/// Objective-review scope discipline. An Objective review settles one criterion;
/// it is not a second round of Task review, and it must come from someone who
/// did not produce what is being reviewed.
pub const OBJECTIVE_REVIEW_SCOPE_DISCIPLINE: &str = "Preserve accepted work and do not repeat Task reviews: an Objective review settles the open criterion only. The reviewer must be distinct from the Runtime-resolved producer of every result it reviews.";

#[cfg(test)]
mod tests {
    use super::*;

    /// The one20 reason, verbatim in substance: a report citing three paths was
    /// sent back because one of them was read as lines 1-1000 of 3084. The
    /// operator had asked for "at least three complete source paths actually
    /// read" and, in the same breath, that truncated output is normal and whole
    /// files must not be re-read. The stricter reading contradicts the operator.
    #[test]
    fn a_rework_demanding_whole_file_reads_contradicts_the_operator() {
        let reason = "第(1)条未满足：验收判据要求 cites at least 3 complete crates/.../*.rs \
                     source paths actually read，但提交的报告仅完整读取了2个文件；而 \
                     crates/gateway/src/api_routes/capability_contract.rs 仅读取了前1000行 \
                     （共3084行），未达到 complete 要求";
        let discipline = contradicted_reading_discipline(reason)
            .expect("a whole-file reading of an operator criterion must be caught");
        assert!(discipline.contains("truncated"), "{discipline}");
    }

    /// The check refuses a refusal, so it must fail open. A rework that names a
    /// deliverable the operator actually required is untouched, and so is any
    /// reason that happens to use none of the enumerated readings.
    #[test]
    fn a_legitimate_rework_reason_is_untouched() {
        for reason in [
            "The report never mentions state persistence at all; the operator asked for it.",
            "Missing the required artifact reference.",
            "结论与工具证据矛盾：receipt 显示未读取该文件。",
            "only partially verified the recovery path", // 'partially verified' is not a read marker
        ] {
            assert_eq!(
                contradicted_reading_discipline(reason),
                None,
                "this reason must not be refused: {reason}"
            );
        }
    }
}
