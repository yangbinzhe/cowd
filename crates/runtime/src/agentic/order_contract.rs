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
    // `整文件` is kept because it is unambiguous: it names a whole file. The two
    // Chinese phrases that used to sit here - `未完整读取` and `非完整读取` - were
    // removed after they produced a live false positive. Both mean "did not read
    // in full", which covers a coverage complaint ("did not read all three
    // artifacts") exactly as well as a truncation complaint ("did not read the
    // whole file"). A reviewer saying the first was refused as if it had said the
    // second, and the Task's review never completed.
    (
        "整文件",
        "the operator prefers ranged reads over whole files",
    ),
];

/// The structural signal: a reviewer counted a partial read against a total and
/// called the shortfall a defect - "lines 1-1000 of 3084", "前1000行（共3084行）",
/// "1000/3084 lines". Comparing a ranged read to a whole file is precisely the
/// reading the operator ruled out, and the pattern survives rewording and
/// translation because it is arithmetic, not prose.
fn contradicted_by_partial_read_arithmetic(reason: &str) -> bool {
    // A read range compared against a total: "lines 1-1000 of 3084",
    // "前1000行（共3084行）", "read 1000 of 3084 lines". Every number attached to a
    // line-count word - on either side of it - is a line count; numbers joined by a
    // connector ("of" / "共" / "total") next to a line word count too. A conflict is
    // two of those numbers, in text order, where the first is smaller.
    //
    // Proximity is the whole point, and it is what the first version lacked. That
    // rule required only that a line word appear somewhere in the text and then
    // compared any two numbers it contained. A live reviewer wrote a reason about a
    // missing `objective_review` call whose evidence refs included an artifact id,
    // and that id's digits supplied the increasing pair - so a legitimate review was
    // refused and the Task was never reviewed. Digits that merely share a paragraph
    // with the word "line" are not arithmetic about lines.
    //
    // Documented limit, accepted deliberately: "the file is 3084 lines and only 1000
    // were read" puts the two numbers in separate clauses with no connector, so it
    // is not caught. Failing open costs a return to today's behaviour, which is the
    // right direction for a check that refuses a refusal.
    let lowered = reason.to_ascii_lowercase();
    let chars: Vec<char> = lowered.chars().collect();
    let is_separator =
        |ch: char| ch.is_whitespace() || matches!(ch, '(' | '（' | ',' | '，' | '/' | '-' | '–' | ':' | '=');

    /// The digit run ending at `from` (exclusive), walking back over separators.
    let digits_before = |from: usize| -> Option<u64> {
        let mut end = from;
        while end > 0 && is_separator(chars[end - 1]) {
            end -= 1;
        }
        let mut begin = end;
        while begin > 0 && chars[begin - 1].is_ascii_digit() && end - begin < 12 {
            begin -= 1;
        }
        if begin == end {
            return None;
        }
        chars[begin..end].iter().collect::<String>().parse::<u64>().ok()
    };

    /// The digit run starting at or after `from`, skipping up to three separators.
    let digits_after = |from: usize| -> Option<u64> {
        let mut begin = from;
        let mut separators = 0;
        while begin < chars.len() && separators <= 3 && is_separator(chars[begin]) {
            begin += 1;
            separators += 1;
        }
        let mut end = begin;
        while end < chars.len() && chars[end].is_ascii_digit() && end - begin < 12 {
            end += 1;
        }
        if begin == end {
            return None;
        }
        chars[begin..end].iter().collect::<String>().parse::<u64>().ok()
    };

    let slice = |from: usize, to: usize| -> String {
        chars[from.clamp(0, chars.len())..to.clamp(0, chars.len())]
            .iter()
            .collect()
    };
    let has_line_word = |from: usize, to: usize| slice(from, to).contains('行') || slice(from, to).contains("line");

    // (position, value), sorted by position so a range reads in the order written.
    let mut found: Vec<(usize, u64)> = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let word_end = if chars[index] == '行' {
            Some(index + 1)
        } else if slice(index, index + 4) == "line" {
            // Consume the plural 's', or "lines 1" reads the word as "line" and the
            // trailing 's' blocks the number that follows it.
            Some(if chars.get(index + 4) == Some(&'s') {
                index + 5
            } else {
                index + 4
            })
        } else {
            None
        };
        let Some(word_end) = word_end else {
            index += 1;
            continue;
        };
        if let Some(value) = digits_after(word_end) {
            found.push((word_end, value));
        }
        if let Some(value) = digits_before(index) {
            found.push((index, value));
        }
        index = word_end;
    }
    if found.len() < 2 {
        // A connector next to a line word ties the numbers on both sides of it into
        // one comparison: "read 1000 of 3084 lines", "lines 1-1000 of 3084".
        for connector in ["of", "共", "total"] {
            let mut cursor = 0;
            while let Some(offset) = lowered[cursor..].find(connector) {
                let at = cursor + offset;
                cursor = at + connector.len();
                let near_a_line_word =
                    has_line_word(at.saturating_sub(14), at) || has_line_word(at + connector.len(), at + connector.len() + 14);
                if !near_a_line_word {
                    continue;
                }
                if let Some(value) = digits_before(at) {
                    found.push((at, value));
                }
                if let Some(value) = digits_after(at + connector.len()) {
                    found.push((at + connector.len(), value));
                }
            }
        }
    }
    found.sort_by_key(|(position, _)| *position);
    found.windows(2).any(|pair| pair[0].1 < pair[1].1)
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
    /// The live false positive, verbatim. A reviewer reported that a Task's
    /// `evidence_refs` held three read receipts and one report artifact but no
    /// record of the required `objective_review` call - a coverage complaint about
    /// evidence, with nothing to do with how any file was read. The gate refused it
    /// and the Task was never reviewed.
    ///
    /// It matched because the artifact id's digits (`art_eae84979dc9649e8a514605ced41684a`)
    /// supplied an increasing pair and the text happened to contain a line word, so
    /// the old arithmetic rule read it as "read 8 of 514605 lines". Proximity is
    /// what separates arithmetic about lines from digits sharing a paragraph.
    #[test]
    fn an_evidence_coverage_complaint_is_not_a_reading_discipline_conflict() {
        let reason = "未满足的具体条款是 acceptance 第(2)条：submits one objective_review call \
                      covering all three artifacts with decision=satisfied。我实际读取到的证据缺口：该 \
                      Task 的 evidence_refs 仅包含三份 read_ref 回执与一份报告 artifact\
                      （artifact://art_eae84979dc9649e8a514605ced41684a），未记录任何 objective_review 调用。";
        assert_eq!(
            contradicted_reading_discipline(reason),
            None,
            "a complaint about missing evidence must not be refused as a reading conflict"
        );
    }

    /// The real one20 reason, which the gate exists to catch: a read range
    /// compared against a total and called a defect.
    #[test]
    fn a_read_range_compared_to_a_total_is_still_caught() {
        for reason in [
            "capability_contract.rs 仅读取了前1000行（共3084行），未达到 complete 要求",
            "the report only covers lines 1-1000 of 3084, so the read is incomplete",
            "read 1000 of 3084 lines and therefore missed the acceptance logic",
        ] {
            assert!(
                contradicted_reading_discipline(reason).is_some(),
                "a read-range-versus-total complaint must still be caught: {reason}"
            );
        }
    }

    #[test]
    fn a_legitimate_rework_reason_is_untouched() {
        for reason in [
            "The report never mentions state persistence at all; the operator asked for it.",
            "Missing the required artifact reference.",
            "结论与工具证据矛盾：receipt 显示未读取该文件。",
            "only partially verified the recovery path", // 'partially verified' is not a read marker
            "未完整读取全部三个 artifact 的 evidence_refs", // 'did not read all three' is coverage, not truncation
            "the artifact cites only 1 of the 3 required source paths",
        ] {
            assert_eq!(
                contradicted_reading_discipline(reason),
                None,
                "this reason must not be refused: {reason}"
            );
        }
    }
}
