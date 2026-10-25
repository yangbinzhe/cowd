//! PLAN Phase 4 / L1: a facet's acceptance must test the facet.
//!
//! # Why
//!
//! Both single-facet probes were declared failed by `ArchitectureQuality` - a
//! gate for a *multi-team architecture review* - even when the model had
//! answered correctly:
//!
//! * **arm E** (`live_minimal_literal_read`) answered with three verbatim source
//!   quotes in 3 rounds and was still `failed` on `architecture_quality`.
//! * **Facet B** (`live_minimal_compute_reduce`) answered 1125 files / 771040
//!   lines with cited receipts and kept the unknown unknown unresolved, again
//!   `failed` on `architecture_quality`.
//!
//! A gate that cannot pass a correct answer is not measuring anything: it turns
//! "the model did the job" into "the model failed" and makes every facet's
//! result unusable. That is the same class of defect as the wall-clock verdict -
//! the measurement, not the work.
//!
//! # What a facet acceptance is
//!
//! A facet tests ONE capability on ONE side. Its acceptance therefore states
//! exactly what that capability must have produced, and nothing else:
//!
//! * **evidence**: which durable tool receipts must exist (which paths were
//!   actually read), so "the model claims it read X" and "the model read X" are
//!   distinguishable;
//! * **answer**: what the final answer must contain, if anything;
//! * **`keep_unresolved`**: whether leaving something unresolved is acceptable.
//!   For a facet whose whole point is honest reporting, that must be true, or the
//!   gate punishes the behaviour it is trying to teach.
//!
//! It deliberately does NOT require teams, tasks, reviews or any collaboration
//! shape: a facet that needs those is not a facet, it is the medium scenario, and
//! the medium scenario has its own gate.

use std::collections::BTreeMap;

use harness_contract::{
    context::WorkspacePathIdentity,
    projection::{ReadEvidenceExtent, ReadEvidenceGrade, ReadEvidencePage, ReadEvidenceRecipient},
};
use serde_json::{json, Value};

/// What one capability facet must have produced to pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetAcceptance {
    /// Identifier of the facet, for the report.
    pub facet_id: String,
    /// Human statement of the single capability under test.
    pub capability: String,
    /// Paths that must have a durable read receipt. Empty means "not gated on
    /// reads".
    pub required_read_paths: Vec<String>,
    /// Substrings the final answer must contain. Empty means "not gated on
    /// wording".
    pub answer_contains: Vec<String>,
    /// Whether leaving part of the objective unresolved is acceptable.
    ///
    /// True for facets whose point is honest reporting. A gate that requires a
    /// complete answer from a task that cannot be completed teaches the model to
    /// invent the missing part.
    pub keep_unresolved_ok: bool,
}

/// The observable outcome of one facet run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FacetOutcome {
    /// Paths that actually have a durable read receipt.
    pub read_paths: Vec<String>,
    /// The final answer text.
    pub answer: String,
    /// Whether the model marked something unresolved.
    pub marked_unresolved: bool,
}

/// Result of gating one facet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetVerdict {
    /// Whether the facet passed.
    pub passed: bool,
    /// Per-requirement outcomes, so a failure says which requirement missed.
    pub requirements: Vec<FacetRequirement>,
}

/// One requirement and whether it held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetRequirement {
    /// What was required.
    pub requirement: String,
    /// Whether it held.
    pub passed: bool,
    /// What was observed, when it did not.
    pub observed: String,
}

impl FacetVerdict {
    /// Machine-readable report payload.
    #[must_use]
    pub fn report(&self) -> Value {
        json!({
            "passed": self.passed,
            "requirements": self
                .requirements
                .iter()
                .map(|requirement| json!({
                    "requirement": requirement.requirement,
                    "passed": requirement.passed,
                    "observed": requirement.observed,
                }))
                .collect::<Vec<_>>(),
        })
    }
}

/// Target extraction for non-read tool predicates only. Input text never
/// supplies content-read evidence.
pub(crate) fn tool_target_paths_from_input(input: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(path) = input.get("path").and_then(serde_json::Value::as_str) {
        out.push(path.to_string());
    }
    if let Some(files) = input.get("files").and_then(serde_json::Value::as_array) {
        out.extend(
            files
                .iter()
                .filter_map(|file| file.get("path").and_then(serde_json::Value::as_str))
                .map(ToString::to_string),
        );
    }
    if let Some(steps) = input.get("steps").and_then(serde_json::Value::as_array) {
        for step in steps {
            if let Some(calls) = step.get("calls").and_then(serde_json::Value::as_array) {
                for call in calls {
                    if let Some(call_input) = call.get("input") {
                        out.extend(tool_target_paths_from_input(call_input));
                    }
                }
            }
            if let Some(step_input) = step.get("input") {
                out.extend(tool_target_paths_from_input(step_input));
            }
        }
    }
    out
}

/// Caller-side pagination state, not a second read authenticator. Runtime owns
/// authentication; this state prevents mixing authenticated pages from different
/// executions, cuts, grants or storage incarnations.
pub(crate) struct ReadEvidencePages {
    execution: String,
    session: String,
    turn: String,
    expected: BTreeMap<String, WorkspacePathIdentity>,
    pages: Vec<ReadEvidencePage>,
    sources: BTreeMap<String, u64>,
    failure: Option<String>,
}

impl ReadEvidencePages {
    pub(crate) fn new(
        execution: &str,
        session: &str,
        turn: &str,
        expected: BTreeMap<String, WorkspacePathIdentity>,
    ) -> Self {
        Self {
            execution: execution.to_string(),
            session: session.to_string(),
            turn: turn.to_string(),
            expected,
            pages: Vec::new(),
            sources: BTreeMap::new(),
            failure: None,
        }
    }

    pub(crate) fn next_after_sequence(&self) -> u64 {
        self.pages.last().map_or(0, |page| page.next_after_sequence)
    }

    pub(crate) fn through(&self) -> Option<u64> {
        self.pages.first().map(|page| page.through)
    }

    pub(crate) fn fail(&mut self, reason: impl Into<String>) {
        self.failure.get_or_insert_with(|| reason.into());
    }

    pub(crate) fn accept(&mut self, page: ReadEvidencePage) -> Result<(), String> {
        let same_identity = self.pages.first().is_none_or(|first| {
            page.execution_id == first.execution_id
                && page.session_id == first.session_id
                && page.turn_id == first.turn_id
                && page.workspace_id == first.workspace_id
                && page.authorization_revision == first.authorization_revision
                && page.redaction_revision == first.redaction_revision
                && page.graph_revision == first.graph_revision
                && page.through == first.through
                && page.source_incarnation == first.source_incarnation
                && page.source_mutation_epoch == first.source_mutation_epoch
        });
        let invalid = self.failure.is_some()
            || self.pages.len() >= 256
            || self.pages.last().is_some_and(|previous| previous.exhausted)
            || page.schema_version != 1
            || page.execution_id != self.execution
            || page.session_id != self.session
            || page.turn_id != self.turn
            || page.source_incarnation.is_empty()
            || page.incomplete
            || page.reason.is_some()
            || page.reads.len() > 16
            || !same_identity
            || self
                .expected
                .values()
                .any(|path| path.workspace_id != page.workspace_id)
            || page.next_after_sequence < self.next_after_sequence()
            || ((!page.exhausted || !page.reads.is_empty())
                && page.next_after_sequence == self.next_after_sequence())
            || page.reads.iter().any(|read| {
                read.delivery.sequence <= self.next_after_sequence()
                    || read.delivery.sequence > page.next_after_sequence
            });
        if invalid {
            self.fail("read evidence identity, authentication or progress invalid");
        }
        for source in &page.sources {
            if self
                .sources
                .get(&source.stream_id)
                .is_some_and(|revision| *revision != source.revision)
            {
                self.fail("read evidence source revision changed");
            }
            self.sources
                .insert(source.stream_id.clone(), source.revision);
        }
        self.pages.push(page);
        match &self.failure {
            Some(reason) => Err(reason.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn paths(&self) -> Vec<String> {
        if self.failure.is_some() {
            return Vec::new();
        }
        self.expected
            .iter()
            .filter_map(|(name, expected)| {
                self.pages
                    .iter()
                    .any(|page| {
                        page.reads.iter().any(|read| {
                            let path = &read.path;
                            let same_path = path.workspace_id == expected.workspace_id
                                && path.repository_id == expected.repository_id
                                && path.workspace_relative_path == expected.workspace_relative_path
                                && path.repository_relative_path
                                    == expected.repository_relative_path
                                && path.object_kind == expected.object_kind;
                            // The producer has already checked the exact Native/Process
                            // recipient against its admitted packet. Root delivery must be
                            // provider-observed; a channel send never upgrades Root grade.
                            let grade_valid = match &read.recipient {
                                ReadEvidenceRecipient::Root { .. } => {
                                    read.grade == ReadEvidenceGrade::ProviderModelObserved
                                }
                                ReadEvidenceRecipient::Agent { .. } => true,
                            };
                            let extent_valid = match &read.delivered_extent {
                                Some(ReadEvidenceExtent::FileLines {
                                    start_line,
                                    line_count,
                                    total_lines,
                                    body_bytes,
                                    source_omitted,
                                    ..
                                }) => {
                                    *start_line > 0
                                        && start_line
                                            .checked_sub(1)
                                            .and_then(|start| start.checked_add(*line_count))
                                            .is_some_and(|end| end <= *total_lines)
                                        && ((*total_lines == 0
                                            && *line_count == 0
                                            && read.source_bytes == 0
                                            && *body_bytes == 0)
                                            || (*line_count > 0 && *body_bytes > 0))
                                        && !(*source_omitted
                                            && *start_line == 1
                                            && line_count == total_lines)
                                }
                                Some(ReadEvidenceExtent::ArtifactChunks {
                                    total_chunks,
                                    chunks,
                                }) => {
                                    if *total_chunks == 0 {
                                        read.source_bytes == 0 && chunks.is_empty()
                                    } else {
                                        !chunks.is_empty()
                                            && chunks.iter().all(|chunk| {
                                                chunk.index < *total_chunks && chunk.body_bytes > 0
                                            })
                                    }
                                }
                                None => false,
                            };
                            same_path
                                && read.body_delivered
                                && grade_valid
                                && extent_valid
                                && read.step.session_id == page.session_id
                                && read.step.turn_id == page.turn_id
                                && !read.call_id.is_empty()
                                && !read.invocation_id.is_empty()
                                && [&read.delivery, &read.acquisition, &read.index].iter().all(
                                    |reference| {
                                        !reference.event_id.is_empty()
                                            && !reference.stream_id.is_empty()
                                            && reference.commit_cursor <= page.through
                                    },
                                )
                        })
                    })
                    .then(|| name.clone())
            })
            .collect()
    }

    pub(crate) fn satisfied(&self) -> bool {
        !self.expected.is_empty() && self.paths().len() == self.expected.len()
    }

    pub(crate) fn exhausted(&self) -> bool {
        self.failure.is_some() || self.pages.last().is_some_and(|page| page.exhausted)
    }

    pub(crate) fn report(&self) -> Value {
        json!({
            "execution_id": self.execution,
            "session_id": self.session,
            "turn_id": self.turn,
            "pages": self.pages,
            "failure": self.failure,
            "read_paths": self.paths(),
            "obligations_satisfied": self.satisfied(),
            "history_complete": false,
            "expected": self.expected,
        })
    }
}

/// Consume saved typed pages with independently registered path identities.
/// Neither timeline previews nor projection activities participate in reads.
pub(crate) fn collect_read_paths(observation: &Value) -> Vec<String> {
    let (Some(execution), Some(session), Some(turn)) = (
        observation.get("execution_id").and_then(Value::as_str),
        observation.get("session_id").and_then(Value::as_str),
        observation.get("turn_id").and_then(Value::as_str),
    ) else {
        return Vec::new();
    };
    let Ok(expected) = serde_json::from_value::<BTreeMap<String, WorkspacePathIdentity>>(
        observation["expected"].clone(),
    ) else {
        return Vec::new();
    };
    let mut collection = ReadEvidencePages::new(execution, session, turn, expected);
    if !observation["failure"].is_null() {
        return Vec::new();
    }
    let Some(pages) = observation.get("pages").and_then(Value::as_array) else {
        return Vec::new();
    };
    for value in pages {
        let Ok(page) = serde_json::from_value(value.clone()) else {
            return Vec::new();
        };
        if collection.accept(page).is_err() {
            return Vec::new();
        }
    }
    collection.paths()
}

/// Gate one facet's outcome.
///
/// Pure: the same outcome and acceptance always produce the same verdict, which
/// is what makes a facet's result comparable across builds.
#[must_use]
pub fn evaluate_facet(acceptance: &FacetAcceptance, outcome: &FacetOutcome) -> FacetVerdict {
    let mut requirements = Vec::new();

    // (1) An answer must exist. A facet that produced nothing has not
    // demonstrated its capability, whatever the evidence says.
    requirements.push(FacetRequirement {
        requirement: "answer_present".to_string(),
        passed: !outcome.answer.trim().is_empty(),
        observed: format!("answer length {}", outcome.answer.trim().len()),
    });

    // (2) Required evidence must exist as durable receipts, not as claims.
    for path in &acceptance.required_read_paths {
        let found = outcome.read_paths.iter().any(|read| read == path);
        requirements.push(FacetRequirement {
            requirement: format!("read_receipt:{path}"),
            passed: found,
            observed: if found {
                format!("durable receipt for {path}")
            } else {
                format!(
                    "no durable receipt for {path} (have: {})",
                    outcome.read_paths.join(", ")
                )
            },
        });
    }

    // (3) The answer must contain what the facet asked for.
    for needle in &acceptance.answer_contains {
        let found = outcome.answer.contains(needle.as_str());
        requirements.push(FacetRequirement {
            requirement: format!("answer_contains:{needle}"),
            passed: found,
            observed: if found {
                "present".to_string()
            } else {
                "absent".to_string()
            },
        });
    }

    // (4) Unresolved is only a failure when the facet did not allow it.
    if !acceptance.keep_unresolved_ok && outcome.marked_unresolved {
        requirements.push(FacetRequirement {
            requirement: "no_unresolved".to_string(),
            passed: false,
            observed: "the answer marked something unresolved".to_string(),
        });
    }

    let passed = requirements.iter().all(|requirement| requirement.passed);
    FacetVerdict {
        passed,
        requirements,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acceptance() -> FacetAcceptance {
        FacetAcceptance {
            facet_id: "live_minimal_literal_read".to_string(),
            capability: "read a known file and quote it verbatim".to_string(),
            required_read_paths: vec![
                "crates/memory/src/lib.rs".to_string(),
                "crates/provider/src/lib.rs".to_string(),
            ],
            answer_contains: vec!["pub mod".to_string()],
            keep_unresolved_ok: true,
        }
    }

    fn outcome() -> FacetOutcome {
        FacetOutcome {
            read_paths: vec![
                "crates/memory/src/lib.rs".to_string(),
                "crates/provider/src/lib.rs".to_string(),
                "crates/harness-eval/src/lib.rs".to_string(),
            ],
            answer: "pub mod compression;".to_string(),
            marked_unresolved: false,
        }
    }

    /// Input parsing remains available to non-read tool predicates; it is not
    /// evidence that any input reached a reader.
    #[test]
    fn structured_tool_inputs_expose_target_names_only() {
        let input = serde_json::json!({
            "steps": [{
                "op": "batch",
                "id": "read-files",
                "calls": [
                    { "tool": "read_file", "input": { "path": "crates/tools/src/lib.rs" } },
                    { "tool": "read_file", "input": { "path": "crates/runtime/src/lib.rs" } }
                ]
            }]
        });
        let paths = tool_target_paths_from_input(&input);
        assert!(paths.contains(&"crates/tools/src/lib.rs".to_string()));
        assert!(paths.contains(&"crates/runtime/src/lib.rs".to_string()));
        assert_eq!(
            tool_target_paths_from_input(&serde_json::json!({ "path": "a.rs" })),
            vec!["a.rs".to_string()]
        );
        assert_eq!(
            tool_target_paths_from_input(&serde_json::json!({ "files": [{ "path": "b.rs" }] })),
            vec!["b.rs".to_string()]
        );
    }

    /// The arm-E failure, fixed: a correct answer with durable receipts passes.
    #[test]
    fn a_correct_answer_with_durable_receipts_passes() {
        let verdict = evaluate_facet(&acceptance(), &outcome());
        assert!(
            verdict.passed,
            "a facet that produced its evidence must pass: {}",
            verdict.report()
        );
        // answer_present + 2 read receipts + 1 answer_contains, and no
        // unresolved requirement because this facet allows it.
        assert_eq!(verdict.requirements.len(), 4);
    }

    /// A claim without a receipt is not evidence. This is the distinction the
    /// whole gate exists to keep.
    #[test]
    fn a_claimed_read_without_a_receipt_fails() {
        let mut bad = outcome();
        bad.read_paths = vec!["crates/harness-eval/src/lib.rs".to_string()];
        let verdict = evaluate_facet(&acceptance(), &bad);
        assert!(!verdict.passed);
        let failed = verdict
            .requirements
            .iter()
            .find(|requirement| !requirement.passed)
            .expect("a failure");
        assert!(failed.requirement.starts_with("read_receipt:"));
        assert!(failed.observed.contains("no durable receipt"));
    }

    /// An empty answer is not a demonstrated capability.
    #[test]
    fn an_empty_answer_fails_even_with_perfect_evidence() {
        let mut bad = outcome();
        bad.answer = "   ".to_string();
        let verdict = evaluate_facet(&acceptance(), &bad);
        assert!(!verdict.passed);
        assert_eq!(
            verdict.requirements[0].requirement, "answer_present",
            "the missing answer must be the first thing reported"
        );
    }

    /// Honest reporting is rewarded, not punished, when the facet allows it.
    #[test]
    fn an_unknown_left_unresolved_is_acceptable_when_the_facet_allows_it() {
        let mut honest = outcome();
        honest.marked_unresolved = true;
        assert!(evaluate_facet(&acceptance(), &honest).passed);
    }

    /// And punished when it does not.
    #[test]
    fn an_unknown_left_unresolved_fails_when_the_facet_forbids_it() {
        let mut strict = acceptance();
        strict.keep_unresolved_ok = false;
        let mut honest = outcome();
        honest.marked_unresolved = true;
        let verdict = evaluate_facet(&strict, &honest);
        assert!(!verdict.passed);
        assert!(verdict
            .requirements
            .iter()
            .any(|requirement| requirement.requirement == "no_unresolved"));
    }

    /// A facet gate must not smuggle in collaboration requirements: those belong
    /// to the medium scenario, and putting them here is what broke both facets.
    #[test]
    fn a_facet_gate_has_no_collaboration_requirements() {
        let verdict = evaluate_facet(&acceptance(), &outcome());
        for requirement in &verdict.requirements {
            for forbidden in ["team", "task", "review", "cross_team", "topic", "agent"] {
                assert!(
                    !requirement.requirement.contains(forbidden),
                    "a facet gate must not require collaboration: {}",
                    requirement.requirement
                );
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod read_proof_tests {
    use super::*;

    // A wire fixture for the consumer boundary, not a claim that these literal
    // fields constitute a Runtime-authenticated receipt.
    pub(crate) fn page() -> ReadEvidencePage {
        serde_json::from_value(json!({
            "schema_version":1,"execution_id":"graph","session_id":"session","turn_id":"turn",
            "workspace_id":"workspace","authorization_revision":1,"redaction_revision":"redaction",
            "graph_revision":2,"through":10,"next_after_sequence":4,"exhausted":false,
            "incomplete":false,"reason":null,"source_incarnation":"incarnation","source_mutation_epoch":0,
            "sources":[{"stream_id":"session-stream","revision":8}],
            "reads":[{
                "path":{"workspace_id":"workspace","repository_id":"repository","workspace_relative_path":"src/a.rs","repository_relative_path":"src/a.rs","object_kind":"file"},
                "scope":{"resource":"file","operation":"read","target":"src/a.rs"},
                "recipient":{"kind":"root","binding":{"root_execution_id":"graph","session_id":"session","turn_id":"turn","root_task_id":"task","task_id":"task","activity_id":"activity","revision":2,"fence":1,"generation":1}},
                "step":{"step_id":"step","session_id":"session","turn_id":"turn","execution_id":"graph","build_ref":"build"},
                "call_id":"call","tool_name":"read_file","invocation_id":"call",
                "delivery":{"stream_id":"session-stream","sequence":4,"event_id":"delivery","commit_cursor":10,"payload_digest":"delivery-digest"},
                "acquisition":{"stream_id":"primary","sequence":1,"event_id":"primary","commit_cursor":8,"payload_digest":"primary-digest"},
                "index":{"stream_id":"index","sequence":1,"event_id":"index","commit_cursor":8,"payload_digest":"index-digest"},
                "source_sha256":"source-digest","source_bytes":10,
                "delivered_extent":{"kind":"file_lines","start_line":2,"line_count":1,"total_lines":4,"ends_with_newline":true,"body_sha256":"window-digest","body_bytes":2,"source_omitted":true},
                "grade":"provider_model_observed","body_delivered":true
            }]
        })).unwrap()
    }

    fn collection() -> ReadEvidencePages {
        ReadEvidencePages::new(
            "graph",
            "session",
            "turn",
            BTreeMap::from([("src/a.rs".to_string(), page().reads[0].path.clone())]),
        )
    }

    #[test]
    fn authenticated_partial_window_satisfies_existing_content_obligation_without_history_complete()
    {
        let mut proof = collection();
        proof.accept(page()).unwrap();
        assert!(proof.satisfied());
        assert!(!proof.exhausted());
        let report = proof.report();
        assert_eq!(report["history_complete"], false);
        assert_eq!(collect_read_paths(&report), vec!["src/a.rs"]);
    }

    #[test]
    fn preview_parent_success_and_projection_path_cannot_create_read_evidence() {
        let old = json!({
            "events":[{"kind":"tool.invocation.completed","payload":{"tool_name":"tool_orchestrate","status":"completed","is_error":false,"input_preview":"src/a.rs"}}],
            "activities":[{"kind":"read","status":"completed","detail":{"path":"src/a.rs"}}]
        });
        assert!(collect_read_paths(&old).is_empty());
        let mut proof = collection();
        let mut parent_only = page();
        parent_only.reads.clear();
        proof.accept(parent_only).unwrap();
        assert!(proof.paths().is_empty());
    }

    #[test]
    fn foreign_repository_suffix_omitted_body_and_wrong_root_grade_are_not_reads() {
        for fault in ["repository", "suffix", "body", "extent", "grade", "future"] {
            let mut candidate = page();
            let read = &mut candidate.reads[0];
            match fault {
                "repository" => read.path.repository_id = "foreign".into(),
                "suffix" => read.path.workspace_relative_path = "foreign/src/a.rs".into(),
                "body" => read.body_delivered = false,
                "extent" => read.delivered_extent = None,
                "grade" => read.grade = ReadEvidenceGrade::AgentChannelSend,
                "future" => read.acquisition.commit_cursor = 11,
                _ => unreachable!(),
            }
            let mut proof = collection();
            proof.accept(candidate).unwrap();
            assert!(proof.paths().is_empty(), "{fault}");
        }
    }

    #[test]
    fn changed_cut_epoch_grant_or_stream_invalidates_previously_satisfied_pages() {
        for fault in [
            "cut",
            "epoch",
            "grant",
            "stream",
            "incarnation",
            "graph",
            "redaction",
            "incomplete",
            "progress",
        ] {
            let mut proof = collection();
            proof.accept(page()).unwrap();
            assert!(proof.satisfied());
            let mut next = page();
            next.next_after_sequence = 5;
            next.reads.clear();
            match fault {
                "cut" => next.through += 1,
                "epoch" => next.source_mutation_epoch += 1,
                "grant" => next.authorization_revision += 1,
                "stream" => next.sources[0].revision += 1,
                "incarnation" => next.source_incarnation = "replaced".into(),
                "graph" => next.graph_revision += 1,
                "redaction" => next.redaction_revision = "changed".into(),
                "incomplete" => next.incomplete = true,
                "progress" => next.next_after_sequence = 4,
                _ => unreachable!(),
            }
            assert!(proof.accept(next).is_err(), "{fault}");
            assert!(proof.paths().is_empty(), "{fault}");
            assert!(collect_read_paths(&proof.report()).is_empty(), "{fault}");
        }
    }

    #[test]
    fn unrelated_history_advances_without_skipping_obligation_and_page_cap_is_finite() {
        let mut proof = collection();
        let mut empty = page();
        empty.reads.clear();
        for sequence in 1..=255 {
            empty.next_after_sequence = sequence;
            proof.accept(empty.clone()).unwrap();
        }
        let mut last = page();
        last.next_after_sequence = 256;
        last.reads[0].delivery.sequence = 256;
        proof.accept(last).unwrap();
        assert!(proof.satisfied());
        empty.next_after_sequence = 257;
        assert!(proof.accept(empty).is_err());
        assert!(!proof.satisfied());
    }

    #[test]
    fn exhausted_empty_history_is_not_empty_file_delivery() {
        let mut proof = collection();
        let mut empty = page();
        empty.reads.clear();
        empty.exhausted = true;
        empty.next_after_sequence = 0;
        proof.accept(empty).unwrap();
        assert!(proof.paths().is_empty());
        let mut candidate = page();
        candidate.reads[0].source_bytes = 0;
        if let Some(ReadEvidenceExtent::FileLines {
            start_line,
            line_count,
            total_lines,
            body_bytes,
            source_omitted,
            ..
        }) = &mut candidate.reads[0].delivered_extent
        {
            *start_line = 1;
            *line_count = 0;
            *total_lines = 0;
            *body_bytes = 0;
            *source_omitted = false;
        }
        let mut proof = collection();
        proof.accept(candidate).unwrap();
        assert!(proof.satisfied());
    }
}
