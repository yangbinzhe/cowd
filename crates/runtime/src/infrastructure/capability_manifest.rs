//! Agent-first Runtime capability catalog and model primer.
//!
//! Models choose small semantic actions while Runtime owns identity,
//! authorization, concurrency, persistence, recovery and terminal truth.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AgentCatalogEntry;
use crate::execution_core::RuntimeExecutionDecision;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapability {
    pub id: String,
    pub summary: String,
    pub recommended_tools: Vec<String>,
    pub when_to_use: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilityManifest {
    pub name: String,
    pub capabilities: Vec<RuntimeCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilityCatalog {
    pub name: String,
    pub templates: Vec<RuntimeTemplateSummary>,
    pub protocols: Vec<RuntimeProtocolSummary>,
    pub operation_groups: Vec<RuntimeOperationGroup>,
    pub action_contracts: Vec<RuntimeActionContract>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProtocolSummary {
    pub protocol_id: String,
    pub version: u32,
    pub availability: String,
    pub summary: String,
    pub role_ids: Vec<String>,
    pub supports_bounded_repair: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTemplateSummary {
    pub template_id: String,
    pub protocol_id: String,
    pub protocol_version: u32,
    pub availability: String,
    pub requires_review: bool,
    pub best_for: Vec<String>,
    #[serde(default)]
    pub role_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOperationGroup {
    pub id: String,
    pub summary: String,
    pub operations: Vec<RuntimeOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOperation {
    pub id: String,
    pub owner: String,
    pub summary: String,
    pub model_intent: String,
    pub validation_gate: String,
    pub output_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeActionContract {
    pub runtime_action: String,
    pub tool_action: String,
    pub when_to_use: String,
    pub required_intent_fields: Vec<String>,
    pub validation: String,
    pub expected_projection: Vec<String>,
}

impl RuntimeCapabilityManifest {
    #[must_use]
    pub fn current() -> Self {
        Self {
            name: "cowd-agent-first-capabilities".to_string(),
            capabilities: vec![
                RuntimeCapability {
                    id: "agent_action_loop".to_string(),
                    summary: "Create and evolve collaborative work through small durable actions."
                        .to_string(),
                    recommended_tools: harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                        .iter()
                        .map(|tool| (*tool).to_string())
                        .collect(),
                    when_to_use: vec![
                        "Use whenever independent roles, parallel work, discussion, review, or replanning improves the objective."
                            .to_string(),
                    ],
                },
                RuntimeCapability {
                    id: "evidence_and_content".to_string(),
                    summary: "Keep long content in normal model output or durable artifacts and pass compact references through actions."
                        .to_string(),
                    recommended_tools: vec![
                        "read_ref".to_string(),
                        "context_retrieve".to_string(),
                        "evidence_retrieve".to_string(),
                        "artifact_commit".to_string(),
                    ],
                    when_to_use: vec![
                        "Use references for source evidence, long reports, and final deliverables."
                            .to_string(),
                    ],
                },
            ],
        }
    }
}

impl RuntimeCapabilityCatalog {
    #[must_use]
    pub fn current() -> Self {
        let action_contracts = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .iter()
            .map(|action| RuntimeActionContract {
                runtime_action: (*action).to_string(),
                tool_action: (*action).to_string(),
                when_to_use: action_purpose(action).to_string(),
                required_intent_fields: Vec::new(),
                validation: "Runtime binds actor identity and validates authorization, revision, invariants, and durable references."
                    .to_string(),
                expected_projection: vec!["program_revision".to_string(), "next_actions".to_string()],
            })
            .collect();
        Self {
            name: "cowd-agent-first-capability-catalog".to_string(),
            templates: Vec::new(),
            protocols: Vec::new(),
            operation_groups: vec![RuntimeOperationGroup {
                id: "agent_collaboration".to_string(),
                summary: "One incremental action protocol shared by root, leads, and members."
                    .to_string(),
                operations: harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                    .iter()
                    .map(|action| RuntimeOperation {
                        id: (*action).to_string(),
                        owner: "runtime.agentic.action_service".to_string(),
                        summary: action_purpose(action).to_string(),
                        model_intent:
                            "Choose the next useful semantic action from current Program state."
                                .to_string(),
                        validation_gate:
                            "Trusted actor binding plus action-specific state invariant."
                                .to_string(),
                        output_refs: vec![
                            "action_receipt".to_string(),
                            "program_revision".to_string(),
                        ],
                    })
                    .collect(),
            }],
            action_contracts,
        }
    }
}

fn action_purpose(action: &str) -> &'static str {
    match action {
        "state_inspect" => "Read a bounded Program, Team, Task, Topic, or Artifact projection.",
        "team_create" => "Create a Team with a model-authored purpose.",
        "agent_invite" => "Invite a catalog-backed Agent into a Team with a focused role.",
        "task_publish" => "Publish bounded work, dependencies, and acceptance criteria.",
        "task_claim" => "Claim eligible work through a Runtime lease.",
        "task_release" => "Release or re-open claimed work without losing history.",
        "task_submit" => "Submit durable artifacts and evidence for review.",
        "task_review" => "Independently accept, challenge, or request rework.",
        "message_publish" => "Publish a durable scoped discussion entry.",
        "artifact_commit" => "Attach a durable content reference and provenance.",
        "objective_review" => {
            "Settle one open Objective criterion with a satisfied/gap/blocked verdict over a set of delivered results."
        }
        "objective_complete_request" => "Ask the Supervisor to verify terminal invariants.",
        _ => "Apply one bounded semantic collaboration action.",
    }
}

/// The minimal, capability-independent core of the system prompt. Kept small and
/// stable so it stays inside the shared Provider cache prefix; per-capability
/// usage is appended by [`render_tool_guidance`] so a model only ever reads the
/// guidance for tools it can actually call (opencode's `${OPENCODE_TOOL_GUIDANCE}`
/// pattern).
#[must_use]
pub fn runtime_capability_primer() -> String {
    [
        "## Agent-first collaboration contract",
        "You own semantic decisions: Team purpose, roles, Tasks, dependencies, discussion, challenge, replanning, synthesis, and WHEN to close the Objective. Runtime owns actor identity, authorization, revisions, idempotency, leases, concurrent execution, durable artifacts, recovery, and terminal truth. Publish independent Tasks without artificial dependencies so Runtime can execute them concurrently. A Task is complete only after a real claimant submits durable evidence and a different Agent accepts it.",
        "## Shortest path first",
        "Prefer parallelizing independent tool calls.",
        "## Text tool-call frame (only if you emit calls as text markup)",
        "Some deployments cannot use native function calls and emit tool calls as a text frame. When you do, use exactly: <｜｜DSML｜｜tool_calls> wrapping one <｜｜DSML｜｜invoke name=\"TOOL\"> per action, each wrapping one <｜｜DSML｜｜parameter name=\"FIELD\" string=\"false\">VALUE</｜｜DSML｜｜parameter> for arrays/objects and string=\"true\" for plain text. Batch by putting SEVERAL invoke blocks in ONE frame - never nest actions inside another tool (no agent_action_batch wrapper): every parameter that the schema types as array or object must be emitted with string=\"false\" and raw JSON inside.",
        "Never move large file bodies into context: read only the range you need, and run counting/aggregation/filtering inside `tool_orchestrate` compute/collect or `execute_code`, returning only a summary plus a `tool://` ref.",
        "## Closure discipline",
        "Runtime tells you what is satisfied and what remains. Finish the outstanding rungs; do not redo finished ones. When `closure.exit_ready` holds, YOU call `objective_complete_request` and Runtime verifies it. If you judge the goal unreachable after repeated attempts with no new progress, use the `blocked` form with a reason, human review and next steps.",
        "## Canonical references",
        "Read Runtime facts through one canonical reference with `read_ref`: task://, agent://, tool://, artifact://, session://. The scheme selects the authorized reader; do not guess field names on legacy readers.",
        "## Instruction precedence",
        "Workspace rules arrive from AGENTS.md-style files (system-level, then this workspace, then nested trees). Precedence: direct instructions from Runtime or the operator > more deeply nested rules > outer rules.",
        "## Recovery",
        "On rejection or restart, inspect current state and continue from the returned revision; never repeat an unchanged failing action.",
        "## Collaboration rules (Agent-first)",
        "1. Do not duplicate delegated work or overlap its write set. While sub-tasks run, you may do independent work within your own authorization, claim, revision and lease fences.",
        "2. When progress depends on workers and no independent work remains, wait with `state_inspect(wait_for_workers=true)`; scale the wait to the outstanding work.",
        "3. Close what you finished: release a claim you no longer own, and withdraw a Task that is no longer needed. Dangling work keeps the Objective open.",
        "4. When you delegate, tell the delegate explicitly: (a) it is NOT alone and must not revert or overwrite others' committed work; (b) whether it may spawn further agents (default: it may NOT, to prevent unbounded recursion).",
        "5. Spawn a Team or a Task only when the work is genuinely large, spans independent responsibility domains, or needs an independent reviewer. For simple or straightforward work you do not need to spawn anything at all.",
        "6. Before each batch of calls send at most one short sentence naming the grouped action; skip it for a single trivial read.",
    ]
    .join("\n")
}

/// Byte ceiling for one primer section.
///
/// The primer lives in the shared Provider cache prefix, so it is entered on
/// every turn of every session. An order that holds on every turn belongs in
/// `agentic::order_contract`, which is the single source every prompt site
/// reads; the primer should carry only what this turn needs. A section that
/// outgrows its ceiling must be split or moved into the contract, not silently
/// expanded - the old guard reported only a whole-primer byte count
/// (`primer must stay small: 4376`), which named neither the rule that grew nor
/// where it should go.
///
/// These ceilings are independent; the whole-primer budget asserted by
/// `primer_is_short_and_states_ownership_and_exit_discipline` stays binding.
const PRIMER_SECTION_BUDGETS: &[(&str, usize)] = &[
    ("## Agent-first collaboration contract", 600),
    ("## Shortest path first", 380),
    (
        "## Text tool-call frame (only if you emit calls as text markup)",
        1040,
    ),
    ("## Closure discipline", 420),
    ("## Canonical references", 260),
    ("## Instruction precedence", 260),
    ("## Recovery", 160),
    ("## Collaboration rules (Agent-first)", 1290),
];

/// Usage guidance for the tools that are ACTUALLY callable on this request.
/// A tool that is not callable gets no guidance, so a model is never told about
/// a capability it cannot use — and the prompt stays proportional to the surface.
#[must_use]
pub fn render_tool_guidance(present: &std::collections::BTreeSet<String>) -> String {
    let execute_guidance = present.contains("execute_code").then(|| {
        let languages = tools::mvp_tool_specs()
            .into_iter()
            .find(|spec| spec.name == "execute_code")
            .and_then(|spec| {
                spec.input_schema["properties"]["language"]["enum"]
                    .as_array()
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("/")
                    })
            })
            .unwrap_or_else(|| "see the current native schema".to_string());
        format!(
            "- `execute_code`: deterministic reduction ({languages}); the current native schema is authoritative. File-tool paths resolve in the leased host workspace; their returned absolute paths are not sandbox aliases. Bounded code runs with cwd=/project, a read-only snapshot of only the explicit workspace-relative input_files: [\"input.txt\"] maps to /project/input.txt, while [] copies no workspace files. Private /tmp is scratch and never writes back to the workspace. Print computed data to stdout; governed results retain stdout, stderr, terminal facts and an archived body reference. This describes the bounded-code contract, not a claim that any process or mount already exists."
        )
    });
    let mut lines: Vec<&str> = Vec::new();
    if present.contains("tool_orchestrate") {
        lines.push(
            "- `tool_orchestrate`: run a bounded PROGRAM of many calls in ONE round. `batch` a known file set, `map` a list, `filter`/`collect` to aggregate (count|first|last|join|sum), `compute` to reduce prior result data (python/javascript, not ambient workspace files). `on_error=collect` returns per-item {ok,output,error,durationMs} instead of aborting.",
        );
    }
    if present.contains("glob_search") {
        lines.push(
            "- `glob_search`: `patterns:[...]` searches several globs in ONE call; prefer it over N single `glob_search` calls.",
        );
    }
    if present.contains("grep_search") {
        lines.push(
            "- `grep_search`: `searches:[...]` runs several independent content searches in ONE ordered batch; `language` sweeps one language's extensions.",
        );
    }
    if let Some(guidance) = execute_guidance.as_deref() {
        lines.push(guidance);
        if present.contains("edit") {
            lines.push(
                "- To persist computed output, you may pass its text to authorized `edit` (action=write) using a workspace-relative path; sandbox execution does not grant write authority.",
            );
            if present.contains("read_file") {
                lines.push(
                    "- Read the persisted file back with `read_file` when verification is needed; a scratch file or successful process is not a delivered or accepted result.",
                );
            }
        }
    }
    if present.contains("read_file") {
        lines.push(
            "- `read_file`: one bounded window; `complete=true` yields a verifiable EOF receipt. On truncation continue from `nextOffset` instead of re-reading whole files. content is line-rendered, not byte-exact: byteLength, sha256 and endsWithNewline describe the original file bytes.",
        );
    }
    if present.contains("objective_review") {
        lines.push(
            "- `objective_review`: settle ONE open Objective criterion with decision `satisfied`, `gap` or `blocked`. It is the only action that satisfies the original user-intent criterion; a Task review, a prose assertion or a re-submitted completion request does not. Call it under a Runtime-bound Agent identity that is distinct from the producer of every result it reviews: give the review to a reviewer Agent in an EXISTING Team (never a new Team) and let that same Agent read each delivered result with `read_ref` or `evidence_retrieve` before it submits one review whose `result_refs` cover the full current set, with the evidence refs those reads produced and a durable `reason_ref`. A retry by that same Agent counts; inviting a new one discards the reads it already made. When a completion gap lists `uncovered_result_refs`, those are exactly the refs no current review covers yet.",
        );
    }
    if present.contains("bash") {
        lines.push(
            "- `bash`: `shell` selects the backend (auto|bash|powershell). Prefer dedicated tools over the shell.",
        );
    }
    if lines.is_empty() {
        String::new()
    } else {
        format!("## Available tool shapes\n{}", lines.join("\n"))
    }
}

#[must_use]
pub fn compact_runtime_capability_primer() -> String {
    runtime_capability_primer()
}

#[must_use]
pub fn runtime_capabilities_response(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
) -> Value {
    runtime_capabilities_response_with_detail(intent, surface, profile, None)
}

#[must_use]
pub fn runtime_capabilities_response_with_detail(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
) -> Value {
    runtime_capabilities_response_with_leased_decision(intent, surface, profile, detail, None)
}

#[must_use]
pub fn runtime_capabilities_response_with_leased_decision(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> Value {
    let available_tools = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .map(|tool| (*tool).to_string())
        .chain([
            "context_retrieve".to_string(),
            "evidence_retrieve".to_string(),
        ])
        .collect::<Vec<_>>();
    runtime_capabilities_response_with_leased_decision_and_tools(
        intent,
        surface,
        profile,
        detail,
        leased_decision,
        &available_tools,
        None,
    )
}

#[must_use]
pub fn runtime_capabilities_response_with_leased_decision_and_tools(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
    leased_decision: Option<&RuntimeExecutionDecision>,
    available_tool_names: &[String],
    agent_catalog_entries: Option<&[AgentCatalogEntry]>,
) -> Value {
    let actions = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .filter(|tool| {
            available_tool_names
                .iter()
                .any(|available| available == **tool)
        })
        .copied()
        .collect::<Vec<_>>();
    let agents = agent_catalog_entries
        .unwrap_or_default()
        .iter()
        .map(|agent| {
            json!({
                "agent_id": agent.agent_id,
                "name": agent.name,
                "description": agent.description,
                "capabilities": agent.capabilities,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "type": "runtime_capabilities",
        "architecture": "agent_first",
        "intent": intent,
        "surface": surface,
        "profile": profile,
        "detail": detail.unwrap_or("summary"),
        "available": !actions.is_empty(),
        "actions": actions,
        "agent_catalog": agents,
        "manifest": RuntimeCapabilityManifest::current(),
        "catalog": RuntimeCapabilityCatalog::current(),
        "strategy_hint": leased_decision.map(|decision| json!({
            "complexity": format!("{:?}", decision.strategy.understanding.complexity),
            "risk": format!("{:?}", decision.strategy.understanding.risk),
            "required_team_count": decision.strategy.understanding.required_team_count,
        })),
        "control_contract": {
            "model_owns": ["team purpose", "roles", "tasks", "dependencies", "discussion", "review decision", "replanning", "synthesis"],
            "runtime_owns": ["identity", "authorization", "CAS", "idempotency", "leases", "concurrent execution", "artifact durability", "recovery", "terminal verification"],
            "long_content": "ordinary model content or workspace files; actions carry compact refs",
            "parallelism": "publish independent Tasks together; Runtime admits their Agent graphs concurrently",
            "recovery": "inspect current state and continue from its revision",
        },
        "completion": {
            "request": harness_contract::agent_action::OBJECTIVE_COMPLETE_REQUEST_TOOL_ID,
            "requires": ["required Teams staffed", "accepted Tasks", "independent reviewers", "durable reviewed final artifact", "evidence", "no objective-level blockers"],
            "accepted_task_disclosures": "accepted Task limitations remain visible but are not re-litigated by a second completion owner",
        },
        "context_retrieval": {
            "available": available_tool_names.iter().any(|tool| tool == "context_retrieve"),
            "tool": "context_retrieve",
        },
        "evidence_retrieval": {
            "available": available_tool_names.iter().any(|tool| tool == "evidence_retrieve"),
            "tool": "evidence_retrieve",
        },
    })
}

#[cfg(test)]
mod prompt_core_tests {
    use super::{PRIMER_SECTION_BUDGETS, render_tool_guidance, runtime_capability_primer};

    fn set(items: &[&str]) -> std::collections::BTreeSet<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    /// Split the primer on its `## ` headings, returning `(heading, bytes)`
    /// with the heading line included in its own section.
    fn primer_sections(primer: &str) -> Vec<(String, usize)> {
        let mut sections: Vec<(String, usize)> = Vec::new();
        for line in primer.split('\n') {
            let is_heading = line.starts_with("## ");
            if is_heading || sections.is_empty() {
                sections.push((line.to_string(), line.len() + 1));
                continue;
            }
            sections.last_mut().expect("section").1 += line.len() + 1;
        }
        sections
    }

    #[test]
    fn every_primer_section_stays_inside_its_own_budget() {
        let primer = runtime_capability_primer();
        let sections = primer_sections(&primer);
        for (heading, budget) in PRIMER_SECTION_BUDGETS {
            let measured = sections
                .iter()
                .find(|(name, _)| name == heading)
                .map(|(_, bytes)| *bytes)
                .unwrap_or_else(|| panic!("primer lost the section {heading}"));
            assert!(
                measured <= *budget,
                "primer section `{heading}` is {} bytes over its {budget}-byte ceiling: \
                 split it, or move the rule into agentic::order_contract where every \
                 prompt site reads it from one place",
                measured.saturating_sub(*budget)
            );
        }
        let declared: usize = PRIMER_SECTION_BUDGETS.iter().map(|(_, b)| b).sum();
        assert!(
            primer.len() <= declared,
            "the primer outgrew the sum of its section ceilings"
        );
    }

    /// A rule that exists in two places has two wordings, and the two drift.
    /// Every order the contract owns must therefore appear exactly once: the
    /// primer may paraphrase the situation, but never restate the rule.
    #[test]
    fn no_primer_section_restates_an_order_contract_rule() {
        use crate::agentic::order_contract;
        let primer = runtime_capability_primer().to_lowercase();
        let rules = [
            order_contract::TASK_AUTHORING_DISCIPLINE,
            order_contract::REVIEWER_SCOPE_DISCIPLINE,
            order_contract::OBJECTIVE_REVIEW_SCOPE_DISCIPLINE,
        ];
        for rule in rules {
            let lowered = rule.to_lowercase();
            let words: Vec<&str> = lowered
                .split(|ch: char| !ch.is_alphanumeric() && ch != '`' && ch != '_')
                .filter(|word| !word.is_empty())
                .collect();
            for window in words.windows(6) {
                let phrase = window.join(" ");
                assert!(
                    !primer.contains(&phrase),
                    "the primer restates an order-contract rule (`{phrase}...`): \
                     the contract is the single source; reference it, do not re-word it"
                );
            }
        }
    }

    #[test]
    fn primer_is_short_and_states_ownership_and_exit_discipline() {
        let primer = runtime_capability_primer();
        assert!(
            primer.len() <= 4_200,
            "primer must stay small: {}",
            primer.len()
        );
        assert!(primer.contains("Shortest path first"));
        assert!(primer.contains("Prefer parallelizing independent tool calls."));
        assert!(primer.contains("closure.exit_ready"));
        assert!(primer.contains("objective_complete_request"));
        assert!(primer.contains("blocked"));
        assert!(!primer.contains("incrementally"));
        assert!(!primer.contains("Do not emit a complete graph"));
    }

    #[test]
    fn tool_guidance_only_mentions_callable_tools() {
        let only_read = render_tool_guidance(&set(&["read_file"]));
        assert!(only_read.contains("read_file"));
        assert!(!only_read.contains("tool_orchestrate"));
        assert!(!only_read.contains("patterns:[...]"));
        assert!(!only_read.contains("compute"));

        let orchestrated = render_tool_guidance(&set(&[
            "read_file",
            "tool_orchestrate",
            "glob_search",
            "execute_code",
        ]));
        assert!(orchestrated.contains("tool_orchestrate"));
        assert!(orchestrated.contains("patterns:[...]"));
        assert!(orchestrated.contains("on_error=collect"));
        assert!(orchestrated.contains("execute_code"));
    }

    #[test]
    fn bounded_code_guidance_uses_spec_languages_and_preserves_namespaces() {
        let present = set(&["execute_code", "edit", "read_file"]);
        let guidance = render_tool_guidance(&present);
        let spec = tools::mvp_tool_specs()
            .into_iter()
            .find(|spec| spec.name == "execute_code")
            .expect("execute_code spec");
        let languages = spec.input_schema["properties"]["language"]["enum"]
            .as_array()
            .expect("language enum");
        let expected = languages
            .iter()
            .map(|language| language.as_str().expect("language string"))
            .collect::<Vec<_>>()
            .join("/");
        assert!(guidance.contains(&format!("({expected})")));
        for fragment in [
            "not sandbox aliases",
            "cwd=/project",
            "[] copies no workspace files",
            "/project/input.txt",
            "/tmp is scratch and never writes back",
            "stdout, stderr, terminal facts",
            "authorized `edit`",
            "Read the persisted file back",
            "not byte-exact",
            "not a claim that any process or mount already exists",
        ] {
            assert!(guidance.contains(fragment), "missing contract: {fragment}");
        }
        for obsolete in ["ruby/lua/go/rust", "only stdout", "output_files"] {
            assert!(
                !guidance.contains(obsolete),
                "obsolete contract: {obsolete}"
            );
        }
    }

    #[test]
    fn bounded_code_bridge_examples_validate_without_granting_authority() {
        let workspace = tempfile::tempdir().expect("workspace");
        let lease = tools::ToolHost::builtin("bridge-schema", workspace.path()).pin_snapshot();
        for (tool, input) in [
            (
                "execute_code",
                serde_json::json!({
                    "language": "python",
                    "input_files": ["input.txt"],
                    "code": "print(sum(bool(line.strip()) for line in open('/project/input.txt')))"
                }),
            ),
            (
                "execute_code",
                serde_json::json!({"language": "python", "input_files": [], "code": "print(3)"}),
            ),
            (
                "edit",
                serde_json::json!({"action": "write", "path": "result.txt", "content": "nonempty_lines=3\n"}),
            ),
            (
                "read_file",
                serde_json::json!({"path": "result.txt", "complete": true}),
            ),
        ] {
            lease.validate_input(tool, &input).expect("bridge schema");
        }
        for input in [
            serde_json::json!({"language": "python", "code": "pass"}),
            serde_json::json!({"language": "ruby", "input_files": [], "code": "puts 3"}),
            serde_json::json!({"language": "python", "input_files": [], "code": "pass", "output_files": ["result.txt"]}),
        ] {
            assert!(lease.validate_input("execute_code", &input).is_err());
        }
        assert!(!workspace.path().join("result.txt").exists());
    }

    #[test]
    fn persistence_guidance_does_not_activate_absent_tools() {
        let guidance = render_tool_guidance(&set(&["execute_code"]));
        assert!(!guidance.contains("`edit`"));
        assert!(!guidance.contains("`read_file`"));
        let guidance = render_tool_guidance(&set(&["execute_code", "edit"]));
        assert!(guidance.contains("authorized `edit`"));
        assert!(!guidance.contains("`read_file`"));
    }

    #[test]
    fn no_callable_tool_yields_no_guidance_section() {
        assert!(render_tool_guidance(&std::collections::BTreeSet::new()).is_empty());
        assert!(render_tool_guidance(&set(&["state_inspect"])).is_empty());
    }
}

#[cfg(test)]
mod collaboration_rule_tests {
    use super::{render_tool_guidance, runtime_capability_primer};

    #[test]
    fn collaboration_rules_pin_waiting_role_separation_and_lifecycle() {
        let primer = runtime_capability_primer();
        assert!(!primer.contains("WAIT for it before yielding"));
        assert!(!primer.contains("your only role is to coordinate"));
        assert!(primer.contains("Do not duplicate delegated work or overlap its write set"));
        assert!(primer.contains("you may do independent work"));
        assert!(primer.contains("authorization, claim, revision and lease fences"));
        assert!(primer.contains("no independent work remains"));
        assert!(primer.contains("release a claim you no longer own"));
        assert!(primer.contains("withdraw a Task that is no longer needed"));
        assert!(primer.contains("must not revert or overwrite others' committed work"));
        assert!(primer.contains("it may NOT, to prevent unbounded recursion"));
        assert!(primer.contains("state_inspect(wait_for_workers=true)"));
    }

    #[test]
    fn collaboration_rules_never_prescribe_a_topology() {
        let primer =
            runtime_capability_primer() + &render_tool_guidance(&std::collections::BTreeSet::new());
        for banned in [
            "must create",
            "至少",
            "至少 N",
            "at least N Team",
            "N 个团队",
        ] {
            assert!(!primer.contains(banned), "prescriptive topology: {banned}");
        }
        assert!(primer.contains(
            "For simple or straightforward work you do not need to spawn anything at all"
        ));
    }
}

#[cfg(test)]
mod project_rule_tests {
    use super::{render_tool_guidance, runtime_capability_primer};

    #[test]
    fn primer_states_project_rule_precedence() {
        let primer = runtime_capability_primer();
        assert!(primer.contains("AGENTS.md-style files"));
        assert!(primer.contains("## Instruction precedence"));
        assert!(primer.contains(
            "direct instructions from Runtime or the operator > more deeply nested rules > outer rules"
        ));
    }

    #[test]
    fn primer_claims_no_second_instruction_mechanism() {
        // The framework already loads layered AGENTS.md/COWD.md/CLAUDE.md; the
        // primer only states precedence and never invents a second loader.
        let primer = runtime_capability_primer();
        assert!(!primer.contains("~/.cowd/AGENTS.md"));
        assert!(!primer.contains("collect_agents_rules"));
        assert!(!render_tool_guidance(&std::collections::BTreeSet::new()).contains("AGENTS.md"));
    }
}
