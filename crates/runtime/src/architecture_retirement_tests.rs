use std::path::{Path, PathBuf};

fn contains_retired_symbol(content: &str, symbol: &str) -> bool {
    let identifier_char = |character: char| character.is_alphanumeric() || character == '_';
    if symbol.chars().all(identifier_char) {
        content
            .split(|character| !identifier_char(character))
            .any(|token| token == symbol)
    } else {
        content.contains(symbol)
    }
}

#[test]
fn retirement_scan_distinguishes_identifiers_from_substrings_without_ignoring_routes() {
    let retired = concat!("Collaboration", "Program");
    assert!(contains_retired_symbol(
        &format!("let x: {retired};"),
        retired
    ));
    assert!(contains_retired_symbol(
        &format!("legacy::{retired}::new()"),
        retired
    ));
    assert!(!contains_retired_symbol(
        "AgenticCollaborationProgramProjectionV1",
        retired
    ));
    assert!(!contains_retired_symbol(
        "AgenticCollaborationProgramStatus",
        retired
    ));
    assert!(contains_retired_symbol(
        "route(\"/api/retired/action\")",
        "/api/retired/"
    ));
}

fn rust_sources(root: &Path, files: &mut Vec<PathBuf>) {
    let mut entries = std::fs::read_dir(root)
        .unwrap_or_else(|error| panic!("read {}: {error}", root.display()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|error| panic!("enumerate {}: {error}", root.display()));
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn production_sources_have_one_agent_first_collaboration_control_plane() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("Runtime crate belongs to the Cowd workspace");
    let roots = [
        workspace.join("crates/runtime/src"),
        workspace.join("crates/gateway/src"),
        workspace.join("crates/harness-contract/src"),
        workspace.join("crates/tools/src"),
    ];
    let retired = [
        concat!("runtime_", "orchestrate"),
        concat!("submit_collaboration_", "decision"),
        concat!("request_collaboration_", "escalation"),
        concat!("ModelCollaborationControl", "DecisionV2"),
        concat!("ModelRuntimeOrchestration", "Input"),
        concat!("normalize_template_", "proposal"),
        concat!("collaboration_", "template"),
        concat!("CollaborationTemplate", "Matcher"),
        concat!("CollaborationTemplate", "Id"),
        concat!("recommended_", "template"),
        concat!("recommended_", "actions"),
        concat!("RuntimeActionSelection", "Report"),
        concat!("RuntimeExecutionAction", "Hint"),
        concat!("action_", "selection"),
        concat!("default_", "templates"),
        concat!("template_", "hint"),
        concat!("Automatic", "Strategy"),
        concat!("propose:", "team"),
        concat!("runtime.", "orchestration"),
        concat!("runtime.", "collaboration_escalation"),
        concat!("pub mod ", "orchestration;"),
        concat!("Collaboration", "Program"),
        concat!("UpdateCollaboration", "ProgramControl"),
        concat!("RecordCrossTeam", "EdgeDelivery"),
        concat!("ClaimCrossTeam", "EdgeDelivery"),
        concat!("ApplyCrossTeam", "EdgePatch"),
        concat!("ApplyCollaboration", "TeamRetirement"),
        concat!("ApplyCollaboration", "ObjectiveNarrowing"),
        concat!("ApplyCollaboration", "ParallelismHint"),
        concat!("apply_collaboration_", "control"),
        concat!("ExecutionWorkRuntime", "State"),
        concat!("ExecutionGraphCommand::", "OfferWork"),
        concat!("ExecutionGraphCommand::", "ProposeWork"),
        concat!("ExecutionGraphCommand::", "BidWork"),
        concat!("ExecutionGraphCommand::", "ClaimWork"),
        concat!("ExecutionGraphCommand::", "HeartbeatWork"),
        concat!("ExecutionGraphCommand::", "ReleaseWork"),
        concat!("ExecutionGraphCommand::", "SubmitWork"),
        concat!("ExecutionGraphCommand::", "AcceptWork"),
        concat!("ExecutionGraphCommand::", "ChallengeWork"),
        concat!("autonomous_", "work"),
        concat!("work_", "states"),
        concat!("Team", "WorkingState"),
        concat!("team_", "working_state"),
        concat!("working_state_", "verified"),
        concat!("team_", "board"),
        concat!("terminal_", "working_state_event"),
        concat!("bound_", "team_packet"),
        concat!("notify_", "team_", "board_revision"),
        concat!("Team", "Template"),
        concat!("team_", "template"),
        concat!("/api/team-", "templates"),
        concat!("requires_managed_collaboration_", "escalation"),
        concat!("ExecutionOrchestration", "Metadata"),
        concat!("ReplaceGraph", "Orchestration"),
        concat!("collaboration_", "receipt"),
        concat!("/api/team-", "templates/instantiate"),
        concat!("/api/runtime/teams/", ":team_id/working-state"),
        concat!("/api/collaboration/", "runs"),
        concat!("Team", "Runtime"),
        concat!("TeamInstantiation", "Request"),
        concat!("TeamInstantiation", "Service"),
        concat!("EphemeralTeam", "Template"),
        concat!("ExecutionNodeKind::", "Subgraph"),
        concat!("ResolveChild", "Execution"),
        concat!("TeamRole", "Assignment"),
        concat!("TeamRole", "Identity"),
        concat!("TeamExecutionCapacity", "Snapshot"),
        concat!("team_", "binding"),
    ];
    let mut violations = Vec::new();
    for root in roots {
        let mut sources = Vec::new();
        rust_sources(&root, &mut sources);
        for source in sources {
            let content = std::fs::read_to_string(&source)
                .unwrap_or_else(|error| panic!("read {}: {error}", source.display()));
            for symbol in retired {
                if contains_retired_symbol(&content, symbol) {
                    violations.push(format!("{}:{symbol}", source.display()));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "retired collaboration control surface returned:\n{}",
        violations.join("\n")
    );
}

#[test]
fn direct_agent_live_terminal_authority_is_confined_to_the_agent_worker() {
    let runtime_src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources = Vec::new();
    rust_sources(&runtime_src, &mut sources);
    let allowed = [
        runtime_src.join("execution_core/services.rs"),
        runtime_src.join("agent/in_process/model_loop.rs"),
        runtime_src.join("architecture_retirement_tests.rs"),
    ];
    let symbols = [
        concat!("complete_agent_", "live_execution"),
        concat!("fail_agent_", "live_execution"),
        concat!("block_agent_", "live_execution"),
        concat!("cancel_agent_", "live_execution"),
        concat!("try_cancel_agent_", "live_execution"),
    ];
    let mut violations = Vec::new();
    for source in sources {
        if allowed.contains(&source) {
            continue;
        }
        let content = std::fs::read_to_string(&source)
            .unwrap_or_else(|error| panic!("read {}: {error}", source.display()));
        for symbol in symbols {
            if content.contains(symbol) {
                violations.push(format!("{}:{symbol}", source.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "direct Agent terminal authority escaped its worker domain:\n{}",
        violations.join("\n")
    );
}
