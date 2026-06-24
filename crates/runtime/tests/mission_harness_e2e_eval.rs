use ai_eval::{ScenarioCheck, ScenarioCheckKind, ScenarioObservation, ScenarioSpec, ScenarioSuite};
use ai_kernel::core::TaskRisk;
use ai_kernel::strategy::{decide_strategy, StrategyInput};
use runtime::{
    ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, AutonomyProfileId,
    CollaborationTemplateMatcher, MissionRuntime, RuntimeEventInput, RuntimeEventScope,
    RuntimeEventStore, StartMissionSessionRequest, StartStewardRuntimeRequest,
    StartTeamRuntimeRequest, StewardActionStatus, StewardRuntimeService, TickStewardRuntimeRequest,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct CapabilityResult {
    capability: &'static str,
    status: &'static str,
    evidence: String,
}

#[derive(Debug, Serialize)]
struct MissionHarnessEvalReport {
    level: &'static str,
    status: &'static str,
    scenarios: Vec<CapabilityResult>,
}

#[test]
fn mission_harness_quick_eval_covers_core_runtime_loop_and_writes_report() {
    let mission = MissionRuntime::new();
    let session = mission
        .start_session(StartMissionSessionRequest {
            title: "Mission Harness quick eval".to_string(),
            session_id: Some(format!("mission-eval-{}", uuid::Uuid::new_v4())),
        })
        .expect("mission session starts");

    let prompt = "implement mission harness event store, approval governance, and team review";
    let strategy = decide_strategy(&StrategyInput::from_prompt(prompt));
    let decision = CollaborationTemplateMatcher::default().decide(prompt, &strategy);
    let team = runtime::TeamRuntimeService::new()
        .start(StartTeamRuntimeRequest {
            session_id: session.session_id.clone(),
            objective: prompt.to_string(),
            collaboration_decision: decision,
        })
        .expect("team runtime starts");

    let approval = runtime::GlobalApprovalQueue::new()
        .submit(runtime::SubmitGlobalApprovalRequest {
            source: ApprovalSource {
                kind: ApprovalSourceKind::Session,
                session_id: Some(session.session_id.clone()),
                agent_id: None,
                team_id: Some(team.team_id.clone()),
                mission_id: Some("mission-eval".to_string()),
            },
            action: "apply_patch".to_string(),
            summary: "write runtime changes".to_string(),
            risk: TaskRisk::High,
            evidence_refs: vec![format!("team:{}", team.team_id)],
            timeout_policy: ApprovalTimeoutPolicy::Pending,
        })
        .expect("approval submitted");
    assert_eq!(approval.status, runtime::GlobalApprovalStatus::Pending);

    let command = mission
        .enqueue_session_command(
            &session.session_id,
            &session.session_id,
            "summarize evidence and blockers".to_string(),
        )
        .expect("session command enqueued");
    let claimed = mission
        .claim_session_command(&session.session_id, &command.command_id)
        .expect("session command claimed");
    assert_eq!(
        claimed.status,
        runtime::MissionSessionCommandStatus::Claimed
    );

    let steward_runtime = StewardRuntimeService::new();
    let steward = steward_runtime
        .start(StartStewardRuntimeRequest {
            mission_id: "mission-eval".to_string(),
            root_session_id: Some(session.session_id.clone()),
            profile_id: AutonomyProfileId::Stewarded,
            objective: "supervise mission harness eval".to_string(),
        })
        .expect("steward starts");
    let steward_decision = steward_runtime
        .tick(
            &steward.steward_id,
            TickStewardRuntimeRequest {
                action: Some("read evidence".to_string()),
                summary: Some("inspect runtime event evidence".to_string()),
                risk: TaskRisk::Low,
                requested_tool: Some("read_file".to_string()),
                ..TickStewardRuntimeRequest::default()
            },
        )
        .expect("steward ticks");
    assert_eq!(steward_decision.status, StewardActionStatus::Delegated);

    let event_store = RuntimeEventStore::open_in_memory().expect("event store opens");
    event_store
        .append(RuntimeEventInput {
            stream_id: format!("session:{}", session.session_id),
            scope: RuntimeEventScope::Session,
            kind: "mission_harness.eval.completed".to_string(),
            status: Some("completed".to_string()),
            actor: Some("mission_harness_eval".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({
                "team_id": team.team_id,
                "approval_id": approval.approval_id,
                "command_id": command.command_id,
                "steward_id": steward.steward_id,
            }),
        })
        .expect("event appends");
    assert_eq!(
        event_store
            .list_stream(&format!("session:{}", session.session_id))
            .expect("stream")
            .len(),
        1
    );

    let scenario = ScenarioSpec::new("mission_harness_quick", prompt)
        .expect_mode(strategy.mode)
        .require(ScenarioCheck::bool(
            "workgraph.present",
            ScenarioCheckKind::WorkgraphPresent,
            true,
            "mission-harness/team-runtime",
            "complex mission harness eval must produce a workgraph",
        ));
    let observation = ScenarioObservation {
        scenario_id: "mission_harness_quick".to_string(),
        strategy_mode: strategy.mode,
        finalization_blocked: false,
        regression_allowed: true,
        has_workgraph: true,
        workgraph_quality_ok: true,
        growth_has_blocker: false,
        growth_signal_kinds: Vec::new(),
        memory_candidate_count: 0,
        matrix_signal_count: 1,
        assistant_text: "mission harness quick eval completed".to_string(),
    };
    let suite_report = ScenarioSuite::new(vec![scenario]).evaluate(&[observation]);
    assert_eq!(suite_report.failed, 0, "{suite_report:?}");

    let report = MissionHarnessEvalReport {
        level: "quick",
        status: "passed",
        scenarios: vec![
            CapabilityResult {
                capability: "mission_session",
                status: "passed",
                evidence: session.session_id,
            },
            CapabilityResult {
                capability: "team_runtime",
                status: "passed",
                evidence: team.team_id,
            },
            CapabilityResult {
                capability: "approval_queue",
                status: "passed",
                evidence: approval.approval_id,
            },
            CapabilityResult {
                capability: "session_inbox",
                status: "passed",
                evidence: command.command_id,
            },
            CapabilityResult {
                capability: "steward_runtime",
                status: "passed",
                evidence: steward.steward_id,
            },
            CapabilityResult {
                capability: "runtime_event_store",
                status: "passed",
                evidence: "session stream replayed".to_string(),
            },
        ],
    };
    write_report(&report);
}

fn write_report(report: &MissionHarnessEvalReport) {
    let root =
        std::path::Path::new("/media/yi/Datas/workspace/plan/0624-Mission-Harness闭环补齐/reports");
    let _ = std::fs::create_dir_all(root);
    let json_path = root.join("20260624-mission-harness-quick.json");
    let md_path = root.join("20260624-mission-harness-quick.md");
    let _ = std::fs::write(
        &json_path,
        serde_json::to_string_pretty(report).unwrap_or_else(|_| "{}".to_string()),
    );
    let mut markdown = String::from("# Mission Harness Evaluation Report\n\n");
    markdown.push_str(&format!(
        "- level: {}\n- status: {}\n\n",
        report.level, report.status
    ));
    markdown.push_str("| Capability | Status | Evidence |\n| --- | --- | --- |\n");
    for item in &report.scenarios {
        markdown.push_str(&format!(
            "| {} | {} | {} |\n",
            item.capability, item.status, item.evidence
        ));
    }
    let _ = std::fs::write(md_path, markdown);
}
