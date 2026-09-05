use super::*;

fn successful_root_outcome_timeline(mut timeline: Value) -> Value {
    let event = json!({
        "kind": "runtime.outcome.recorded.v1",
        "status": "succeeded",
        "payload": {
            "identity": {"execution_graph_ref": "root"},
            "terminal": {"class": "succeeded"}
        }
    });
    let object = timeline
        .as_object_mut()
        .expect("acceptance timeline fixture must be an object");
    object
        .entry("events")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .expect("timeline events must be an array")
        .push(event);
    timeline
}

fn completed_tool_event() -> Value {
    json!({
        "kind": "tool.invocation.completed",
        "status": "completed",
        "payload": {
            "status": "completed",
            "invocation_id": "tool-invocation-1",
            "tool_call_id": "tool-call-1",
            "tool_name": "read_file",
            "input_preview": "{\"path\":\"Cargo.toml\"}",
            "is_error": false
        }
    })
}

fn tool_acceptance() -> LiveAcceptance {
    LiveAcceptance::RequiresToolEvidence {
        tool_name: "read_file",
        target_path: "Cargo.toml",
    }
}

#[test]
fn explicit_live_scenario_selection_is_the_activation_authority() {
    let selected = BTreeSet::from([GROUP_THEORY_SCENARIO_ID.to_string()]);
    assert!(scenario_enabled(
        Some(&selected),
        GROUP_THEORY_SCENARIO_ID,
        false
    ));
    assert!(!scenario_enabled(
        Some(&selected),
        LARGE_SCALE_SCENARIO_ID,
        true
    ));
}

#[test]
fn legacy_expensive_scenario_opt_in_only_applies_without_selection() {
    assert!(scenario_enabled(None, GROUP_THEORY_SCENARIO_ID, true));
    assert!(!scenario_enabled(None, GROUP_THEORY_SCENARIO_ID, false));
}

#[test]
fn autonomous_deepseek_template_is_frozen_and_complete() {
    let template: AutonomousDeepseekTemplate = serde_json::from_str(include_str!(
        "../../templates/autonomous-collaboration-deepseek-v1.json"
    ))
    .expect("autonomous template");
    assert_eq!(template.schema_version, 2);
    assert_eq!(template.scenario_id, AUTONOMOUS_DEEPSEEK_SCENARIO_ID);
    assert_eq!(template.output_path, AUTONOMOUS_DEEPSEEK_OUTPUT_PATH);
    assert!(template.prompt_template.contains("{minimum_teams}"));
    assert!(template.prompt_template.contains("{minimum_agents}"));
    assert!(template.prompt_template.contains("{minimum_tasks}"));
    assert!(template.prompt_template.contains("task_supersede"));
    assert!(template
        .prompt_template
        .contains("objective_complete_request"));
    assert!(!template.prompt_template.contains("Team A"));
    assert!(!template.prompt_template.contains("恰好"));
}

fn agentic_program_projection(
    status: &str,
    include_evidence: bool,
) -> runtime::AgenticProgramProjection {
    let evidence_refs = include_evidence
        .then(|| vec!["evidence:read".to_string()])
        .unwrap_or_default();
    let teams = json!({
        "team-research": {"team_id":"team-research","name":"Research","mission":"research","objective":null,"topic_ref":"topic:research","created_by":"root","member_ids":["agent-a","agent-review"],"task_ids":["task-a"]},
        "team-synthesis": {"team_id":"team-synthesis","name":"Synthesis","mission":"synthesize","objective":null,"topic_ref":"topic:synthesis","created_by":"root","member_ids":["agent-b","agent-review"],"task_ids":["task-b"]}
    });
    let agents = json!({
        "agent-a": {"agent_id":"agent-a","team_id":"team-research","role":"researcher","mission":"research","required_capabilities":["read"],"invited_by":"root"},
        "agent-b": {"agent_id":"agent-b","team_id":"team-synthesis","role":"synthesizer","mission":"synthesize","required_capabilities":["read"],"invited_by":"root"},
        "agent-review": {"agent_id":"agent-review","team_id":"team-synthesis","role":"reviewer","mission":"review","required_capabilities":["read"],"invited_by":"root"}
    });
    let tasks = json!({
        "task-a": {"task_id":"task-a","team_id":"team-research","title":"research","objective":"research","acceptance":"evidence","required_capabilities":["read"],"depends_on":[],"status":"accepted","claimant":"agent-a","claim_generation":1,"claim_execution_id":"execution-a","claimed_at_ms":1,"lease_expires_at_ms":2,"artifact_refs":["artifact:a"],"evidence_refs":evidence_refs.clone(),"unresolved":[],"review_reason":"accepted","reviewed_by":"agent-review","failed_attempts":0,"review_generation":1,"failed_review_attempts":0,"last_failure":null,"replacement_task_refs":[],"supersede_evidence_refs":[],"superseded_reason":null,"superseded_by":null},
        "task-b": {"task_id":"task-b","team_id":"team-synthesis","title":"synthesis","objective":"synthesize","acceptance":"evidence","required_capabilities":["read"],"depends_on":["task-a"],"status":"accepted","claimant":"agent-b","claim_generation":1,"claim_execution_id":"execution-b","claimed_at_ms":1,"lease_expires_at_ms":2,"artifact_refs":["artifact:final"],"evidence_refs":evidence_refs,"unresolved":[],"review_reason":"accepted","reviewed_by":"agent-review","failed_attempts":0,"review_generation":1,"failed_review_attempts":0,"last_failure":null,"replacement_task_refs":[],"supersede_evidence_refs":[],"superseded_reason":null,"superseded_by":null}
    });
    let artifacts = json!({
        "artifact:a": {"artifact_ref":"artifact:a","content_ref":"content:a","kind":"research","title":"research","relates_to":["task-a"],"committed_by":"agent-a"},
        "artifact:final": {"artifact_ref":"artifact:final","content_ref":AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,"kind":"report","title":AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,"relates_to":["task-b"],"committed_by":"agent-b"}
    });
    serde_json::from_value(json!({
        "program_id": "program:test",
        "objective_id": "objective:test",
        "session_id": "session-test",
        "turn_id": "turn-test",
        "root_execution_id": "root",
        "required_team_count": 2,
        "objective_summary": "test autonomous collaboration",
        "model_lease": "model:test",
        "permission_ceiling": "read-only",
        "resource_scopes": ["read:crates"],
        "revision": 9,
        "status": status,
        "teams": teams,
        "agents": agents,
        "tasks": tasks,
        "topics": {"topic:program:test": [{"entry_id":"entry-1","revision":8,"actor_id":"agent-a","summary":"finding","content_ref":null,"refs":["artifact:a"]}]},
        "artifacts": artifacts,
        "final_artifact_ref": "artifact:final",
        "completion_request": {"action_id":"complete-1","requested_by":"root","program_revision":8,"final_artifact_ref":"artifact:final","evidence_refs":["evidence:read"],"unresolved":[]},
        "objective_verdict": (status == "verified").then(|| json!({"goal_id":"objective:test","goal_revision":1,"terminal_fence":"fence","authority_revision":9,"kind":"satisfied"})),
        "unresolved": []
    })).expect("typed Agent-first Program fixture")
}

#[test]
fn agentic_evidence_integrity_rejects_accepted_tasks_without_evidence() {
    assert!(agentic_evidence_integrity(&agentic_program_projection("verified", true)).passed());
    assert!(!agentic_evidence_integrity(&agentic_program_projection("verified", false)).passed());
}

#[test]
fn autonomous_acceptance_requires_verified_agent_first_facts_and_physical_overlap() {
    let acceptance = LiveAcceptance::AutonomousCollaboration {
        minimum_teams: 2,
        minimum_agents: 3,
        minimum_tasks: 2,
        minimum_reviews: 2,
        minimum_cross_team_edges: 1,
        minimum_topics: 1,
        output_path: AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
    };
    let response = format!("已提交 {AUTONOMOUS_DEEPSEEK_OUTPUT_PATH}");
    let timeline = successful_root_outcome_timeline(json!({}));
    let physical = json!({"activities":[
        {"agent_instance_id":"agent-a","team_run_id":"team-research","status":"completed","started_at_ms":1,"completed_at_ms":10},
        {"agent_instance_id":"agent-b","team_run_id":"team-synthesis","status":"completed","started_at_ms":2,"completed_at_ms":8}
    ]});
    let verified = agentic_program_projection("verified", true);
    assert!(
        acceptance
            .evaluate(
                &response,
                &timeline,
                &[physical.clone()],
                Some(&verified),
                "root"
            )
            .passed
    );
    let open = agentic_program_projection("open", true);
    assert!(
        !acceptance
            .evaluate(&response, &timeline, &[physical], Some(&open), "root")
            .passed
    );
}

#[test]
fn release_certification_requires_every_registered_core_scenario() {
    let complete = RELEASE_CERTIFICATION_SCENARIOS
        .iter()
        .map(|scenario_id| json!({"scenario_id": scenario_id, "status": "passed"}))
        .collect::<Vec<_>>();
    assert!(release_certification_scenarios_present(&complete));

    let incomplete = complete
        .into_iter()
        .filter(|scenario| {
            scenario.get("scenario_id").and_then(Value::as_str)
                != Some(IMPLICIT_COLLABORATION_SCENARIO_ID)
        })
        .collect::<Vec<_>>();
    assert!(!release_certification_scenarios_present(&incomplete));
}

#[test]
fn unknown_or_unscheduled_explicit_scenarios_fail_before_dispatch() {
    let selected = BTreeSet::from([GROUP_THEORY_SCENARIO_ID.to_string()]);
    let unregistered = BTreeSet::new();
    let errors = live_scenario_selection_errors(Some(&selected), &unregistered);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["scenario_id"], GROUP_THEORY_SCENARIO_ID);
    assert!(!live_scenario_selection_passed(Some(&selected), &errors, 0));

    let registered = BTreeSet::from([GROUP_THEORY_SCENARIO_ID.to_string()]);
    let errors = live_scenario_selection_errors(Some(&selected), &registered);
    assert!(errors.is_empty());
    assert!(!live_scenario_selection_passed(Some(&selected), &errors, 0));
    assert!(live_scenario_selection_passed(Some(&selected), &errors, 1));
}

#[test]
fn live_prompt_carries_an_explicit_shared_provider_token_lease() {
    let controlled = controlled_live_prompt(
        "live_group_theory_ai_research_simulation",
        "complete the research".to_string(),
        5_000_000,
    );
    let (header, prompt) = controlled.split_once('\n').expect("control header");
    let encoded = header
        .strip_prefix("COWD_EVAL_CONTROL ")
        .expect("typed evaluation prefix");
    let control: Value = serde_json::from_str(encoded).expect("control JSON");
    assert_eq!(control["corpus_id"], "live-scenarios-v1");
    assert_eq!(control["provider_constraint"], "normal");
    assert_eq!(
        control["resource_scopes"],
        json!(["provider", "provider_account", "provider_token_pool"])
    );
    assert_eq!(control["max_total_tokens"], 5_000_000);
    assert!(control["budget_lease_id"]
        .as_str()
        .is_some_and(|id| id.starts_with("live-scenario:live_group_theory")));
    assert_eq!(prompt, "complete the research");

    let tool_controlled = controlled_live_prompt(
        "live_tool_evidence",
        "read the manifest".to_string(),
        10_000,
    );
    let (tool_header, _) = tool_controlled
        .split_once('\n')
        .expect("tool control header");
    let tool_control: Value = serde_json::from_str(
        tool_header
            .strip_prefix("COWD_EVAL_CONTROL ")
            .expect("typed evaluation prefix"),
    )
    .expect("tool control JSON");
    assert_eq!(
        tool_control["resource_scopes"],
        json!([
            "provider",
            "provider_account",
            "provider_token_pool",
            "read:Cargo.toml"
        ])
    );

    let autonomous = controlled_live_prompt(
        AUTONOMOUS_DEEPSEEK_SCENARIO_ID,
        "run autonomous collaboration".to_string(),
        5_000_000,
    );
    let (autonomous_header, _) = autonomous.split_once('\n').expect("control header");
    let autonomous_control: Value = serde_json::from_str(
        autonomous_header
            .strip_prefix("COWD_EVAL_CONTROL ")
            .expect("typed evaluation prefix"),
    )
    .expect("autonomous control JSON");
    assert_eq!(
        autonomous_control["resource_scopes"],
        json!([
            "provider",
            "provider_account",
            "provider_token_pool",
            "read:group-theory-ai-autonomous-evaluation.html",
            "write:group-theory-ai-autonomous-evaluation.html"
        ])
    );
}

#[test]
fn live_health_contracts_accept_only_semantically_ready_payloads() {
    let fixtures = [
        (LiveHealthContract::Gateway, json!({"status": "healthy"})),
        (
            LiveHealthContract::Runtime,
            json!({"ok": true, "execution": {"lifecycle": "open", "last_error": null}}),
        ),
        (LiveHealthContract::RuntimeOutbox, json!({"healthy": true})),
        (
            LiveHealthContract::RuntimeControlPlane,
            json!({"readiness": {"production_ready": true, "required_blocked": 0}}),
        ),
        (
            LiveHealthContract::EvolutionProjectors,
            json!({
                "projector": {"worker_running": true, "consecutive_failures": 0, "dead_letter_count": 0},
                "outcome_projector": {"worker_running": true, "consecutive_failures": 0, "dlq_count": 0}
            }),
        ),
        (
            LiveHealthContract::SurfaceHost,
            json!({
                "status": "ready",
                "host": {
                    "failed_count": 0,
                    "circuit_open_count": 0,
                    "task_ownership": {"overloaded": false}
                }
            }),
        ),
    ];
    for (contract, payload) in fixtures {
        let observation = semantic_health_observation("/probe", contract, payload);
        assert_eq!(observation["status"], "passed", "{observation}");
        assert_eq!(observation["failed_checks"], json!([]));
    }
}

#[test]
fn http_success_with_non_ready_control_plane_fails_closed() {
    let observation = semantic_health_observation(
        "/api/runtime/control-plane",
        LiveHealthContract::RuntimeControlPlane,
        json!({
            "status": "attention",
            "readiness": {"production_ready": false, "required_blocked": 1}
        }),
    );

    assert_eq!(observation["status"], "failed");
    assert_eq!(observation["failed_checks"].as_array().unwrap().len(), 2);
    assert_eq!(
        observation["reason"],
        "HTTP transport succeeded but the endpoint semantic health contract failed"
    );
}

#[test]
fn missing_health_fields_never_default_to_success() {
    for contract in [
        LiveHealthContract::Gateway,
        LiveHealthContract::Runtime,
        LiveHealthContract::RuntimeOutbox,
        LiveHealthContract::RuntimeControlPlane,
        LiveHealthContract::EvolutionProjectors,
        LiveHealthContract::SurfaceHost,
    ] {
        let observation = semantic_health_observation("/probe", contract, json!({}));
        assert_eq!(observation["status"], "failed", "{observation}");
        assert!(!observation["failed_checks"].as_array().unwrap().is_empty());
    }
}

#[test]
fn root_terminal_requires_completed_synthesis_not_child_progress() {
    let pending = json!({
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "completed"},
            {"node_id": "tools", "kind": "tool_batch", "status": "running"}
        ]}
    });
    assert_eq!(
        root_execution_terminal_state(&pending),
        RootExecutionTerminal::Pending
    );

    let completed = json!({
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "completed"},
            {"node_id": "synthesis", "kind": "synthesize", "status": "completed"}
        ]}
    });
    assert_eq!(
        root_execution_terminal_state(&completed),
        RootExecutionTerminal::Completed
    );
}

#[test]
fn root_terminal_reports_terminal_failure_without_synthesis() {
    let failed = json!({
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "failed"}
        ]}
    });
    assert!(matches!(
        root_execution_terminal_state(&failed),
        RootExecutionTerminal::Failed(_)
    ));
}

#[test]
fn root_progress_fingerprint_tracks_streaming_output_without_graph_changes() {
    let first = json!({
        "revision": 7,
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "running"}
        ]},
        "live": {
            "revision": 11,
            "status": "calling_model",
            "output_bytes": 1024,
            "last_progress_at_ms": 100
        }
    });
    let second = json!({
        "revision": 7,
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "running"}
        ]},
        "live": {
            "revision": 12,
            "status": "calling_model",
            "output_bytes": 2048,
            "last_progress_at_ms": 200
        }
    });
    let first_statuses = root_node_statuses(&first);
    let second_statuses = root_node_statuses(&second);

    assert_eq!(first_statuses, second_statuses);
    assert_ne!(
        root_progress_fingerprint(&first, &first_statuses),
        root_progress_fingerprint(&second, &second_statuses)
    );
}

#[test]
fn blocked_team_is_terminal_for_waiting_but_unfinished_agent_is_not() {
    let blocked_teams = ProjectedTeamHealth {
        agent_count: 12,
        completed_agents: 12,
        failed_agents: 0,
        team_count: 4,
        completed_teams: 1,
        failed_teams: 2,
        pending_tasks: 0,
        completion_verdict_pending: false,
    };
    assert!(!blocked_teams.has_pending_work());
    assert!(!blocked_teams.satisfies(1));

    let live_agent = ProjectedTeamHealth {
        agent_count: 12,
        completed_agents: 11,
        failed_agents: 0,
        team_count: 4,
        completed_teams: 1,
        failed_teams: 0,
        pending_tasks: 1,
        completion_verdict_pending: false,
    };
    assert!(live_agent.has_pending_work());
}

#[test]
fn descendant_live_terminal_never_forces_root_terminal_polling() {
    let child = ExecutionLiveObservation {
        fingerprint: "child".to_string(),
        summary: json!({
            "execution_id": "child-execution",
            "live_status": "complete",
        }),
        response_body_bytes: 1,
    };
    assert!(!live_terminal_belongs_to_root(&child, "root-execution"));

    let root = ExecutionLiveObservation {
        fingerprint: "root".to_string(),
        summary: json!({
            "execution_id": "root-execution",
            "live_status": "complete",
        }),
        response_body_bytes: 1,
    };
    assert!(live_terminal_belongs_to_root(&root, "root-execution"));
}

#[test]
fn team_acceptance_does_not_pass_without_a_real_projection_team_or_agents() {
    let answer =
        "runtime memory gateway event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
    let receipts = json!({"evidence": [
        {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
        {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
    ]});
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(answer, &receipts, &[], None, "root");
    assert!(!result.passed);
    let program = agentic_program_projection("verified", true);
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 2,
        minimum_claimed_cross_team_edges: 1,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(receipts.clone()),
        &[json!({
            "execution_id": "root",
            "revision": 1,
            "activities": [
                {"agent_instance_id":"agent-a","team_run_id":"team-research","status":"completed","started_at_ms":1,"completed_at_ms":10},
                {"agent_instance_id":"agent-b","team_run_id":"team-synthesis","status":"completed","started_at_ms":2,"completed_at_ms":8}
            ]
        })],
        Some(&program),
        "root",
    );
    assert!(result.passed);
}

#[test]
fn architecture_acceptance_rejects_failed_team_even_when_prose_claims_evidence() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs；但无法确认，因为没有任何文件内容的读取证据";
    let mut program = agentic_program_projection("blocked", true);
    program.tasks.get_mut("task-b").expect("task").status = runtime::AgenticTaskStatus::Blocked;
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]}),
        &[json!({"execution_id":"root","revision":1,"activities":[]})],
        Some(&program),
        "root",
    );
    assert!(!result.passed);
}

#[test]
fn partial_team_is_terminal_unsuccessful_not_pending_work() {
    let mut program = agentic_program_projection("open", true);
    program.tasks.get_mut("task-b").expect("task").status = runtime::AgenticTaskStatus::Blocked;
    let health = projected_team_health(&program);

    assert_eq!(health.failed_teams, 1);
    assert!(!health.has_pending_work());
    assert!(!health.satisfies(1));
}

#[test]
fn architecture_quality_uses_durable_runtime_evidence_not_response_language() {
    let quality = architecture_quality(
        &json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]}),
        &[json!({
            "execution_id": "root",
            "revision": 3,
        })],
        None,
    );
    assert_eq!(quality.score, quality.required);
}

#[test]
fn large_scale_presentation_gate_rejects_old_concatenated_terminal() {
    let old = "team-runtime: # Verified Team evidence bundle\nRuntime delivery facts: 2/2\n[truncated]\n并发波次、关键瓶颈、失效模式、容量边界、扩大规模：Op";
    let checks = large_scale_presentation_checks(old);
    assert!(checks
        .iter()
        .any(|check| check["name"] == "presentation_transport_clean" && check["passed"] == false));
    assert!(checks
        .iter()
        .any(|check| check["name"] == "presentation_complete_ending" && check["passed"] == false));
}

#[test]
fn large_scale_presentation_gate_accepts_complete_synthesized_terminal() {
    let response = format!("## 已验证事实\n{}\n\n12/12 目标源码已完整读取到 EOF。\n12/12 目标源码已由两个不同 Agent 身份独立完整读取到 EOF。\n\n## 源码推断\n边界推断。\n\n## 未执行的模拟\n本次未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩容结论\n\n判定：适合在当前单节点边界内继续扩大协作规模，但横向扩展必须先完成持久层分片。", LARGE_SCALE_SOURCE_PATHS[..6].join(" "));
    assert!(large_scale_presentation_checks(&response)
        .iter()
        .all(|check| check["passed"] == true));
}

#[test]
fn scale_recommendation_requires_subject_and_decision_in_the_same_semantic_block() {
    assert!(!has_scale_recommendation("## 扩大规模结论\n结论完整。"));
    assert!(!has_scale_recommendation("- 扩容建议"));
    assert!(!has_scale_recommendation(
        "当前系统需要扩容。\n\n建议先完成持久层分片。"
    ));
    assert!(has_scale_recommendation(
        "暂不建议扩容，需先消除恢复串行瓶颈。"
    ));
    assert!(has_scale_recommendation(
        "The system is suitable to scale out, but must shard the event store first."
    ));
    assert!(!has_scale_recommendation(
        "## 扩大规模\n结论待定。\n\n## 其他事项\n系统适合当前工作。"
    ));
}

#[test]
fn observed_qwen_scale_conclusion_is_semantically_complete() {
    let observed = "判定：适合在当前单节点边界内继续扩大协作规模，但横向扩展存在明确架构前提。\n\n- 扩容建议：在单节点内可继续增加 Team/角色规模；若需跨节点横向扩展，应先行引入事件存储分片或 Postgres 后端。";
    assert!(has_scale_recommendation(observed));
}

#[test]
fn observed_deepseek_scale_section_is_semantically_complete() {
    let observed = "## 9. 是否适合继续扩大规模\n\n**结论：适合，以中等规模为当前安全边界。** 各层容量独立成闸；但在进一步扩大前应处理四项结构性项。";
    assert!(has_scale_recommendation(observed));
}

#[test]
fn large_scale_presentation_gate_rejects_positive_phrase_with_coverage_failure() {
    let response =
        "12/12 目标源码已完整读取到 EOF。源码完整覆盖维度：未通过；不能将本次任务判定为完全通过。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_complete_source_coverage" && check["passed"] == false
    }));
}

#[test]
fn large_scale_presentation_gate_rejects_independent_review_contradiction() {
    let response = "12/12 目标源码已完整读取到 EOF。12/12 目标源码已由两个不同 Agent 身份独立完整读取到 EOF。但 reviewer 未独立重读源码。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_independent_source_review" && check["passed"] == false
    }));
}

#[test]
fn complete_source_receipt_gate_requires_attested_exact_content_for_every_target() {
    fn receipt(path: &str, sequence: u64) -> Value {
        json!({
            "observed_at_sequence": sequence,
            "tool_name": "read_file",
            "target": {
                "kind": "workspace",
                "scope": {
                    "access_mode": "read",
                    "coverage": "exact_content",
                    "path": {
                        "workspace_relative_path": path,
                        "observed_revision_or_digest": "a".repeat(64),
                    }
                }
            }
        })
    }

    let complete = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .enumerate()
        .map(|(index, path)| receipt(path, index as u64 + 1))
        .collect::<Vec<_>>();
    assert_eq!(
        complete_exact_source_receipt_paths(
            &json!({"observed_acceptance": {"observed_evidence": complete}}),
            &[],
        )
        .len(),
        LARGE_SCALE_SOURCE_PATHS.len()
    );

    let mut incomplete = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .take(11)
        .enumerate()
        .map(|(index, path)| receipt(path, index as u64 + 1))
        .collect::<Vec<_>>();
    let mut bounded = receipt(LARGE_SCALE_SOURCE_PATHS[11], 12);
    bounded["target"]["scope"]["coverage"] = json!("scoped_content");
    incomplete.push(bounded);
    let observed = complete_exact_source_receipt_paths(
        &json!({"observed_acceptance": {"observed_evidence": incomplete}}),
        &[],
    );
    assert_eq!(observed.len(), 11);
    assert!(!observed.contains(LARGE_SCALE_SOURCE_PATHS[11]));
}

#[test]
fn independent_source_review_gate_requires_distinct_role_receipts_for_every_target() {
    fn receipt(path: &str, sequence: u64, role: &str) -> Value {
        json!({
            "observed_at_sequence": sequence,
            "tool_name": "read_file",
            "target": {
                "kind": "workspace",
                "scope": {
                    "access_mode": "read",
                    "coverage": "exact_content",
                    "path": {
                        "workspace_relative_path": path,
                        "observed_revision_or_digest": "b".repeat(64),
                    }
                }
            },
            "evidence_ref": {
                "evidence_ref": {
                    "id": format!("agent-tool:team-graph:team-a:{role}:1:1:{sequence}:read_file:digest:read-receipt")
                }
            }
        })
    }

    let mut receipts = Vec::new();
    for (index, path) in LARGE_SCALE_SOURCE_PATHS.iter().enumerate() {
        receipts.push(receipt(path, index as u64 * 2 + 1, "team-a-investigator"));
        receipts.push(receipt(path, index as u64 * 2 + 2, "team-a-reviewer"));
    }
    assert_eq!(
        independently_reviewed_complete_source_receipt_paths(
            &json!({"observed_acceptance": {"observed_evidence": receipts}}),
            &[],
        )
        .len(),
        LARGE_SCALE_SOURCE_PATHS.len()
    );

    let investigator_only = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .enumerate()
        .map(|(index, path)| receipt(path, index as u64 + 1, "investigator"))
        .collect::<Vec<_>>();
    assert!(independently_reviewed_complete_source_receipt_paths(
        &json!({"observed_acceptance": {"observed_evidence": investigator_only}}),
        &[],
    )
    .is_empty());
    assert_eq!(
            receipt_agent_identity(
                "agent-tool:team-graph:program:team-a:0:role-a5684f8888daf18c:1:1:2:read_file:digest:read-receipt"
            ),
            Some("agent-tool:team-graph:program:team-a:0:role-a5684f8888daf18c")
        );
    assert!(receipt_agent_identity(
        "agent-tool:graph:role-a5684f8888daf18c:not-a-slot:1:2:read_file:receipt"
    )
    .is_none());

    let duplicate_reads_from_one_agent = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .enumerate()
        .flat_map(|(index, path)| {
            [
                receipt(path, index as u64 * 2 + 1, "role-a5684f8888daf18c"),
                receipt(path, index as u64 * 2 + 2, "role-a5684f8888daf18c"),
            ]
        })
        .collect::<Vec<_>>();
    assert!(independently_reviewed_complete_source_receipt_paths(
        &json!({"observed_acceptance": {"observed_evidence": duplicate_reads_from_one_agent}}),
        &[],
    )
    .is_empty());
}

#[test]
fn group_theory_gate_requires_exact_reads_from_the_unique_sink_team() {
    fn receipt(path: &str, sequence: u64, semantic_team_id: &str) -> Value {
        json!({
            "observed_at_sequence": sequence,
            "tool_name": "read_file",
            "target": {
                "kind": "workspace",
                "scope": {
                    "access_mode": "read",
                    "coverage": "exact_content",
                    "path": {
                        "workspace_relative_path": path,
                        "observed_revision_or_digest": "d".repeat(64),
                    }
                }
            },
            "evidence_ref": {
                "evidence_ref": {
                    "id": format!("agent-tool:team-graph:program:{semantic_team_id}:0:role:1:1:{sequence}:read_file:digest:read-receipt")
                }
            }
        })
    }

    let observed = |receipt_team: &str| {
        let receipts = GROUP_THEORY_SOURCE_PATHS
            .iter()
            .enumerate()
            .map(|(index, path)| receipt(path, index as u64 + 1, receipt_team))
            .collect::<Vec<_>>();
        json!({"observed_acceptance": {"observed_evidence": receipts}})
    };
    let program = agentic_program_projection("verified", true);
    let terminal = terminal_semantic_team_ids(&program);
    assert_eq!(terminal, BTreeSet::from(["team-synthesis".to_string()]));
    assert!(complete_exact_source_receipt_paths_for_semantic_teams(
        &observed("team-research"),
        &[],
        &terminal,
    )
    .is_empty());
    assert_eq!(
        complete_exact_source_receipt_paths_for_semantic_teams(
            &observed("team-synthesis"),
            &[],
            &terminal,
        )
        .len(),
        GROUP_THEORY_SOURCE_PATHS.len()
    );
}

#[test]
fn source_receipt_gate_rejects_acquisition_receipts_not_promoted_to_agent_acceptance() {
    let raw_receipt = json!({
        "observed_at_sequence": 1,
        "tool_name": "read_file",
        "target": {
            "kind": "workspace",
            "scope": {
                "access_mode": "read",
                "coverage": "exact_content",
                "path": {
                    "workspace_relative_path": LARGE_SCALE_SOURCE_PATHS[0],
                    "observed_revision_or_digest": "c".repeat(64),
                }
            }
        },
        "evidence_ref": {
            "evidence_ref": {
                "id": "agent-tool:team-graph:team-a:reviewer:1:1:1:read_file:digest:read-receipt"
            }
        }
    });

    assert!(complete_exact_source_receipt_paths(
        &json!({"durable_tool_receipts": [raw_receipt]}),
        &[],
    )
    .is_empty());
}

#[test]
fn large_scale_presentation_gate_rejects_receipt_only_content_review_caveat() {
    let response = "## 已验证事实\ncrates/runtime/src/agentic/program.rs crates/runtime/src/agentic/action_service.rs crates/runtime/src/agentic/execution.rs crates/runtime/src/conversation/host.rs crates/runtime/src/execution_core/services.rs crates/runtime/src/recovery/runtime_event_reactor.rs\n\n12/12 目标源码已完整读取到 EOF。\n12/12 目标源码已由两个不同 Agent 身份独立完整读取到 EOF。\n\n## 源码推断\nreviewer 仅在收据层级确认，正文未保留，内容级复核未完成。\n\n## 未执行的模拟\n本次未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_independent_source_review" && check["passed"] == false
    }));
}

#[test]
fn large_scale_transport_gate_allows_generic_source_identifier_examples() {
    let response = "## 已验证事实\ncrates/runtime/src/agentic/program.rs\n\n源码中的通用图标识格式为 `team-graph:{team_id}`。\n\n## 源码推断\n推断。\n\n## 未执行的模拟\n未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_transport_clean" && check["passed"] == true
    }));
}

#[test]
fn projected_team_health_uses_only_agent_first_task_facts() {
    let program = agentic_program_projection("verified", true);
    let health = projected_team_health(&program);

    assert!(health.satisfies(2));
    assert_eq!(health.team_count, 2);
    assert_eq!(health.completed_teams, 2);
    assert_eq!(health.agent_count, 3);
    assert_eq!(health.completed_agents, 3);
}

#[test]
fn team_acceptance_waits_for_running_descendant_work() {
    let mut program = agentic_program_projection("open", true);
    let pending = program.tasks.get_mut("task-b").expect("task");
    pending.status = runtime::AgenticTaskStatus::Published;
    pending.claimant = None;
    pending.reviewed_by = None;
    let health = projected_team_health(&program);

    assert!(
        health.has_pending_work(),
        "unclaimed Published Task can still run"
    );
    let mut completion_requested = agentic_program_projection("completion_requested", true);
    completion_requested.tasks.values_mut().for_each(|task| {
        task.status = runtime::AgenticTaskStatus::Accepted;
    });
    let completion_health = projected_team_health(&completion_requested);
    assert!(
        completion_health.has_pending_work(),
        "supervisor verdict remains a live Program transition"
    );
    assert!(LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .requires_descendant_team_closure());
    assert!(!LiveAcceptance::ArchitectureQuality {
        minimum_teams: 0,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .requires_descendant_team_closure());
}

#[test]
fn architecture_acceptance_requires_accepted_cross_team_dependencies() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
    let receipts = json!({"evidence": [
        {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
        {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
    ]});
    let program = agentic_program_projection("verified", true);
    assert_eq!(accepted_cross_team_dependency_count(&program), 1);
    let projection = json!({"execution_id":"root","revision":1,"activities":[
        {"agent_instance_id":"agent-a","team_run_id":"team-research","status":"completed","started_at_ms":1,"completed_at_ms":10},
        {"agent_instance_id":"agent-b","team_run_id":"team-synthesis","status":"completed","started_at_ms":2,"completed_at_ms":8}
    ]});
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 2,
        minimum_claimed_cross_team_edges: 1,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(receipts),
        &[projection],
        Some(&program),
        "root",
    );

    assert!(result.passed);
}

#[test]
fn architecture_acceptance_does_not_reject_durable_execution_for_hallucinated_paths_in_prose() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/not-a-real-module/src/memory.rs";
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 0,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]})),
        &[json!({
            "execution_id": "root",
            "revision": 1,
        })],
        None,
        "root",
    );
    assert!(result.passed);
}

#[test]
fn source_path_extraction_stops_at_cjk_punctuation_before_explanation() {
    let paths = source_paths(
        "证据：`crates/runtime/src/lib.rs`：模块注释说明职责；另见 crates/memory/src/lib.rs。",
    );
    assert_eq!(
        paths,
        BTreeSet::from([
            "crates/memory/src/lib.rs".to_string(),
            "crates/runtime/src/lib.rs".to_string(),
        ])
    );
}

#[test]
fn tool_acceptance_rejects_answer_without_runtime_evidence() {
    let result =
        tool_acceptance().evaluate("Cargo.toml", &json!({"events": []}), &[], None, "root");
    assert!(!result.passed);
}

#[test]
fn tool_acceptance_requires_completed_runtime_receipt_and_succeeded_outcome() {
    let result = tool_acceptance().evaluate(
        "Cargo.toml version 0.9.712",
        &successful_root_outcome_timeline(json!({"events": [completed_tool_event()]})),
        &[],
        None,
        "root",
    );
    assert!(result.passed);

    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [{
            "kind": "tool.invocation.failed",
            "status": "failed",
            "payload": {
                "status": "failed",
                "invocation_id": "tool-invocation-1",
                "tool_call_id": "tool-call-1",
                "tool_name": "read_file"
            }
        }]})),
        &[],
        None,
        "root",
    );
    assert!(!result.passed);

    let mut wrong_tool = completed_tool_event();
    wrong_tool["payload"]["tool_name"] = json!("glob_search");
    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [wrong_tool]})),
        &[],
        None,
        "root",
    );
    assert!(!result.passed, "an unrelated successful tool must not pass");

    let mut wrong_target = completed_tool_event();
    wrong_target["payload"]["input_preview"] = json!("{\"path\":\"README.md\"}");
    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [wrong_target]})),
        &[],
        None,
        "root",
    );
    assert!(
        !result.passed,
        "a successful read of the wrong target must not pass"
    );
}

#[test]
fn provider_metadata_and_zero_tool_count_are_not_live_tool_evidence() {
    let result = tool_acceptance().evaluate(
        "I read Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [{
                "kind": "provider.request.packed",
                "payload": {"capabilities": {"tool_calls": "supported/configured"}}
            }, {"tool_calls": 0}]})),
        &[json!({"usage": [{"detail": {"tool_calls": 0}}]})],
        None,
        "root",
    );
    assert!(
        !result.passed,
        "metadata and a declared zero count must never validate a claimed tool run"
    );
}

#[test]
fn partial_failed_or_missing_root_outcome_fails_closed() {
    for (status, class) in [("failed", "failed"), ("partial", "partial")] {
        let timeline = json!({"events": [
            completed_tool_event(),
            {
                "kind": "runtime.outcome.recorded.v1",
                "status": status,
                "payload": {
                    "identity": {"execution_graph_ref": "root"},
                    "terminal": {"class": class}
                }
            }
        ]});
        let result = tool_acceptance().evaluate("Cargo.toml", &timeline, &[], None, "root");
        assert!(!result.passed, "{status}/{class} must fail closed");
    }

    let missing = tool_acceptance().evaluate(
        "Cargo.toml",
        &json!({"events": [completed_tool_event()]}),
        &[],
        None,
        "root",
    );
    assert!(!missing.passed);
}

#[test]
fn scenario_metrics_sum_only_canonical_token_usage_records() {
    let timeline = json!({
        "token_speed": {
            "token_usage": [
                {"input": 10, "output": 5, "cache_create": 2, "cache_read": 3},
                {"input": 7, "output": 11, "cache_create": 0, "cache_read": 4}
            ],
            "model_telemetry": {
                "first_token_latency_ms": 125,
                "wall_tokens_per_second": 42.5,
                "active_tokens_per_second": 56.0
            }
        },
        "tool_summary": {"count": 2},
        "team_session": {"runtime_run_count": 2}
    });
    let metrics = scenario_metrics(
        &timeline,
        &[json!({"agents": [{"id":"agent"}], "teams": [{"id":"team"}]})],
        None,
        Duration::from_secs(2),
    );

    assert_eq!(metrics["input_tokens"], 17);
    assert_eq!(metrics["output_tokens"], 16);
    assert_eq!(metrics["cache_creation_input_tokens"], 2);
    assert_eq!(metrics["cache_read_input_tokens"], 7);
    assert_eq!(metrics["cache_tokens"], 9);
    assert_eq!(metrics["total_tokens"], 42);
    assert_eq!(metrics["token_usage_records"], 2);
    assert_eq!(metrics["tool_calls"], 2);
    assert_eq!(metrics["model_rounds"], 2);
    assert_eq!(metrics["first_token_latency_ms"], 125);
    assert_eq!(metrics["wall_tokens_per_second"], 42.5);
}

#[test]
fn scenario_metrics_prefer_unique_provider_attempt_outcomes_and_exact_prefix_evidence() {
    let packed = |request_id: &str, prompt: u64, reusable: u64, cold: bool| {
        json!({
            "kind": "context.provider_request_packed",
            "payload": {
                "type": "ProviderRequestPacked",
                "request_id": request_id,
                "model": "deepseek-v4-flash",
                "cache_identity_sha256": "cohort-a",
                "model_visible_prompt_bytes": prompt,
                "reusable_prefix_bytes": reusable,
                "exact_prefix_extension": reusable > 0,
                "cache_cold_leader": cold,
                "waited_for_cache_warmup": !cold
            }
        })
    };
    let outcome = |request_id: &str, miss: u64, read: u64| {
        json!({
            "kind": "context.provider_attempt_outcome",
            "payload": {
                "type": "ProviderAttemptOutcome",
                "request_id": request_id,
                "terminal_status": "completed",
                "usage_status": "known",
                "usage": {
                    "input_tokens": miss,
                    "output_tokens": 2,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": read
                }
            }
        })
    };
    let first_outcome = outcome("request-1", 100, 0);
    let timeline = json!({
        "session_events": [
            packed("request-1", 1000, 0, true),
            first_outcome.clone(),
            packed("request-2", 1100, 1000, false),
            outcome("request-2", 10, 90),
            first_outcome
        ],
        "team_session": {"runtime_run_count": 2},
        "token_speed": {"token_usage": [{"input": 9999, "output": 9999}]}
    });
    let metrics = scenario_metrics(&timeline, &[], None, Duration::from_secs(1));

    assert_eq!(metrics["input_tokens"], 110);
    assert_eq!(metrics["cache_read_input_tokens"], 90);
    assert_eq!(metrics["provider_attempt_count"], 2);
    assert_eq!(metrics["provider_attempt_usage_unknown_count"], 0);
    assert_eq!(metrics["cache_cold_leader_count"], 1);
    assert_eq!(metrics["cache_waiter_count"], 1);
    assert_eq!(metrics["structural_reuse_ratio_bp"], 4_761);
    assert_eq!(metrics["warm_structural_reuse_ratio_bp"], 9_090);
}

#[test]
fn scenario_metrics_aggregate_deduplicated_root_and_child_graph_usage() {
    let root = json!({
        "graph": {
            "graph_id": "root",
            "nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 0}},
                {"node_id": "tool", "kind": "tool_batch", "status": "completed", "usage": {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0, "tool_calls": 1}},
                {"node_id": "agent", "kind": "agent_task", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 1}},
                {"node_id": "synthesis", "kind": "synthesize", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 1}},
                {"node_id": "child", "kind": "subgraph", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 1}}
            ]
        }
    });
    let child = json!({
        "graph": {
            "graph_id": "child",
            "nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 13, "output_tokens": 5, "cached_tokens": 1, "tool_calls": 0}}
            ]
        }
    });
    let metrics = scenario_metrics(
        &json!({"token_speed": {"token_usage": []}}),
        &[root.clone(), child, root],
        None,
        Duration::from_secs(2),
    );

    assert_eq!(metrics["input_tokens"], 34);
    assert_eq!(metrics["output_tokens"], 13);
    assert_eq!(metrics["cache_creation_input_tokens"], 0);
    assert_eq!(metrics["cache_read_input_tokens"], 4);
    assert_eq!(metrics["cache_tokens"], 4);
    assert_eq!(metrics["tool_calls"], 1);
    assert_eq!(metrics["model_rounds"], 2);
    assert_eq!(metrics["token_usage_records"], 3);
    assert_eq!(metrics["effective_models"], json!(["deepseek-v4-flash"]));
}

#[test]
fn scenario_metrics_preserve_agent_first_population_after_terminal_cleanup() {
    let program = agentic_program_projection("verified", true);
    let metrics = scenario_metrics(
        &json!({"token_speed": {"token_usage": []}}),
        &[],
        Some(&program),
        Duration::from_secs(1),
    );

    assert_eq!(metrics["agent_count"], 3);
    assert_eq!(metrics["team_count"], 2);
    assert_eq!(metrics["accepted_agentic_task_count"], 2);
}

#[test]
fn live_metric_summary_uses_observed_scenario_values() {
    let metrics = aggregate_scenario_metrics(&[
        json!({"metrics": {
            "input_tokens": 10,
            "output_tokens": 2,
            "cache_tokens": 3,
            "total_tokens": 15,
            "model_rounds": 1,
            "tool_calls": 0,
            "agent_count": 0,
            "team_count": 0,
            "wall_ms": 100,
            "first_token_latency_ms": 40
        }}),
        json!({"metrics": {
            "input_tokens": 20,
            "output_tokens": 5,
            "cache_tokens": 0,
            "total_tokens": 25,
            "model_rounds": 2,
            "tool_calls": 3,
            "agent_count": 4,
            "team_count": 1,
            "wall_ms": 300,
            "first_token_latency_ms": 80
        }}),
    ]);

    assert_eq!(metrics["total_tokens"], 40);
    assert_eq!(metrics["model_rounds"], 3);
    assert_eq!(metrics["tool_calls"], 3);
    assert_eq!(metrics["max_agent_count"], 4);
    assert_eq!(metrics["max_team_count"], 1);
    assert_eq!(metrics["wall_ms"]["p95"], 300);
    assert_eq!(metrics["first_token_latency_ms"]["min"], 40);
}

#[test]
fn collaboration_comparison_uses_public_child_team_evidence_not_root_metrics() {
    let comparison = collaboration_comparison(&[
        json!({
            "scenario_id": "live_single_architecture_baseline",
            "metrics": {"wall_ms": 100},
            "acceptance": {"quality": {"score": 9}}
        }),
        json!({
            "scenario_id": "live_team_projection",
            "status": "passed",
            // Root graph only: the actual Team Agents run in a child
            // graph and are represented by the public acceptance check.
            "metrics": {"agent_count": 0, "wall_ms": 200},
            "acceptance": {
                "quality": {"score": 9},
                "checks": [{
                    "name": "accepted_agent_first_teams",
                    "passed": true,
                    "engaged_agents": 3,
                    "accepted_agents": 3,
                    "teams": 3,
                    "completed_teams": 3
                }, {
                    "name": "accepted_cross_team_dependencies",
                    "passed": true,
                    "observed": 2
                }]
            }
        }),
    ]);

    assert_eq!(comparison["status"], "passed");
    assert_eq!(comparison["team_capability"]["passed"], true);
}

#[test]
fn live_timeout_is_complexity_aware_and_not_default_capped() {
    let direct = LiveScenarioTimeout::direct().with_cap(None);
    let team = LiveScenarioTimeout::team().with_cap(None);
    assert!(team.nominal_wait > direct.nominal_wait);
    assert!(team.absolute_wait > direct.absolute_wait);

    let capped = team.with_cap(Some(Duration::from_secs(600)));
    assert_eq!(capped.nominal_wait, Duration::from_secs(600));
    assert_eq!(capped.absolute_wait, Duration::from_secs(600));
    assert_eq!(capped.inactivity_wait, Duration::from_secs(600));

    // An accidentally tiny operator cap cannot make the team scenario
    // fail before it has had one normal progress window.
    assert_eq!(
        team.with_cap(Some(Duration::from_secs(30))).absolute_wait,
        team.absolute_wait
    );

    let sixteen = LiveScenarioTimeout::large_scale(16);
    let twenty_four = LiveScenarioTimeout::large_scale(24);
    assert!(sixteen.absolute_wait > sixteen.nominal_wait);
    assert!(twenty_four.absolute_wait > sixteen.absolute_wait);
}

#[test]
fn first_provider_response_uses_the_full_complexity_deadline() {
    let team = LiveScenarioTimeout::team();
    assert!(!team.should_abort_for_no_progress(Duration::from_secs(1_799), 0));
    assert!(team.should_abort_for_no_progress(Duration::from_secs(1_800), 0));
    assert!(!team.should_abort_for_no_progress(Duration::from_secs(1_800), 1));
    assert!(
        !team.should_abort_for_inactivity(Duration::from_secs(181), Duration::from_secs(181), 0,),
        "a submitted user message is not provider progress"
    );
    assert!(team.should_abort_for_inactivity(
        Duration::from_secs(361),
        Duration::from_secs(601),
        1,
    ));
}
