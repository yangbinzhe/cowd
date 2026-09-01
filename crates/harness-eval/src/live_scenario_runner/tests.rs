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
    assert_eq!(template.schema_version, 1);
    assert_eq!(template.scenario_id, AUTONOMOUS_DEEPSEEK_SCENARIO_ID);
    assert_eq!(template.output_path, AUTONOMOUS_DEEPSEEK_OUTPUT_PATH);
    for prompt in [&template.prompt_16, &template.prompt_24] {
        assert!(prompt.contains("propose_work"));
        assert!(prompt.contains("bid/claim"));
        assert!(prompt.contains("challenge"));
        assert!(prompt.contains("A→D、B→D、C→D"));
        assert!(prompt.contains(AUTONOMOUS_DEEPSEEK_OUTPUT_PATH));
    }
    assert_eq!(
        harness_contract::strategy::explicit_team_count(&template.prompt_16),
        4,
        "16-Agent role and WorkItem counts must not contaminate Team cardinality"
    );
    assert_eq!(
        harness_contract::strategy::explicit_team_count(&template.prompt_24),
        4,
        "24-Agent role and WorkItem counts must not contaminate Team cardinality"
    );
}

fn autonomous_projection(reread_verified: bool) -> Value {
    json!({
        "revision": 3,
        "agents": [{"id": "agent-a", "status": "completed"}],
        "teams": [{"id": "team-a", "status": "completed"}],
        "activities": [{
            "activity_id": "activity:discussion:challenge-a",
            "kind": "discussion",
            "status": "completed"
        }],
        "graph": {
            "graph_id": "team-graph-a",
            "nodes": [{
                "node_id": "work-a",
                "work": {
                    "output_artifact_kinds": ["research", "analysis", "simulation", "report"]
                },
                "work_state": {
                    "status": "accepted",
                    "claim": {"claimant_instance_id": "agent-a"},
                    "review_findings": ["revise the counterexample"],
                    "reviews": [{
                        "reviewer_instance_id": "reviewer-a",
                        "verdict": "challenge",
                        "submission_ref": "artifact://work-a-v1"
                    }]
                }
            }],
            "autonomous_work": [{
                "work_id": "agent-work-a",
                "work": {
                    "proposed_by": "agent-proposer",
                    "output_artifact_kinds": ["research"]
                },
                "state": {
                    "status": "accepted",
                    "claim": {"claimant_instance_id": "agent-a"},
                    "bids": [{
                        "bidder_instance_id": "agent-a",
                        "rationale": "independent capability match"
                    }],
                    "reviews": [{
                        "reviewer_instance_id": "reviewer-b",
                        "verdict": "accept",
                        "submission_ref": "team-board:agent-work-a"
                    }]
                }
            }],
            "orchestration": {"collaboration_program": {"edges": [{
                "edge_id": "edge-a",
                "state": "claimed",
                "delivery_receipt": {"receipt_id": "delivery-a"},
                "claim_receipt": {"receipt_id": "claim-a"}
            }]}}
        },
        "delivery_envelope": {"workspace_materializations": [{
            "receipt_id": "materialize-a",
            "target_path": AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
            "sha256": "a".repeat(64),
            "bytes": 4096,
            "reread_verified": reread_verified
        }]}
    })
}

#[test]
fn autonomy_metrics_deduplicate_runtime_facts_and_fail_closed_on_reread() {
    let projection = autonomous_projection(true);
    let metrics = autonomy_metrics(
        &[projection.clone(), projection],
        AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
    );
    assert_eq!(metrics.work_items, 2);
    assert_eq!(metrics.proposed_work_items, 1);
    assert_eq!(metrics.distinct_proposers, 1);
    assert_eq!(metrics.bid_count, 1);
    assert_eq!(metrics.distinct_bidders, 1);
    assert_eq!(metrics.accepted_work_items, 2);
    assert_eq!(metrics.claimed_work_items, 2);
    assert_eq!(metrics.autonomous_claimed_work_items, 1);
    assert_eq!(metrics.distinct_claimants, 1);
    assert_eq!(metrics.review_count, 2);
    assert_eq!(metrics.distinct_reviewers, 2);
    assert_eq!(metrics.challenge_findings, 1);
    assert_eq!(metrics.challenged_work_items, 1);
    assert_eq!(metrics.unresolved_challenged_work_items, 0);
    assert_eq!(metrics.discussions, 1);
    assert_eq!(metrics.output_artifact_kinds, 4);
    assert_eq!(metrics.output_materializations, 1);

    let internal_provider_graph = json!({
        "graph": {
            "graph_id": "execution-graph-provider-turn",
            "nodes": (0..20).map(|index| json!({
                "node_id": format!("model-work-{index}"),
                "work": {"output_artifact_kinds": ["internal_model_step"]},
                "work_state": {"status": "accepted"}
            })).collect::<Vec<_>>()
        }
    });
    let scoped = autonomy_metrics(
        &[autonomous_projection(true), internal_provider_graph],
        AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
    );
    assert_eq!(scoped.work_items, 2);
    assert_eq!(scoped.output_artifact_kinds, 4);

    let missing_reread = autonomy_metrics(
        &[autonomous_projection(false)],
        AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
    );
    assert_eq!(missing_reread.output_materializations, 0);
}

#[test]
fn autonomous_acceptance_requires_claim_review_discussion_edge_and_materialization() {
    let acceptance = LiveAcceptance::AutonomousCollaboration {
        minimum_teams: 1,
        minimum_agents: 1,
        minimum_work_items: 1,
        minimum_proposals: 1,
        minimum_bids: 1,
        minimum_claims: 1,
        minimum_reviews: 1,
        minimum_challenges: 1,
        minimum_cross_team_edges: 1,
        minimum_discussions: 1,
        output_path: AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
    };
    let response = format!("已提交 {AUTONOMOUS_DEEPSEEK_OUTPUT_PATH}");
    let timeline = successful_root_outcome_timeline(json!({}));
    assert!(
        acceptance
            .evaluate(&response, &timeline, &[autonomous_projection(true)], "root",)
            .passed
    );
    assert!(
        !acceptance
            .evaluate(
                &response,
                &timeline,
                &[autonomous_projection(false)],
                "root",
            )
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
    .evaluate(
        answer,
        &receipts,
        &[json!({"agents": [], "teams": []})],
        "root",
    );
    assert!(!result.passed);
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(receipts.clone()),
        &[json!({
            "revision": 1,
            "agents": [
                {"id": "agent-1", "status": "completed"},
                {"id": "agent-2", "status": "completed"},
                {"id": "agent-3", "status": "completed"}
            ],
            "teams": [{"id": "team-1", "status": "completed"}],
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_program": {"edges": []}}
            }
        })],
        "root",
    );
    assert!(result.passed);
}

#[test]
fn architecture_acceptance_rejects_failed_team_even_when_prose_claims_evidence() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs；但无法确认，因为没有任何文件内容的读取证据";
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
        &[json!({
            "revision": 1,
            "agents": [
                {"status": "completed"},
                {"status": "failed"},
                {"status": "blocked"}
            ],
            "teams": [{"status": "failed"}],
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_program": {"edges": []}}
            }
        })],
        "root",
    );
    assert!(!result.passed);
}

#[test]
fn partial_team_is_terminal_unsuccessful_not_pending_work() {
    let health = projected_team_health(&[json!({
        "agents": [{"id": "agent-1", "status": "completed"}],
        "teams": [{"id": "team-1", "status": "partial"}],
    })]);

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
            "revision": 3,
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_program": {"edges": []}}
            }
        })],
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
    let response = "## 已验证事实\n`crates/runtime/src/orchestration/mod.rs` `crates/runtime/src/orchestration/compiler.rs` `crates/runtime/src/team/instantiation.rs` `crates/runtime/src/conversation/host.rs` `crates/runtime/src/execution_core/services.rs` `crates/runtime/src/recovery/runtime_event_reactor.rs`\n\n12/12 目标源码已完整读取到 EOF。\n12/12 目标源码已由 investigator 与 reviewer 独立完整读取到 EOF。\nE/F 结构化交接已完整消费。\n\n## 源码推断\n边界推断。\n\n## 未执行的模拟\n本次未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩容结论\n\n判定：适合在当前单节点边界内继续扩大协作规模，但横向扩展必须先完成持久层分片。";
    assert!(large_scale_presentation_checks(response)
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
fn large_scale_presentation_gate_rejects_topology_only_handoff() {
    let response = "## 已验证事实\n`crates/runtime/src/orchestration/mod.rs` `crates/runtime/src/orchestration/compiler.rs` `crates/runtime/src/team/instantiation.rs` `crates/runtime/src/conversation/host.rs` `crates/runtime/src/execution_core/services.rs` `crates/runtime/src/recovery/runtime_event_reactor.rs`\n\nTeam E 未能看到 Team A/B 的结构化结果。\n\n## 源码推断\n推断。\n\n## 未执行的模拟\n未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_cross_team_handoff_consumed" && check["passed"] == false
    }));
}

#[test]
fn large_scale_presentation_gate_rejects_negated_handoff_claim() {
    let response = "## 已验证事实\n`crates/runtime/src/orchestration/mod.rs` `crates/runtime/src/orchestration/compiler.rs` `crates/runtime/src/team/instantiation.rs` `crates/runtime/src/conversation/host.rs` `crates/runtime/src/execution_core/services.rs` `crates/runtime/src/recovery/runtime_event_reactor.rs`\n\nF 未能消费完整上游，因此 E/F 结构化交接已完整消费不能被确认。\n\n## 源码推断\n推断。\n\n## 未执行的模拟\n未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_cross_team_handoff_consumed" && check["passed"] == false
    }));
}

#[test]
fn large_scale_presentation_gate_rejects_positive_phrase_with_coverage_failure() {
    let response = "## 已验证事实\n`crates/runtime/src/orchestration/mod.rs` `crates/runtime/src/orchestration/compiler.rs` `crates/runtime/src/team/instantiation.rs` `crates/runtime/src/conversation/host.rs` `crates/runtime/src/execution_core/services.rs` `crates/runtime/src/recovery/runtime_event_reactor.rs`\n\n12/12 目标源码已完整读取到 EOF。\nE/F 结构化交接已完整消费。\n源码完整覆盖维度：未通过；不能将本次任务判定为完全通过。\n\n## 源码推断\n推断。\n\n## 未执行的模拟\n未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_complete_source_coverage" && check["passed"] == false
    }));
}

#[test]
fn large_scale_presentation_gate_rejects_independent_review_contradiction() {
    let response = "## 已验证事实\n`crates/runtime/src/orchestration/mod.rs` `crates/runtime/src/orchestration/compiler.rs` `crates/runtime/src/team/instantiation.rs` `crates/runtime/src/conversation/host.rs` `crates/runtime/src/execution_core/services.rs` `crates/runtime/src/recovery/runtime_event_reactor.rs`\n\n12/12 目标源码已完整读取到 EOF。\n12/12 目标源码已由 investigator 与 reviewer 独立完整读取到 EOF。\nE/F 结构化交接已完整消费。\n但 reviewer 未独立重读源码。\n\n## 源码推断\n推断。\n\n## 未执行的模拟\n未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
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

    let projection = |receipt_team: &str| {
        let receipts = GROUP_THEORY_SOURCE_PATHS
            .iter()
            .enumerate()
            .map(|(index, path)| receipt(path, index as u64 + 1, receipt_team))
            .collect::<Vec<_>>();
        json!({
            "agents": [
                {"id": "a", "status": "completed"},
                {"id": "b", "status": "completed"},
                {"id": "c", "status": "completed"},
                {"id": "d", "status": "completed"}
            ],
            "teams": [
                {"id": "team-a", "status": "completed"},
                {"id": "team-b", "status": "completed"},
                {"id": "team-c", "status": "completed"},
                {"id": "team-d", "status": "completed"}
            ],
            "observed_acceptance": {"observed_evidence": receipts},
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_program": {
                    "edges": [
                        {"from": "team-a:1", "to": "team-d:1", "edge_id": "a-d", "state": "claimed", "delivery_receipt": {}, "claim_receipt": {}},
                        {"from": "team-b:1", "to": "team-d:1", "edge_id": "b-d", "state": "claimed", "delivery_receipt": {}, "claim_receipt": {}},
                        {"from": "team-c:1", "to": "team-d:1", "edge_id": "c-d", "state": "claimed", "delivery_receipt": {}, "claim_receipt": {}}
                    ],
                    "team_instances": [
                        {"instance_id": "team-a:1", "semantic_node_id": "team-a", "required": true},
                        {"instance_id": "team-b:1", "semantic_node_id": "team-b", "required": true},
                        {"instance_id": "team-c:1", "semantic_node_id": "team-c", "required": true},
                        {"instance_id": "team-d:1", "semantic_node_id": "team-d", "required": true}
                    ]
                }}
            }
        })
    };
    let response = format!(
        "C4 研究 调研 分析 处理 模拟 {}",
        GROUP_THEORY_SOURCE_PATHS.join(" ")
    );
    let acceptance = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 4,
        minimum_claimed_cross_team_edges: 3,
        evidence_profile: ArchitectureEvidenceProfile::GroupTheoryFinalSynthesis,
    };

    let predecessor_only =
        acceptance.evaluate(&response, &json!({}), &[projection("team-a")], "root");
    assert!(predecessor_only.checks.iter().any(|check| {
        check["name"] == "runtime_attested_terminal_team_source_review"
            && check["passed"] == false
            && check["observed"] == 0
    }));

    let sink_review = acceptance.evaluate(&response, &json!({}), &[projection("team-d")], "root");
    assert!(sink_review.checks.iter().any(|check| {
        check["name"] == "runtime_attested_terminal_team_source_review"
            && check["passed"] == true
            && check["observed"] == GROUP_THEORY_SOURCE_PATHS.len()
    }));
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
    let response = "## 已验证事实\n`crates/runtime/src/orchestration/mod.rs` `crates/runtime/src/orchestration/compiler.rs` `crates/runtime/src/team/instantiation.rs` `crates/runtime/src/conversation/host.rs` `crates/runtime/src/execution_core/services.rs` `crates/runtime/src/recovery/runtime_event_reactor.rs`\n\n12/12 目标源码已完整读取到 EOF。\n12/12 目标源码已由 investigator 与 reviewer 独立完整读取到 EOF。\nE/F 结构化交接已完整消费。\n\n## 源码推断\nreviewer 仅在收据层级确认，正文未保留，内容级复核未完成。\n\n## 未执行的模拟\n本次未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_independent_source_review" && check["passed"] == false
    }));
}

#[test]
fn large_scale_transport_gate_allows_generic_source_identifier_examples() {
    let response = "## 已验证事实\n`crates/runtime/src/orchestration/mod.rs` `crates/runtime/src/orchestration/compiler.rs` `crates/runtime/src/team/instantiation.rs` `crates/runtime/src/conversation/host.rs` `crates/runtime/src/execution_core/services.rs` `crates/runtime/src/recovery/runtime_event_reactor.rs`\n\n源码中的通用图标识格式为 `team-graph:{team_id}`。E/F 结构化交接已完整消费。\n\n## 源码推断\n推断。\n\n## 未执行的模拟\n未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_transport_clean" && check["passed"] == true
    }));
}

#[test]
fn projected_team_health_uses_child_team_task_displays() {
    let root = json!({
        "execution_id": "root",
        "agents": [],
        "teams": [{"id": "team-1", "status": "completed"}],
    });
    let child = json!({
        "execution_id": "team-graph:team-1",
        "teams": [{
            "id": "team-1",
            "status": "completed",
            "detail": {"tasks": [
                {"run_id": "researcher-1", "status": "completed"},
                {"run_id": "researcher-2", "status": "completed"},
                {"run_id": "researcher-3", "status": "completed"},
                {"run_id": "synthesizer-1", "status": "completed"}
            ]}
        }],
    });

    let health = projected_team_health(&[root, child]);

    assert!(health.satisfies(1));
    assert_eq!(health.team_count, 1);
    assert_eq!(health.completed_teams, 1);
    assert_eq!(health.agent_count, 4);
    assert_eq!(health.completed_agents, 4);
}

#[test]
fn projected_team_health_accepts_completed_single_role_teams() {
    let health = projected_team_health(&[json!({
        "agents": [{"id": "agent-1", "status": "completed"}],
        "teams": [{"id": "team-1", "status": "completed"}],
    })]);

    assert!(health.satisfies(1));
}

#[test]
fn team_acceptance_waits_for_running_descendant_work() {
    let health = projected_team_health(&[json!({
        "agents": [
            {"id": "agent-complete", "status": "completed"},
            {"id": "agent-running", "status": "running"}
        ],
        "teams": [
            {"id": "team-complete", "status": "completed"},
            {"id": "team-running", "status": "running"}
        ]
    })]);

    assert!(health.has_pending_work());
    assert!(LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .requires_descendant_team_closure());
    assert!(LiveAcceptance::EscalatedTeam {
        minimum_teams: 3,
        minimum_escalations: 1,
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
fn architecture_acceptance_requires_claimed_fan_in_for_multi_team_merge() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
    let receipts = json!({"evidence": [
        {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
        {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
    ]});
    let projection = json!({
        "revision": 1,
        "agents": [
            {"id": "a-1", "status": "completed"}, {"id": "a-2", "status": "completed"},
            {"id": "b-1", "status": "completed"}, {"id": "b-2", "status": "completed"},
            {"id": "c-1", "status": "completed"}, {"id": "c-2", "status": "completed"}
        ],
        "teams": [
            {"id": "team-a", "status": "completed"},
            {"id": "team-b", "status": "completed"},
            {"id": "team-c", "status": "completed"}
        ],
        "graph": {
            "graph_id": "root",
            "orchestration": {"collaboration_program": {"edges": [
                {"edge_id": "a-to-c", "state": "claimed", "delivery_receipt": {}, "claim_receipt": {}},
                {"edge_id": "b-to-c", "state": "claimed", "delivery_receipt": {}, "claim_receipt": {}}
            ]}}
        }
    });

    assert_eq!(claimed_cross_team_edge_count(&[projection.clone()]), 2);
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 3,
        minimum_claimed_cross_team_edges: 2,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(receipts),
        &[projection],
        "root",
    );

    assert!(result.passed);
}

#[test]
fn escalation_acceptance_requires_a_durable_applied_agent_receipt() {
    let projection = json!({
        "agents": [
            {"id": "a-1", "status": "completed"}, {"id": "a-2", "status": "completed"},
            {"id": "b-1", "status": "completed"}, {"id": "b-2", "status": "completed"},
            {"id": "c-1", "status": "completed"}, {"id": "c-2", "status": "completed"}
        ],
        "teams": [
            {"id": "team-a", "status": "completed"},
            {"id": "team-b", "status": "completed"},
            {"id": "team-c", "status": "completed"}
        ],
        "graph": {
            "graph_id": "root",
            "orchestration": {"collaboration_escalations": [
                {"escalation_id": "attested-add-team", "applied_graph_revision": 4}
            ]}
        }
    });

    assert_eq!(applied_escalation_count(&[projection.clone()]), 1);
    let result = LiveAcceptance::EscalatedTeam {
        minimum_teams: 3,
        minimum_escalations: 1,
    }
    .evaluate(
        "completed with durable evidence",
        &successful_root_outcome_timeline(json!({})),
        &[projection],
        "root",
    );

    assert!(result.passed);
}

#[test]
fn escalation_acceptance_rejects_unapplied_or_missing_receipts() {
    let projection = json!({
        "agents": [
            {"id": "a-1", "status": "completed"}, {"id": "a-2", "status": "completed"},
            {"id": "b-1", "status": "completed"}, {"id": "b-2", "status": "completed"},
            {"id": "c-1", "status": "completed"}, {"id": "c-2", "status": "completed"}
        ],
        "teams": [
            {"id": "team-a", "status": "completed"},
            {"id": "team-b", "status": "completed"},
            {"id": "team-c", "status": "completed"}
        ],
        "graph": {
            "graph_id": "root",
            "orchestration": {"collaboration_escalations": [
                {"escalation_id": "uncommitted-add-team", "applied_graph_revision": 0}
            ]}
        }
    });

    let result = LiveAcceptance::EscalatedTeam {
        minimum_teams: 3,
        minimum_escalations: 1,
    }
    .evaluate(
        "completed with durable evidence",
        &Value::Null,
        &[projection],
        "root",
    );

    assert!(!result.passed);
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
            "revision": 1,
            "agents": [],
            "teams": [],
            "graph": {
                "graph_id": "root",
                "orchestration": {"collaboration_program": {"edges": []}}
            }
        })],
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
    let result = tool_acceptance().evaluate("Cargo.toml", &json!({"events": []}), &[], "root");
    assert!(!result.passed);
}

#[test]
fn tool_acceptance_requires_completed_runtime_receipt_and_succeeded_outcome() {
    let result = tool_acceptance().evaluate(
        "Cargo.toml version 0.9.712",
        &successful_root_outcome_timeline(json!({"events": [completed_tool_event()]})),
        &[],
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
        "root",
    );
    assert!(!result.passed);

    let mut wrong_tool = completed_tool_event();
    wrong_tool["payload"]["tool_name"] = json!("glob_search");
    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [wrong_tool]})),
        &[],
        "root",
    );
    assert!(!result.passed, "an unrelated successful tool must not pass");

    let mut wrong_target = completed_tool_event();
    wrong_target["payload"]["input_preview"] = json!("{\"path\":\"README.md\"}");
    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [wrong_target]})),
        &[],
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
        let result = tool_acceptance().evaluate("Cargo.toml", &timeline, &[], "root");
        assert!(!result.passed, "{status}/{class} must fail closed");
    }

    let missing = tool_acceptance().evaluate(
        "Cargo.toml",
        &json!({"events": [completed_tool_event()]}),
        &[],
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
        Duration::from_secs(2),
    );

    assert_eq!(metrics["input_tokens"], 17);
    assert_eq!(metrics["output_tokens"], 16);
    assert_eq!(metrics["cache_tokens"], 9);
    assert_eq!(metrics["total_tokens"], 42);
    assert_eq!(metrics["token_usage_records"], 2);
    assert_eq!(metrics["tool_calls"], 2);
    assert_eq!(metrics["model_rounds"], 2);
    assert_eq!(metrics["first_token_latency_ms"], 125);
    assert_eq!(metrics["wall_tokens_per_second"], 42.5);
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
        Duration::from_secs(2),
    );

    assert_eq!(metrics["input_tokens"], 34);
    assert_eq!(metrics["output_tokens"], 13);
    assert_eq!(metrics["cache_tokens"], 4);
    assert_eq!(metrics["tool_calls"], 1);
    assert_eq!(metrics["model_rounds"], 2);
    assert_eq!(metrics["token_usage_records"], 3);
    assert_eq!(metrics["effective_models"], json!(["deepseek-v4-flash"]));
}

#[test]
fn scenario_metrics_preserve_completed_agent_population_after_terminal_cleanup() {
    let projection = json!({
        "agents": [],
        "teams": [{
            "id": "team-a",
            "status": "completed",
            "detail": {
                "tasks": [
                    {"run_id": "investigator", "status": "completed"},
                    {"run_id": "reviewer", "status": "completed"}
                ]
            }
        }]
    });

    let metrics = scenario_metrics(
        &json!({"token_speed": {"token_usage": []}}),
        &[projection],
        Duration::from_secs(1),
    );

    assert_eq!(metrics["agent_count"], 2);
    assert_eq!(metrics["team_count"], 1);
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
                    "name": "completed_evidence_team",
                    "passed": true,
                    "agents": 6,
                    "completed_agents": 6,
                    "teams": 3,
                    "completed_teams": 3
                }, {
                    "name": "claimed_cross_team_edges",
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
