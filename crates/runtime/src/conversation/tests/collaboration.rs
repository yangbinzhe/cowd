    #[test]
    fn automatic_team_downgrades_when_no_relevant_bounded_scope_exists() {
        let root = tempfile::tempdir().expect("focus workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime")).expect("runtime scope");
        assert!(bounded_workspace_focus_scopes(
            root.path(),
            "inspect a frontend webui that is not in this workspace",
            2,
            false,
            false,
        )
        .is_empty());
    }

    #[test]
    fn runtime_control_and_todo_updates_do_not_lock_the_workspace() {
        let scopes = resource_scopes_for_tool_calls(&[
            ModelToolCall {
                id: "todo".into(),
                name: "todo_write".into(),
                input: r#"{"todos":[]}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "team".into(),
                name: "runtime_orchestrate".into(),
                input: r#"{"intent":"review","operation":"propose","proposal":{"mutation_id":"review","nodes":[{"node_id":"review-team","recipe":"team","objective":"review"}],"reason":"review as a team"}}"#.into(),
                depends_on: Vec::new(),
            },
        ]);

        assert!(scopes.is_empty());
    }

    #[test]
    fn coverage_collapses_broad_discovery_but_keeps_direct_files_distinct() {
        let discovery = tool_batch_coverage_keys(&[
            ModelToolCall {
                id: "snapshot".into(),
                name: "workspace_snapshot".into(),
                input: r#"{"include_files":true}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "glob".into(),
                name: "glob_search".into(),
                input: r#"{"pattern":"**/*.rs"}"#.into(),
                depends_on: Vec::new(),
            },
        ]);
        assert_eq!(
            discovery,
            BTreeSet::from(["discovery:workspace".to_string()])
        );

        let direct = tool_batch_coverage_keys(&[
            ModelToolCall {
                id: "runtime".into(),
                name: "read_file".into(),
                input: r#"{"file_path":"/work/crates/runtime/src/lib.rs"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "memory".into(),
                name: "read_file".into(),
                input: r#"{"file_path":"/work/crates/memory/src/lib.rs"}"#.into(),
                depends_on: Vec::new(),
            },
        ]);
        assert_eq!(direct.len(), 2);
        assert!(direct.contains("evidence:read_file:crates/runtime/src/lib.rs"));
        assert!(direct.contains("evidence:read_file:crates/memory/src/lib.rs"));
    }

    #[test]
    fn bounded_scope_coverage_collapses_related_files_to_component_zone() {
        let scopes = tool_batch_scope_keys(&[
            ModelToolCall {
                id: "runtime-lib".into(),
                name: "read_file".into(),
                input: r#"{"file_path":"/work/crates/runtime/src/lib.rs"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "runtime-session".into(),
                name: "read_file".into(),
                input: r#"{"file_path":"/work/crates/runtime/src/session/session.rs"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "memory".into(),
                name: "read_file".into(),
                input: r#"{"file_path":"/work/crates/memory/src/lib.rs"}"#.into(),
                depends_on: Vec::new(),
            },
        ]);

        assert_eq!(
            scopes,
            BTreeSet::from(["crates/memory".to_string(), "crates/runtime".to_string(),])
        );
    }

    #[test]
    fn runtime_orchestration_isolated_after_workspace_tool_batch() {
        let calls = vec![
            ModelToolCall {
                id: "read".into(),
                name: "read_file".into(),
                input: r#"{"file_path":"Cargo.toml"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "team".into(),
                name: "runtime_orchestrate".into(),
                input: r#"{"intent":"review","operation":"propose","proposal":{"mutation_id":"review","nodes":[{"node_id":"review-team","recipe":"team","objective":"review"}],"reason":"review as a team"}}"#.into(),
                depends_on: Vec::new(),
            },
        ];

        let batches = tool_batches_for_turn(&calls).expect("batches");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0][0].name, "read_file");
        assert_eq!(batches[1][0].name, "runtime_orchestrate");
        assert_eq!(
            resource_scopes_for_tool_calls(&batches[0]),
            vec!["read:Cargo.toml"]
        );
        assert!(resource_scopes_for_tool_calls(&batches[1]).is_empty());
    }

    #[test]
    fn managed_agent_escalation_runs_after_its_source_evidence_batch() {
        let calls = vec![
            ModelToolCall {
                id: "source".into(),
                name: "read_file".into(),
                input: r#"{\"path\":\"crates/runtime/src/lib.rs\"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "escalate".into(),
                name: "request_collaboration_escalation".into(),
                input: r#"{\"reason\":\"independent verification\",\"requested_add_team\":{}}"#
                    .into(),
                // Provider dependency declarations are optional and cannot be
                // relied on for this Runtime-owned receipt fence.
                depends_on: Vec::new(),
            },
        ];

        let batches = tool_batches_for_turn(&calls).expect("batches");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0][0].name, "read_file");
        assert_eq!(batches[1][0].name, "request_collaboration_escalation");
        assert!(batches[1][0].depends_on.is_empty());
    }

    #[test]
    fn only_semantic_team_proposal_consumes_the_turn_collaboration_lease() {
        let request_team = ModelToolCall {
            id: "team".into(),
            name: "runtime_orchestrate".into(),
            input: r#"{"intent":"review","operation":"propose","proposal":{"mutation_id":"review","nodes":[{"node_id":"review-team","recipe":"team","objective":"review"}],"reason":"review as a team"}}"#.into(),
            depends_on: Vec::new(),
        };
        let inspect = ModelToolCall {
            id: "inspect".into(),
            name: "runtime_orchestrate".into(),
            input: r#"{"intent":"inspect current runtime","operation":"inspect"}"#.into(),
            depends_on: Vec::new(),
        };
        let ordinary = ModelToolCall {
            id: "read".into(),
            name: "read_file".into(),
            input: r#"{"path":"Cargo.toml"}"#.into(),
            depends_on: Vec::new(),
        };
        let team_named_ordinary_tool = ModelToolCall {
            id: "board".into(),
            name: "team_board".into(),
            input: r#"{"operation":"inspect"}"#.into(),
            depends_on: Vec::new(),
        };

        assert!(requests_team_orchestration(std::slice::from_ref(
            &request_team
        )));
        assert!(!requests_team_orchestration(std::slice::from_ref(&inspect)));
        assert!(!requests_team_orchestration(std::slice::from_ref(
            &ordinary
        )));
        assert!(requests_runtime_orchestration(std::slice::from_ref(
            &inspect
        )));
        assert!(!requests_runtime_orchestration(std::slice::from_ref(
            &ordinary
        )));
        assert!(!requests_runtime_orchestration(std::slice::from_ref(
            &team_named_ordinary_tool
        )));
    }

    #[test]
    fn capability_receipt_advances_root_control_plane_to_proposal_only() {
        let capability = ModelToolCall {
            id: "catalog".to_string(),
            name: "runtime_capabilities".to_string(),
            input: r#"{"detail":"team_templates"}"#.to_string(),
            depends_on: Vec::new(),
        };
        let phase = root_control_plane_phase_after_tool_batch(
            RootControlPlanePhase::CapabilityOrProposal,
            std::slice::from_ref(&capability),
            &BTreeSet::from([capability.id.clone()]),
        );
        assert_eq!(phase, RootControlPlanePhase::ProposalOnly);
        assert_eq!(
            phase.required_tool_choice(),
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID
        );
        assert_eq!(
            RootControlPlanePhase::CapabilityOrProposal.required_tool_choice(),
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
            "an explicit Team request cannot be satisfied by catalog inspection alone"
        );
    }

    #[test]
    fn root_collaboration_instruction_makes_team_cardinality_and_custom_role_boundary_explicit() {
        let instruction = root_collaboration_decision_instruction(
            3,
            &[],
            harness_contract::policy::PermissionMode::ReadOnly,
        );
        assert!(instruction.contains("exactly 3 `workstreams`"));
        assert!(instruction.contains("one workstream is one proposed Team"));
        assert!(instruction.contains("nonempty `team.team_key`"));
        assert!(instruction.contains("team.roles"));
        assert!(instruction.contains("only [read, search]"));
        assert!(instruction.contains("JSON objects tagged by `kind`"));
        assert!(instruction.contains("Do not send a template"));
        assert!(instruction.contains("Do not split roles from one requested Team"));
        assert!(instruction.contains("submit_collaboration_decision"));
    }

    #[test]
    fn root_collaboration_scope_guard_accepts_exact_named_sources_and_rejects_substitution() {
        let required = vec![
            "read:crates/runtime/src/conversation/host.rs".to_string(),
            "read:crates/harness-contract/src/orchestration.rs".to_string(),
        ];
        let call = |evidence_contract: &str| ModelToolCall {
            id: "proposal".to_string(),
            name: harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID
                .to_string(),
            input: format!(
                r#"{{"schema_version":2,"decision_id":"source-coverage","intent":"team","workstreams":[{{"workstream_id":"source","objective":"read named source","team":{{"team_key":"source-team","roles":[{{"role_id":"reader","responsibility":"read source","required_capabilities":["read"]}}]}},"evidence_contract":[{evidence_contract}],"managed_agent_escalation":"none"}}],"reason":"test"}}"#
            ),
            depends_on: Vec::new(),
        };
        assert_eq!(
            missing_root_collaboration_evidence_scopes(
                &[call(
                    r#"{"kind":"evidence_scope","operation":"read","resource":"crates/runtime/src/conversation/host.rs"},{"kind":"evidence_scope","operation":"read","resource":"crates/harness-contract/src/orchestration.rs"}"#,
                )],
                &required,
            ),
            None
        );
        assert_eq!(
            missing_root_collaboration_evidence_scopes(
                &[call(
                    r#"{"kind":"evidence_scope","operation":"read","resource":"cowd-test-evidence/workspace-check-v702.log"}"#
                )],
                &required,
            ),
            Some(required)
        );
    }

    #[test]
    fn completed_root_team_forces_a_text_only_presentation_turn() {
        let verified = BTreeSet::from(["runtime-team:audited".to_string()]);
        let obligation = test_collaboration_obligation(1);
        assert!(root_team_terminal_requires_text_only(
            false,
            Some(&obligation),
            &verified
        ));
        assert!(!root_team_terminal_requires_text_only(
            true,
            Some(&obligation),
            &verified
        ));
        assert!(!root_team_terminal_requires_text_only(
            false, None, &verified
        ));
        assert!(!root_team_terminal_requires_text_only(
            false,
            Some(&obligation),
            &BTreeSet::new()
        ));
    }

    #[test]
    fn only_a_successful_team_proposal_satisfies_root_control_plane_action() {
        let proposal = ModelToolCall {
            id: "team".to_string(),
            name: harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID
                .to_string(),
            input: r#"{"schema_version":2,"decision_id":"review","intent":"review","workstreams":[{"workstream_id":"review-team","objective":"review","team":{"team_key":"review-team","roles":[{"role_id":"reviewer","responsibility":"review","required_capabilities":["read"],"output_artifacts":["summary","evidence"]}],"result":{"required_artifacts":["summary","evidence"],"evidence_required":true,"synthesis_required":true}}}],"reason":"independent review"}"#.to_string(),
            depends_on: Vec::new(),
        };
        assert_eq!(
            root_control_plane_phase_after_tool_batch(
                RootControlPlanePhase::ProposalOnly,
                std::slice::from_ref(&proposal),
                &BTreeSet::new(),
            ),
            RootControlPlanePhase::ProposalOnly,
            "failed or absent tool results must not unlock ordinary tools"
        );
        assert_eq!(
            root_control_plane_phase_after_tool_batch(
                RootControlPlanePhase::ProposalOnly,
                std::slice::from_ref(&proposal),
                &BTreeSet::from([proposal.id.clone()]),
            ),
            RootControlPlanePhase::ProposalSubmitted
        );
    }

    #[test]
    fn runtime_orchestration_dependency_runs_before_dependent_workspace_tools() {
        let calls = vec![
            ModelToolCall {
                id: "team".into(),
                name: "runtime_orchestrate".into(),
                input: r#"{"intent":"review","operation":"propose","proposal":{"mutation_id":"review","nodes":[{"node_id":"review-team","recipe":"team","objective":"review"}],"reason":"review as a team"}}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "read".into(),
                name: "read_file".into(),
                input: r#"{"file_path":"Cargo.toml"}"#.into(),
                depends_on: vec!["team".into()],
            },
        ];

        let batches = tool_batches_for_turn(&calls).expect("batches");
        assert_eq!(batches[0][0].name, "runtime_orchestrate");
        assert_eq!(batches[1][0].name, "read_file");
        assert!(batches[1][0].depends_on.is_empty());
    }

    #[test]
    fn uncommitted_transcript_entries_are_rolled_back_to_commit_boundary() {
        let mut messages = vec![
            ConversationMessage::user_text("committed"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "provider effect".to_string(),
            }]),
            ConversationMessage::tool_result("tool", "write", "done", false),
        ];
        messages.truncate(1);
        assert_eq!(messages, vec![ConversationMessage::user_text("committed")]);
    }

    #[test]
    fn turn_resolver_scope_requires_session_and_graph() {
        let ticket = NodeExecutionTicket {
            graph_id: "graph-a".to_string(),
            node_id: "node-a".to_string(),
            executor_kind: "inline_model".to_string(),
            service_class: harness_contract::execution_graph::ExecutionServiceClass::Interactive,
            attempt: 1,
            idempotency_key: "scope-test".to_string(),
            payload_ref: r#"{"session_id":"shared-session"}"#.to_string(),
        };

        assert!(turn_scope_matches(&ticket, "shared-session", "graph-a"));
        assert!(!turn_scope_matches(&ticket, "shared-session", "graph-b"));
        assert!(!turn_scope_matches(&ticket, "other-session", "graph-a"));
    }

    #[test]
    fn failed_tool_names_are_stable_and_deduplicated() {
        let messages = vec![
            ConversationMessage::tool_result("a", "runtime_orchestrate", "failed", true),
            ConversationMessage::tool_result("b", "runtime_orchestrate", "failed", true),
            ConversationMessage::tool_result("c", "read_file", "ok", false),
        ];
        assert_eq!(failed_tool_names(&messages), vec!["runtime_orchestrate"]);
    }

    #[test]
    fn retryable_semantic_receipt_is_identified_without_parsing_provider_prose() {
        let messages = vec![ConversationMessage::tool_result(
            "team",
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
            r#"runtime orchestration rejected: {"kind":"runtime_orchestration_rejected","recovery_hints":[{"code":"collaboration_compile_completion_terminal_role_missing","retryable":true}]}"#,
            true,
        )];
        assert_eq!(
            retryable_collaboration_compile_diagnostic(&messages).as_deref(),
            Some("collaboration_compile_completion_terminal_role_missing")
        );
        assert!(!has_admitted_program_receipt(&messages));
    }

    #[test]
    fn terminal_program_failure_is_not_model_repairable() {
        let messages = vec![ConversationMessage::tool_result(
            "team",
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
            r#"runtime orchestration blocked: {"kind":"runtime_orchestration_rejected","recovery_hints":[{"code":"collaboration_terminal_program_failed","retryable":false}],"collaboration_program":{"program_id":"program-1","lifecycle":"failed"}}"#,
            true,
        )];
        assert_eq!(retryable_collaboration_compile_diagnostic(&messages), None);
        assert!(has_admitted_program_receipt(&messages));
    }

    #[test]
    fn completed_program_is_also_an_admitted_program() {
        let messages = vec![ConversationMessage::tool_result(
            "team",
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
            r#"runtime orchestration completed: {"collaboration_program":{"program_id":"program-ok","lifecycle":"completed","required_team_count":1,"completed_required_instance_ids":["team:1"],"terminal_diagnostics":[]}}"#,
            false,
        )];
        assert!(has_admitted_program_receipt(&messages));
        assert!(has_completed_program_terminal(&messages));
    }

    #[test]
    fn evidence_saturation_converges_main_turns_without_child_aggressiveness() {
        assert_eq!(evidence_saturation_limit(true), 2);
        assert_eq!(evidence_saturation_limit(false), 3);

        let first = ModelToolCall {
            id: "read-a".into(),
            name: "read_file".into(),
            input: r#"{"path":"src/lib.rs","offset":0,"limit":80}"#.into(),
            depends_on: Vec::new(),
        };
        let second = ModelToolCall {
            id: "read-b".into(),
            name: "read_file".into(),
            input: r#"{"path":"src/lib.rs","offset":80,"limit":80}"#.into(),
            depends_on: Vec::new(),
        };
        assert_eq!(
            tool_batch_coverage_keys(&[first]),
            tool_batch_coverage_keys(&[second]),
            "offset-only rereads must count toward the bounded convergence threshold"
        );
    }

    #[test]
    fn required_write_gets_one_bounded_replan_before_read_only_synthesis() {
        assert!(should_recover_missing_required_write(
            true,
            false,
            true,
            &[],
            false,
            0,
        ));
        assert!(!should_recover_missing_required_write(
            true,
            false,
            true,
            &[],
            false,
            1,
        ));
        assert!(!should_recover_missing_required_write(
            true,
            false,
            true,
            &["src/lib.rs".into()],
            false,
            0,
        ));
        assert!(!should_recover_missing_required_write(
            true,
            true,
            true,
            &[],
            false,
            0,
        ));
    }

    #[test]
    fn tool_batch_fingerprint_ignores_provider_generated_call_ids() {
        let one = ModelToolCall {
            id: "provider-a".into(),
            name: "read_file".into(),
            input: r#"{\"path\":\"Cargo.toml\"}"#.into(),
            depends_on: Vec::new(),
        };
        let two = ModelToolCall {
            id: "provider-b".into(),
            ..one.clone()
        };
        assert_eq!(
            tool_batch_fingerprint(&[one]),
            tool_batch_fingerprint(&[two])
        );
    }

    #[test]
    fn capability_query_fingerprint_ignores_paraphrased_intent_but_respects_detail() {
        let first = ModelToolCall {
            id: "provider-a".into(),
            name: "runtime_capabilities".into(),
            input: r#"{"intent":"检查当前运行时能力"}"#.into(),
            depends_on: Vec::new(),
        };
        let paraphrased = ModelToolCall {
            id: "provider-b".into(),
            input: r#"{"intent":"请再告诉我有哪些团队能力"}"#.into(),
            ..first.clone()
        };
        let templates = ModelToolCall {
            id: "provider-c".into(),
            input: r#"{"intent":"查看团队","detail":"team_templates"}"#.into(),
            ..first.clone()
        };
        assert_eq!(
            tool_batch_fingerprint(&[first.clone()]),
            tool_batch_fingerprint(&[paraphrased])
        );
        assert_ne!(
            tool_batch_fingerprint(&[first]),
            tool_batch_fingerprint(&[templates])
        );
    }

    #[test]
    fn unusable_final_output_requires_one_governed_recovery() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("crates/runtime/src"))
            .expect("runtime source root");
        std::fs::write(
            workspace.path().join("crates/runtime/src/lib.rs"),
            "pub mod runtime;",
        )
        .expect("runtime source");
        assert_eq!(
            final_answer_recovery_reason("   ", workspace.path()),
            Some("empty final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason("<tool_call><function=read_file>", workspace.path()),
            Some("simulated tool-call markup in a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason(
                "Let me try once more to read the gateway sources:",
                workspace.path()
            ),
            Some("unfinished work preamble was presented as a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason(
                "Let me get the remaining critical evidence:",
                workspace.path()
            ),
            Some("unfinished work preamble was presented as a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason(
                "团队已创建但部分节点被阻塞。让我继续收集完整证据，同时查看可用的工具。",
                workspace.path()
            ),
            Some("unfinished work preamble was presented as a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason(
                "用 glob 查找 memory crate 中实际存在的文件：",
                workspace.path()
            ),
            Some("unfinished work preamble was presented as a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason(
                "Gateway 文件较大，需要小段读取。同时搜索 memory store trait 和 gateway session 核心。",
                workspace.path()
            ),
            Some("unfinished work preamble was presented as a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason(
                "让我尝试使用 execute_code 来获取完整文件内容。",
                workspace.path()
            ),
            Some("unfinished work preamble was presented as a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason(
                "<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"read_file\"></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>",
                workspace.path()
            ),
            Some("simulated tool-call markup in a final answer".to_string())
        );
        assert_eq!(
            final_answer_recovery_reason("evidence: crates/runtime/src/lib.rs", workspace.path()),
            None
        );
        assert_eq!(
            final_answer_recovery_reason(
                "evidence directory: crates/runtime/src/; file: crates/runtime/src/lib.rs",
                workspace.path()
            ),
            None,
            "directory references are not falsely validated as source files"
        );
        assert_eq!(
            final_answer_recovery_reason(
                "evidence: crates/runtime/src/missing.rs",
                workspace.path()
            ),
            None,
            "prose path existence is not an execution or evidence authority"
        );
        assert_eq!(
            strip_trailing_simulated_tool_markup(
                "Verified conclusion.\n<tool_call><function=read_file></function></tool_call>"
                    .to_string()
            ),
            "Verified conclusion."
        );
        assert_eq!(
            strip_trailing_simulated_tool_markup(
                "Verified conclusion.\n<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"read_file\"></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"
                    .to_string()
            ),
            "Verified conclusion."
        );
        assert_eq!(
            strip_trailing_simulated_tool_markup(
                "<tool_call><function=read_file></function></tool_call>".to_string()
            ),
            "<tool_call><function=read_file></function></tool_call>"
        );
        assert_eq!(
            strip_trailing_simulated_tool_markup(
                "Verified conclusion.\n<function=read_file><parameter=path>src/lib.rs".to_string()
            ),
            "Verified conclusion."
        );
    }

    #[test]
    fn structured_terminal_json_is_not_corrupted_by_prose_evidence_normalization() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("crates/runtime/src"))
            .expect("runtime source root");
        std::fs::write(workspace.path().join("crates/runtime/src/lib.rs"), "lib")
            .expect("runtime source");
        std::fs::write(workspace.path().join("crates/runtime/src/host.rs"), "host")
            .expect("host source");
        let json = r#"{"implementation":"done","source_verification":"crates/runtime/src/lib.rs"}"#;
        let tools = vec![ConversationMessage::tool_result(
            "read-host",
            "read_file",
            "verified crates/runtime/src/host.rs",
            false,
        )];

        assert_eq!(
            normalize_terminal_answer_with_evidence(
                json,
                &tools,
                workspace.path(),
                "审查当前 workspace 源代码并给出 source evidence",
            ),
            json
        );
    }

    #[test]
    fn delegated_focus_uses_its_own_terminal_contract_before_parent_aggregation() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("crates/memory/src"))
            .expect("memory source root");
        std::fs::write(workspace.path().join("crates/memory/src/lib.rs"), "lib")
            .expect("memory source");
        let role_result = r#"{"findings":"memory owns durable recall","evidence":"crates/memory/src/lib.rs","unresolved":"none"}"#;
        let parent_objective = "综合团队结论，并给出至少两个实际源码路径作为证据。";

        assert_eq!(
            final_answer_recovery_reason_for_execution_scope(
                role_result,
                workspace.path(),
                parent_objective,
                true,
            ),
            None,
            "a completed bounded role must not inherit aggregate evidence cardinality"
        );
        assert_eq!(
            final_answer_recovery_reason_for_execution_scope(
                role_result,
                workspace.path(),
                parent_objective,
                false,
            ),
            None,
            "parent evidence cardinality is verified from typed receipts, not model-authored prose paths"
        );
        assert!(
            final_answer_recovery_reason_for_execution_scope(
                "<tool_call><function=read_file></function></tool_call>",
                workspace.path(),
                parent_objective,
                true,
            )
            .is_some(),
            "delegated roles still retain terminal protocol safety checks"
        );
    }

    #[test]
    fn bounded_focus_converges_when_its_exact_evidence_contract_is_complete() {
        let read_scope = vec!["read:crates/runtime".to_string()];

        assert!(
            should_force_focus_synthesis(true, &read_scope, false, false, true),
            "a completed exact read contract must immediately enter text-only synthesis"
        );
        assert!(should_force_focus_synthesis(
            true,
            &["verify_upstream_change:crates/runtime".to_string()],
            false,
            false,
            true,
        ));
        assert!(
            should_force_focus_synthesis(true, &read_scope, false, true, true),
            "a retained terminal candidate must finalize as soon as its exact read is verified"
        );
        assert!(
            should_force_focus_synthesis(true, &read_scope, true, false, false),
            "a bounded read role must converge after repeated responsibility-zone saturation"
        );
        assert!(
            should_force_focus_synthesis(
                true,
                &["write:src/lib.rs".to_string()],
                false,
                false,
                true,
            ),
            "effect contracts must synthesize immediately after their exact obligation completes"
        );
        assert!(!should_force_focus_synthesis(
            false,
            &read_scope,
            true,
            true,
            true,
        ));
        assert!(!should_force_focus_synthesis(
            true,
            &["read:crates".to_string()],
            false,
            false,
            false,
        ));
    }

    #[test]
    fn delegated_write_obligation_comes_from_the_agent_focus_not_the_root_strategy() {
        assert!(!required_write_for_turn(
            true,
            true,
            &["read:evidence/source.rs".to_string()],
        ));
        assert!(required_write_for_turn(
            true,
            true,
            &[
                "read:evidence/source.rs".to_string(),
                "write:evidence/report.html".to_string(),
            ],
        ));
        assert!(required_write_for_turn(true, false, &[]));
    }

    #[test]
    fn successful_required_scope_closes_focus_even_when_a_sibling_tool_failed() {
        let required = vec!["network:*".to_string()];

        assert!(
            focus_acceptance_is_met(true, &required, &[]),
            "batch-level failures must not erase a successful scoped receipt"
        );
        assert!(!focus_acceptance_is_met(
            true,
            &required,
            &["network:*".to_string()]
        ));
        assert!(!focus_acceptance_is_met(false, &required, &[]));
    }

    #[test]
    fn runtime_replan_is_injected_as_private_system_guidance() {
        let intervention = RuntimeIntervention {
            goal_id: "goal".to_string(),
            kind: RuntimeInterventionKind::Replan,
            reason: "invoke write_file for the exact target".to_string(),
            evidence_refs: vec!["execution_node:model-1".to_string()],
            expected_graph_revision: None,
        };
        let item = runtime_replan_context_item("model-1", Some(&intervention))
            .expect("replan context item");

        assert_eq!(item.authority, ContextAuthority::System);
        assert_eq!(item.visibility, ContextVisibility::Private);
        assert!(item
            .content
            .contains("invoke write_file for the exact target"));
        assert_eq!(item.evidence, intervention.evidence_refs);
        assert!(runtime_replan_context_item("model-1", None).is_none());
    }

    #[test]
    fn delegated_mutation_rejects_repeated_reads_after_required_pre_read() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("fixtures")).expect("fixtures directory");
        std::fs::write(workspace.path().join("fixtures/target.txt"), "before\n")
            .expect("target fixture");
        let pending = vec!["write:fixtures/target.txt".to_string()];
        let observed = BTreeSet::from(["read:fixtures/target.txt".to_string()]);
        let reread = vec![ModelToolCall {
            id: "read-again".to_string(),
            name: "read_file".to_string(),
            input: r#"{"path":"fixtures/target.txt"}"#.to_string(),
            depends_on: Vec::new(),
        }];
        let write = vec![ModelToolCall {
            id: "write".to_string(),
            name: "write_file".to_string(),
            input: r#"{"path":"fixtures/target.txt","content":"after\n"}"#.to_string(),
            depends_on: Vec::new(),
        }];

        assert_eq!(
            pending_focus_write_action_violation(&pending, &observed, &reread, workspace.path(),),
            Some(pending.clone())
        );
        assert_eq!(
            pending_focus_write_action_violation(&pending, &observed, &write, workspace.path()),
            None
        );
    }

    #[test]
    fn completed_orchestration_receipt_requires_typed_team_presentation() {
        let calls = vec![ModelToolCall {
            id: "team-1".to_string(),
            name: "runtime_orchestrate".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        }];
        let receipt = serde_json::json!({
            "status": "completed",
            "terminal_summary": "Checked Team conclusion."
        })
        .to_string();
        let raw = vec![ConversationMessage::tool_result(
            "team-1",
            "runtime_orchestrate",
            receipt.clone(),
            false,
        )];
        let compacted = vec![ConversationMessage::tool_result(
            "team-1",
            "runtime_orchestrate",
            format!("durable evidence receipt: {receipt}"),
            false,
        )];

        assert!(completed_orchestration_terminal_summary(
            &calls,
            &raw,
            std::path::Path::new("."),
            false,
        )
        .is_none());
        assert!(completed_orchestration_terminal_summary(
            &calls,
            &compacted,
            std::path::Path::new("."),
            false,
        )
        .is_none());

        let invalid = vec![ConversationMessage::tool_result(
            "team-1",
            "runtime_orchestrate",
            serde_json::json!({
                "status": "completed",
                "terminal_summary": "Evidence: crates/does-not-exist/src/lib.rs"
            })
            .to_string(),
            false,
        )];
        assert!(completed_orchestration_terminal_summary(
            &calls,
            &invalid,
            std::path::Path::new("."),
            false,
        )
        .is_none());

        let workspace = tempfile::tempdir().expect("workspace");
        for path in ["crates/runtime/src/lib.rs", "crates/memory/src/lib.rs"] {
            let path = workspace.path().join(path);
            std::fs::create_dir_all(path.parent().expect("source parent")).expect("source parent");
            std::fs::write(path, "pub mod checked;").expect("source");
        }
        let evidenced = vec![ConversationMessage::tool_result(
            "team-1",
            "runtime_orchestrate",
            serde_json::json!({
                "status": "completed",
                "terminal_summary": "Evidence: crates/runtime/src/lib.rs and crates/memory/src/lib.rs"
            })
            .to_string(),
            false,
        )];
        assert!(completed_orchestration_terminal_summary(
            &calls,
            &evidenced,
            workspace.path(),
            true,
        )
        .is_none());

        let typed = vec![ConversationMessage::tool_result(
            "team-1",
            "runtime_orchestrate",
            serde_json::json!({
                "status": "completed",
                "working_state_verified": true,
                "terminal_summary": "Checked Team conclusion.",
                "execution": {"terminal_result_available": true},
                "delivery_envelope": {
                    "envelope_id": "team-envelope",
                    "revision": 3,
                    "objective_id": "root-objective",
                    "pipeline_status": "completed",
                    "delivery_status": "satisfied",
                    "created_at_ms": 10
                },
                "terminal_presentation": {
                    "presentation_id": "team-presentation",
                    "attempt_id": "team-attempt",
                    "envelope_id": "team-envelope",
                    "envelope_revision": 3,
                    "state": "validating",
                    "answer_origin": "team_synthesizer",
                    "generated_at_ms": 11
                }
            })
            .to_string(),
            false,
        )];
        assert_eq!(
            completed_orchestration_terminal_summary(&calls, &typed, workspace.path(), true)
                .as_deref(),
            Some("Checked Team conclusion."),
            "only a current typed TeamSynthesizer presentation may bypass the root narrator"
        );

        let typed_children = vec![ConversationMessage::tool_result(
            "team-1",
            "runtime_orchestrate",
            serde_json::json!({
                "status": "completed",
                "working_state_verified": true,
                "execution": {"terminal_result_available": true},
                "team_terminals": [
                    {
                        "team_id": "team-a",
                        "working_state_verified": true,
                        "terminal_summary": "First checked conclusion.",
                        "delivery_envelope": {
                            "envelope_id": "team-a-envelope",
                            "revision": 3,
                            "objective_id": "team-a-objective",
                            "pipeline_status": "completed",
                            "delivery_status": "satisfied",
                            "created_at_ms": 10
                        },
                        "terminal_presentation": {
                            "presentation_id": "team-a-presentation",
                            "attempt_id": "team-a-attempt",
                            "envelope_id": "team-a-envelope",
                            "envelope_revision": 3,
                            "state": "validating",
                            "answer_origin": "team_synthesizer",
                            "generated_at_ms": 11
                        }
                    },
                    {
                        "team_id": "team-b",
                        "working_state_verified": true,
                        "terminal_summary": "Second checked conclusion.",
                        "delivery_envelope": {
                            "envelope_id": "team-b-envelope",
                            "revision": 4,
                            "objective_id": "team-b-objective",
                            "pipeline_status": "completed",
                            "delivery_status": "satisfied",
                            "created_at_ms": 12
                        },
                        "terminal_presentation": {
                            "presentation_id": "team-b-presentation",
                            "attempt_id": "team-b-attempt",
                            "envelope_id": "team-b-envelope",
                            "envelope_revision": 4,
                            "state": "validating",
                            "answer_origin": "team_synthesizer",
                            "generated_at_ms": 13
                        }
                    }
                ]
            })
            .to_string(),
            false,
        )];
        let carrier = completed_orchestration_terminal_summary(
            &calls,
            &typed_children,
            workspace.path(),
            true,
        )
        .expect("all child Team carriers are verified");
        assert!(is_collaboration_evidence_carrier(&carrier));
        assert!(carrier.contains("team-a: First checked conclusion."));
        assert!(carrier.contains("team-b: Second checked conclusion."));
        assert_eq!(
            retained_orchestration_terminal_candidate(
                &typed_children,
                workspace.path(),
                "combine the checked Team conclusions",
            )
            .as_deref(),
            Some(carrier.as_str()),
            "a duplicate Team request must reuse the verified typed carrier as synthesis evidence"
        );
    }

    #[test]
    fn collaboration_quality_gate_rejects_explicit_truncation_and_missing_evidence() {
        let objective = "最终结论必须列出至少三个完整源码路径，明确区分已验证事实、源码推断与未执行的模拟，并给出并发波次、关键瓶颈、失效模式和容量边界。";
        let invalid = "## 已验证事实\n- crates/runtime/src/lib.rs\n- crates/memory/src/lib.rs\n[truncated]\n源码推断、未执行的模拟、并发波次、关键瓶颈、失效模式、容量边界：Op";
        let findings = collaboration_answer_quality_findings(invalid, objective);
        assert!(findings
            .iter()
            .any(|finding| finding.contains("[truncated]")));
        assert!(findings
            .iter()
            .any(|finding| finding.contains("requires at least 3")));
    }

    #[test]
    fn collaboration_quality_gate_accepts_complete_required_sections() {
        let objective = "最终结论必须列出至少三个完整源码路径，明确区分已验证事实、源码推断与未执行的模拟，并给出并发波次、关键瓶颈、失效模式和容量边界。";
        let answer = "## 已验证事实\n- `crates/runtime/src/lib.rs`\n- `crates/memory/src/lib.rs`\n- `crates/gateway/src/lib.rs`\n\n## 源码推断\n边界由事件连接。\n\n## 未执行的模拟\n本次未执行压力模拟。\n\n## 并发波次、关键瓶颈、失效模式与容量边界\n结论完整。";
        assert_eq!(
            collaboration_answer_quality_findings(answer, objective),
            Vec::<String>::new()
        );
    }

    #[test]
    fn collaboration_quality_gate_is_language_neutral_at_sentence_end() {
        let objective = "给出已验证事实。";
        assert!(collaboration_answer_quality_findings("已验证事实：结论可扩展", objective)
            .is_empty());
        assert!(collaboration_answer_quality_findings("Verified facts: scalable", "verified facts")
            .is_empty());
        assert!(collaboration_answer_quality_findings("```text\n未闭合", objective)
            .iter()
            .any(|finding| finding.contains("unclosed code fence")));
    }

    #[test]
    fn collaboration_quality_gate_enforces_only_explicit_verbatim_claims() {
        let objective = "普通引用“不是合同”；最终必须原样包含声明“12/12 已完成”，并原样给出“交接已消费”。";
        assert_eq!(
            required_verbatim_claims(objective),
            BTreeSet::from(["12/12 已完成".to_string(), "交接已消费".to_string()])
        );
        let findings = collaboration_answer_quality_findings("普通引用不是合同。", objective);
        assert_eq!(
            findings
                .iter()
                .filter(|finding| finding.contains("required verbatim claim"))
                .count(),
            2
        );
        assert!(collaboration_answer_quality_findings(
            "验收：12/12 已完成；交接已消费。",
            objective
        )
        .is_empty());
        assert!(required_verbatim_claims("术语“Program”只是普通引用。").is_empty());

        let acceptance_objective = "若事实成立，最终结论必须原样给出验收声明“E/F 结构化交接已完整消费”；还必须原样包含结构化覆盖声明“12/12 目标源码已完整读取到 EOF”和独立复核声明“12/12 目标源码已由 investigator 与 reviewer 独立完整读取到 EOF”。";
        assert_eq!(
            required_verbatim_claims(acceptance_objective),
            BTreeSet::from([
                "12/12 目标源码已完整读取到 EOF".to_string(),
                "12/12 目标源码已由 investigator 与 reviewer 独立完整读取到 EOF".to_string(),
                "E/F 结构化交接已完整消费".to_string(),
            ])
        );
    }

    #[test]
    fn collaboration_quality_gate_rejects_topology_without_semantic_handoff() {
        let objective = "Team E 必须实际消费 A/B 的完整结构化交接。";
        let invalid = "Team E 未能看到 Team A/B 的结构化结果。";
        let valid = "E/F 结构化交接已完整消费。";

        assert!(collaboration_answer_quality_findings(invalid, objective)
            .iter()
            .any(|finding| finding.contains("semantic handoff")));
        assert!(collaboration_answer_quality_findings(valid, objective).is_empty());
    }

    #[test]
    fn collaboration_quality_gate_rejects_negated_handoff_claim() {
        let objective =
            "必须实际消费上游结构化交接；若事实成立，原样声明 E/F 结构化交接已完整消费。";
        let invalid = "F 未能消费完整上游；因此 E/F 结构化交接已完整消费不能被确认。";

        assert!(collaboration_answer_quality_findings(invalid, objective)
            .iter()
            .any(|finding| finding.contains("semantic handoff")));
    }

    #[test]
    fn hierarchical_partition_never_slices_a_semantic_result() {
        let results = vec![
            "A".repeat(40),
            "B".repeat(40),
            "C".repeat(120),
            "D".repeat(20),
        ];
        let partitions = partition_complete_collaboration_results(results.clone(), 64);
        assert_eq!(partitions.concat(), results);
        assert!(partitions.iter().flatten().all(|result| {
            result
                .chars()
                .all(|character| matches!(character, 'A' | 'B' | 'C' | 'D'))
        }));
        assert!(partitions
            .iter()
            .any(|partition| partition == &vec!["C".repeat(120)]));
    }

    #[test]
    fn intermediate_quality_gate_requires_every_source_path_to_survive() {
        let source = "`crates/runtime/src/lib.rs` and `crates/memory/src/lib.rs`.";
        let incomplete = "Preserved `crates/runtime/src/lib.rs`.";
        assert!(
            collaboration_intermediate_quality_findings(incomplete, source)
                .iter()
                .any(|finding| finding.contains("crates/memory/src/lib.rs"))
        );
        let complete = "Preserved `crates/runtime/src/lib.rs` and `crates/memory/src/lib.rs`.";
        assert!(collaboration_intermediate_quality_findings(complete, source).is_empty());
    }

    #[test]
    fn terminal_presentation_does_not_mutate_citations_from_current_filesystem() {
        let workspace = tempfile::tempdir().expect("workspace");
        let answer = "Architecture conclusion.\nEvidence: crates/runtime/src/lib.rs\nPossible follow-up: crates/memory/src/store.rs";
        assert_eq!(
            final_answer_recovery_reason_for_objective(
                answer,
                workspace.path(),
                "给出至少两个实际源码路径作为证据",
            ),
            None
        );
        assert_eq!(
            normalize_terminal_answer_with_evidence(
                answer,
                &[],
                workspace.path(),
                "给出至少两个实际源码路径作为证据",
            ),
            answer
        );
    }

    #[test]
    fn focus_synthesis_receives_committed_tool_content_as_authoritative_context() {
        let calls = vec![ModelToolCall {
            id: "read-runtime".to_string(),
            name: "read_file".to_string(),
            input: r#"{"path":"crates/runtime/src/lib.rs"}"#.to_string(),
            depends_on: Vec::new(),
        }];
        let messages = vec![ConversationMessage::tool_result(
            "read-runtime",
            "read_file",
            r#"Tool `read_file` completed. Evidence: tool://runtime-source. {"file":{"filePath":"crates/runtime/src/lib.rs","content":"pub mod conversation;"}}"#,
            false,
        )];

        let item = focus_synthesis_evidence_context_item(
            "tools-1",
            &calls,
            &messages,
            &["findings".to_string()],
        )
        .expect("Focus evidence packet");

        assert_eq!(item.authority, ContextAuthority::System);
        assert_eq!(item.visibility, ContextVisibility::Private);
        assert_eq!(item.evidence, vec!["tool_call:read-runtime"]);
        assert!(item.content.contains("crates/runtime/src/lib.rs"));
        assert!(item.content.contains("pub mod conversation;"));
        assert!(item.content.contains("actual committed, role-local"));
        assert!(item.content.contains("[findings]"));
        assert!(item
            .content
            .contains("Native structured output, JSON, Markdown headings"));
    }

    #[test]
    fn terminal_evidence_digest_preserves_all_receipts_and_complete_content() {
        let messages = (0..40)
            .map(|index| {
                ConversationMessage::tool_result(
                    format!("read-{index}"),
                    "read_file",
                    format!(
                        "receipt-{index} {} COMPLETE_RECEIPT_{index}",
                        "source-content ".repeat(400)
                    ),
                    false,
                )
            })
            .collect::<Vec<_>>();

        let digest = terminal_evidence_digest(&messages);

        assert!(digest.contains("receipt-0"));
        assert!(digest.contains("COMPLETE_RECEIPT_0"));
        assert!(digest.contains("receipt-39"));
        assert!(digest.contains("COMPLETE_RECEIPT_39"));
        assert_eq!(digest.matches("### Receipt ").count(), 40);
    }

    #[test]
    fn terminal_recovery_budget_tracks_complexity_and_explicit_limit() {
        use harness_contract::core::TaskComplexity;

        let simple =
            crate::execution_core::SafetyFusePolicy::derive(128_000, TaskComplexity::Simple, None);
        let strategic = crate::execution_core::SafetyFusePolicy::derive(
            128_000,
            TaskComplexity::Strategic,
            None,
        );
        let constrained = crate::execution_core::ExecutionBudgetLease {
            explicit_user_limit: Some(2),
            ..strategic.clone()
        };

        assert_eq!(terminal_recovery_retry_budget(&simple), 1);
        assert_eq!(terminal_recovery_retry_budget(&strategic), 3);
        assert_eq!(terminal_recovery_retry_budget(&constrained), 1);
    }

    #[test]
    fn terminal_usage_preserves_descendant_agent_usage_without_double_counting() {
        let live = harness_contract::projection::ExecutionLiveState {
            revision: 4,
            status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
            status_detail: None,
            turn_id: Some("turn-1".to_string()),
            started_at_ms: 1,
            updated_at_ms: 2,
            last_progress_at_ms: 2,
            context_usage: None,
            metrics: harness_contract::projection::RunMetricsProjection {
                input_tokens: 12_000,
                output_tokens: 800,
                total_tokens: 12_800,
                ..Default::default()
            },
            latency: Default::default(),
            output_preview: None,
            output_preview_start_bytes: 0,
            output_bytes: 0,
            output_parts: Vec::new(),
            terminal_ref: None,
            error: None,
        };

        assert_eq!(terminal_aggregate_usage(0, 0, Some(&live)), (12_000, 800));
        assert_eq!(
            terminal_aggregate_usage(13_000, 900, Some(&live)),
            (13_000, 900)
        );
        assert_eq!(terminal_aggregate_usage(10, 5, None), (10, 5));
    }

    #[test]
    fn recognized_provider_protocol_failure_has_one_dedicated_retry_budget() {
        assert!(RuntimeError::with_provider_failure_metadata(
            "invalid sse frame: malformed compatibility tool-call frame",
            None,
            true,
            crate::execution_core::graph::ResourceResultClass::Failed,
        )
        .is_provider_tool_protocol_failure());
        assert!(
            !RuntimeError::new("connection reset while reading provider stream")
                .is_provider_tool_protocol_failure()
        );
        assert_eq!(
            provider_protocol_intervention_kind(1),
            RuntimeInterventionKind::Replan
        );
        assert_eq!(
            provider_protocol_intervention_kind(2),
            RuntimeInterventionKind::Block
        );
        assert_eq!(
            provider_failure_intervention_kind_after_receipt(false),
            RuntimeInterventionKind::Synthesize
        );
        assert_eq!(
            provider_failure_intervention_kind_after_receipt(true),
            RuntimeInterventionKind::Block
        );
    }

    #[test]
    fn terminal_gate_rejects_internal_protocol_and_respects_strict_json_contract() {
        let mut envelope = harness_contract::outcome::DeliveryEnvelope {
            envelope_id: "delivery-1".to_string(),
            revision: 1,
            objective_id: "goal-1".to_string(),
            pipeline_status: harness_contract::outcome::PipelineStatus::Completed,
            delivery_status: harness_contract::outcome::DeliveryStatus::Satisfied,
            branch_terminals: Vec::new(),
            verified_receipts: Vec::new(),
            verified_artifacts: Vec::new(),
            verified_effects: Vec::new(),
            coverage: Default::default(),
            unresolved: Vec::new(),
            conflicts: Vec::new(),
            cancellation: None,
            user_answer_contract: Default::default(),
            created_at_ms: 1,
        };
        assert!(qualified_root_answer("A clear user answer.", &envelope));
        assert!(!qualified_root_answer(
            r#"<tool_call>{"name":"read_file"}</tool_call>"#,
            &envelope,
        ));
        assert!(!qualified_root_answer(r#"{"ok":true}"#, &envelope));
        envelope.user_answer_contract.format =
            harness_contract::outcome::UserAnswerFormat::StrictJson;
        assert!(qualified_root_answer(r#"{"ok":true}"#, &envelope));
        assert!(!qualified_root_answer("not json", &envelope));
    }

    #[test]
    fn negated_json_requirement_keeps_user_visible_markdown_contract() {
        assert!(!objective_requires_strict_json(
            "最后给出自然语言答案，不要求JSON。"
        ));
        assert!(!objective_requires_strict_json(
            "Answer in prose, no JSON required."
        ));
        assert!(objective_requires_strict_json("Return strict JSON only."));
        assert!(objective_requires_strict_json("输出机器可读 JSON。"));
    }

    #[test]
    fn alternative_json_format_does_not_reject_a_markdown_terminal_candidate() {
        assert!(!objective_requires_strict_json(
            "Give every required Team output field using native structured output, JSON, Markdown headings, or `Field: value` labels."
        ));
        assert!(!objective_requires_strict_json(
            "最终结论可用 JSON、Markdown 标题或 Field: value 标签。"
        ));
    }
