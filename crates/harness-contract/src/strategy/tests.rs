use super::*;

#[test]
fn continuation_phrases_produce_typed_latest_eligible_reference() {
    let continued = understand(&StrategyInput::from_prompt("继续上一组团队处理剩余问题"));
    assert_eq!(
        continued.collaboration_reference,
        CollaborationReference::LatestEligible
    );
    let ordinary = understand(&StrategyInput::from_prompt("重构这个模块"));
    assert_eq!(
        ordinary.collaboration_reference,
        CollaborationReference::None
    );
}

#[test]
fn explicit_team_cardinality_is_distinct_from_agent_cardinality() {
    assert_eq!(explicit_team_count("启动两个研究团队并行核查"), 2);
    assert_eq!(explicit_team_count("start three research teams"), 3);
    assert_eq!(explicit_team_count("请使用恰好3个Team完成真实任务"), 3);
    assert_eq!(
        understand(&StrategyInput::from_prompt(
            "恰好 3 个 turn-scoped custom Team workstream，全部真实执行"
        ))
        .required_team_count,
        3,
        "Arabic Team counts must survive common turn-scoped/custom qualifiers"
    );
    assert_eq!(
        explicit_team_count("初始 Program 合同**恰好只有两个** required Team obligation"),
        2,
        "an explicit required-Team contract must preserve its cardinality"
    );
    assert_eq!(
        understand(&StrategyInput::from_prompt(
            "初始 Program 合同**恰好只有两个** required Team obligation"
        ))
        .required_team_count,
        2
    );
    assert_eq!(explicit_team_count("启动三个 Team 并行核查"), 3);
    assert_eq!(
            explicit_team_count("建立恰好三个回合级自定义 Team workstream，名称、角色和顺序不得改变"),
            3,
            "a user may qualify a requested Team with turn-scoped/custom wording without losing cardinality"
        );
    assert_eq!(
        explicit_team_count("必须实际启动三个协作 Team，Team A、B、C 分工汇合"),
        3
    );
    assert_eq!(
        understand(&StrategyInput::from_prompt(
            "必须实际启动三个协作 Team，Team A、B、C 分工汇合"
        ))
        .required_team_count,
        3
    );
    assert_eq!(
            understand(&StrategyInput::from_prompt(
                "这是复杂架构审查：必须实际启动三个协作 Team，不可用一个 Team 或模型文本替代。Team A 独立审查 runtime，Team B 独立审查 memory 与 gateway；两者可并行。Team C 必须在收到 A 和 B 的经过授权的证据/摘要后，汇合并审查跨组件边界，再综合最终结论。不得在 A/B 的事实交接完成前启动 Team C 的实质审查。"
            ))
            .required_team_count,
            3,
            "a later singular Team constraint must not erase the preceding explicit three-Team requirement"
        );
    assert!(explicit_team_fan_in_required(
        "必须实际启动三个协作 Team，Team A、B 完成后由 Team C 汇合"
    ));
    assert!(!explicit_team_fan_in_required("启动三个协作 Team 并行核查"));
    assert_eq!(
        explicit_team_count("一个团队负责研究，另一个团队负责复核"),
        2
    );
    assert_eq!(explicit_team_count("启动三个 Agent 组成一个团队"), 1);
    assert_eq!(explicit_team_count("启动三个 Agent 并行分析"), 0);
    assert_eq!(
        explicit_team_count("第一个团队研究，第二个团队复核，第三个团队汇总"),
        3
    );
    assert_eq!(explicit_team_count("第一组研究，第二组复核，第三组汇总"), 3);
    assert_eq!(explicit_team_count("共 4 组并行检查"), 4);
    assert_eq!(
        understand(&StrategyInput::from_prompt("必须实际启动协作团队")).required_team_count,
        1
    );
    assert_eq!(
        understand(&StrategyInput::from_prompt(
            "第一个团队研究，第二个团队复核，第三个团队汇总"
        ))
        .required_team_count,
        3
    );
    let catalog_only = understand(&StrategyInput::from_prompt(
        "第一步：只查看团队模板目录，不要创建团队，不要启动协作。",
    ));
    assert_eq!(
        catalog_only.required_team_count, 0,
        "viewing the team catalog while explicitly forbidding team creation must not force a Team"
    );
    assert!(catalog_only.forbids_team);
    let current = understand(&StrategyInput::from_prompt("启动两个团队核查"));
    let mut legacy = serde_json::to_value(&current).expect("serialize understanding");
    legacy
        .as_object_mut()
        .expect("understanding object")
        .remove("required_team_count");
    let restored = serde_json::from_value::<TaskUnderstanding>(legacy)
        .expect("legacy understanding remains readable");
    assert_eq!(restored.required_team_count, 0);
}

#[test]
fn native_managed_escalation_contract_is_not_a_model_optional_field() {
    let required = understand(&StrategyInput::from_prompt(
            "必须让 Team A 的 Agent 实际调用 request_collaboration_escalation；完成后创建后续 Team，并保留 Runtime-attested receipt。",
        ));
    assert!(required.requires_managed_collaboration_escalation);

    let catalog_only = understand(&StrategyInput::from_prompt(
        "只展示 request_collaboration_escalation 的 schema，不要启动 Team。",
    ));
    assert!(!catalog_only.requires_managed_collaboration_escalation);
}

#[test]
fn observed_network_tool_batch_recomputes_the_complete_strategy_contract() {
    let mut decision = decide_strategy(&StrategyInput::from_prompt("继续处理"));
    decision
        .retarget_for_tool_requirements(
            ExecutionPattern::Explore,
            true,
            false,
            true,
            "provider emitted parallel external research calls",
        )
        .expect("network tool requirements are supported by Explore");

    assert!(decision.understanding.requires_external_facts);
    assert!(decision.understanding.requires_tool_evidence);
    assert!(decision.understanding.requests_parallelism);
    assert!(decision.uses_modifier(ExecutionModifier::WithExternalResearch));
    assert!(decision.uses_modifier(ExecutionModifier::Parallel));
    assert!(!decision.uses_modifier(ExecutionModifier::WithGuardrails));
    assert!(decision.uses_gate(ExecutionPolicyGate::Budget));
}

#[test]
fn observed_mutation_tool_batch_adds_permission_and_guardrails() {
    let mut decision = decide_strategy(&StrategyInput::from_prompt("继续处理"));
    decision
        .retarget_for_tool_requirements(
            ExecutionPattern::Execute,
            false,
            true,
            false,
            "provider emitted a workspace mutation",
        )
        .expect("mutation requirements are supported by Execute");

    assert!(decision.understanding.requires_write);
    assert!(decision.uses_modifier(ExecutionModifier::WithGuardrails));
    assert!(decision.uses_gate(ExecutionPolicyGate::Permission));
}

#[test]
fn parallel_read_batch_preserves_critical_risk_gates_with_execute_pattern() {
    let mut decision = decide_strategy(
        &StrategyInput::from_prompt("审查关键架构风险").with_risk_override(TaskRisk::Critical),
    );
    decision
        .retarget_for_tool_requirements(
            ExecutionPattern::Explore,
            false,
            false,
            true,
            "provider emitted parallel read calls",
        )
        .expect("parallel reads must preserve the admitted critical-risk gates");

    assert_eq!(decision.pattern, ExecutionPattern::Execute);
    assert!(decision.uses_modifier(ExecutionModifier::Parallel));
    assert!(decision.uses_gate(ExecutionPolicyGate::Risk));
    assert!(decision.uses_gate(ExecutionPolicyGate::Approval));
    assert!(decision
        .reasons
        .iter()
        .any(|reason| reason.contains("required policy gates")));
}

fn with_proven_team_benefit(prompt: &str) -> StrategyInput {
    let mut input = StrategyInput::from_prompt(prompt);
    input.experience = Some(StrategyExperienceSummary {
        sample_count: 3,
        success_rate_bp: 10_000,
        verification_block_rate_bp: 0,
        context_pressure_rate_bp: 0,
        multi_agent_lift_rate_bp: 8_000,
        multi_agent_lift_sample_count: 3,
        average_duration_ms: 20_000,
        average_total_tokens: 1_200,
        average_coordination_cost_ms: 1_000,
        actual_cost_sample_count: 3,
    });
    input.candidate_costs.insert(
        ExecutionCandidateKind::Direct,
        StrategyCandidateCostSummary {
            sample_count: 3,
            average_critical_path_ms: 40_000,
            average_total_tokens: 1_000,
            average_coordination_cost_ms: 0,
            calibration_source: "test:observed-direct".to_string(),
        },
    );
    input.candidate_costs.insert(
        ExecutionCandidateKind::Team,
        StrategyCandidateCostSummary {
            sample_count: 3,
            average_critical_path_ms: 20_000,
            average_total_tokens: 1_200,
            average_coordination_cost_ms: 1_000,
            calibration_source: "test:observed-team".to_string(),
        },
    );
    input
}

fn proposal(pattern: ExecutionPattern, modifiers: Vec<ExecutionModifier>) -> StrategyProposal {
    StrategyProposal {
        pattern,
        modifiers,
        template: None,
        confidence: 90,
        rationale: "test proposal".to_string(),
    }
}

fn assert_contract_legal(decision: &StrategyDecision) {
    assert!(decision
        .modifiers
        .iter()
        .all(|modifier| decision.pattern.supports_modifier(*modifier)));
    assert!(decision
        .gates
        .iter()
        .all(|gate| decision.pattern.supports_gate(*gate)));
}

fn paired_calibration(
    id: impl std::fmt::Display,
    demonstrates_positive_lift: bool,
) -> PairedStrategyCalibrationEvidence {
    let mut evidence = PairedStrategyCalibrationEvidence {
        evaluation_ref: format!("harness_eval.auto_strategy_paired.v1:{id}"),
        corpus_sha256: "a".repeat(64),
        workspace_revision: "workspace-revision".to_string(),
        provider_account_ref: "provider-account".to_string(),
        baseline_pattern: ExecutionPattern::Direct,
        baseline_duration_ms: 100,
        baseline_quality_score_bp: 8_000,
        candidate_duration_ms: if demonstrates_positive_lift { 80 } else { 120 },
        candidate_quality_score_bp: 8_000,
        blind_judge_completed: true,
        baseline_total_tokens: 100,
        candidate_total_tokens: 150,
        candidate_duplicate_tool_ratio_bp: 0,
        admission_channel: None,
        report_sha256: "b".repeat(64),
        rubric_sha256: "c".repeat(64),
        binary_sha256: "d".repeat(64),
        frontend_workspace_revision: "frontend-revision".to_string(),
        model_revision: "test-model".to_string(),
        judge_model_revision: "test-judge".to_string(),
        invariant_fingerprint: "e".repeat(64),
    };
    evidence.admission_channel = evidence.registered_admission_channel();
    evidence
}

#[test]
fn routes_simple_question_to_direct() {
    let decision = decide_strategy(&StrategyInput::from_prompt("解释一下这个函数有什么用"));

    assert_eq!(decision.pattern, ExecutionPattern::Direct);
    assert!(decision.confidence >= 80);
    assert!(!decision.uses_modifier(ExecutionModifier::WithVerifier));
}

#[test]
fn explicit_tool_evidence_and_team_requests_do_not_fall_back_to_direct() {
    let tool = decide_strategy(&StrategyInput::from_prompt(
        "必须通过只读工具读取 Cargo.toml 并提供证据",
    ));
    assert_eq!(tool.pattern, ExecutionPattern::Explore);
    assert!(tool.understanding.requires_tool_evidence);
    assert!(!tool.understanding.requires_external_facts);

    let team = decide_strategy(&StrategyInput::from_prompt(
        "请实际启动协作团队，分别审查 runtime、memory、gateway 后综合结论",
    ));
    assert_eq!(team.pattern, ExecutionPattern::Collaborate);
    assert!(team.understanding.requests_multi_agent);
}

#[test]
fn explicit_tool_prohibition_does_not_create_external_evidence_work() {
    let prompt = "只回答 7 乘以 8 的结果。不要调用工具，不要组队。";
    let decision = decide_strategy(&StrategyInput::from_prompt(prompt));

    assert!(prompt_explicitly_forbids_tool_use(prompt));
    assert_eq!(decision.pattern, ExecutionPattern::Direct);
    assert!(!decision.understanding.requires_external_facts);
    assert!(!decision.understanding.requests_multi_agent);
}

#[test]
fn webui_architecture_review_is_not_misclassified_as_external_web_research() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "审查 runtime、gateway 和 webui 的架构边界并综合本地代码证据",
    ));

    assert!(!decision.understanding.requires_external_facts);
}

#[test]
fn routes_bounded_write_to_execute_with_bounded_modifier() {
    let decision = decide_strategy(
        &StrategyInput::from_prompt("修复这个单文件小问题")
            .with_explicit_write(true)
            .with_changed_files(1),
    );

    assert_eq!(decision.pattern, ExecutionPattern::Execute);
    assert!(decision.uses_modifier(ExecutionModifier::WithGuardrails));
    assert!(decision.uses_modifier(ExecutionModifier::BoundedChange));
}

#[test]
fn routes_architecture_work_to_execute() {
    let decision = decide_strategy(&with_proven_team_benefit(
        "全面重构 runtime gateway service crate 的架构，做完整阶段规划",
    ));

    assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
    assert_eq!(decision.understanding.complexity, TaskComplexity::Strategic);
    assert!(decision.uses_modifier(ExecutionModifier::WithVerifier));
    assert_eq!(decision.policy_version, "strategy-decision-v5");
    assert!(decision
        .required_capabilities
        .contains(&KernelCapability::ExecutionGraph));
    assert!(decision
        .required_capabilities
        .contains(&KernelCapability::VerificationLedger));
}

#[test]
fn routes_parallel_research_to_explore_with_parallel_modifier() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "并行调研最新 AI harness 实践并汇总",
    ));

    assert_eq!(decision.pattern, ExecutionPattern::Explore);
    assert!(decision.uses_modifier(ExecutionModifier::WithExternalResearch));
    assert!(decision.uses_modifier(ExecutionModifier::Parallel));
}

#[test]
fn routes_explicit_chinese_web_research_through_an_external_research_strategy() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "公开技术标准研究报告，请自行搜索，收集全部信息并生成完整报告",
    ));

    assert!(decision.understanding.requires_external_facts);
    assert!(decision.uses_modifier(ExecutionModifier::WithExternalResearch));
    assert_ne!(decision.pattern, ExecutionPattern::Direct);
}

#[test]
fn routes_fact_verification_with_network_tools_to_external_research() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "发起一个团队并行完成公开技术标准事实核验简报，必须实际调用网络工具并引用真实来源。",
    ));

    assert!(decision.understanding.requires_external_facts);
    assert!(decision.understanding.requests_multi_agent);
    assert!(decision.uses_modifier(ExecutionModifier::WithExternalResearch));
    assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
}

#[test]
fn preserves_explicit_agent_cardinality_from_the_user_objective() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "发起团队并行核验，三个研究员分别负责官方事实、产业信息、风险争议，最后综合。",
    ));

    assert_eq!(decision.understanding.independent_workstreams, 3);
    assert!(decision.understanding.requests_multi_agent);
}

#[test]
fn recognizes_persisted_html_artifacts_and_sequential_teams() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "用一个团队深度调研资料，然后另一个团队负责生成一套 HTML 研究报告网站",
    ));

    assert!(decision.understanding.requires_write);
    assert!(decision.understanding.requests_multi_agent);
    assert_eq!(decision.understanding.independent_workstreams, 2);
}

#[test]
fn narrative_generation_without_an_artifact_is_not_a_workspace_write() {
    let decision = decide_strategy(&StrategyInput::from_prompt("根据现有证据生成一段简短结论"));

    assert!(!decision.understanding.requires_write);
}

#[test]
fn explicit_read_only_language_suppresses_incidental_write_terms() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "并行阅读当前工作区 README.md 并审查架构，不要修改文件",
    ));

    assert!(!decision.understanding.requires_write);
    assert!(!decision.understanding.requires_external_facts);
    assert!(decision.understanding.requests_parallelism);
}

#[test]
fn read_only_research_environment_does_not_turn_result_outputs_into_workspace_writes() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "四个 Team 在只读环境中完成调研和模拟，输出最终建议；只能使用只读工具，不要调用 bash 或任何写工具。",
    ));

    assert!(!decision.understanding.requires_write);
    assert!(decision.understanding.requests_multi_agent);
}

#[test]
fn local_source_research_does_not_request_external_fact_transport() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "启动两个研究团队并行调研当前工作区源码并给出代码证据",
    ));

    assert!(!decision.understanding.requires_external_facts);
    assert!(decision.understanding.requests_multi_agent);
}

#[test]
fn explicit_external_research_overrides_local_source_context() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "调研当前工作区源码，并联网核对最新官方来源",
    ));

    assert!(decision.understanding.requires_external_facts);
}

#[test]
fn read_only_team_evidence_prompt_does_not_cross_join_output_and_file_terms() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
            "真实回归验证：必须启动一个含两个并行 Agent 的团队，仅检查 /home/yi/AI/Moon 目录，不修改文件。两个 Agent 分别负责文件清单和日志内容核查，每个 Agent 至少执行 3 次只读工具调用；最后汇总各自工具数、产出和综合结论。",
        ));

    assert!(!decision.understanding.requires_write);
    assert!(decision.understanding.requests_multi_agent);
    assert!(decision.understanding.requests_parallelism);
}

#[test]
fn read_only_team_prompt_with_any_file_quantifier_does_not_select_an_implementer() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
            "必须启动一个包含两个并行研究 Agent 和一个综合 Agent 的团队，仅检查 /home/yi/AI/Moon，不修改任何文件，最后由综合 Agent 汇总结论。",
        ));

    assert!(!decision.understanding.requires_write);
    assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
}

#[test]
fn explicit_read_only_team_keeps_modified_cardinality_without_write_escalation() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
            "请在只读模式下组建一个包含3个并行智能体的研究团队，对当前工作区 /home/yi/AI/Moon 做架构核查。每个智能体必须实际调用至少3次只读工作区工具获取证据，不得修改任何文件；最后由主智能体综合三方证据。",
        ));

    assert!(!decision.understanding.requires_write);
    assert_eq!(decision.understanding.independent_workstreams, 3);
    assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
}

#[test]
fn tool_call_and_recommendation_counts_do_not_invent_agent_cardinality() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "每个 Agent 执行3次只读工具调用，最后提出3条建议。",
    ));

    assert_eq!(decision.understanding.independent_workstreams, 1);
}

#[test]
fn persisted_artifact_still_requires_write_when_existing_files_are_read_only() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "不要修改现有文件，生成一个新的 HTML 报告文件",
    ));

    assert!(decision.understanding.requires_write);
}

#[test]
fn chinese_report_delivery_is_a_persisted_artifact() {
    let objective = "请启动2个研究团队开展调研，然后使用一个智能体统一整理，形成专业研究报告（HTML版），放到独立文件夹下";
    let decision = decide_strategy(&StrategyInput::from_prompt(objective));

    assert!(decision.understanding.requires_write);
    assert!(!explicit_team_owns_persisted_artifact(objective));
    assert!(explicit_team_owns_persisted_artifact(
        "用一个团队调研资料，然后另一个团队负责生成 HTML 研究报告网站"
    ));
    assert!(explicit_team_owns_persisted_artifact(
        "启动一个团队生成 HTML 报告文件"
    ));
    assert!(explicit_team_owns_persisted_artifact(
        "前两个团队并行研究，第三个生成并写入 HTML 报告文件"
    ));
    assert!(!explicit_team_owns_persisted_artifact(
        "请启动两个团队分别研讨，两个团队讨论后形成统一的 HTML 方案并落盘"
    ));

    let current_research_with_local_delivery = "请启动2个研究团队开展今年 AI 发展趋势调研，然后使用一个智能体形成 HTML 报告，并保存到本地目标目录";
    let decision = decide_strategy(&StrategyInput::from_prompt(
        current_research_with_local_delivery,
    ));
    assert!(decision.understanding.requires_external_facts);
    assert!(decision.understanding.requires_write);
    assert!(!explicit_team_owns_persisted_artifact(
        current_research_with_local_delivery
    ));
}

#[test]
fn routes_multi_agent_request_to_collaborate() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "使用多 Agent 协同完成复杂架构分析",
    ));

    assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
    assert!(decision.uses_modifier(ExecutionModifier::WithReviewer));
    assert_contract_legal(&decision);
}

#[test]
fn deterministic_candidate_corpus_has_six_cases_per_candidate() {
    let direct = [
        "explain this constant",
        "summarize one paragraph",
        "answer a stable question",
        "clarify this name",
        "describe one function",
        "give a concise definition",
    ];
    let parallel = [
        "parallel read evidence for this API",
        "并行调研当前工具证据",
        "simultaneously inspect independent read-only facts",
        "fanout read-only checks and summarize",
        "多路读取证据但不要组队",
        "parallel research latest references",
    ];
    let team = [
        "全面审查 runtime gateway frontend 三个独立责任域并综合",
        "analyze runtime gateway webui as independent ownership domains",
        "deep architecture review across runtime memory matrix",
        "全面核对 gateway tui webui 的独立职责和验收",
        "plan runtime gateway frontend backend responsibilities with independent judgment",
        "cross-check runtime gateway memory matrix as separate accountable domains",
    ];
    for prompt in direct {
        let decision = decide_strategy(&StrategyInput::from_prompt(prompt));
        assert_eq!(
            decision.selected_candidate,
            ExecutionCandidateKind::Direct,
            "{prompt}"
        );
    }
    for prompt in parallel {
        let decision = decide_strategy(&StrategyInput::from_prompt(prompt));
        assert_eq!(
            decision.selected_candidate,
            ExecutionCandidateKind::ParallelTools,
            "{prompt}"
        );
    }
    for prompt in team {
        let decision = decide_strategy(&with_proven_team_benefit(prompt));
        assert_eq!(
            decision.selected_candidate,
            ExecutionCandidateKind::Team,
            "{prompt}"
        );
        assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
        assert!(!decision.understanding.requests_multi_agent);
    }
}

#[test]
fn candidate_resource_constraints_and_explicit_override_are_deterministic() {
    let prompt = "全面审查 runtime gateway frontend 三个独立责任域并综合";
    let no_team = StrategyResourceSnapshot {
        team_available: false,
        team_slots: 0,
        ..StrategyResourceSnapshot::default()
    };
    let constrained =
        decide_strategy(&StrategyInput::from_prompt(prompt).with_resource_snapshot(no_team));
    assert_ne!(constrained.selected_candidate, ExecutionCandidateKind::Team);

    let provider_constrained = StrategyResourceSnapshot {
        provider_concurrency_penalty_bp: 9_000,
        ..StrategyResourceSnapshot::default()
    };
    let constrained = decide_strategy(
        &StrategyInput::from_prompt(prompt).with_resource_snapshot(provider_constrained),
    );
    assert_ne!(constrained.selected_candidate, ExecutionCandidateKind::Team);

    let explicit = decide_strategy(&StrategyInput::from_prompt(
        "必须启动 Team 分别负责 runtime gateway frontend 并综合",
    ));
    assert_eq!(explicit.selected_candidate, ExecutionCandidateKind::Team);
    assert_eq!(explicit.pattern, ExecutionPattern::Collaborate);
}

#[test]
fn policy_disabled_candidates_cannot_be_reenabled_by_positive_experience() {
    let input = with_proven_team_benefit("全面审查 runtime gateway frontend 三个独立责任域并综合");
    let decision = StrategyRouter::new(StrategyPolicy {
        enable_multi_agent: false,
        ..StrategyPolicy::default()
    })
    .decide(&input);
    assert_ne!(decision.selected_candidate, ExecutionCandidateKind::Team);
    assert!(decision
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
        .is_some_and(|estimate| !estimate.eligible));

    let parallel = StrategyRouter::new(StrategyPolicy {
        enable_parallel_evidence: false,
        ..StrategyPolicy::default()
    })
    .decide(&StrategyInput::from_prompt(
        "并行读取多个独立证据并进行汇总",
    ));
    assert_ne!(
        parallel.selected_candidate,
        ExecutionCandidateKind::ParallelTools
    );
    assert!(parallel
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == ExecutionCandidateKind::ParallelTools)
        .is_some_and(|estimate| !estimate.eligible));
}

#[test]
fn explicit_simple_team_uses_hard_resources_not_auto_benefit_heuristics() {
    let explicit = decide_strategy(&StrategyInput::from_prompt(
        "必须启动 Team，让两个 Agent 分别回答 hello 并汇总。",
    ));
    assert_eq!(explicit.selected_candidate, ExecutionCandidateKind::Team);

    let unavailable = decide_strategy(
        &StrategyInput::from_prompt("必须启动 Team，让两个 Agent 回答 hello。")
            .with_resource_snapshot(StrategyResourceSnapshot {
                team_available: false,
                team_slots: 0,
                ..StrategyResourceSnapshot::default()
            }),
    );
    assert_eq!(
        unavailable.selected_candidate,
        ExecutionCandidateKind::Direct
    );
}

#[test]
fn explicit_team_negative_candidate_benefit_emits_surface_cost_warning() {
    let mut input =
        StrategyInput::from_prompt("必须启动 Team 分别负责 runtime gateway frontend 并综合");
    input.candidate_costs.insert(
        ExecutionCandidateKind::Team,
        StrategyCandidateCostSummary {
            sample_count: 3,
            average_critical_path_ms: 200_000,
            average_total_tokens: 50_000,
            average_coordination_cost_ms: 20_000,
            calibration_source: "test:negative-team".to_string(),
        },
    );
    input.resource_snapshot.provider_concurrency_penalty_bp = 10_000;

    let decision = decide_strategy(&input);
    let team = decision
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
        .expect("Team estimate");

    assert_eq!(decision.selected_candidate, ExecutionCandidateKind::Team);
    assert!(team.effective_duration_ms() >= team.estimated_serial_ms);
    assert!(decision.reasons.iter().any(|reason| {
        reason.contains("no measured duration advantage or paired quality proof")
            && reason.contains("surface must show the cost warning")
    }));
}

#[test]
fn explicit_team_prohibition_blocks_auto_team_for_multiple_domains() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "不要组队，也不要启动多 Agent；只用单一 owner 审查 runtime、gateway、webui 三个责任域。",
    ));

    assert!(decision.understanding.forbids_team);
    assert!(!decision.understanding.requests_multi_agent);
    assert_ne!(decision.selected_candidate, ExecutionCandidateKind::Team);
    assert_ne!(decision.pattern, ExecutionPattern::Collaborate);
}

#[test]
fn single_owner_multi_subsystem_review_keeps_strategic_execution_budget() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "请单独完成复杂架构审查，不要启动团队：分别分析 runtime、memory、gateway 的职责边界。",
    ));

    assert!(decision.understanding.forbids_team);
    assert!(!decision.understanding.requests_multi_agent);
    assert_eq!(decision.understanding.complexity, TaskComplexity::Strategic);
    assert_ne!(decision.selected_candidate, ExecutionCandidateKind::Team);
}

#[test]
fn every_candidate_records_integer_costs_and_snapshot_provenance() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "全面审查 runtime gateway frontend 三个独立责任域，分别给出工具证据后综合",
    ));
    assert_eq!(decision.candidate_estimates.len(), 3);
    assert_eq!(
        decision
            .candidate_estimates
            .iter()
            .map(|estimate| estimate.candidate)
            .collect::<Vec<_>>(),
        vec![
            ExecutionCandidateKind::Direct,
            ExecutionCandidateKind::ParallelTools,
            ExecutionCandidateKind::Team,
        ]
    );
    assert_eq!(decision.resource_snapshot.version, "strategy-resource-v1");
    assert_eq!(
        decision.resource_snapshot.provenance,
        MeasureProvenance::Assumed
    );
    assert_eq!(decision.resource_snapshot.sample_count, 0);
}

#[test]
fn candidate_cost_history_is_not_reused_or_divided_across_topologies() {
    let mut input =
        StrategyInput::from_prompt("必须启动 Team 分别负责 runtime gateway frontend 并综合");
    input.candidate_costs.insert(
        ExecutionCandidateKind::Direct,
        StrategyCandidateCostSummary {
            sample_count: 3,
            average_critical_path_ms: 40_000,
            average_total_tokens: 1_000,
            average_coordination_cost_ms: 0,
            calibration_source: "test:direct".to_string(),
        },
    );
    input.candidate_costs.insert(
        ExecutionCandidateKind::Team,
        StrategyCandidateCostSummary {
            sample_count: 3,
            average_critical_path_ms: 30_000,
            average_total_tokens: 1_500,
            average_coordination_cost_ms: 2_000,
            calibration_source: "test:team".to_string(),
        },
    );

    let decision = decide_strategy(&input);
    let direct = decision
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == ExecutionCandidateKind::Direct)
        .expect("direct estimate");
    let parallel = decision
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == ExecutionCandidateKind::ParallelTools)
        .expect("parallel estimate");
    let team = decision
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
        .expect("team estimate");

    assert_eq!(direct.estimated_critical_path_ms, 40_000);
    assert_eq!(team.estimated_critical_path_ms, 30_000);
    assert_ne!(team.estimated_critical_path_ms, 10_000);
    assert_eq!(direct.duration_calibration_source, "test:direct");
    assert_eq!(team.duration_calibration_source, "test:team");
    assert_eq!(parallel.duration_provenance, MeasureProvenance::Assumed);
    assert_eq!(parallel.duration_calibration_source, "assumed-policy-v1");
}

#[test]
fn fast_failed_team_runs_never_become_cheap_candidate_cost_calibration() {
    let input =
        StrategyInput::from_prompt("必须启动 Team 分别审查 runtime gateway frontend 并综合");
    let understanding = understand(&input);
    let mut store = StrategyExperienceStore::new();
    for index in 0..3 {
        store.record(StrategyExperienceRecord {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            selected_pattern: ExecutionPattern::Collaborate,
            selected_candidate: Some(ExecutionCandidateKind::Team),
            succeeded: false,
            verification_blocked: index == 2,
            context_pressure: false,
            composite_execution: false,
            multi_agent_positive_lift: false,
            created_at_ms: index,
            actual_duration_ms: 10,
            actual_input_tokens: 1,
            actual_output_tokens: 1,
            actual_cached_tokens: 0,
            actual_coordination_cost_ms: 1,
            paired_calibration: None,
        });
    }

    assert!(store
        .cost_summary_for_candidate(&understanding, ExecutionCandidateKind::Team)
        .is_none());
    let enriched = store.enrich_input(input);
    assert!(!enriched
        .candidate_costs
        .contains_key(&ExecutionCandidateKind::Team));
}

#[test]
fn partial_team_then_successful_fallback_is_not_a_pure_candidate_cost_sample() {
    let input =
        StrategyInput::from_prompt("必须启动 Team 分别审查 runtime gateway frontend 并综合");
    let understanding = understand(&input);
    let mut store = StrategyExperienceStore::new();
    store.record(StrategyExperienceRecord {
        domain: understanding.domain,
        complexity: understanding.complexity,
        risk: understanding.risk,
        selected_pattern: ExecutionPattern::Direct,
        selected_candidate: Some(ExecutionCandidateKind::Direct),
        succeeded: true,
        verification_blocked: false,
        context_pressure: false,
        composite_execution: true,
        multi_agent_positive_lift: false,
        created_at_ms: 1,
        actual_duration_ms: 2,
        actual_input_tokens: 1,
        actual_output_tokens: 1,
        actual_cached_tokens: 0,
        actual_coordination_cost_ms: 1,
        paired_calibration: None,
    });

    assert!(store
        .cost_summary_for_candidate(&understanding, ExecutionCandidateKind::Direct)
        .is_none());
}

#[test]
fn topology_neutral_bounded_write_review_is_selected_by_cost_not_team_keywords() {
    let prompt = FROZEN_TEAM_CALIBRATION_TASKS
        .iter()
        .find(|(task_id, _)| *task_id == "AS-T04-bounded-implementation-review")
        .map(|(_, prompt)| *prompt)
        .expect("frozen write task");
    let decision = decide_strategy(&StrategyInput::from_prompt(prompt));

    assert!(!decision.understanding.requests_multi_agent);
    assert_ne!(decision.selected_candidate, ExecutionCandidateKind::Team);
}

#[test]
fn negative_team_constraint_is_not_routed_as_collaboration() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "请单人执行这次架构审查，不要启动团队或多 Agent。",
    ));

    assert!(!decision.understanding.requests_multi_agent);
    assert_ne!(decision.pattern, ExecutionPattern::Collaborate);
}

#[test]
fn rejects_model_proposal_with_unsupported_modifier() {
    let decision = decide_strategy(&StrategyInput::from_prompt("解释这个函数").with_proposal(
        proposal(
            ExecutionPattern::Execute,
            vec![ExecutionModifier::Background],
        ),
    ));

    assert_eq!(decision.pattern, ExecutionPattern::Direct);
    assert_eq!(decision.source, StrategyDecisionSource::Deterministic);
    assert!(!decision.uses_modifier(ExecutionModifier::Background));
    assert!(decision
        .reasons
        .iter()
        .any(|reason| reason.contains("rejected by contract policy")));
    assert_contract_legal(&decision);
}

#[test]
fn accepts_model_proposal_with_supported_modifier() {
    let decision = decide_strategy(&StrategyInput::from_prompt("解释这个函数").with_proposal(
        proposal(ExecutionPattern::Explore, vec![ExecutionModifier::Parallel]),
    ));

    assert_eq!(decision.pattern, ExecutionPattern::Explore);
    assert_eq!(decision.source, StrategyDecisionSource::ModelValidated);
    assert!(decision.uses_modifier(ExecutionModifier::Parallel));
    assert_contract_legal(&decision);
}

#[test]
fn six_patterns_generate_their_key_policy_gates() {
    use ExecutionPolicyGate::{Approval, Budget, Permission, Risk};

    let direct = decide_strategy(&StrategyInput::from_prompt("解释这个值"));
    let explore = decide_strategy(
        &StrategyInput::from_prompt("收集资料并更新记录")
            .with_explicit_write(true)
            .with_proposal(proposal(ExecutionPattern::Explore, Vec::new())),
    );
    let execute = decide_strategy(
        &StrategyInput::from_prompt("force push secret change").with_explicit_write(true),
    );
    let deliberate = decide_strategy(
        &StrategyInput::from_prompt("对两个方案做 tradeoff").with_changed_files(21),
    );
    let collaborate = decide_strategy(
        &StrategyInput::from_prompt("使用多 Agent 协同分析 runtime gateway memory 的 secret 变更")
            .with_explicit_write(true)
            .with_proposal(proposal(ExecutionPattern::Collaborate, Vec::new())),
    );
    let supervise = decide_strategy(
        &StrategyInput::from_prompt("后台持续监控 secret 变更")
            .with_explicit_write(true)
            .with_proposal(proposal(ExecutionPattern::Supervise, Vec::new())),
    );

    assert_eq!(direct.pattern, ExecutionPattern::Direct);
    assert_eq!(direct.gates, vec![Budget]);
    assert_eq!(explore.pattern, ExecutionPattern::Explore);
    assert_eq!(explore.gates, vec![Budget, Permission]);
    assert_eq!(execute.pattern, ExecutionPattern::Execute);
    assert_eq!(execute.gates, vec![Budget, Permission, Risk, Approval]);
    assert_eq!(deliberate.pattern, ExecutionPattern::Deliberate);
    assert_eq!(deliberate.gates, vec![Budget, Risk]);
    assert_eq!(collaborate.pattern, ExecutionPattern::Collaborate);
    assert_eq!(collaborate.gates, vec![Budget, Permission, Risk, Approval]);
    assert_eq!(supervise.pattern, ExecutionPattern::Supervise);
    assert_eq!(supervise.gates, vec![Budget, Permission, Risk, Approval]);

    for decision in [direct, explore, execute, deliberate, collaborate, supervise] {
        assert_contract_legal(&decision);
    }
}

#[test]
fn same_input_is_stable_across_all_six_patterns() {
    let cases = [
        ("解释一下这个函数有什么用", ExecutionPattern::Direct),
        (
            "调研最新 AI harness 实践并汇总证据",
            ExecutionPattern::Explore,
        ),
        ("实现并修复这个单文件小问题", ExecutionPattern::Execute),
        (
            "权衡两个架构方案并解决冲突方案",
            ExecutionPattern::Deliberate,
        ),
        (
            "使用多 Agent 协同完成复杂架构分析",
            ExecutionPattern::Collaborate,
        ),
        ("后台持续监控这项长期运行任务", ExecutionPattern::Supervise),
    ];

    for (prompt, expected_pattern) in cases {
        let input = StrategyInput::from_prompt(prompt);
        let first = decide_strategy(&input);
        let second = decide_strategy(&input);
        let wire = serde_json::to_value(&first).expect("strategy decision wire payload");

        assert_eq!(first.pattern, expected_pattern, "prompt: {prompt}");
        assert_eq!(first, second, "prompt: {prompt}");
        assert_eq!(wire["pattern"], expected_pattern.as_str());
        assert!(wire.get("mode").is_none());
    }
}

#[test]
fn critical_risk_requires_approval() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "force push 并 reset --hard 清理所有内容",
    ));

    assert_eq!(decision.pattern, ExecutionPattern::Execute);
    assert!(decision.uses_gate(ExecutionPolicyGate::Risk));
    assert!(decision.uses_gate(ExecutionPolicyGate::Approval));
}

#[test]
fn strategy_experience_can_downgrade_low_lift_multi_agent() {
    let decision = decide_strategy(
        &StrategyInput::from_prompt("使用多 Agent 协同完成复杂架构分析").with_experience(
            StrategyExperienceSummary {
                sample_count: 5,
                success_rate_bp: 5000,
                verification_block_rate_bp: 0,
                context_pressure_rate_bp: 0,
                multi_agent_lift_rate_bp: 2000,
                multi_agent_lift_sample_count: 5,
                average_duration_ms: 0,
                average_total_tokens: 0,
                average_coordination_cost_ms: 0,
                actual_cost_sample_count: 0,
            },
        ),
    );

    assert_eq!(decision.pattern, ExecutionPattern::Execute);
    assert!(decision
        .reasons
        .iter()
        .any(|reason| reason.contains("low multi-agent lift")));
}

#[test]
fn strategy_experience_store_summarizes_comparable_records() {
    let input = StrategyInput::from_prompt("使用多 Agent 协同完成复杂架构分析");
    let understanding = understand(&input);
    let mut store = StrategyExperienceStore::new();
    for index in 0..4 {
        store.record(StrategyExperienceRecord {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            selected_pattern: ExecutionPattern::Collaborate,
            selected_candidate: Some(ExecutionCandidateKind::Team),
            succeeded: index < 3,
            verification_blocked: index == 3,
            context_pressure: index >= 2,
            composite_execution: false,
            multi_agent_positive_lift: index == 0,
            created_at_ms: index,
            actual_duration_ms: 100 + index,
            actual_input_tokens: 10,
            actual_output_tokens: 5,
            actual_cached_tokens: 0,
            actual_coordination_cost_ms: 2,
            paired_calibration: Some(paired_calibration(index, index == 0)),
        });
    }

    let summary = store.summary_for(&understanding).expect("summary");

    assert_eq!(summary.sample_count, 4);
    assert_eq!(summary.success_rate_bp, 7500);
    assert_eq!(summary.verification_block_rate_bp, 2500);
    assert_eq!(summary.context_pressure_rate_bp, 5000);
    assert_eq!(summary.multi_agent_lift_rate_bp, 0);
    assert_eq!(summary.multi_agent_lift_sample_count, 0);
    assert_eq!(summary.average_total_tokens, 15);
    assert_eq!(summary.actual_cost_sample_count, 4);
}

#[test]
fn paired_calibration_import_is_provenance_gated_and_idempotent() {
    let mut records = Vec::new();
    let mut samples = Vec::new();
    let mut comparisons = Vec::new();
    for (task_id, prompt) in FROZEN_TEAM_CALIBRATION_TASKS {
        let understanding = understand(&StrategyInput::from_prompt(prompt.to_string()));
        comparisons.push(serde_json::json!({
            "task_id": task_id,
            "strongest_non_team_baseline": "direct",
            "valid_pair_count": 3,
        }));
        for repetition in 0..3 {
            records.push(StrategyExperienceRecord {
                    domain: understanding.domain,
                    complexity: understanding.complexity,
                    risk: understanding.risk,
                    selected_pattern: ExecutionPattern::Collaborate,
                    selected_candidate: Some(ExecutionCandidateKind::Team),
                    succeeded: true,
                    verification_blocked: false,
                    context_pressure: false,
                    composite_execution: false,
                    multi_agent_positive_lift: false,
                    created_at_ms: 0,
                    actual_duration_ms: 80,
                    actual_input_tokens: 10,
                    actual_output_tokens: 5,
                    actual_cached_tokens: 0,
                    actual_coordination_cost_ms: 2,
                    paired_calibration: Some(PairedStrategyCalibrationEvidence {
                        evaluation_ref: format!(
                            "harness_eval.auto_strategy_paired.v1:auto-strategy-v1:{task_id}:{repetition}"
                        ),
                        corpus_sha256:
                            "d8dc4ba671dacd7a12b41d0cbe17d1cb4f2d5f5055cb2b9e7cefab2bb8c22e3c"
                                .to_string(),
                        workspace_revision: "workspace-revision".to_string(),
                        provider_account_ref: "provider-account".to_string(),
                        baseline_pattern: ExecutionPattern::Direct,
                        baseline_duration_ms: 100,
                        baseline_quality_score_bp: 8_000,
                        candidate_duration_ms: 80,
                        candidate_quality_score_bp: 8_000,
                        blind_judge_completed: true,
                        baseline_total_tokens: 15,
                        candidate_total_tokens: 15,
                        candidate_duplicate_tool_ratio_bp: 0,
                        admission_channel: Some(StrategyCalibrationAdmissionChannel::Speed),
                        report_sha256: String::new(),
                        rubric_sha256: String::new(),
                        binary_sha256: String::new(),
                        frontend_workspace_revision: String::new(),
                        model_revision: String::new(),
                        judge_model_revision: String::new(),
                        invariant_fingerprint: String::new(),
                    }),
                });
            for (condition, critical_path_ms) in
                [("direct", 100), ("parallel_tools", 110), ("auto", 80)]
            {
                samples.push(serde_json::json!({
                        "task_id": task_id,
                        "repetition": repetition,
                        "warmup": false,
                        "condition": condition,
                        "status": "completed",
                        "execution_graph_id": format!("graph-{task_id}-{repetition}-{condition}"),
                        "ttft_observed": true,
                        "usage_observed": true,
                        "cost_observed": true,
                        "evaluation_control_observed": true,
                        "evaluation_token_limit": 12_000,
                        "evaluation_tokens_consumed": 15,
                        "evaluation_budget_observed": true,
                        "evaluation_budget_breached": false,
                        "models_used": ["test-model"],
                        "critical_path_ms": critical_path_ms,
                        "quality_bp": 8_000,
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cached_tokens": 0,
                        "merge_cost_ms": if condition == "auto" { 2 } else { 0 },
                        "max_tool_concurrency_observed": if condition == "direct" { 1 } else { 2 },
                        "parallel_tool_batches": if condition == "direct" { 0 } else { 1 },
                        "judge": {
                            "judge_isolation_verified": true,
                            "observed_models": ["test-judge"]
                        },
                        "workspace_reset_verified": true,
                        "workspace_mutation_verified": true,
                        "workspace_changed_paths": if task_id == "AS-T04-bounded-implementation-review" {
                            vec!["fixtures/auto-strategy-write/target.txt"]
                        } else {
                            Vec::<&str>::new()
                        },
                        "write_attempt_paths": if task_id == "AS-T04-bounded-implementation-review" {
                            vec!["fixtures/auto-strategy-write/target.txt"]
                        } else {
                            Vec::<&str>::new()
                        },
                        "workspace_mutation_error": serde_json::Value::Null,
                    }));
            }
        }
    }
    let condition_invariants = serde_json::json!({
        "permission_mode": "danger-full-access",
        "workspace_fixture": "workspace-auto-strategy-frozen",
        "mutation_fixture_reset": "per-sample-pristine-full-workspace-sha256",
        "tool_catalog": "same-binary-runtime-inspected",
        "provider_fallbacks": "disabled",
    });
    let invariant_fingerprint = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&condition_invariants).unwrap())
    );
    let report = serde_json::json!({
        "kind": "harness_eval.auto_strategy_paired.v1",
        "status": "passed",
        "gate": {
            "passed": true,
            "claim_allowed": true,
            "judge_isolation_gate": true,
            "workspace_reset_gate": true,
            "workspace_mutation_gate": true,
            "automatic_team_materialization_gate": true,
            "baseline_topology_isolation_gate": true,
            "hard_budget_lease_gate": true,
            "tool_topology_observation_gate": true
        },
        "provenance": {
            "corpus_id": "auto-strategy-v1",
            "corpus_sha256": "d8dc4ba671dacd7a12b41d0cbe17d1cb4f2d5f5055cb2b9e7cefab2bb8c22e3c",
            "rubric_sha256": "3c2672ad0038c5b63abc6d6f724380d3a339e5921559dcb0b5c39e1a63039eba",
            "workspace_revision": "workspace-revision",
            "frontend_workspace_revision": "frontend-revision",
            "backend_source_archive_sha256": "c".repeat(64),
            "frontend_source_archive_sha256": "d".repeat(64),
            "provider_account_ref": "provider-account",
            "binary_sha256": "b".repeat(64),
            "provider": "test-model",
            "judge_model": "test-judge",
            "condition_invariant_fingerprint": invariant_fingerprint,
            "condition_invariants": condition_invariants,
            "seed": 20_260_716,
            "temperature_milli": 0,
            "warmup_per_task": 1,
            "repetitions": 3,
        },
        "samples": samples,
        "task_comparisons": comparisons,
        "strategy_calibration_records": records,
    });
    let mut store = StrategyExperienceStore::new();
    assert_eq!(store.import_paired_evaluation_report(&report), Ok(12));
    assert_eq!(store.import_paired_evaluation_report(&report), Ok(0));
    assert!(store.records[0].multi_agent_positive_lift);
    let first_understanding = understand(&StrategyInput::from_prompt(
        FROZEN_TEAM_CALIBRATION_TASKS[0].1,
    ));
    assert_eq!(
        store
            .summary_for(&first_understanding)
            .map(|summary| summary.multi_agent_lift_sample_count),
        Some(3)
    );

    let mut rejected = report;
    rejected["gate"]["claim_allowed"] = serde_json::Value::Bool(false);
    assert!(StrategyExperienceStore::new()
        .import_paired_evaluation_report(&rejected)
        .is_err());
}

#[test]
fn paired_lift_uses_the_registered_speed_and_quality_channels() {
    let mut speed = paired_calibration("speed", true);
    speed.candidate_quality_score_bp = 7_900;
    speed.admission_channel = speed.registered_admission_channel();
    assert_eq!(
        speed.admission_channel,
        Some(StrategyCalibrationAdmissionChannel::Speed)
    );
    assert!(speed.demonstrates_positive_lift());

    let mut quality = paired_calibration("quality", true);
    quality.candidate_duration_ms = 105;
    quality.candidate_quality_score_bp = 9_000;
    quality.candidate_total_tokens = 250;
    quality.admission_channel = quality.registered_admission_channel();
    assert_eq!(
        quality.admission_channel,
        Some(StrategyCalibrationAdmissionChannel::Quality)
    );
    assert!(quality.demonstrates_positive_lift());

    quality.candidate_duplicate_tool_ratio_bp = 1_500;
    quality.admission_channel = quality.registered_admission_channel();
    assert_eq!(quality.admission_channel, None);
    assert!(!quality.demonstrates_positive_lift());
}

#[test]
fn negative_benefit_is_exact_profile_scoped_expiring_and_veto_only() {
    let prompt = "全面审查 runtime gateway frontend 三个独立责任域并综合";
    let mut input = with_proven_team_benefit(prompt);
    let understanding = understand(&input);
    let workload = StrategyWorkloadFingerprint::from_input(&input, &understanding).digest();
    let profile = "a".repeat(64);
    input.resource_snapshot.provider_profile_fingerprint = profile.clone();
    let observation = NegativeBenefitObservation {
        workload_fingerprint_sha256: workload,
        provider_profile_fingerprint: profile.clone(),
        baseline_candidate: ExecutionCandidateKind::Direct,
        baseline_duration_ms: 40_000,
        baseline_quality_score_bp: 8_500,
        team_duration_ms: 56_000,
        team_quality_score_bp: 7_700,
        report_sha256: "b".repeat(64),
        provenance_ref: "harness_eval.auto_strategy_paired.v1:negative".to_string(),
        observed_at_ms: 1_000,
        expires_at_ms: 2_000,
    };
    let mut store = StrategyExperienceStore::new();
    store.record_negative_benefit(observation).unwrap();

    let candidate_costs = input.candidate_costs.clone();
    let positive_experience = input.experience.clone();
    let enriched = |now_ms| {
        let mut enriched = store.enrich_input_at(input.clone(), now_ms);
        enriched.candidate_costs.clone_from(&candidate_costs);
        enriched.experience.clone_from(&positive_experience);
        enriched
    };
    let vetoed = decide_strategy(&enriched(1_500));
    assert_ne!(vetoed.selected_candidate, ExecutionCandidateKind::Team);
    assert!(vetoed
        .reasons
        .iter()
        .any(|reason| reason.contains("vetoed")));

    let expired = decide_strategy(&enriched(2_000));
    assert_eq!(expired.selected_candidate, ExecutionCandidateKind::Team);

    let mut other_profile = enriched(1_500);
    other_profile.resource_snapshot.provider_profile_fingerprint = "c".repeat(64);
    assert_eq!(
        decide_strategy(&other_profile).selected_candidate,
        ExecutionCandidateKind::Team
    );

    let explicit =
        StrategyInput::from_prompt("必须启动 Team 分别审查 runtime gateway frontend 并综合");
    assert_eq!(
        decide_strategy(&store.enrich_input_at(explicit, 1_500)).selected_candidate,
        ExecutionCandidateKind::Team
    );
}

#[test]
fn independent_evidence_obligation_materializes_automatic_team_without_history() {
    let decision = decide_strategy(&StrategyInput::from_prompt(
        "全面审查 runtime gateway frontend 三个独立责任域，分别给出工具证据后综合",
    ));
    assert_eq!(decision.selected_candidate, ExecutionCandidateKind::Team);
    assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
    assert!(decision
        .reasons
        .iter()
        .any(|reason| { reason.contains("independently verifiable responsibility domains") }));
    let team = decision
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == ExecutionCandidateKind::Team)
        .unwrap();
    assert_eq!(team.duration_provenance, MeasureProvenance::Assumed);
    assert_eq!(team.quality_provenance, MeasureProvenance::Assumed);
}

#[test]
fn unknown_duration_is_excluded_and_equal_duration_uses_stable_direct_tie_break() {
    let input = StrategyInput::from_prompt("summarize the current state");
    let understanding = understand(&input);
    let resources = StrategyResourceSnapshot::default();
    let mut direct = candidate_estimate(
        ExecutionCandidateKind::Direct,
        true,
        1_000,
        1_000,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        Vec::new(),
    );
    direct.duration_provenance = MeasureProvenance::Observed;
    let mut parallel = candidate_estimate(
        ExecutionCandidateKind::ParallelTools,
        true,
        1_000,
        1_000,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        Vec::new(),
    );
    parallel.duration_provenance = MeasureProvenance::Observed;
    assert_eq!(
        select_execution_candidate(
            &understanding,
            &[parallel.clone(), direct.clone()],
            &resources
        ),
        ExecutionCandidateKind::Direct
    );

    parallel.estimated_critical_path_ms = 1;
    parallel.duration_provenance = MeasureProvenance::Unknown;
    assert_eq!(
        select_execution_candidate(&understanding, &[parallel, direct], &resources),
        ExecutionCandidateKind::Direct
    );
}

#[test]
fn failed_positive_claim_can_import_only_a_negative_veto() {
    let input =
        StrategyInput::from_prompt("全面审查 runtime gateway frontend 三个独立责任域并综合");
    let fingerprint = StrategyWorkloadFingerprint::from_input(&input, &understand(&input)).digest();
    let report = serde_json::json!({
        "kind": "harness_eval.auto_strategy_paired.v1",
        "status": "failed",
        "gate": {
            "passed": false,
            "claim_allowed": false,
            "provenance_complete": true,
            "budget_observation_complete": true,
            "judge_isolation_gate": true,
            "workspace_reset_gate": true,
            "baseline_topology_isolation_gate": true
        },
        "negative_benefit_observations": [{
            "workload_fingerprint_sha256": fingerprint,
            "provider_profile_fingerprint": "a".repeat(64),
            "baseline_candidate": "direct",
            "baseline_duration_ms": 40_000,
            "baseline_quality_score_bp": 8_500,
            "team_duration_ms": 56_000,
            "team_quality_score_bp": 7_700,
            "report_sha256": "",
            "provenance_ref": "harness_eval.auto_strategy_paired.v1:test",
            "observed_at_ms": 1_000,
            "expires_at_ms": 2_000
        }]
    });
    let mut store = StrategyExperienceStore::new();
    assert!(store.import_paired_evaluation_report(&report).is_err());
    assert_eq!(store.import_negative_benefit_report(&report), Ok(1));
    assert_eq!(store.records.len(), 0);
    assert_eq!(store.negative_benefit_observations.len(), 1);
    assert!(is_sha256(
        &store.negative_benefit_observations[0].report_sha256
    ));
}

#[test]
fn explicit_workspace_sources_become_root_evidence_constraints() {
    let understanding = understand(&StrategyInput::from_prompt(
            "只读检查 crates/runtime/src/orchestration/mod.rs 和 crates/runtime/src/team/template_candidate.rs，不修改文件",
        ));
    assert_eq!(
        understanding.required_workspace_evidence_scopes,
        vec![
            "read:crates/runtime/src/orchestration/mod.rs".to_string(),
            "read:crates/runtime/src/team/template_candidate.rs".to_string(),
        ]
    );
}

#[test]
fn explicit_collaboration_obligation_freezes_exact_cardinality() {
    let understanding = understand(&StrategyInput::from_prompt(
        "启动三个研究团队，分别检查三个证据域",
    ));
    let obligation = CollaborationExecutionObligation::for_selected_team(
        &understanding,
        1,
        ["zeta", "alpha", "alpha"].map(str::to_string),
    )
    .expect("explicit Team obligation");
    assert_eq!(
        obligation.source,
        CollaborationObligationSource::ExplicitRequest
    );
    assert_eq!(obligation.minimum_team_count, 3);
    assert_eq!(obligation.exact_team_count, Some(3));
    assert_eq!(obligation.required_focus_ids, vec!["alpha", "zeta"]);
}

#[test]
fn automatic_collaboration_obligation_is_a_nonzero_minimum() {
    let understanding = understand(&StrategyInput::from_prompt(
        "全面审查 runtime、gateway、frontend 三个独立责任域，分别取得工具证据后汇总",
    ));
    assert_eq!(understanding.required_team_count, 0);
    let obligation = CollaborationExecutionObligation::for_selected_team(
        &understanding,
        3,
        ["runtime", "gateway", "frontend"].map(str::to_string),
    )
    .expect("automatic Team obligation");
    assert_eq!(
        obligation.source,
        CollaborationObligationSource::AutomaticStrategy
    );
    assert_eq!(obligation.minimum_team_count, 3);
    assert_eq!(obligation.exact_team_count, None);
}

#[test]
fn singular_explicit_team_freezes_one_exact_team() {
    let understanding = understand(&StrategyInput::from_prompt(
        "请启动协作团队审查架构并给出工具证据",
    ));
    assert_eq!(understanding.required_team_count, 1);
    assert!(understanding.requests_multi_agent);
    let obligation =
        CollaborationExecutionObligation::for_selected_team(&understanding, 1, std::iter::empty())
            .expect("uncounted explicit Team obligation");
    assert_eq!(
        obligation.source,
        CollaborationObligationSource::ExplicitRequest
    );
    assert_eq!(obligation.minimum_team_count, 1);
    assert_eq!(obligation.exact_team_count, Some(1));
}

#[test]
fn automatic_team_width_uses_generic_responsibility_units_not_product_names() {
    let input = StrategyInput::from_prompt(
        "请对三个独立责任域分别取得工具证据并交叉核验，最后统一综合结论",
    );
    let decision = decide_strategy(&input);
    assert_eq!(decision.understanding.required_team_count, 0);
    assert_eq!(decision.understanding.independent_workstreams, 3);
    assert!(decision.understanding.requires_tool_evidence);
    assert_eq!(decision.selected_candidate, ExecutionCandidateKind::Team);
}

#[test]
fn topology_non_prescription_does_not_become_a_singular_team_obligation() {
    let prompt = "请对三个独立责任域分别取得只读工具证据并交叉核验，最后统一综合结论。责任域一核查策略选择与执行义务，责任域二核查状态持久化与恢复，责任域三核查最终验收与投影。必须列出至少三个本次实际读取的完整 crates/.../*.rs 源码路径；不要自行指定 Team、Agent、角色、模板或编排拓扑。";
    let decision = decide_strategy(&StrategyInput::from_prompt(prompt));

    assert_eq!(decision.understanding.required_team_count, 0);
    assert_eq!(decision.understanding.independent_workstreams, 3);
    assert!(!decision.understanding.requests_multi_agent);
    assert_eq!(decision.selected_candidate, ExecutionCandidateKind::Team);
    assert_eq!(explicit_team_count(prompt), 0);
    assert!(!explicit_team_execution_required(prompt));
}

#[test]
fn positive_team_cardinality_survives_a_separate_topology_non_prescription() {
    let prompt = "必须启动三个 Team 分别审查三个责任域；不要自行添加额外角色或模板。";
    let understanding = understand(&StrategyInput::from_prompt(prompt));

    assert_eq!(understanding.required_team_count, 3);
    assert!(understanding.requests_multi_agent);
}

#[test]
fn collaboration_obligation_rejects_zero_or_forbidden_team() {
    let automatic = understand(&StrategyInput::from_prompt(
        "全面审查 runtime、gateway、frontend 三个独立责任域，分别取得工具证据后汇总",
    ));
    assert!(CollaborationExecutionObligation::for_selected_team(
        &automatic,
        0,
        ["runtime".to_string()],
    )
    .is_err());

    let forbidden = understand(&StrategyInput::from_prompt(
        "不要组队，也不要启动多 Agent；只检查 runtime、gateway、frontend 三个责任域",
    ));
    assert!(
        CollaborationExecutionObligation::for_selected_team(&forbidden, 3, std::iter::empty(),)
            .is_err()
    );
}
