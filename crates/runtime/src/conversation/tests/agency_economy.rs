use std::fs;

use crate::SystemPromptBuilder;

fn r1_submit_call(task_ref: &str, call_id: &str) -> ModelToolCall {
    ModelToolCall {
        binding: None,
        id: call_id.into(),
        name: harness_contract::agent_action::TASK_SUBMIT_TOOL_ID.into(),
        input: serde_json::json!({
            "task_ref": task_ref,
            "deliverable": {"kind": "report", "title": "Ordinary response"}
        })
        .to_string(),
        depends_on: Vec::new(),
    }
}

#[tokio::test]
async fn r1_one_intent_submit_keeps_same_attempt_authority() {
    for saved_draft in [false, true] {
        let fixture = protocol_fixture();
        let parent_id = "r1-one-intent-parent";
        let ticket = register_protocol_graph(
            &fixture,
            "execute",
            &fixture.author_ref,
            parent_id,
            "r1-one-intent-conversation",
            1,
        );
        let claimed = fixture
            .services
            .agent_action_service()
            .apply(&fixture.agent_envelope(
                "r1-claim",
                &fixture.author_ref,
                parent_id,
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: fixture.task_ref.clone(),
                    reason: None,
                }),
            ))
            .expect("claim the real Task");
        assert_eq!(claimed.status, AgentActionStatus::Applied);

        let parent = fixture
            .services
            .graph_state_store()
            .load(parent_id)
            .expect("AgentTask graph");
        let packet: AgentTaskPacket =
            serde_json::from_str(&parent.nodes[0].payload_ref).expect("AgentTask packet");
        let foreign_task_ref = applied_ref(
            fixture.services.as_ref(),
            &fixture.envelope(
                "r1-foreign-task",
                AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: fixture.team_ref.clone(),
                    title: "Foreign Task".into(),
                    objective: "Produce a separate deliverable".into(),
                    acceptance: "Only this Task may submit its body".into(),
                    criterion_source: Default::default(),
                    acceptance_checks: Vec::new(),
                    required_capabilities: Vec::new(),
                    depends_on: Vec::new(),
                    obligation_refs: Vec::new(),
                    purpose: harness_contract::agent_action::TaskPurpose::Delivery,
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                }),
            ),
        );
        let foreign_scope = AgenticContentDraftScope {
            execution_id: packet.graph_id().into(),
            node_id: packet.node_id().into(),
            attempt: packet.attempt,
            actor_id: format!("{}:{}", packet.agent_id(), packet.run_id()),
            task_id: Some(foreign_task_ref),
        };
        let visibility_scope = format!("session:{}", fixture.root.session_id);
        persist_agentic_content_draft_for_scope(
            fixture.services.as_ref(),
            &foreign_scope,
            visibility_scope.clone(),
            "This body belongs to a different Task.",
        )
        .await
        .expect("persist foreign Task body");
        let mut missing = vec![r1_submit_call(&fixture.task_ref, "r1-foreign-draft")];
        resolve_explicit_agentic_content_refs(
            fixture.services.as_ref(),
            &ticket,
            &ConversationMessage::assistant(Vec::new()),
            &mut missing,
        )
        .await
        .expect("resolve without a matching draft");
        let unresolved: TaskSubmitInput =
            serde_json::from_str(&missing[0].input).expect("unresolved submission");
        assert!(unresolved
            .deliverable
            .as_ref()
            .expect("deliverable intent")
            .resolved_content_ref
            .is_none());
        let refused = fixture
            .services
            .submit_agent_action(&fixture.agent_envelope(
                "r1-foreign-draft",
                &fixture.author_ref,
                parent_id,
                AgentAction::TaskSubmit(unresolved),
            ))
            .await
            .expect("durable rejection");
        assert_eq!(refused.status, AgentActionStatus::Rejected);
        assert_eq!(
            refused.error.unwrap().code,
            "task_submission_has_no_artifact"
        );
        let before = fixture
            .services
            .agent_action_service()
            .project(&fixture.root.program_id)
            .expect("claimed state");
        assert_eq!(
            before.tasks[&fixture.task_ref].status,
            crate::AgenticTaskStatus::Claimed
        );
        assert!(before.artifacts.is_empty());

        let report = "The claimant's ordinary response is the requested deliverable.";
        let input = if saved_draft {
            recover_one_shot_report_draft(
                fixture.services.as_ref(),
                &ticket,
                &fixture.task_ref,
                &visibility_scope,
                report,
            )
            .await
        } else {
            let mut calls = vec![r1_submit_call(&fixture.task_ref, "r1-one-intent-submit")];
            resolve_explicit_agentic_content_refs(
                fixture.services.as_ref(),
                &ticket,
                &ConversationMessage::assistant(vec![
                    ContentBlock::Text {
                        text: "Earlier longer response material. ".repeat(20),
                    },
                    ContentBlock::Text {
                        text: report.into(),
                    },
                ]),
                &mut calls,
            )
            .await
            .expect("resolve current normal response");
            assert_eq!(calls.len(), 1);
            serde_json::from_str(&calls[0].input).expect("Runtime-bound submission")
        };
        assert!(input.artifact_refs.is_empty());
        assert!(input.evidence_refs.is_empty());
        let submit = fixture.agent_envelope(
            "r1-one-intent-submit",
            &fixture.author_ref,
            parent_id,
            AgentAction::TaskSubmit(input.clone()),
        );
        let wrong_claim = fixture.agent_envelope(
            "r1-wrong-claim",
            &fixture.author_ref,
            "r1-unrelated-execution",
            AgentAction::TaskSubmit(input),
        );
        let fenced = fixture
            .services
            .agent_action_service()
            .apply(&wrong_claim)
            .unwrap();
        assert_eq!(fenced.status, AgentActionStatus::Rejected);
        assert_eq!(fenced.error.unwrap().code, "task_claim_fence_mismatch");
        let submitted = fixture.services.submit_agent_action(&submit).await.unwrap();
        assert_eq!(
            submitted.status,
            AgentActionStatus::Applied,
            "{submitted:?}"
        );
        let program = fixture
            .services
            .agent_action_service()
            .project(&fixture.root.program_id)
            .unwrap();
        let task = &program.tasks[&fixture.task_ref];
        assert_eq!(task.status, crate::AgenticTaskStatus::Submitted);
        assert_eq!(
            task.purpose,
            harness_contract::agent_action::TaskPurpose::Delivery
        );
        assert_eq!(task.claimant.as_deref(), Some(fixture.author_ref.as_str()));
        assert_eq!(task.artifact_refs.len(), 1);
        assert_eq!(program.artifacts.len(), 1);
        assert!(program.objective_verdict.is_none());
        let artifact = &program.artifacts[&task.artifact_refs[0]];
        assert_eq!(artifact.committed_by, fixture.author_ref);
        assert_eq!(artifact.relates_to, vec![fixture.task_ref.clone()]);
        assert_eq!(artifact.claim_execution_id, task.claim_execution_id);
        assert_eq!(artifact.claim_generation, Some(task.claim_generation));
        let content = fixture
            .services
            .artifact_store()
            .resolve(&artifact.content_ref)
            .unwrap();
        assert_eq!(
            fixture
                .services
                .artifact_store()
                .read(&content, &visibility_scope, None)
                .await
                .unwrap(),
            report.as_bytes()
        );
        let replay = fixture.services.submit_agent_action(&submit).await.unwrap();
        assert!(replay.duplicate);
        assert_eq!(replay.receipt_id, submitted.receipt_id);
        assert_eq!(replay.changed_refs, submitted.changed_refs);
        assert_eq!(replay.revision, submitted.revision);
        let self_review = fixture.agent_envelope(
            "r1-self-review",
            &fixture.author_ref,
            parent_id,
            AgentAction::TaskReview(TaskReviewInput {
                task_ref: fixture.task_ref.clone(),
                decision: TaskReviewDecision::Accept,
                evidence_refs: vec![artifact.content_ref.clone()],
                reason: "The producer must not accept its own result".into(),
                operator_requirement: None,
            }),
        );
        let review = fixture
            .services
            .agent_action_service()
            .apply(&self_review)
            .unwrap();
        assert_eq!(review.status, AgentActionStatus::Rejected);
        assert_eq!(review.error.unwrap().code, "self_review_forbidden");
        let after = fixture
            .services
            .agent_action_service()
            .project(&fixture.root.program_id)
            .unwrap();
        assert_eq!(after.artifacts.len(), 1);
        assert_eq!(
            after.tasks[&fixture.task_ref].status,
            crate::AgenticTaskStatus::Submitted
        );
        assert!(after.objective_verdict.is_none());
        let checkpoint: serde_json::Value = serde_json::from_str(
            &super::compact_agentic_program_checkpoint(Some(&fixture.services), &after),
        )
        .unwrap();
        assert_eq!(
            checkpoint["awaiting_independent_review"][0]["task_ref"],
            fixture.task_ref
        );
        assert_eq!(
            checkpoint["awaiting_independent_review"][0]["artifact_refs"][0],
            task.artifact_refs[0]
        );
    }
}

fn r1_assert_no_absolute_economy(output: &str) {
    for fragments in [
        ["Never re-read a file", " an accepted Task already read"],
        ["Batch every independent", " read/search"],
        ["Decide the whole answer", " before the first call"],
        ["every call you can issue", " in this response, issue now"],
        [
            "every re-read of already-read content",
            " is avoidable spend",
        ],
        ["Batch independent work", " into one round"],
        ["Batch independent work", " into ONE response"],
        ["instead of re-reading", " source files"],
        ["scope each Task", " to a batch of related reads"],
        ["issue independent calls", " in the same response"],
        ["Batch independent tool calls", " into one turn"],
        ["batch every call", " you can issue now"],
        [
            "Do not repeatedly read overlapping ranges",
            " of the same small file.",
        ],
        [
            "Do not use execute_code to slice a text file",
            " when read_file or grep_search can gather the evidence.",
        ],
    ] {
        let forbidden = fragments.concat();
        assert!(
            !output.contains(&forbidden),
            "absolute economy directive: {forbidden}"
        );
    }
}

#[derive(Clone)]
struct R1AgencyRequestClient {
    requests: Arc<Mutex<Vec<ApiRequest>>>,
    source_path: Option<std::path::PathBuf>,
}

impl ApiClient for R1AgencyRequestClient {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let step = {
            let mut requests = self.requests.lock().expect("captured requests");
            requests.push(request);
            requests.len()
        };
        if let Some(source_path) = &self.source_path {
            if step <= 2 {
                if step == 2 {
                    fs::write(source_path, "source revision two").expect("update source version");
                }
                return Box::pin(stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: format!("r1-source-read-{step}"),
                        name: "read_file".into(),
                        input: serde_json::json!({"path": source_path}).to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]));
            }
        }
        Box::pin(stream::iter(vec![Err(RuntimeError::new(
            "HTTP 402 Payment Required: deterministic R1 request fixture stopped",
        ))]))
    }
}

#[tokio::test]
async fn r1_agency_economy_absolute_protocol_is_absent() {
    let system = SystemPromptBuilder::new().build();
    let system_text = system.join("\n");
    assert!(system_text.contains("You own semantic decisions"));
    assert!(system_text.contains("authorization, revisions, idempotency, leases"));
    assert!(system_text.contains("a different Agent accepts it"));
    assert!(system_text.contains("Prefer parallelizing independent tool calls."));
    assert!(system_text.contains("read only the range you need"));
    let callable = ["read_file", "tool_orchestrate", "execute_code"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let guidance = crate::capability_manifest::render_tool_guidance(&callable);
    assert!(guidance.contains("`read_file`"));
    assert!(guidance.contains("`tool_orchestrate`"));
    assert!(guidance.contains("`batch`"));
    assert!(guidance.contains("`compute`"));
    assert!(guidance.contains("`execute_code`"));

    let fixture = protocol_fixture();
    let program = fixture
        .services
        .agent_action_service()
        .project(&fixture.root.program_id)
        .unwrap();
    let checkpoint_text =
        super::compact_agentic_program_checkpoint(Some(&fixture.services), &program);
    let checkpoint: serde_json::Value = serde_json::from_str(&checkpoint_text).unwrap();
    assert!(checkpoint.get("efficiency").is_none());
    assert_eq!(checkpoint["required_team_count"], 1);
    assert_eq!(checkpoint["tasks"][0]["task_ref"], fixture.task_ref);
    assert!(checkpoint.get("closure").is_some());
    assert!(checkpoint.get("artifacts").is_some());
    assert!(checkpoint["instruction"]
        .as_str()
        .unwrap()
        .contains("only the Goal verifier"));

    let requests = Arc::new(Mutex::new(Vec::new()));
    let services = crate::RuntimeServices::in_memory().unwrap();
    let mut runtime = crate::ConversationRuntime::new(
        Session::new(),
        R1AgencyRequestClient {
            requests: Arc::clone(&requests),
            source_path: None,
        },
        RootPresentationRecoveryReader,
        PermissionPolicy::new(PermissionMode::ReadOnly),
        system,
    )
    .without_memory();
    runtime.set_active_model("test-model");
    let (_, result) = submit_test_owned_conversation_turn(
        runtime,
        services,
        "Create exactly 2 Teams. Use read_file to inspect crates/runtime/src/lib.rs and retain source evidence",
        &SharedPrompter::none(),
        test_execution_lineage(),
    )
    .await;
    let summary = result.expect("host materializes the honest provider-failure terminal");
    assert_eq!(
        summary.terminal_completion,
        harness_contract::goal::GoalCompletion::Partial
    );
    assert!(
        summary
            .final_answer
            .contains("deterministic R1 request fixture stopped"),
        "the blocked terminal must retain the actual fixture failure: {summary:?}"
    );
    let captured = requests.lock().unwrap();
    let request = captured.first().expect("real host-assembled request");
    let trusted = request.prompt.trusted_system.join("\n");
    assert!(trusted.contains("You own semantic decisions"));
    let control = request
        .prompt
        .contextual_packets
        .iter()
        .find(|packet| {
            packet
                .content
                .contains("This Objective requires at least 2 real Team execution(s)")
        })
        .expect("required-control production context");
    assert!(control.content.contains("read:crates/runtime/src/lib.rs"));
    assert!(control
        .content
        .contains("independent review and all required Teams are complete"));
    assert!(control
        .content
        .contains(crate::agentic::order_contract::TASK_AUTHORING_DISCIPLINE));
    assert!(control
        .content
        .contains("Runtime binds identities, leases and execution"));
    r1_assert_no_absolute_economy(&system_text);
    r1_assert_no_absolute_economy(&checkpoint_text);
    r1_assert_no_absolute_economy(&trusted);
    for packet in &request.prompt.contextual_packets {
        r1_assert_no_absolute_economy(&packet.content);
    }
    let runtime_text = request
        .prompt
        .runtime_context
        .iter()
        .map(String::as_str)
        .chain(
            request
                .prompt
                .contextual_packets
                .iter()
                .map(|packet| packet.content.as_str()),
        )
        .collect::<Vec<_>>()
        .join("\n");
    r1_assert_no_absolute_economy(&runtime_text);
    assert!(runtime_text.contains("## Runtime evidence plan"));
    assert!(runtime_text.contains(
        "Do not keep collecting evidence after the available evidence is enough for a staged answer."
    ));
    use crate::evidence_planner::EvidenceAcquisitionMode;
    for (objective, mode) in [
        ("Review README.md", EvidenceAcquisitionMode::SmallEvidence),
        (
            "Summarize the supplied observation",
            EvidenceAcquisitionMode::MediumEvidence,
        ),
        (
            "Inspect the entire workspace",
            EvidenceAcquisitionMode::LargeEvidence,
        ),
        (
            "Review the architecture across runtime and gateway",
            EvidenceAcquisitionMode::ComplexEvidence,
        ),
    ] {
        let plan = crate::evidence_planner::plan_evidence(objective);
        assert_eq!(plan.mode, mode);
        let rendered = crate::evidence_planner::evidence_plan_prompt(&plan);
        r1_assert_no_absolute_economy(&rendered);
        assert!(rendered.contains(&format!("mode={:?}; intent={:?}", plan.mode, plan.intent)));
        assert!(rendered.contains(&format!("reason={}", plan.reason)));
        assert!(rendered.contains("Recommended evidence path:\n"));
        assert!(rendered.contains("\nAvoid:\n"));
        assert!(rendered.contains("Delegation/team guidance:\n"));
        assert!(rendered.contains(
            "Do not keep collecting evidence after the available evidence is enough for a staged answer."
        ));
        for call in plan.recommended_calls.iter().take(6) {
            assert!(rendered.contains(&format!("`{}` with ", call.name)));
        }
        let preferred = "For README/docs review, prefer full read or batch read plus diff-oriented checks before any range-by-range reading.";
        if mode == EvidenceAcquisitionMode::SmallEvidence {
            assert!(rendered.contains(preferred));
            assert!(rendered.contains("`workspace_snapshot` with "));
            assert!(rendered.contains("`tool_orchestrate` with "));
        } else {
            assert!(!rendered.contains(preferred));
        }
    }
    for objective in [
        "Read the supplied source file",
        "Explain the supplied observation",
    ] {
        let decision = crate::execution_core::build_runtime_execution_decision(objective, None);
        for compact in [false, true] {
            let guidance = crate::execution_core::model_affordance::runtime_execution_guidance_prompt_with_tool_exposure_mode(
                &decision, None, compact,
            );
            r1_assert_no_absolute_economy(&guidance);
        }
    }
}

#[derive(Clone)]
struct R1VersionedSourceReader {
    reads: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl ToolExecutor for R1VersionedSourceReader {
    async fn execute_output(
        &self,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if name != "read_file" {
            return Err(ToolError::new(
                "only source reads are authorized in this fixture",
            ));
        }
        let input: serde_json::Value = serde_json::from_str(input).unwrap();
        let body = fs::read_to_string(input["path"].as_str().unwrap()).unwrap();
        self.reads.lock().unwrap().push(body.clone());
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            body,
        ))
    }

    fn available_tool_names(&self) -> Vec<String> {
        vec!["read_file".into()]
    }

    fn registered_tool_effect(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        RootPresentationRecoveryReader.registered_tool_effect(name, input)
    }
}

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for R1VersionedSourceReader {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        let body = self
            .execute_output(&request.tool_name, &request.input)
            .await
            .unwrap();
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(body.model_text().to_string()),
            error: None,
            evidence_ref: format!("tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
        }
    }
}

#[tokio::test]
async fn r1_agency_economy_changed_source_can_be_read_again() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let reader = R1VersionedSourceReader {
        reads: Arc::clone(&reads),
    };
    let services =
        crate::RuntimeServices::in_memory_with_tool_host(Arc::new(reader.clone())).unwrap();
    let source_path = services.workspace_root().join("r1-source.txt");
    fs::write(&source_path, "source revision one").unwrap();
    let session = Session::new();
    let session_store = Arc::new(crate::test_support::session_store());
    session_store
        .create_session(&session::SessionRecord {
            session_id: session.session_id.clone(),
            platform: "test".into(),
            chat_id: "r1-versioned-source".into(),
            user_id: None,
            model: None,
            created_at: "2026-10-16T00:00:00Z".into(),
            last_activity: "2026-10-16T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "manual".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .await
        .expect("create the real raw-evidence Session");
    let mut runtime = crate::ConversationRuntime::new(
        session,
        R1AgencyRequestClient {
            requests: Arc::clone(&requests),
            source_path: Some(source_path),
        },
        reader,
        PermissionPolicy::new(PermissionMode::ReadOnly),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
        session_store,
    ))
    .with_artifact_store(Arc::clone(services.artifact_store()));
    runtime.set_active_model("test-model");
    let (_, result) = submit_test_owned_conversation_turn(
        runtime,
        services,
        "Read the supplied source; if it changes, read its new version before drawing a conclusion.",
        &SharedPrompter::none(),
        test_execution_lineage(),
    )
    .await;
    let summary = result.expect("host materializes the honest provider-failure terminal");
    assert_eq!(
        summary.terminal_completion,
        harness_contract::goal::GoalCompletion::Partial
    );
    assert!(
        summary
            .final_answer
            .contains("deterministic R1 request fixture stopped"),
        "the blocked terminal must retain the actual fixture failure: {summary:?}"
    );
    assert_eq!(
        *reads.lock().unwrap(),
        vec!["source revision one", "source revision two"]
    );
    let captured = requests.lock().unwrap();
    assert!(
        captured.len() >= 3,
        "both tool outcomes must reach the model"
    );
    for (call_id, expected) in [
        ("r1-source-read-1", "source revision one"),
        ("r1-source-read-2", "source revision two"),
    ] {
        assert!(captured.iter().flat_map(|request| request.messages.iter()).flat_map(|message| &message.blocks).any(|block| {
            matches!(block, ContentBlock::ToolResult { tool_use_id, output, is_error: false, .. }
                if tool_use_id == call_id && output.contains(expected))
        }), "current-version read must have a real successful result: {call_id}");
    }
}
